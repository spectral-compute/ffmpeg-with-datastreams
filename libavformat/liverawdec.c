#include "config_components.h"

#include "avformat.h"
#include "demux.h"
#include "internal.h"
#include "libavutil/mem.h"
#include "libavutil/opt.h"
#include "libavutil/thread.h"
#include "libavutil/time.h"

#include <stdatomic.h>

/**
 * A packet in the queue.
 *
 * These are written by worker() and read by get_packet().
 */
typedef struct FFLiveRawDemuxerPacket
{
    struct FFLiveRawDemuxerPacket *next;
    int size;
    int64_t pos;
    uint8_t buffer[];
} FFLiveRawDemuxerPacket;

/**
 * Private data for this format.
 */
typedef struct FFLiveRawDemuxerContext
{
    const AVClass *class;
    int raw_packet_size; ///< Maximum size of a packet in bytes.
    int raw_packet_poll; ///< Time, in µs, to wait for data before giving up.

    atomic_int error; ///< Set if the worker thread expreriences an error.
    atomic_int state; ///< Set when the worker thread should terminate.

    FFLiveRawDemuxerPacket *head; ///< The first packet in the queue.
    FFLiveRawDemuxerPacket *tail; ///< The last packet in the queue.

    pthread_mutex_t mutex; ///< Protects the queue (head and tail).
    pthread_t thread; ///< Worker thread.
} FFLiveRawDemuxerContext;

/**
 * Worker thread for a container.
 *
 * This has to be a thread because the avio_* API doesn't support non-blocking
 * IO.
 */
static void *worker(void *arg)
{
    int e = 0;
    FFLiveRawDemuxerPacket *packet = NULL;
    AVFormatContext *context = arg;
    FFLiveRawDemuxerContext *priv_context = context->priv_data;
    size_t packet_alloc = priv_context->raw_packet_size +
                          sizeof(FFLiveRawDemuxerPacket);

    /* Keep looping until either the reader is finished or the end of the input.
       If there's an error, we early return. */
    while (!atomic_load(&priv_context->state) && !avio_feof(context->pb)) {
        // Allocate a queue packet.
        if (!packet && !(packet = av_mallocz(packet_alloc))) {
            atomic_store(&priv_context->error, AVERROR(ENOMEM));
            break;
        }

        // See where we're reading from.
        if (packet->pos = avio_tell(context->pb) < 0) {
            e = (int)packet->pos;
            break;
        }

        // Read more data.
        packet->size = avio_read_partial(context->pb, packet->buffer,
                                         priv_context->raw_packet_size);
        if (packet->size < 0) {
            e = (int)packet->size;
            break;
        }
        else if (packet->size == 0)
            continue;

        // Atomically append to the queue.
        if (e = AVERROR(pthread_mutex_lock(&priv_context->mutex)))
            break;

        if (priv_context->tail) {
            priv_context->tail->next = packet;
            priv_context->tail = packet;
        }
        else {
            priv_context->head = packet;
            priv_context->tail = packet;
        }
        packet = NULL; // We no longer own this packet.

        if (e = AVERROR(pthread_mutex_unlock(&priv_context->mutex)))
            break;
    }

    /* Clean up and set the error. */
    if (packet) {
        av_free(packet);
    }
    if (e) {
        atomic_store(&priv_context->error, e);
    }
    return NULL;
}

/**
 * Atomically try getting a packet (written by worker()) from the queue.
 */
static int get_packet(FFLiveRawDemuxerPacket **packet, AVFormatContext *context)
{
    int e = 0;
    FFLiveRawDemuxerPacket *new_packet = NULL;
    FFLiveRawDemuxerContext *priv_context = context->priv_data;

    /* Lock. */
    if (e = pthread_mutex_lock(&priv_context->mutex))
        return AVERROR(e);

    /* Extract any error. */
    // This works because if we don't get an error at this point, then the queue
    // is OK, even if there's an error later (since the queue can't change now).
    if (e = atomic_load(&priv_context->error)) {
        int e2 = 0;
        if (e2 = pthread_mutex_unlock(&priv_context->mutex))
            return AVERROR(e2);
        return e;
    }

    /* Extract the packet. */
    new_packet = priv_context->head;

    /* Move the linked-list along. */
    if (new_packet && new_packet->next)
        priv_context->head = new_packet->next;
    else {
        priv_context->head = NULL;
        priv_context->tail = NULL;
    }

    /* Unlock, and return. */
    if (e = pthread_mutex_unlock(&priv_context->mutex))
        return AVERROR(e);
    *packet = new_packet;
    return 0;
}

/**
 * Get the most recent current timestamp of any stream in a given format context.
 */
static int64_t get_cur_ts(const AVFormatContext *context, AVRational time_base)
{
    int64_t ts = INT64_MIN;
    AVRational tb = {1, 1};
    for (int i = 0; i < context->nb_streams; i++) {
        const FFStream *stream = (const FFStream *)context->streams[i];
        int64_t sts = stream->cur_dts; // TODO: Atomicity.

        if (sts == AV_NOPTS_VALUE)
            continue;
        if (av_compare_ts(sts, stream->pub.time_base, ts, tb) <= 0)
            continue;
        ts = sts;
        tb = stream->pub.time_base;
    }
    return av_rescale_q_rnd(ts, tb, time_base, AV_ROUND_UP);
}

static int read_packet(AVFormatContext *context, AVPacket *packet)
{
    int e = 0;
    int64_t ts = 0;
    FFLiveRawDemuxerPacket *raw_packet = NULL;
    FFLiveRawDemuxerContext *priv_context = context->priv_data;

    /* See if there's a packet in the queue, and extract it if so. */
    if (e = get_packet(&raw_packet, context))
        return e;

    /* If there's no packet, wait and try again. */
    if (!raw_packet) {
        // Wait.
        if (e = av_usleep(priv_context->raw_packet_poll))
            return e;

        // Try getting a packet again.
        if (e = get_packet(&raw_packet, context))
            return e;
    }

    /* Get the PTS corresponding to reception. */
    ts = get_cur_ts(context->sync_ts_to, context->streams[0]->time_base);

    /* Create the AVPacket and fill it in. */
    // If there's no packet in the queue, emit an empty packet to make the
    // muxer/demuxer happy to proceed up to this point in time.
    if (e = av_new_packet(packet, raw_packet ? raw_packet->size : 0))
        return e;
    if (raw_packet) {
        memcpy(packet->data, raw_packet->buffer, raw_packet->size);
        packet->pos = raw_packet->pos;
    }
    packet->pts = ts;
    packet->dts = ts;

    /* We're now finished with the raw queue packet. */
    av_free(raw_packet);
    return 0;
}

/**
 * Get a timebase that's common between all the streams in a format context.
 */
static AVRational get_common_time_base(const AVFormatContext *context)
{
    AVRational result = { 1, 1 };
    AVRational def = { 1, AV_TIME_BASE };
    for (int i = 0; i < context->nb_streams; i++)
        result = av_gcd_q(context->streams[i]->time_base, result, 1 << 30, def);
    return result;
}

static int read_header(AVFormatContext *context)
{
    int e = 0;
    AVStream *stream = NULL;
    FFLiveRawDemuxerContext *priv_context = context->priv_data;

    /* Validate the options. */
    if (!context->sync_ts_to) {
        av_log(context, AV_LOG_ERROR, "No input to synchronize to specified\n");
        return AVERROR(EINVAL);
    }

    /* Create a raw-codec stream for the data we read. */
    stream = avformat_new_stream(context, NULL);
    if (!stream)
        return AVERROR(ENOMEM);

    stream->codecpar->codec_type = AVMEDIA_TYPE_DATA;
    stream->codecpar->codec_id = AV_CODEC_ID_NONE;
    stream->time_base = get_common_time_base(context->sync_ts_to);

    /* Create the mutex/thread state. */
    if (e = pthread_mutex_init(&priv_context->mutex, NULL))
        return AVERROR(e);
    if (e = pthread_create(&priv_context->thread, NULL, worker, context))
        return AVERROR(e);
    atomic_init(&priv_context->error, 0);
    atomic_init(&priv_context->state, 0);

    /* Done :) */
    return 0;
}

static int read_close(AVFormatContext *context)
{
    int e = 0;
    FFLiveRawDemuxerPacket *next = NULL;
    FFLiveRawDemuxerContext *priv_context = context->priv_data;

    /* Wait for the worker thread. */
    atomic_store(&priv_context->state, 1);
    if (e = pthread_join(priv_context->thread, NULL))
        return AVERROR(e);

    /* Free things. */
    next = priv_context->head;
    while (next) {
        FFLiveRawDemuxerPacket *current = next;
        next = next->next;
        av_free(current);
    }
    if (e = pthread_mutex_destroy(&priv_context->mutex))
        return AVERROR(e);

    /* Return the error from the worker thread. */
    return priv_context->error;
}

static const AVOption options[] = {
    { "raw_packet_size", "", offsetof(FFLiveRawDemuxerContext, raw_packet_size),
      AV_OPT_TYPE_INT, { .i64 = 1024 }, 1, INT_MAX,
      AV_OPT_FLAG_DECODING_PARAM },
    { "raw_packet_poll", "", offsetof(FFLiveRawDemuxerContext, raw_packet_poll),
      AV_OPT_TYPE_INT, { .i64 = 1000 }, 1, INT_MAX,
      AV_OPT_FLAG_DECODING_PARAM },
    { NULL }
};

static const AVClass demuxer_class = {
    .class_name = "generic live raw demuxer",
    .item_name = av_default_item_name,
    .option = options,
    .version = LIBAVUTIL_VERSION_INT,
};

const FFInputFormat ff_livedata_demuxer = {
    .p.name = "livedata",
    .p.long_name = NULL_IF_CONFIG_SMALL("live raw data"),
    .p.flags = 0,
    .p.priv_class = &demuxer_class,
    .read_header = read_header,
    .read_packet = read_packet,
    .read_close = read_close,
    .raw_codec_id = AV_CODEC_ID_NONE,
    .priv_data_size = sizeof(FFLiveRawDemuxerContext),
};

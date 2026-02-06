#include "stream.h"
#include <glib.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <libavcodec/avcodec.h>
#include "metafile.h"
#include "epoll.h"
#include "log.h"
#include "main.h"
#include "packet.h"
#include "forward.h"
#include "recaux.h"
#include "mix.h"
#include "output.h"


#define MAXBUFLEN 65535
#ifndef AV_INPUT_BUFFER_PADDING_SIZE
#define AV_INPUT_BUFFER_PADDING_SIZE 0
#endif
#ifndef FF_INPUT_BUFFER_PADDING_SIZE
#define FF_INPUT_BUFFER_PADDING_SIZE 0
#endif
#define ALLOCLEN (MAXBUFLEN + AV_INPUT_BUFFER_PADDING_SIZE + FF_INPUT_BUFFER_PADDING_SIZE)


// stream is locked
void stream_close(stream_t *stream) {
	if (stream->fd == -1)
		return;
	epoll_del(stream->fd);
	close(stream->fd);
	stream->fd = -1;
	stream_mix_free(stream);
}

void stream_free(stream_t *stream) {
	g_free(stream);
}


static void stream_handler(handler_t *handler) {
	stream_t *stream = handler->ptr;
	unsigned char *buf = NULL;

	log_info_call = stream->metafile->name;
	log_info_stream = stream->name;

	//dbg("poll event for %s", stream->name);

	while (true) {
		pthread_mutex_lock(&stream->lock);

		if (stream->fd == -1)
			break;

		buf = malloc(ALLOCLEN);
		int ret = read(stream->fd, buf, MAXBUFLEN);
		if (ret == 0) {
			ilog(LOG_INFO, "EOF on stream %s", stream->name);
			stream_close(stream);
			break;
		}
		else if (ret < 0) {
			if (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK)
				break;
			ilog(LOG_INFO, "Read error on stream %s: %s", stream->name, strerror(errno));
			stream_close(stream);
			break;
		}

		// got a packet
		pthread_mutex_unlock(&stream->lock);

		if (forward_to){
			if (forward_packet(stream->metafile,buf,ret)) // leaves buf intact
				__atomic_add_fetch(&stream->metafile->forward_failed, 1, __ATOMIC_RELAXED);
			else
				__atomic_add_fetch(&stream->metafile->forward_count, 1, __ATOMIC_RELAXED);
		}
		if (decoding_enabled)
			packet_process(stream, buf, ret); // consumes buf
		else
			free(buf);

		buf = NULL;
	}

	pthread_mutex_unlock(&stream->lock);
	if (buf)
		free(buf);
	log_info_call = NULL;
	log_info_stream = NULL;
}


// mf is locked
static stream_t *stream_get(metafile_t *mf, unsigned long id) {
	if (mf->streams->len <= id)
		g_ptr_array_set_size(mf->streams, id + 1);
	stream_t *ret = g_ptr_array_index(mf->streams, id);
	if (ret)
		goto out;

	ret = g_new0(stream_t, 1);
	g_ptr_array_index(mf->streams, id) = ret;
	pthread_mutex_init(&ret->lock, NULL);
	ret->fd = -1;
	ret->id = id;
	ret->metafile = mf;
	ret->tag = (unsigned long) -1;
	ret->start_time_us = now_us();

out:
	return ret;
}


// mf is locked
void stream_open(metafile_t *mf, unsigned long id, char *name) {
	dbg("opening stream %lu/%s", id, name);

	stream_t *stream = stream_get(mf, id);

	stream->name = g_string_chunk_insert(mf->gsc, name);

	char fnbuf[PATH_MAX];
	snprintf(fnbuf, sizeof(fnbuf), "/proc/rtpengine/%u/calls/%s/%s", ktable, mf->parent, name);

	stream->fd = open(fnbuf, O_RDONLY | O_NONBLOCK);
	if (stream->fd == -1) {
		ilog(LOG_ERR, "Failed to open kernel stream %s: %s", fnbuf, strerror(errno));
		return;
	}

	// add to epoll
	stream->handler.ptr = stream;
	stream->handler.func = stream_handler;
	epoll_add(stream->fd, EPOLLIN, &stream->handler);
}

void stream_details(metafile_t *mf, unsigned long id, unsigned int tag, unsigned int media_sdp_id,
		unsigned int channel_slot)
{
	stream_t *stream = stream_get(mf, id);
	stream->tag = tag;
	stream->media_sdp_id = media_sdp_id;
	if (channel_slot >= mix_num_inputs) {
		stream->channel_slot = channel_slot % mix_num_inputs;
		ilog(LOG_ERR, "Channel slot %u is greater than the maximum number of inputs %u, setting to %u",
				channel_slot, mix_num_inputs, stream->channel_slot);
	}
	else
		stream->channel_slot = channel_slot;
}

void stream_forwarding_on(metafile_t *mf, unsigned long id, unsigned int on) {
	stream_t *stream = stream_get(mf, id);
	dbg("Setting forwarding flag to %u for stream #%lu", on, stream->id);
	stream->forwarding_on = on ? 1 : 0;
}


void stream_mix_init(stream_t *stream, metafile_t *mf) {
	if (mix_method != MM_STEREO)
		return;
	
	if (stream->stream_mix)
		return; // Already initialized
	
	// Skip RTCP streams - usually odd IDs in rtpengine
	if (stream->id % 2 != 0) {
		dbg("Skipping stream-level mixer for RTCP stream #%lu", stream->id);
		return;
	}

	dbg("Initializing stream-level mixer for stream #%lu", stream->id);
	
	pthread_mutex_init(&stream->stream_mix_lock, NULL);
	
	// Determine which channel this stream feeds into (L or R)
	// Priority 1: Explicit channel slot from metadata
	// Priority 2: Rank based on number of already initialized media mixers (order of appearance)
	unsigned int channel_idx;
	if (stream->channel_slot > 0) {
		channel_idx = (stream->channel_slot - 1) % 2;
	} else {
		unsigned int media_count = 0;
		for (unsigned int i = 0; i < mf->streams->len; i++) {
			stream_t *s = g_ptr_array_index(mf->streams, i);
			if (s && s->stream_mix && s != stream)
				media_count++;
		}
		channel_idx = media_count % 2;
	}
	
	dbg("Stream #%lu (slot=%u) assigned to global mixer channel %u", 
	    stream->id, stream->channel_slot, channel_idx);
	
	// Create a multiplexing sink that can feed both file and TLS mixers
	mux_sink_t *mux = g_new0(mux_sink_t, 1);
	mux_sink_init(mux);
	stream->stream_mix_out_sink = &mux->sink;

	// Add file global mixer as a destination if in stereo mode
	if (mf->mix) {
		sink_t *file_sink = g_new0(sink_t, 1);
		mix_sink_init(file_sink, NULL, &mf->mix, resample_audio);
		file_sink->mixer_idx = channel_idx;
		mux->sinks[mux->num_sinks++] = file_sink;
		dbg("Added file mixer destination to stream #%lu muxer", stream->id);
	}

	// Add TLS global mixer as a destination if in stereo mode
	if (tls_mixed && mf->tls_mix) {
		sink_t *tls_sink = g_new0(sink_t, 1);
		mix_sink_init(tls_sink, NULL, &mf->tls_mix, tls_resample);
		tls_sink->mixer_idx = channel_idx;
		mux->sinks[mux->num_sinks++] = tls_sink;
		dbg("Added TLS mixer destination to stream #%lu muxer", stream->id);
	}

	if (mux->num_sinks == 0) {
		ilog(LOG_ERR, "No destinations for stream #%lu mixer, aborting", stream->id);
		g_free(mux);
		stream->stream_mix_out_sink = NULL;
		return;
	}
	
	// Create stream-level mixer (mono output) using MM_DIRECT for proper audio mixing
	unsigned int num_inputs = MAX(mf->media_rec_slots, (unsigned int) mix_num_inputs);
	stream->stream_mix = mix_new_method(&stream->stream_mix_lock, stream->stream_mix_out_sink, 
	                                    num_inputs, MM_DIRECT);
	
	// Set channel slots to 1 so each SSRC gets its own slot and they are mixed together
	mix_set_channel_slots(stream->stream_mix, 1);
	
	dbg("Stream #%lu mixer initialized successfully with %d destinations", stream->id, mux->num_sinks);
}


void stream_mix_free(stream_t *stream) {
	if (!stream || !stream->stream_mix)
		return;
	
	dbg("Freeing stream-level mixer for stream #%lu", stream->id);
	
	mix_destroy(stream->stream_mix);
	stream->stream_mix = NULL;
	
	if (stream->stream_mix_out_sink) {
		mux_sink_t *mux = stream->stream_mix_out_sink->mux;
		for (int i = 0; i < mux->num_sinks; i++) {
			sink_close(mux->sinks[i]);
			g_free(mux->sinks[i]);
		}
		g_free(mux);
		stream->stream_mix_out_sink = NULL;
	}
}

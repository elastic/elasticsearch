/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Pressure controller for avoiding OOM due to many concurrent outstanding {@link TransportGetVirtualBatchedCompoundCommitChunkAction}
 * requests. Tracks the memory used by in-progress requests and rejects a request if the memory to be used exceeds a limit.
 *
 * This is a last resort mechanism to prevent OOM. Hitting the pressure limit means there are numerous requests that have been started (does
 * not care about queued requests), have reserved memory for their chunks, and are waiting for their response to be formed and sent over
 * the wire. A new request that requests a chunk with a size that exceeds the limit will be rejected with an
 * {@link EsRejectedExecutionException}. Otherwise, we count the memory for the chunk, which will be released when the response is sent.
 *
 * Note that there are a couple more safeguards that should help avoid reaching the limit in the first place. First, search nodes should be
 * typically sending requests with lengths of size up to 128KiB (see chunk size setting of {@link CacheBlobReaderService}). Second,
 * the VBCC chunk thread pool of {@link Stateless} that services this action, has a limited max size even on larger servers. For this latter
 * point, keep in mind that responses are sent from the transport threads, and the pressure mechanism is also useful for this, as a
 * preventive mechanism to avoid having too many responses sitting in memory.
 */
public class GetVirtualBatchedCompoundCommitChunksPressure {

    public static final String CURRENT_CHUNKS_BYTES_METRIC = "es.get_vbcc_chunk.memory.current";
    public static final String CHUNK_REQUESTS_REJECTED_METRIC = "es.get_vbcc_chunk.memory.rejected.total";

    public static final Setting<ByteSizeValue> CHUNKS_BYTES_LIMIT = Setting.memorySizeSetting(
        "stateless.get_vbcc_chunk.memory.limit",
        "5%",
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(GetVirtualBatchedCompoundCommitChunksPressure.class);

    private final AtomicLong currentChunksBytes = new AtomicLong(0);
    private final long chunksBytesLimit;

    private final LongUpDownCounter metricCurrentChunksBytes;
    private final LongCounter metricRejections;

    public GetVirtualBatchedCompoundCommitChunksPressure(Settings settings, MeterRegistry meterRegistry) {
        this.chunksBytesLimit = CHUNKS_BYTES_LIMIT.get(settings).getBytes();
        metricCurrentChunksBytes = meterRegistry.registerLongUpDownCounter(
            CURRENT_CHUNKS_BYTES_METRIC,
            "Current bytes used by VBCC chunk requests, attributed by the shard of each admitted request",
            "bytes"
        );
        metricRejections = meterRegistry.registerLongCounter(
            CHUNK_REQUESTS_REJECTED_METRIC,
            "Number of chunk requests rejected due to memory pressure, attributed by the bytes of each rejected request",
            "count"
        );
    }

    public Releasable markChunkStarted(int bytes) {
        long currentChunksBytes = this.currentChunksBytes.addAndGet(bytes);
        if (currentChunksBytes > chunksBytesLimit) {
            long bytesWithoutChunk = currentChunksBytes - bytes;
            this.currentChunksBytes.getAndAdd(-bytes);
            this.metricRejections.incrementBy(1);
            throw new EsRejectedExecutionException(
                "rejected execution of VBCC chunk request ["
                    + "current_chunks_bytes="
                    + bytesWithoutChunk
                    + ", "
                    + "request="
                    + bytes
                    + ", "
                    + "chunks_bytes_limit="
                    + chunksBytesLimit
                    + "]"
            );
        }
        this.metricCurrentChunksBytes.add(bytes);
        logger.trace(() -> Strings.format("added chunk request with [%d] bytes", bytes));
        // We assert that the releasable is called only once. If the assertion fails, this means the memory is adjusted twice.
        return Releasables.assertOnce(() -> {
            this.currentChunksBytes.getAndAdd(-bytes);
            this.metricCurrentChunksBytes.add(-bytes);
            logger.trace(() -> Strings.format("removed chunk request with [%d] bytes", bytes));
        });
    }

    public long getCurrentChunksBytes() {
        return currentChunksBytes.get();
    }
}

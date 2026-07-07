/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Immutable snapshot of cumulative I/O counters for a {@link StorageObject} instance.
 * <p>
 * Counters are produced live by a {@link StorageObjectMetricsCounters} held by each
 * provider implementation and snapshotted via {@link StorageObject#metrics()} when
 * the format reader needs to surface I/O numbers in the ES|QL query profile. Snapshots
 * are immutable values so they ride the wire alongside operator status without
 * additional synchronisation.
 * <p>
 * The split (mutable counter struct vs. immutable snapshot record) mirrors the
 * {@code BlobStoreActionStats} / GCS {@code Collector} pattern used by repository plugins.
 *
 * @param requestCount  number of read requests issued against the underlying store
 *                      (including retries)
 * @param requestNanos  cumulative wall time spent opening read streams against the store
 *                      (best-effort: providers measure between calling the SDK's
 *                      {@code getObject}/{@code reader}/{@code openInputStream} and the stream
 *                      being returned; the time spent draining bytes off the network after the
 *                      stream is returned is not currently captured)
 * @param bytesRead     planned read bytes against the underlying store, before any decompression
 *                      (best-effort: providers record the requested range length or the
 *                      response Content-Length at stream-open time, not the bytes actually
 *                      drained — close enough for a per-query I/O budget, not for measuring
 *                      end-of-stream truncation)
 * @param retryCount    number of automatic retries triggered by the underlying client. Only
 *                      tracked at the {@code RetryableStorageObject} decorator boundary today;
 *                      SDK-internal retry counts (AWS / GCS / Azure) are not yet wired
 */
public record StorageObjectMetrics(long requestCount, long requestNanos, long bytesRead, long retryCount)
    implements
        Writeable,
        ToXContentObject {

    public static final StorageObjectMetrics ZERO = new StorageObjectMetrics(0L, 0L, 0L, 0L);

    public StorageObjectMetrics {
        assert requestCount >= 0 && requestNanos >= 0 && bytesRead >= 0 && retryCount >= 0
            : "all counters must be non-negative, got " + requestCount + "/" + requestNanos + "/" + bytesRead + "/" + retryCount;
    }

    public StorageObjectMetrics(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(requestCount);
        out.writeVLong(requestNanos);
        out.writeVLong(bytesRead);
        out.writeVLong(retryCount);
    }

    /**
     * Returns the element-wise sum of this and {@code other}. Overflow wraps silently — these
     * are metrics counters, not invariants; a wrapped value is preferable to throwing through
     * the producer's {@code metrics()} call chain. Reaching {@code Long.MAX_VALUE} on any field
     * (~9.2 * 10^18) is implausible in practice (each counter would need a corresponding number
     * of requests / nanoseconds / bytes / retries on a single object).
     */
    public StorageObjectMetrics add(StorageObjectMetrics other) {
        return new StorageObjectMetrics(
            requestCount + other.requestCount,
            requestNanos + other.requestNanos,
            bytesRead + other.bytesRead,
            retryCount + other.retryCount
        );
    }

    public boolean isZero() {
        return requestCount == 0 && requestNanos == 0 && bytesRead == 0 && retryCount == 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, org.elasticsearch.xcontent.ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("request_count", requestCount);
        builder.field("request_nanos", requestNanos);
        builder.field("bytes_read", bytesRead);
        builder.field("retry_count", retryCount);
        builder.endObject();
        return builder;
    }
}

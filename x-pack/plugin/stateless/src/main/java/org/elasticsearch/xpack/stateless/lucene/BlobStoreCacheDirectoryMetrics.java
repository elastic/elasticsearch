/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class BlobStoreCacheDirectoryMetrics implements DirectoryMetrics.PluggableMetrics<BlobStoreCacheDirectoryMetrics> {

    public static final String NAME = "blob_store_cache";

    public static final String CACHE_MISS_WAIT_NANOS_HEADER = "cache_miss_wait_nanos";

    private long waitTime;
    private long waits;
    private long waitBytes;

    public BlobStoreCacheDirectoryMetrics() {}

    public BlobStoreCacheDirectoryMetrics(StreamInput in) throws IOException {
        this.waitTime = in.readLong();
        this.waits = in.readLong();
        this.waitBytes = in.readLong();
    }

    private BlobStoreCacheDirectoryMetrics(long waitTime, long waits, long waitBytes) {
        this.waitTime = waitTime;
        this.waits = waits;
        this.waitBytes = waitBytes;
    }

    @Override
    public BlobStoreCacheDirectoryMetrics copy() {
        return new BlobStoreCacheDirectoryMetrics(waitTime, waits, waitBytes);
    }

    @Override
    public Supplier<BlobStoreCacheDirectoryMetrics> delta() {
        BlobStoreCacheDirectoryMetrics snapshot = copy();
        return () -> copy().minus(snapshot);
    }

    private BlobStoreCacheDirectoryMetrics minus(BlobStoreCacheDirectoryMetrics snapshot) {
        return new BlobStoreCacheDirectoryMetrics(waitTime - snapshot.waitTime, waits - snapshot.waits, waitBytes - snapshot.waitBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("wait_time", waitTime);
        builder.field("waits", waits);
        builder.field("wait_bytes", waitBytes);
        return builder;
    }

    public void add(long time, long bytes) {
        ++waits;
        waitTime += time;
        waitBytes += bytes;
    }

    public long getWaitTime() {
        return waitTime;
    }

    public long getWaits() {
        return waits;
    }

    public long getWaitBytes() {
        return waitBytes;
    }

    @Override
    public BlobStoreCacheDirectoryMetrics merge(BlobStoreCacheDirectoryMetrics other) {
        return new BlobStoreCacheDirectoryMetrics(waitTime + other.waitTime, waits + other.waits, waitBytes + other.getWaitBytes());
    }

    /**
     * Returns cache-miss wait metrics as header entries.
     * Returns an empty map when {@code waits == 0} — no cache activity means no useful signal.
     * This is intentionally asymmetric with {@link StoreMetrics#entries()}, which always emits.
     */
    @Override
    public Map<String, String> entries() {
        if (waits == 0) {
            return Map.of();
        }
        return Map.of(CACHE_MISS_WAIT_NANOS_HEADER, Long.toString(waitTime));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(waitTime);
        out.writeLong(waits);
        out.writeLong(waitBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobStoreCacheDirectoryMetrics that = (BlobStoreCacheDirectoryMetrics) o;
        return waitTime == that.waitTime && waits == that.waits && waitBytes == that.waitBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(waitTime, waits, waitBytes);
    }
}

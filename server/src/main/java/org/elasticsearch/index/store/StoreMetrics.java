/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

public class StoreMetrics implements DirectoryMetrics.PluggableMetrics<StoreMetrics> {
    public static final String NAME = "store";
    public static final String BYTES_READ_RESPONSE_HEADER = "store_bytes_read";
    public static final PluggableDirectoryMetricsHolder<StoreMetrics> NOOP_HOLDER = PluggableDirectoryMetricsHolder.noop(
        new StoreMetrics() {
            @Override
            public void addBytesRead(long amount) {}
        }
    );

    private long bytesRead;

    public StoreMetrics(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public StoreMetrics() {}

    public StoreMetrics(StreamInput in) throws IOException {
        this.bytesRead = in.readVLong();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(bytesRead);
    }

    public long getBytesRead() {
        return bytesRead;
    }

    @Override
    public StoreMetrics copy() {
        return new StoreMetrics(bytesRead);
    }

    @Override
    public StoreMetrics merge(StoreMetrics other) {
        return new StoreMetrics(bytesRead + other.bytesRead);
    }

    @Override
    public Map<String, String> entries() {
        return Map.of(BYTES_READ_RESPONSE_HEADER, Long.toString(bytesRead));
    }

    @Override
    public Supplier<StoreMetrics> delta() {
        StoreMetrics snapshot = copy();

        return () -> copy().minus(snapshot);
    }

    private StoreMetrics minus(StoreMetrics snapshot) {
        return new StoreMetrics(bytesRead - snapshot.bytesRead);
    }

    public void addBytesRead(long amount) {
        bytesRead += amount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("bytesRead", bytesRead);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreMetrics that = (StoreMetrics) o;
        return bytesRead == that.bytesRead;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(bytesRead);
    }
}

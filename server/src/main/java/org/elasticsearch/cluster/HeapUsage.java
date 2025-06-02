/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Record representing the heap usage for a single cluster node
 */
public record HeapUsage(String nodeId, String nodeName, long totalBytes, long freeBytes) implements ToXContentFragment, Writeable {

    public HeapUsage(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.nodeName);
        out.writeVLong(this.totalBytes);
        out.writeVLong(this.freeBytes);
    }

    public XContentBuilder toShortXContent(XContentBuilder builder) throws IOException {
        builder.field("node_name", this.nodeName);
        builder.humanReadableField("total_heap_bytes", "total", ByteSizeValue.ofBytes(this.totalBytes));
        builder.humanReadableField("used_heap_bytes", "used", ByteSizeValue.ofBytes(this.usedBytes()));
        builder.humanReadableField("free_heap_bytes", "free", ByteSizeValue.ofBytes(this.freeBytes));
        builder.field("free_heap_percent", truncatePercent(this.freeHeapAsPercentage()));
        builder.field("used_heap_percent", truncatePercent(this.usedHeapAsPercentage()));
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", this.nodeId);
        toShortXContent(builder);
        return builder;
    }

    public double freeHeapAsPercentage() {
        return 100.0 * freeBytes / (double) totalBytes;
    }

    public double usedHeapAsPercentage() {
        return 100.0 - freeHeapAsPercentage();
    }

    public long usedBytes() {
        return totalBytes - freeBytes;
    }

    private static double truncatePercent(double pct) {
        return Math.round(pct * 10.0) / 10.0;
    }
}

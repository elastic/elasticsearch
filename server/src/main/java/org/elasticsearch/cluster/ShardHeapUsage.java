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
 * Record representing an estimate of the heap used by allocated shards and ongoing merges on a particular node
 */
public record ShardHeapUsage(String nodeId, String nodeName, long totalBytes, long estimatedUsageBytes)
    implements
        ToXContentFragment,
        Writeable {

    public ShardHeapUsage {
        assert estimatedUsageBytes <= totalBytes;
    }

    public ShardHeapUsage(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.nodeName);
        out.writeVLong(this.totalBytes);
        out.writeVLong(this.estimatedUsageBytes);
    }

    public XContentBuilder toShortXContent(XContentBuilder builder) throws IOException {
        builder.field("node_name", this.nodeName);
        builder.humanReadableField("total_heap_bytes", "total", ByteSizeValue.ofBytes(this.totalBytes));
        builder.humanReadableField("estimated_usage_bytes", "used", ByteSizeValue.ofBytes(this.estimatedUsageBytes));
        builder.humanReadableField("estimated_free_bytes", "free", ByteSizeValue.ofBytes(this.estimatedFreeBytes()));
        builder.field("estimated_free_percent", truncatePercent(this.freeHeapAsPercentage()));
        builder.field("estimated_usage_percent", truncatePercent(this.estimatedUsageAsPercentage()));
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", this.nodeId);
        toShortXContent(builder);
        return builder;
    }

    public double freeHeapAsPercentage() {
        return 100.0 - estimatedUsageAsPercentage();
    }

    public double estimatedUsageAsPercentage() {
        return 100.0 * estimatedUsageBytes / (double) totalBytes;
    }

    public long estimatedFreeBytes() {
        return totalBytes - estimatedUsageBytes;
    }

    private static double truncatePercent(double pct) {
        return Math.round(pct * 10.0) / 10.0;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for field mappings, useful for estimating the overhead of {@link MappingLookup} on data nodes.
 * Should be used at node or index level, and not at shard level, since the mappings may be shared across the shards of an index.
 */
public class FieldMappingStats implements Writeable, ToXContentFragment {

    private static final class Fields {
        static final String FIELD_MAPPINGS = "field_mappings";
        static final String TOTAL_COUNT = "total_count";
        static final String TOTAL_ESTIMATED_OVERHEAD = "total_estimated_overhead";
        static final String TOTAL_ESTIMATED_OVERHEAD_IN_BYTES = "total_estimated_overhead_in_bytes";
    }

    private long totalCount;
    private long totalEstimatedOverhead;

    public FieldMappingStats() {

    }

    public FieldMappingStats(StreamInput in) throws IOException {
        totalCount = in.readVLong();
        totalEstimatedOverhead = in.readVLong();
    }

    public FieldMappingStats(long totalCount, long totalEstimatedOverhead) {
        this.totalCount = totalCount;
        this.totalEstimatedOverhead = totalEstimatedOverhead;
    }

    public void add(@Nullable FieldMappingStats other) {
        this.totalCount += other == null ? 0 : other.totalCount;
        this.totalEstimatedOverhead += other == null ? 0 : other.totalEstimatedOverhead;
    }

    public long getTotalCount() {
        return this.totalCount;
    }

    public ByteSizeValue getTotalEstimatedOverhead() {
        return new ByteSizeValue(totalEstimatedOverhead);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCount);
        out.writeVLong(totalEstimatedOverhead);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FIELD_MAPPINGS);
        builder.field(Fields.TOTAL_COUNT, getTotalCount());
        builder.humanReadableField(Fields.TOTAL_ESTIMATED_OVERHEAD_IN_BYTES, Fields.TOTAL_ESTIMATED_OVERHEAD, getTotalEstimatedOverhead());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMappingStats that = (FieldMappingStats) o;
        return totalCount == that.totalCount && totalEstimatedOverhead == that.totalEstimatedOverhead;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalCount, totalEstimatedOverhead);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Node stats for mappings, useful for estimating the overhead of {@link MappingLookup} on data nodes.
 * Should be used at node or index level, and not at shard level, since the mappings may be shared across the shards of an index.
 */
public class NodeMappingStats implements Writeable, ToXContentFragment {

    public static final NodeFeature SEGMENT_LEVEL_FIELDS_STATS = new NodeFeature("mapper.segment_level_fields_stats");

    private static final class Fields {
        static final String MAPPINGS = "mappings";
        static final String TOTAL_COUNT = "total_count";
        static final String TOTAL_ESTIMATED_OVERHEAD = "total_estimated_overhead";
        static final String TOTAL_ESTIMATED_OVERHEAD_IN_BYTES = "total_estimated_overhead_in_bytes";
        static final String TOTAL_SEGMENTS = "total_segments";
        static final String TOTAL_SEGMENT_FIELDS = "total_segment_fields";
        static final String AVERAGE_FIELDS_PER_SEGMENT = "average_fields_per_segment";
    }

    private long totalCount;
    private long totalEstimatedOverhead;
    private long totalSegments;
    private long totalSegmentFields;

    public NodeMappingStats() {

    }

    public NodeMappingStats(StreamInput in) throws IOException {
        totalCount = in.readVLong();
        totalEstimatedOverhead = in.readVLong();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            totalSegments = in.readVLong();
            totalSegmentFields = in.readVLong();
        }
    }

    public NodeMappingStats(long totalCount, long totalEstimatedOverhead, long totalSegments, long totalSegmentFields) {
        this.totalCount = totalCount;
        this.totalEstimatedOverhead = totalEstimatedOverhead;
        this.totalSegments = totalSegments;
        this.totalSegmentFields = totalSegmentFields;
    }

    public void add(@Nullable NodeMappingStats other) {
        if (other == null) return;
        this.totalCount += other.totalCount;
        this.totalEstimatedOverhead += other.totalEstimatedOverhead;
        this.totalSegments += other.totalSegments;
        this.totalSegmentFields += other.totalSegmentFields;
    }

    public long getTotalCount() {
        return this.totalCount;
    }

    public ByteSizeValue getTotalEstimatedOverhead() {
        return ByteSizeValue.ofBytes(totalEstimatedOverhead);
    }

    public long getTotalSegments() {
        return totalSegments;
    }

    public long getTotalSegmentFields() {
        return totalSegmentFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCount);
        out.writeVLong(totalEstimatedOverhead);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeVLong(totalSegments);
            out.writeVLong(totalSegmentFields);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MAPPINGS);
        builder.field(Fields.TOTAL_COUNT, getTotalCount());
        builder.humanReadableField(Fields.TOTAL_ESTIMATED_OVERHEAD_IN_BYTES, Fields.TOTAL_ESTIMATED_OVERHEAD, getTotalEstimatedOverhead());
        builder.field(Fields.TOTAL_SEGMENTS, totalSegments);
        builder.field(Fields.TOTAL_SEGMENT_FIELDS, totalSegmentFields);
        builder.field(Fields.AVERAGE_FIELDS_PER_SEGMENT, totalSegments == 0 ? 0 : totalSegmentFields / totalSegments);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeMappingStats that = (NodeMappingStats) o;
        return totalCount == that.totalCount
            && totalEstimatedOverhead == that.totalEstimatedOverhead
            && totalSegments == that.totalSegments
            && totalSegmentFields == that.totalSegmentFields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalCount, totalEstimatedOverhead, totalSegments, totalSegmentFields);
    }
}

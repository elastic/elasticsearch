/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datatiers;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * {@link DataTiersFeatureSetUsage} represents the xpack usage for data tiers.
 * This includes things like the number of nodes per tier, indices, shards, etc.
 * See {@link TierSpecificStats} for the stats that are tracked on a per-tier
 * basis.
 */
public class DataTiersFeatureSetUsage extends XPackFeatureUsage {
    private final Map<String, TierSpecificStats> tierStats;

    public DataTiersFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.tierStats = in.readMap(TierSpecificStats::new);
    }

    public DataTiersFeatureSetUsage(Map<String, TierSpecificStats> tierStats) {
        super(XPackField.DATA_TIERS, true, true);
        this.tierStats = tierStats;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    public Map<String, TierSpecificStats> getTierStats() {
        return Collections.unmodifiableMap(tierStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(tierStats, StreamOutput::writeWriteable);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        for (Map.Entry<String, TierSpecificStats> entry : tierStats.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(tierStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DataTiersFeatureSetUsage other = (DataTiersFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(tierStats, other.tierStats);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * {@link TierSpecificStats} represents statistics about nodes in a single
     * tier, for example, how many nodes there are, the index count, shard
     * count, etc.
     */
    public static class TierSpecificStats implements Writeable, ToXContentObject {

        public final int nodeCount;
        public final int indexCount;
        public final int totalShardCount;
        public final int primaryShardCount;
        public final long docCount;
        public final long totalByteCount;
        public final long primaryByteCount;
        public final long primaryByteCountMedian;
        public final long primaryShardBytesMAD;

        public TierSpecificStats(StreamInput in) throws IOException {
            this.nodeCount = in.readVInt();
            this.indexCount = in.readVInt();
            this.totalShardCount = in.readVInt();
            this.primaryShardCount = in.readVInt();
            this.docCount = in.readVLong();
            this.totalByteCount = in.readVLong();
            this.primaryByteCount = in.readVLong();
            this.primaryByteCountMedian = in.readVLong();
            this.primaryShardBytesMAD = in.readVLong();
        }

        public TierSpecificStats(
            int nodeCount,
            int indexCount,
            int totalShardCount,
            int primaryShardCount,
            long docCount,
            long totalByteCount,
            long primaryByteCount,
            long primaryByteCountMedian,
            long primaryShardBytesMAD
        ) {
            this.nodeCount = nodeCount;
            this.indexCount = indexCount;
            this.totalShardCount = totalShardCount;
            this.primaryShardCount = primaryShardCount;
            this.docCount = docCount;
            this.totalByteCount = totalByteCount;
            this.primaryByteCount = primaryByteCount;
            this.primaryByteCountMedian = primaryByteCountMedian;
            this.primaryShardBytesMAD = primaryShardBytesMAD;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.nodeCount);
            out.writeVInt(this.indexCount);
            out.writeVInt(this.totalShardCount);
            out.writeVInt(this.primaryShardCount);
            out.writeVLong(this.docCount);
            out.writeVLong(this.totalByteCount);
            out.writeVLong(this.primaryByteCount);
            out.writeVLong(this.primaryByteCountMedian);
            out.writeVLong(this.primaryShardBytesMAD);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("node_count", nodeCount);
            builder.field("index_count", indexCount);
            builder.field("total_shard_count", totalShardCount);
            builder.field("primary_shard_count", primaryShardCount);
            builder.field("doc_count", docCount);
            builder.humanReadableField("total_size_bytes", "total_size", ByteSizeValue.ofBytes(totalByteCount));
            builder.humanReadableField("primary_size_bytes", "primary_size", ByteSizeValue.ofBytes(primaryByteCount));
            builder.humanReadableField(
                "primary_shard_size_avg_bytes",
                "primary_shard_size_avg",
                ByteSizeValue.ofBytes(primaryShardCount == 0 ? 0 : (primaryByteCount / primaryShardCount))
            );
            builder.humanReadableField(
                "primary_shard_size_median_bytes",
                "primary_shard_size_median",
                ByteSizeValue.ofBytes(primaryByteCountMedian)
            );
            builder.humanReadableField(
                "primary_shard_size_mad_bytes",
                "primary_shard_size_mad",
                ByteSizeValue.ofBytes(primaryShardBytesMAD)
            );
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                this.nodeCount,
                this.indexCount,
                this.totalShardCount,
                this.primaryShardCount,
                this.totalByteCount,
                this.primaryByteCount,
                this.docCount,
                this.primaryByteCountMedian,
                this.primaryShardBytesMAD
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TierSpecificStats other = (TierSpecificStats) obj;
            return nodeCount == other.nodeCount
                && indexCount == other.indexCount
                && totalShardCount == other.totalShardCount
                && primaryShardCount == other.primaryShardCount
                && docCount == other.docCount
                && totalByteCount == other.totalByteCount
                && primaryByteCount == other.primaryByteCount
                && primaryByteCountMedian == other.primaryByteCountMedian
                && primaryShardBytesMAD == other.primaryShardBytesMAD;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}

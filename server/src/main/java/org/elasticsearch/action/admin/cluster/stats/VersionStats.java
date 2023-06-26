/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@link VersionStats} calculates statistics for index creation versions mapped to the number of
 * indices, primary shards, and size of primary shards on disk. This is used from
 * {@link ClusterStatsIndices} and exposed as part of the {@code "/_cluster/stats"} API.
 */
public final class VersionStats implements ToXContentFragment, Writeable {

    private final Set<SingleVersionStats> versionStats;

    public static VersionStats of(Metadata metadata, List<ClusterStatsNodeResponse> nodeResponses) {
        final Map<Version, Integer> indexCounts = new HashMap<>();
        final Map<Version, Integer> primaryShardCounts = new HashMap<>();
        final Map<Version, Long> primaryByteCounts = new HashMap<>();
        final Map<String, List<ShardStats>> indexPrimaryShardStats = new HashMap<>();

        // Build a map from index name to primary shard stats
        for (ClusterStatsNodeResponse r : nodeResponses) {
            for (ShardStats shardStats : r.shardsStats()) {
                if (shardStats.getShardRouting().primary()) {
                    indexPrimaryShardStats.compute(shardStats.getShardRouting().getIndexName(), (name, stats) -> {
                        if (stats == null) {
                            List<ShardStats> newStats = new ArrayList<>();
                            newStats.add(shardStats);
                            return newStats;
                        } else {
                            stats.add(shardStats);
                            return stats;
                        }
                    });
                }
            }
        }

        // Loop through all indices in the metadata, building the counts as needed
        for (Map.Entry<String, IndexMetadata> cursor : metadata.indices().entrySet()) {
            IndexMetadata indexMetadata = cursor.getValue();
            // Increment version-specific index counts
            indexCounts.compute(indexMetadata.getCreationVersion(), (v, i) -> {
                if (i == null) {
                    return 1;
                } else {
                    return i + 1;
                }
            });
            // Increment version-specific primary shard counts
            primaryShardCounts.compute(indexMetadata.getCreationVersion(), (v, i) -> {
                if (i == null) {
                    return indexMetadata.getNumberOfShards();
                } else {
                    return i + indexMetadata.getNumberOfShards();
                }
            });
            // Increment version-specific primary shard sizes
            primaryByteCounts.compute(indexMetadata.getCreationVersion(), (v, i) -> {
                String indexName = indexMetadata.getIndex().getName();
                long indexPrimarySize = indexPrimaryShardStats.getOrDefault(indexName, Collections.emptyList())
                    .stream()
                    .mapToLong(stats -> stats.getStats().getStore().sizeInBytes())
                    .sum();
                if (i == null) {
                    return indexPrimarySize;
                } else {
                    return i + indexPrimarySize;
                }
            });
        }
        List<SingleVersionStats> calculatedStats = new ArrayList<>(indexCounts.size());
        for (Map.Entry<Version, Integer> indexVersionCount : indexCounts.entrySet()) {
            Version v = indexVersionCount.getKey();
            SingleVersionStats singleStats = new SingleVersionStats(
                v,
                indexVersionCount.getValue(),
                primaryShardCounts.getOrDefault(v, 0),
                primaryByteCounts.getOrDefault(v, 0L)
            );
            calculatedStats.add(singleStats);
        }
        return new VersionStats(calculatedStats);
    }

    VersionStats(Collection<SingleVersionStats> versionStats) {
        this.versionStats = Collections.unmodifiableSet(new TreeSet<>(versionStats));
    }

    VersionStats(StreamInput in) throws IOException {
        this.versionStats = Collections.unmodifiableSet(new TreeSet<>(in.readList(SingleVersionStats::new)));
    }

    public Set<SingleVersionStats> versionStats() {
        return this.versionStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("versions");
        for (SingleVersionStats stat : versionStats) {
            stat.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(versionStats);
    }

    @Override
    public int hashCode() {
        return versionStats.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        VersionStats other = (VersionStats) obj;
        return versionStats.equals(other.versionStats);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class SingleVersionStats implements ToXContentObject, Writeable, Comparable<SingleVersionStats> {

        public final Version version;
        public final int indexCount;
        public final int primaryShardCount;
        public final long totalPrimaryByteCount;

        SingleVersionStats(Version version, int indexCount, int primaryShardCount, long totalPrimaryByteCount) {
            this.version = version;
            this.indexCount = indexCount;
            this.primaryShardCount = primaryShardCount;
            this.totalPrimaryByteCount = totalPrimaryByteCount;
        }

        SingleVersionStats(StreamInput in) throws IOException {
            this.version = Version.readVersion(in);
            this.indexCount = in.readVInt();
            this.primaryShardCount = in.readVInt();
            this.totalPrimaryByteCount = in.readVLong();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("version", version.toString());
            builder.field("index_count", indexCount);
            builder.field("primary_shard_count", primaryShardCount);
            builder.humanReadableField("total_primary_bytes", "total_primary_size", ByteSizeValue.ofBytes(totalPrimaryByteCount));
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Version.writeVersion(this.version, out);
            out.writeVInt(this.indexCount);
            out.writeVInt(this.primaryShardCount);
            out.writeVLong(this.totalPrimaryByteCount);
        }

        @Override
        public int compareTo(SingleVersionStats o) {
            if (this.equals(o)) {
                return 0;
            }
            if (this.version.equals(o.version)) {
                // never 0, this is to make the comparator consistent with equals
                return -1;
            }
            return this.version.compareTo(o.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, indexCount, primaryShardCount, totalPrimaryByteCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            SingleVersionStats other = (SingleVersionStats) obj;
            return version.equals(other.version)
                && indexCount == other.indexCount
                && primaryShardCount == other.primaryShardCount
                && totalPrimaryByteCount == other.totalPrimaryByteCount;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}

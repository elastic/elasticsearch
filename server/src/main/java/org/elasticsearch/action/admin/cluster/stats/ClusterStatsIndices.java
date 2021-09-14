/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.List;

public class ClusterStatsIndices implements ToXContentFragment {

    private int indexCount;
    private ShardStats shards;
    private DocsStats docs;
    private StoreStats store;
    private FieldDataStats fieldData;
    private QueryCacheStats queryCache;
    private CompletionStats completion;
    private SegmentsStats segments;
    private final AnalysisStats analysis;
    private final MappingStats mappings;
    private final VersionStats versions;

    public ClusterStatsIndices(List<ClusterStatsNodeResponse> nodeResponses, MappingStats mappingStats,
                               AnalysisStats analysisStats, VersionStats versionStats) {
        ObjectObjectHashMap<String, ShardStats> countsPerIndex = new ObjectObjectHashMap<>();

        this.docs = new DocsStats();
        this.store = new StoreStats();
        this.fieldData = new FieldDataStats();
        this.queryCache = new QueryCacheStats();
        this.completion = new CompletionStats();
        this.segments = new SegmentsStats();

        for (ClusterStatsNodeResponse r : nodeResponses) {
            for (org.elasticsearch.action.admin.indices.stats.ShardStats shardStats : r.shardsStats()) {
                ShardStats indexShardStats = countsPerIndex.get(shardStats.getShardRouting().getIndexName());
                if (indexShardStats == null) {
                    indexShardStats = new ShardStats();
                    countsPerIndex.put(shardStats.getShardRouting().getIndexName(), indexShardStats);
                }

                indexShardStats.total++;

                CommonStats shardCommonStats = shardStats.getStats();

                if (shardStats.getShardRouting().primary()) {
                    indexShardStats.primaries++;
                    docs.add(shardCommonStats.docs);
                }
                store.add(shardCommonStats.store);
                fieldData.add(shardCommonStats.fieldData);
                queryCache.add(shardCommonStats.queryCache);
                completion.add(shardCommonStats.completion);
                segments.add(shardCommonStats.segments);
            }
        }

        shards = new ShardStats();
        indexCount = countsPerIndex.size();
        for (ObjectObjectCursor<String, ShardStats> indexCountsCursor : countsPerIndex) {
            shards.addIndexShardCount(indexCountsCursor.value);
        }

        this.mappings = mappingStats;
        this.analysis = analysisStats;
        this.versions = versionStats;
    }

    public int getIndexCount() {
        return indexCount;
    }

    public ShardStats getShards() {
        return this.shards;
    }

    public DocsStats getDocs() {
        return docs;
    }

    public StoreStats getStore() {
        return store;
    }

    public FieldDataStats getFieldData() {
        return fieldData;
    }

    public QueryCacheStats getQueryCache() {
        return queryCache;
    }

    public CompletionStats getCompletion() {
        return completion;
    }

    public SegmentsStats getSegments() {
        return segments;
    }

    public MappingStats getMappings() {
        return mappings;
    }

    public AnalysisStats getAnalysis() {
        return analysis;
    }

    public VersionStats getVersions() {
        return versions;
    }

    static final class Fields {
        static final String COUNT = "count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, indexCount);
        shards.toXContent(builder, params);
        docs.toXContent(builder, params);
        store.toXContent(builder, params);
        fieldData.toXContent(builder, params);
        queryCache.toXContent(builder, params);
        completion.toXContent(builder, params);
        segments.toXContent(builder, params);
        if (mappings != null) {
            mappings.toXContent(builder, params);
        }
        if (analysis != null) {
            analysis.toXContent(builder, params);
        }
        if (versions != null) {
            versions.toXContent(builder, params);
        }
        return builder;
    }

    public static class ShardStats implements ToXContentFragment {

        int indices;
        int total;
        int primaries;

        // min/max
        int minIndexShards = -1;
        int maxIndexShards = -1;
        int minIndexPrimaryShards = -1;
        int maxIndexPrimaryShards = -1;
        double minIndexReplication = -1;
        double totalIndexReplication = 0;
        double maxIndexReplication = -1;

        public ShardStats() {
        }

        /**
         * number of indices in the cluster
         */
        public int getIndices() {
            return this.indices;
        }

        /**
         * total number of shards in the cluster
         */
        public int getTotal() {
            return this.total;
        }

        /**
         * total number of primary shards in the cluster
         */
        public int getPrimaries() {
            return this.primaries;
        }

        /**
         * returns how many *redundant* copies of the data the cluster holds - running with no replicas will return 0
         */
        public double getReplication() {
            if (primaries == 0) {
                return 0;
            }
            return (((double) (total - primaries)) / primaries);
        }

        /**
         * the maximum number of shards (primary+replicas) an index has
         */
        public int getMaxIndexShards() {
            return this.maxIndexShards;
        }

        /**
         * the minimum number of shards (primary+replicas) an index has
         */
        public int getMinIndexShards() {
            return this.minIndexShards;
        }

        /**
         * average number of shards (primary+replicas) across the indices
         */
        public double getAvgIndexShards() {
            if (this.indices == 0) {
                return -1;
            }
            return ((double) this.total) / this.indices;
        }

        /**
         * the maximum number of primary shards an index has
         */
        public int getMaxIndexPrimaryShards() {
            return this.maxIndexPrimaryShards;
        }

        /**
         * the minimum number of primary shards an index has
         */
        public int getMinIndexPrimaryShards() {
            return this.minIndexPrimaryShards;
        }

        /**
         * the average number primary shards across the indices
         */
        public double getAvgIndexPrimaryShards() {
            if (this.indices == 0) {
                return -1;
            }
            return ((double) this.primaries) / this.indices;
        }

        /**
         * minimum replication factor across the indices. See {@link #getReplication}
         */
        public double getMinIndexReplication() {
            return this.minIndexReplication;
        }

        /**
         * average replication factor across the indices. See {@link #getReplication}
         */
        public double getAvgIndexReplication() {
            if (indices == 0) {
                return -1;
            }
            return this.totalIndexReplication / this.indices;
        }

        /**
         * maximum replication factor across the indices. See {@link #getReplication}
         */
        public double getMaxIndexReplication() {
            return this.maxIndexReplication;
        }

        public void addIndexShardCount(ShardStats indexShardCount) {
            this.indices++;
            this.primaries += indexShardCount.primaries;
            this.total += indexShardCount.total;
            this.totalIndexReplication += indexShardCount.getReplication();
            if (this.indices == 1) {
                // first index, uninitialized.
                minIndexPrimaryShards = indexShardCount.primaries;
                maxIndexPrimaryShards = indexShardCount.primaries;
                minIndexShards = indexShardCount.total;
                maxIndexShards = indexShardCount.total;
                minIndexReplication = indexShardCount.getReplication();
                maxIndexReplication = minIndexReplication;
            } else {
                minIndexShards = Math.min(minIndexShards, indexShardCount.total);
                minIndexPrimaryShards = Math.min(minIndexPrimaryShards, indexShardCount.primaries);
                minIndexReplication = Math.min(minIndexReplication, indexShardCount.getReplication());

                maxIndexShards = Math.max(maxIndexShards, indexShardCount.total);
                maxIndexPrimaryShards = Math.max(maxIndexPrimaryShards, indexShardCount.primaries);
                maxIndexReplication = Math.max(maxIndexReplication, indexShardCount.getReplication());
            }
        }

        static final class Fields {
            static final String SHARDS = "shards";
            static final String TOTAL = "total";
            static final String PRIMARIES = "primaries";
            static final String REPLICATION = "replication";
            static final String MIN = "min";
            static final String MAX = "max";
            static final String AVG = "avg";
            static final String INDEX = "index";
        }

        private void addIntMinMax(String field, int min, int max, double avg, XContentBuilder builder) throws IOException {
            builder.startObject(field);
            builder.field(Fields.MIN, min);
            builder.field(Fields.MAX, max);
            builder.field(Fields.AVG, avg);
            builder.endObject();
        }

        private void addDoubleMinMax(String field, double min, double max, double avg, XContentBuilder builder) throws IOException {
            builder.startObject(field);
            builder.field(Fields.MIN, min);
            builder.field(Fields.MAX, max);
            builder.field(Fields.AVG, avg);
            builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.SHARDS);
            if (indices > 0) {

                builder.field(Fields.TOTAL, total);
                builder.field(Fields.PRIMARIES, primaries);
                builder.field(Fields.REPLICATION, getReplication());

                builder.startObject(Fields.INDEX);
                addIntMinMax(Fields.SHARDS, minIndexShards, maxIndexShards, getAvgIndexShards(), builder);
                addIntMinMax(Fields.PRIMARIES, minIndexPrimaryShards, maxIndexPrimaryShards, getAvgIndexPrimaryShards(), builder);
                addDoubleMinMax(Fields.REPLICATION, minIndexReplication, maxIndexReplication, getAvgIndexReplication(), builder);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "total [" + total + "] primaries [" + primaries + "]";
        }
    }
}

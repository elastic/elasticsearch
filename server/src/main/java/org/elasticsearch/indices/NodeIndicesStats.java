/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardCountStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Global information on indices stats running on a specific node.
 */
public class NodeIndicesStats implements Writeable, ChunkedToXContent {

    private static final TransportVersion VERSION_SUPPORTING_STATS_BY_INDEX = TransportVersions.V_8_5_0;
    private static final Map<Index, List<IndexShardStats>> EMPTY_STATS_BY_SHARD = Map.of();

    private final CommonStats stats;
    private final Map<Index, List<IndexShardStats>> statsByShard;
    private final Map<Index, CommonStats> statsByIndex;

    public NodeIndicesStats(StreamInput in) throws IOException {
        stats = new CommonStats(in);

        statsByShard = new HashMap<>();
        int entries = in.readVInt();
        for (int i = 0; i < entries; i++) {
            Index index = new Index(in);
            int indexShardListSize = in.readVInt();
            List<IndexShardStats> indexShardStats = new ArrayList<>(indexShardListSize);
            for (int j = 0; j < indexShardListSize; j++) {
                indexShardStats.add(new IndexShardStats(in));
            }
            statsByShard.put(index, indexShardStats);
        }

        if (in.getTransportVersion().onOrAfter(VERSION_SUPPORTING_STATS_BY_INDEX)) {
            statsByIndex = in.readMap(Index::new, CommonStats::new);
        } else {
            statsByIndex = new HashMap<>();
        }
    }

    public NodeIndicesStats(
        CommonStats oldStats,
        Map<Index, CommonStats> statsByIndex,
        Map<Index, List<IndexShardStats>> statsByShard,
        boolean includeShardsStats
    ) {
        if (includeShardsStats) {
            this.statsByShard = Objects.requireNonNull(statsByShard);
        } else {
            this.statsByShard = EMPTY_STATS_BY_SHARD;
        }
        this.statsByIndex = Objects.requireNonNull(statsByIndex);

        // make a total common stats from old ones and current ones
        this.stats = oldStats;
        for (List<IndexShardStats> shardStatsList : statsByShard.values()) {
            for (IndexShardStats indexShardStats : shardStatsList) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    stats.add(shardStats.getStats());
                }
            }
        }
        for (CommonStats indexStats : statsByIndex.values()) {
            stats.add(indexStats);
        }
    }

    @Nullable
    public StoreStats getStore() {
        return stats.getStore();
    }

    @Nullable
    public DocsStats getDocs() {
        return stats.getDocs();
    }

    @Nullable
    public IndexingStats getIndexing() {
        return stats.getIndexing();
    }

    @Nullable
    public GetStats getGet() {
        return stats.getGet();
    }

    @Nullable
    public SearchStats getSearch() {
        return stats.getSearch();
    }

    @Nullable
    public MergeStats getMerge() {
        return stats.getMerge();
    }

    @Nullable
    public RefreshStats getRefresh() {
        return stats.getRefresh();
    }

    @Nullable
    public FlushStats getFlush() {
        return stats.getFlush();
    }

    @Nullable
    public WarmerStats getWarmer() {
        return stats.getWarmer();
    }

    @Nullable
    public FieldDataStats getFieldData() {
        return stats.getFieldData();
    }

    @Nullable
    public QueryCacheStats getQueryCache() {
        return stats.getQueryCache();
    }

    @Nullable
    public RequestCacheStats getRequestCache() {
        return stats.getRequestCache();
    }

    @Nullable
    public CompletionStats getCompletion() {
        return stats.getCompletion();
    }

    @Nullable
    public SegmentsStats getSegments() {
        return stats.getSegments();
    }

    @Nullable
    public TranslogStats getTranslog() {
        return stats.getTranslog();
    }

    @Nullable
    public RecoveryStats getRecoveryStats() {
        return stats.getRecoveryStats();
    }

    @Nullable
    public BulkStats getBulk() {
        return stats.getBulk();
    }

    @Nullable
    public ShardCountStats getShardCount() {
        return stats.getShards();
    }

    @Nullable
    public NodeMappingStats getNodeMappingStats() {
        return stats.getNodeMappings();
    }

    @Nullable
    public DenseVectorStats getDenseVectorStats() {
        return stats.getDenseVectorStats();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        out.writeMap(statsByShard, StreamOutput::writeWriteable, StreamOutput::writeCollection);
        if (out.getTransportVersion().onOrAfter(VERSION_SUPPORTING_STATS_BY_INDEX)) {
            out.writeMap(statsByIndex);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeIndicesStats that = (NodeIndicesStats) o;
        return stats.equals(that.stats) && statsByShard.equals(that.statsByShard) && statsByIndex.equals(that.statsByIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats, statsByShard, statsByIndex);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {

        return Iterators.concat(

            Iterators.single((builder, params) -> {
                builder.startObject(Fields.INDICES);
                return stats.toXContent(builder, params);
            }),

            switch (NodeStatsLevel.of(outerParams, NodeStatsLevel.NODE)) {

                case NODE -> Collections.<ToXContent>emptyIterator();

                case INDICES -> Iterators.concat(
                    ChunkedToXContentHelper.startObject(Fields.INDICES),
                    Iterators.map(createCommonStatsByIndex().entrySet().iterator(), entry -> (builder, params) -> {
                        builder.startObject(entry.getKey().getName());
                        entry.getValue().toXContent(builder, params);
                        return builder.endObject();
                    }),
                    ChunkedToXContentHelper.endObject()
                );

                case SHARDS -> Iterators.concat(
                    ChunkedToXContentHelper.startObject(Fields.SHARDS),
                    Iterators.flatMap(
                        statsByShard.entrySet().iterator(),
                        entry -> Iterators.concat(
                            ChunkedToXContentHelper.startArray(entry.getKey().getName()),
                            Iterators.flatMap(
                                entry.getValue().iterator(),
                                indexShardStats -> Iterators.concat(
                                    Iterators.single(
                                        (b, p) -> b.startObject().startObject(String.valueOf(indexShardStats.getShardId().getId()))
                                    ),
                                    Iterators.flatMap(Iterators.forArray(indexShardStats.getShards()), Iterators::<ToXContent>single),
                                    Iterators.single((b, p) -> b.endObject().endObject())
                                )
                            ),
                            ChunkedToXContentHelper.endArray()
                        )
                    ),
                    ChunkedToXContentHelper.endObject()
                );
            },

            ChunkedToXContentHelper.endObject()
        );
    }

    private Map<Index, CommonStats> createCommonStatsByIndex() {
        Map<Index, CommonStats> statsMap = new HashMap<>();

        for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
            if (statsMap.containsKey(entry.getKey()) == false) {
                statsMap.put(entry.getKey(), new CommonStats());
            }

            for (IndexShardStats indexShardStats : entry.getValue()) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    statsMap.get(entry.getKey()).add(shardStats.getStats());
                }
            }
        }

        for (Map.Entry<Index, CommonStats> entry : statsByIndex.entrySet()) {
            statsMap.computeIfAbsent(entry.getKey(), k -> new CommonStats()).add(entry.getValue());
        }

        return statsMap;
    }

    public List<IndexShardStats> getShardStats(Index index) {
        return statsByShard.get(index);
    }

    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
    }
}

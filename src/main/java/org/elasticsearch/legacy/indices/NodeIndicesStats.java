/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.legacy.indices;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.legacy.action.admin.indices.stats.CommonStats;
import org.elasticsearch.legacy.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.legacy.action.admin.indices.stats.ShardStats;
import org.elasticsearch.legacy.common.Nullable;
import org.elasticsearch.legacy.common.io.stream.StreamInput;
import org.elasticsearch.legacy.common.io.stream.StreamOutput;
import org.elasticsearch.legacy.common.io.stream.Streamable;
import org.elasticsearch.legacy.common.xcontent.ToXContent;
import org.elasticsearch.legacy.common.xcontent.XContentBuilder;
import org.elasticsearch.legacy.common.xcontent.XContentBuilderString;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.cache.filter.FilterCacheStats;
import org.elasticsearch.legacy.index.cache.id.IdCacheStats;
import org.elasticsearch.legacy.index.engine.SegmentsStats;
import org.elasticsearch.legacy.index.fielddata.FieldDataStats;
import org.elasticsearch.legacy.index.flush.FlushStats;
import org.elasticsearch.legacy.index.get.GetStats;
import org.elasticsearch.legacy.index.indexing.IndexingStats;
import org.elasticsearch.legacy.index.merge.MergeStats;
import org.elasticsearch.legacy.index.percolator.stats.PercolateStats;
import org.elasticsearch.legacy.index.refresh.RefreshStats;
import org.elasticsearch.legacy.index.search.stats.SearchStats;
import org.elasticsearch.legacy.index.shard.DocsStats;
import org.elasticsearch.legacy.index.shard.ShardId;
import org.elasticsearch.legacy.index.shard.service.IndexShard;
import org.elasticsearch.legacy.index.store.StoreStats;
import org.elasticsearch.legacy.index.suggest.stats.SuggestStats;
import org.elasticsearch.legacy.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Global information on indices stats running on a specific node.
 */
public class NodeIndicesStats implements Streamable, Serializable, ToXContent {

    private CommonStats stats;
    private Map<Index, List<IndexShardStats>> statsByShard;

    NodeIndicesStats() {
    }

    public NodeIndicesStats(CommonStats oldStats, Map<Index, List<IndexShardStats>> statsByShard) {
        //this.stats = stats;
        this.statsByShard = statsByShard;

        // make a total common stats from old ones and current ones
        this.stats = oldStats;
        for (List<IndexShardStats> shardStatsList : statsByShard.values()) {
            for (IndexShardStats indexShardStats : shardStatsList) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    stats.add(shardStats.getStats());
                }
            }
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
    public PercolateStats getPercolate() {
        return stats.getPercolate();
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
    public FieldDataStats getFieldData() {
        return stats.getFieldData();
    }

    @Nullable
    public FilterCacheStats getFilterCache() {
        return stats.getFilterCache();
    }

    @Nullable
    public IdCacheStats getIdCache() {
        return stats.getIdCache();
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
    public SuggestStats getSuggest() {
        return stats.getSuggest();
    }

    public static NodeIndicesStats readIndicesStats(StreamInput in) throws IOException {
        NodeIndicesStats stats = new NodeIndicesStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        stats = CommonStats.readCommonStats(in);
        if (in.readBoolean()) {
            int entries = in.readVInt();
            statsByShard = Maps.newHashMap();
            for (int i = 0; i < entries; i++) {
                Index index = Index.readIndexName(in);
                int indexShardListSize = in.readVInt();
                List<IndexShardStats> indexShardStats = Lists.newArrayListWithCapacity(indexShardListSize);
                for (int j = 0; j < indexShardListSize; j++) {
                    indexShardStats.add(IndexShardStats.readIndexShardStats(in));
                }
                statsByShard.put(index, indexShardStats);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        out.writeBoolean(statsByShard != null);
        if (statsByShard != null) {
            out.writeVInt(statsByShard.size());
            for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
                entry.getKey().writeTo(out);
                out.writeVInt(entry.getValue().size());
                for (IndexShardStats indexShardStats : entry.getValue()) {
                    indexShardStats.writeTo(out);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String level = params.param("level", "node");
        boolean isLevelValid = "node".equalsIgnoreCase(level) || "indices".equalsIgnoreCase(level) || "shards".equalsIgnoreCase(level);
        if (!isLevelValid) {
            return builder;
        }

        // "node" level
        builder.startObject(Fields.INDICES);
        stats.toXContent(builder, params);

        if ("indices".equals(level)) {
            Map<Index, CommonStats> indexStats = createStatsByIndex();
            builder.startObject(Fields.INDICES);
            for (Map.Entry<Index, CommonStats> entry : indexStats.entrySet()) {
                builder.startObject(entry.getKey().name());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        } else if ("shards".equals(level)) {
            builder.startObject("shards");
            for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
                builder.startArray(entry.getKey().name());
                for (IndexShardStats indexShardStats : entry.getValue()) {
                    builder.startObject().startObject(String.valueOf(indexShardStats.getShardId().getId()));
                    for (ShardStats shardStats : indexShardStats.getShards()) {
                        shardStats.toXContent(builder, params);
                    }
                    builder.endObject().endObject();
                }
                builder.endArray();
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    private Map<Index, CommonStats> createStatsByIndex() {
        Map<Index, CommonStats> statsMap = Maps.newHashMap();
        for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
            if (!statsMap.containsKey(entry.getKey())) {
                statsMap.put(entry.getKey(), new CommonStats());
            }

            for (IndexShardStats indexShardStats : entry.getValue()) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    statsMap.get(entry.getKey()).add(shardStats.getStats());
                }
            }
        }

        return statsMap;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
    }
}

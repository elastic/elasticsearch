/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.io.Serializable;

/**
 * Global information on indices stats running on a specific node.
 */
public class NodeIndicesStats implements Streamable, Serializable, ToXContent {

    private CommonStats stats;

    NodeIndicesStats() {
    }

    public NodeIndicesStats(CommonStats stats) {
        this.stats = stats;
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

    public static NodeIndicesStats readIndicesStats(StreamInput in) throws IOException {
        NodeIndicesStats stats = new NodeIndicesStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        stats = CommonStats.readCommonStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.INDICES);
        stats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
    }
}

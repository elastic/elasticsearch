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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.cache.CacheStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;

import java.io.IOException;
import java.io.Serializable;

/**
 * Global information on indices stats running on a specific node.
 *
 *
 */
public class NodeIndicesStats implements Streamable, Serializable, ToXContent {

    private StoreStats storeStats;

    private DocsStats docsStats;

    private IndexingStats indexingStats;

    private GetStats getStats;

    private SearchStats searchStats;

    private CacheStats cacheStats;

    private MergeStats mergeStats;

    private RefreshStats refreshStats;

    private FlushStats flushStats;

    NodeIndicesStats() {
    }

    public NodeIndicesStats(StoreStats storeStats, DocsStats docsStats, IndexingStats indexingStats, GetStats getStats, SearchStats searchStats, CacheStats cacheStats, MergeStats mergeStats, RefreshStats refreshStats, FlushStats flushStats) {
        this.storeStats = storeStats;
        this.docsStats = docsStats;
        this.indexingStats = indexingStats;
        this.getStats = getStats;
        this.searchStats = searchStats;
        this.cacheStats = cacheStats;
        this.mergeStats = mergeStats;
        this.refreshStats = refreshStats;
        this.flushStats = flushStats;
    }

    public StoreStats store() {
        return this.storeStats;
    }

    /**
     * The size of the index storage taken on the node.
     */
    public StoreStats getStore() {
        return storeStats;
    }

    public DocsStats docs() {
        return this.docsStats;
    }

    public DocsStats getDocs() {
        return this.docsStats;
    }

    public IndexingStats indexing() {
        return indexingStats;
    }

    public IndexingStats getIndexing() {
        return indexing();
    }

    public GetStats get() {
        return this.getStats;
    }

    public GetStats getGet() {
        return this.getStats;
    }

    public SearchStats search() {
        return this.searchStats;
    }

    public SearchStats getSearch() {
        return this.searchStats;
    }

    public CacheStats cache() {
        return this.cacheStats;
    }

    public CacheStats getCache() {
        return this.cache();
    }

    public MergeStats merge() {
        return this.mergeStats;
    }

    public MergeStats getMerge() {
        return this.mergeStats;
    }

    public RefreshStats refresh() {
        return this.refreshStats;
    }

    public RefreshStats getRefresh() {
        return this.refresh();
    }

    public FlushStats flush() {
        return this.flushStats;
    }

    public FlushStats getFlush() {
        return this.flushStats;
    }

    public static NodeIndicesStats readIndicesStats(StreamInput in) throws IOException {
        NodeIndicesStats stats = new NodeIndicesStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        storeStats = StoreStats.readStoreStats(in);
        docsStats = DocsStats.readDocStats(in);
        indexingStats = IndexingStats.readIndexingStats(in);
        getStats = GetStats.readGetStats(in);
        searchStats = SearchStats.readSearchStats(in);
        cacheStats = CacheStats.readCacheStats(in);
        mergeStats = MergeStats.readMergeStats(in);
        refreshStats = RefreshStats.readRefreshStats(in);
        flushStats = FlushStats.readFlushStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        storeStats.writeTo(out);
        docsStats.writeTo(out);
        indexingStats.writeTo(out);
        getStats.writeTo(out);
        searchStats.writeTo(out);
        cacheStats.writeTo(out);
        mergeStats.writeTo(out);
        refreshStats.writeTo(out);
        flushStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.INDICES);

        storeStats.toXContent(builder, params);
        docsStats.toXContent(builder, params);
        indexingStats.toXContent(builder, params);
        getStats.toXContent(builder, params);
        searchStats.toXContent(builder, params);
        cacheStats.toXContent(builder, params);
        mergeStats.toXContent(builder, params);
        refreshStats.toXContent(builder, params);
        flushStats.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
    }
}

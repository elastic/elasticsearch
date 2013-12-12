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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.Version;
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
import org.elasticsearch.index.percolator.stats.PercolateStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.io.IOException;

/**
 */
public class CommonStats implements Streamable, ToXContent {

    public CommonStats() {
        this(CommonStatsFlags.NONE);
    }

    public CommonStats(CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (CommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs:
                    docs = new DocsStats();
                    break;
                case Store:
                    store = new StoreStats();
                    break;
                case Indexing:
                    indexing = new IndexingStats();
                    break;
                case Get:
                    get = new GetStats();
                    break;
                case Search:
                    search = new SearchStats();
                    break;
                case Merge:
                    merge = new MergeStats();
                    break;
                case Refresh:
                    refresh = new RefreshStats();
                    break;
                case Flush:
                    flush = new FlushStats();
                    break;
                case Warmer:
                    warmer = new WarmerStats();
                    break;
                case FilterCache:
                    filterCache = new FilterCacheStats();
                    break;
                case IdCache:
                    idCache = new IdCacheStats();
                    break;
                case FieldData:
                    fieldData = new FieldDataStats();
                    break;
                case Completion:
                    completion = new CompletionStats();
                    break;
                case Segments:
                    segments = new SegmentsStats();
                    break;
                case Percolate:
                    percolate = new PercolateStats();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }


    public CommonStats(IndexShard indexShard, CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (CommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs:
                    docs = indexShard.docStats();
                    break;
                case Store:
                    store = indexShard.storeStats();
                    break;
                case Indexing:
                    indexing = indexShard.indexingStats(flags.types());
                    break;
                case Get:
                    get = indexShard.getStats();
                    break;
                case Search:
                    search = indexShard.searchStats(flags.groups());
                    break;
                case Merge:
                    merge = indexShard.mergeStats();
                    break;
                case Refresh:
                    refresh = indexShard.refreshStats();
                    break;
                case Flush:
                    flush = indexShard.flushStats();
                    break;
                case Warmer:
                    warmer = indexShard.warmerStats();
                    break;
                case FilterCache:
                    filterCache = indexShard.filterCacheStats();
                    break;
                case IdCache:
                    idCache = indexShard.idCacheStats();
                    break;
                case FieldData:
                    fieldData = indexShard.fieldDataStats(flags.fieldDataFields());
                    break;
                case Completion:
                    completion = indexShard.completionStats(flags.completionDataFields());
                    break;
                case Segments:
                    segments = indexShard.segmentStats();
                    break;
                case Percolate:
                    percolate = indexShard.shardPercolateService().stats();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }

    @Nullable
    public DocsStats docs;

    @Nullable
    public StoreStats store;

    @Nullable
    public IndexingStats indexing;

    @Nullable
    public GetStats get;

    @Nullable
    public SearchStats search;

    @Nullable
    public MergeStats merge;

    @Nullable
    public RefreshStats refresh;

    @Nullable
    public FlushStats flush;

    @Nullable
    public WarmerStats warmer;

    @Nullable
    public FilterCacheStats filterCache;

    @Nullable
    public IdCacheStats idCache;

    @Nullable
    public FieldDataStats fieldData;

    @Nullable
    public PercolateStats percolate;

    @Nullable
    public CompletionStats completion;

    @Nullable
    public SegmentsStats segments;

    /**
     * Weight in the cluster.  Only calculated before returning to over xcontent and never streamed.
     */
    @Nullable
    public Weight weight;
    
    public void add(CommonStats stats) {
        if (docs == null) {
            if (stats.getDocs() != null) {
                docs = new DocsStats();
                docs.add(stats.getDocs());
            }
        } else {
            docs.add(stats.getDocs());
        }
        if (store == null) {
            if (stats.getStore() != null) {
                store = new StoreStats();
                store.add(stats.getStore());
            }
        } else {
            store.add(stats.getStore());
        }
        if (indexing == null) {
            if (stats.getIndexing() != null) {
                indexing = new IndexingStats();
                indexing.add(stats.getIndexing());
            }
        } else {
            indexing.add(stats.getIndexing());
        }
        if (get == null) {
            if (stats.getGet() != null) {
                get = new GetStats();
                get.add(stats.getGet());
            }
        } else {
            get.add(stats.getGet());
        }
        if (search == null) {
            if (stats.getSearch() != null) {
                search = new SearchStats();
                search.add(stats.getSearch());
            }
        } else {
            search.add(stats.getSearch());
        }
        if (merge == null) {
            if (stats.getMerge() != null) {
                merge = new MergeStats();
                merge.add(stats.getMerge());
            }
        } else {
            merge.add(stats.getMerge());
        }
        if (refresh == null) {
            if (stats.getRefresh() != null) {
                refresh = new RefreshStats();
                refresh.add(stats.getRefresh());
            }
        } else {
            refresh.add(stats.getRefresh());
        }
        if (flush == null) {
            if (stats.getFlush() != null) {
                flush = new FlushStats();
                flush.add(stats.getFlush());
            }
        } else {
            flush.add(stats.getFlush());
        }
        if (warmer == null) {
            if (stats.getWarmer() != null) {
                warmer = new WarmerStats();
                warmer.add(stats.getWarmer());
            }
        } else {
            warmer.add(stats.getWarmer());
        }
        if (filterCache == null) {
            if (stats.getFilterCache() != null) {
                filterCache = new FilterCacheStats();
                filterCache.add(stats.getFilterCache());
            }
        } else {
            filterCache.add(stats.getFilterCache());
        }

        if (idCache == null) {
            if (stats.getIdCache() != null) {
                idCache = new IdCacheStats();
                idCache.add(stats.getIdCache());
            }
        } else {
            idCache.add(stats.getIdCache());
        }

        if (fieldData == null) {
            if (stats.getFieldData() != null) {
                fieldData = new FieldDataStats();
                fieldData.add(stats.getFieldData());
            }
        } else {
            fieldData.add(stats.getFieldData());
        }
        if (percolate == null) {
            if (stats.getPercolate() != null) {
                percolate = new PercolateStats();
                percolate.add(stats.getPercolate());
            }
        } else {
            percolate.add(stats.getPercolate());
        }
        if (completion == null) {
            if (stats.getCompletion() != null) {
                completion = new CompletionStats();
                completion.add(stats.getCompletion());
            }
        } else {
            completion.add(stats.getCompletion());
        }
        if (segments == null) {
            if (stats.getSegments() != null) {
                segments = new SegmentsStats();
                segments.add(stats.getSegments());
            }
        } else {
            segments.add(stats.getSegments());
        }
    }

    @Nullable
    public DocsStats getDocs() {
        return this.docs;
    }

    @Nullable
    public StoreStats getStore() {
        return store;
    }

    @Nullable
    public IndexingStats getIndexing() {
        return indexing;
    }

    @Nullable
    public GetStats getGet() {
        return get;
    }

    @Nullable
    public SearchStats getSearch() {
        return search;
    }

    @Nullable
    public MergeStats getMerge() {
        return merge;
    }

    @Nullable
    public RefreshStats getRefresh() {
        return refresh;
    }

    @Nullable
    public FlushStats getFlush() {
        return flush;
    }

    @Nullable
    public WarmerStats getWarmer() {
        return this.warmer;
    }

    @Nullable
    public FilterCacheStats getFilterCache() {
        return this.filterCache;
    }

    @Nullable
    public IdCacheStats getIdCache() {
        return this.idCache;
    }

    @Nullable
    public FieldDataStats getFieldData() {
        return this.fieldData;
    }

    @Nullable
    public PercolateStats getPercolate() {
        return percolate;
    }

    @Nullable
    public CompletionStats getCompletion() {
        return completion;
    }

    @Nullable
    public SegmentsStats getSegments() {
        return segments;
    }

    public static CommonStats readCommonStats(StreamInput in) throws IOException {
        CommonStats stats = new CommonStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            docs = DocsStats.readDocStats(in);
        }
        if (in.readBoolean()) {
            store = StoreStats.readStoreStats(in);
        }
        if (in.readBoolean()) {
            indexing = IndexingStats.readIndexingStats(in);
        }
        if (in.readBoolean()) {
            get = GetStats.readGetStats(in);
        }
        if (in.readBoolean()) {
            search = SearchStats.readSearchStats(in);
        }
        if (in.readBoolean()) {
            merge = MergeStats.readMergeStats(in);
        }
        if (in.readBoolean()) {
            refresh = RefreshStats.readRefreshStats(in);
        }
        if (in.readBoolean()) {
            flush = FlushStats.readFlushStats(in);
        }
        if (in.readBoolean()) {
            warmer = WarmerStats.readWarmerStats(in);
        }
        if (in.readBoolean()) {
            filterCache = FilterCacheStats.readFilterCacheStats(in);
        }
        if (in.readBoolean()) {
            idCache = IdCacheStats.readIdCacheStats(in);
        }
        if (in.readBoolean()) {
            fieldData = FieldDataStats.readFieldDataStats(in);
        }
        if (in.readBoolean()) {
            percolate = PercolateStats.readPercolateStats(in);
        }
        if (in.getVersion().onOrAfter(Version.V_0_90_4)) {
            if (in.readBoolean()) {
                completion = CompletionStats.readCompletionStats(in);
            }
        }
        if (in.getVersion().after(Version.V_0_90_6)) {
            if (in.readBoolean()) {
                segments = SegmentsStats.readSegmentsStats(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (docs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            docs.writeTo(out);
        }
        if (store == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            store.writeTo(out);
        }
        if (indexing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indexing.writeTo(out);
        }
        if (get == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            get.writeTo(out);
        }
        if (search == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            search.writeTo(out);
        }
        if (merge == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            merge.writeTo(out);
        }
        if (refresh == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            refresh.writeTo(out);
        }
        if (flush == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            flush.writeTo(out);
        }
        if (warmer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            warmer.writeTo(out);
        }
        if (filterCache == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            filterCache.writeTo(out);
        }
        if (idCache == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            idCache.writeTo(out);
        }
        if (fieldData == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            fieldData.writeTo(out);
        }
        if (percolate == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            percolate.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_0_90_4)) {
            if (completion == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                completion.writeTo(out);
            }
        }
        if (out.getVersion().after(Version.V_0_90_6)) {
            if (segments == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                segments.writeTo(out);
            }
        }
    }

    // note, requires a wrapping object
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (docs != null) {
            docs.toXContent(builder, params);
        }
        if (store != null) {
            store.toXContent(builder, params);
        }
        if (indexing != null) {
            indexing.toXContent(builder, params);
        }
        if (get != null) {
            get.toXContent(builder, params);
        }
        if (search != null) {
            search.toXContent(builder, params);
        }
        if (merge != null) {
            merge.toXContent(builder, params);
        }
        if (refresh != null) {
            refresh.toXContent(builder, params);
        }
        if (flush != null) {
            flush.toXContent(builder, params);
        }
        if (warmer != null) {
            warmer.toXContent(builder, params);
        }
        if (filterCache != null) {
            filterCache.toXContent(builder, params);
        }
        if (idCache != null) {
            idCache.toXContent(builder, params);
        }
        if (fieldData != null) {
            fieldData.toXContent(builder, params);
        }
        if (percolate != null) {
            percolate.toXContent(builder, params);
        }
        if (completion != null) {
            completion.toXContent(builder, params);
        }
        if (segments != null) {
            segments.toXContent(builder, params);
        }
        if (weight != null) {
            weight.toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Weight of the shard with respect to the node it is on and the whole cluster.
     */
    static class Weight implements ToXContent {
        private final WeightSnapshot clusterWeight;
        private final WeightSnapshot nodeWeight;
        
        public Weight(WeightSnapshot clusterWeight, WeightSnapshot nodeWeight) {
            this.clusterWeight = clusterWeight;
            this.nodeWeight = nodeWeight;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.WEIGHT);
            builder.startObject(Fields.CLUSTER_WEIGHT);
            clusterWeight.toXContent(builder, params);
            builder.endObject();
            builder.startObject(Fields.NODE_WEIGHT);
            nodeWeight.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }
        
        static final class Fields {
            static final XContentBuilderString WEIGHT = new XContentBuilderString("weight");
            static final XContentBuilderString CLUSTER_WEIGHT = new XContentBuilderString("cluster");
            static final XContentBuilderString NODE_WEIGHT = new XContentBuilderString("node");
        }
    }

    /**
     * Weights calculated using moving averages.  Weights are scaled so one is an average shard.
     */
    static class WeightSnapshot implements ToXContent {
        private final float weight1Minute, weight5Minute, weight15Minute, weight1Hour, weight1Day, weight1Week;
        
        public WeightSnapshot(float weight1Minute, float weight5Minute, float weight15Minute, float weight1Hour, float weight1Day,
                float weight1Week) {
            this.weight1Minute = weight1Minute;
            this.weight5Minute = weight5Minute;
            this.weight15Minute = weight15Minute;
            this.weight1Hour = weight1Hour;
            this.weight1Day = weight1Day;
            this.weight1Week = weight1Week;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.ONE_MINUTE, weight1Minute);
            builder.field(Fields.FIVE_MINUTE, weight5Minute);
            builder.field(Fields.FIFTEEN_MINUTE, weight15Minute);
            builder.field(Fields.ONE_HOUR, weight1Hour);
            builder.field(Fields.ONE_DAY, weight1Day);
            builder.field(Fields.ONE_WEEK, weight1Week);
            
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString ONE_MINUTE = new XContentBuilderString("one_minute_average");
            static final XContentBuilderString FIVE_MINUTE = new XContentBuilderString("five_minute_average");
            static final XContentBuilderString FIFTEEN_MINUTE = new XContentBuilderString("fifteen_minute_average");
            static final XContentBuilderString ONE_HOUR = new XContentBuilderString("one_hour_average");
            static final XContentBuilderString ONE_DAY = new XContentBuilderString("one_day_average");
            static final XContentBuilderString ONE_WEEK = new XContentBuilderString("one_week_average");
        }
    }
}

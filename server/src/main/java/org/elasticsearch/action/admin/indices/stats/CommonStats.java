/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardCountStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class CommonStats implements Writeable, ToXContentFragment {

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
    public QueryCacheStats queryCache;

    @Nullable
    public FieldDataStats fieldData;

    @Nullable
    public CompletionStats completion;

    @Nullable
    public SegmentsStats segments;

    @Nullable
    public TranslogStats translog;

    @Nullable
    public RequestCacheStats requestCache;

    @Nullable
    public RecoveryStats recoveryStats;

    @Nullable
    public BulkStats bulk;

    @Nullable
    public ShardCountStats shards;

    public CommonStats() {
        this(CommonStatsFlags.NONE);
    }

    public CommonStats(CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (CommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs -> docs = new DocsStats();
                case Store -> store = new StoreStats();
                case Indexing -> indexing = new IndexingStats();
                case Get -> get = new GetStats();
                case Search -> search = new SearchStats();
                case Merge -> merge = new MergeStats();
                case Refresh -> refresh = new RefreshStats();
                case Flush -> flush = new FlushStats();
                case Warmer -> warmer = new WarmerStats();
                case QueryCache -> queryCache = new QueryCacheStats();
                case FieldData -> fieldData = new FieldDataStats();
                case Completion -> completion = new CompletionStats();
                case Segments -> segments = new SegmentsStats();
                case Translog -> translog = new TranslogStats();
                case RequestCache -> requestCache = new RequestCacheStats();
                case Recovery -> recoveryStats = new RecoveryStats();
                case Bulk -> bulk = new BulkStats();
                case Shards -> shards = new ShardCountStats();
                default -> throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }

    public CommonStats(IndicesQueryCache indicesQueryCache, IndexShard indexShard, CommonStatsFlags flags) {
        CommonStatsFlags.Flag[] setFlags = flags.getFlags();
        for (CommonStatsFlags.Flag flag : setFlags) {
            try {
                switch (flag) {
                    case Docs -> docs = indexShard.docStats();
                    case Store -> store = indexShard.storeStats();
                    case Indexing -> indexing = indexShard.indexingStats();
                    case Get -> get = indexShard.getStats();
                    case Search -> search = indexShard.searchStats(flags.groups());
                    case Merge -> merge = indexShard.mergeStats();
                    case Refresh -> refresh = indexShard.refreshStats();
                    case Flush -> flush = indexShard.flushStats();
                    case Warmer -> warmer = indexShard.warmerStats();
                    case QueryCache -> queryCache = indicesQueryCache.getStats(indexShard.shardId());
                    case FieldData -> fieldData = indexShard.fieldDataStats(flags.fieldDataFields());
                    case Completion -> completion = indexShard.completionStats(flags.completionDataFields());
                    case Segments -> segments = indexShard.segmentStats(flags.includeSegmentFileSizes(), flags.includeUnloadedSegments());
                    case Translog -> translog = indexShard.translogStats();
                    case RequestCache -> requestCache = indexShard.requestCache().stats();
                    case Recovery -> recoveryStats = indexShard.recoveryStats();
                    case Bulk -> bulk = indexShard.bulkStats();
                    case Shards ->
                        // Setting to 1 because the single IndexShard passed to this method implies 1 shard
                        shards = new ShardCountStats(1);
                    default -> throw new IllegalStateException("Unknown Flag: " + flag);
                }
            } catch (AlreadyClosedException e) {
                // shard is closed - no stats is fine
            }
        }
    }

    public CommonStats(StreamInput in) throws IOException {
        docs = in.readOptionalWriteable(DocsStats::new);
        store = in.readOptionalWriteable(StoreStats::new);
        indexing = in.readOptionalWriteable(IndexingStats::new);
        get = in.readOptionalWriteable(GetStats::new);
        search = in.readOptionalWriteable(SearchStats::new);
        merge = in.readOptionalWriteable(MergeStats::new);
        refresh = in.readOptionalWriteable(RefreshStats::new);
        flush = in.readOptionalWriteable(FlushStats::new);
        warmer = in.readOptionalWriteable(WarmerStats::new);
        queryCache = in.readOptionalWriteable(QueryCacheStats::new);
        fieldData = in.readOptionalWriteable(FieldDataStats::new);
        completion = in.readOptionalWriteable(CompletionStats::new);
        segments = in.readOptionalWriteable(SegmentsStats::new);
        translog = in.readOptionalWriteable(TranslogStats::new);
        requestCache = in.readOptionalWriteable(RequestCacheStats::new);
        recoveryStats = in.readOptionalWriteable(RecoveryStats::new);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            bulk = in.readOptionalWriteable(BulkStats::new);
        }
        shards = in.readOptionalWriteable(ShardCountStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(docs);
        out.writeOptionalWriteable(store);
        out.writeOptionalWriteable(indexing);
        out.writeOptionalWriteable(get);
        out.writeOptionalWriteable(search);
        out.writeOptionalWriteable(merge);
        out.writeOptionalWriteable(refresh);
        out.writeOptionalWriteable(flush);
        out.writeOptionalWriteable(warmer);
        out.writeOptionalWriteable(queryCache);
        out.writeOptionalWriteable(fieldData);
        out.writeOptionalWriteable(completion);
        out.writeOptionalWriteable(segments);
        out.writeOptionalWriteable(translog);
        out.writeOptionalWriteable(requestCache);
        out.writeOptionalWriteable(recoveryStats);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeOptionalWriteable(bulk);
        }
        out.writeOptionalWriteable(shards);
    }

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
        if (queryCache == null) {
            if (stats.getQueryCache() != null) {
                queryCache = new QueryCacheStats();
                queryCache.add(stats.getQueryCache());
            }
        } else {
            queryCache.add(stats.getQueryCache());
        }

        if (fieldData == null) {
            if (stats.getFieldData() != null) {
                fieldData = new FieldDataStats();
                fieldData.add(stats.getFieldData());
            }
        } else {
            fieldData.add(stats.getFieldData());
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
        if (translog == null) {
            if (stats.getTranslog() != null) {
                translog = new TranslogStats();
                translog.add(stats.getTranslog());
            }
        } else {
            translog.add(stats.getTranslog());
        }
        if (requestCache == null) {
            if (stats.getRequestCache() != null) {
                requestCache = new RequestCacheStats();
                requestCache.add(stats.getRequestCache());
            }
        } else {
            requestCache.add(stats.getRequestCache());
        }
        if (recoveryStats == null) {
            if (stats.getRecoveryStats() != null) {
                recoveryStats = new RecoveryStats();
                recoveryStats.add(stats.getRecoveryStats());
            }
        } else {
            recoveryStats.add(stats.getRecoveryStats());
        }
        if (bulk == null) {
            if (stats.getBulk() != null) {
                bulk = new BulkStats();
                bulk.add(stats.getBulk());
            }
        } else {
            bulk.add(stats.getBulk());
        }
        if (stats.shards != null) {
            if (shards == null) {
                shards = stats.shards;
            } else {
                shards = shards.add(stats.shards);
            }
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
    public QueryCacheStats getQueryCache() {
        return this.queryCache;
    }

    @Nullable
    public FieldDataStats getFieldData() {
        return this.fieldData;
    }

    @Nullable
    public CompletionStats getCompletion() {
        return completion;
    }

    @Nullable
    public SegmentsStats getSegments() {
        return segments;
    }

    @Nullable
    public TranslogStats getTranslog() {
        return translog;
    }

    @Nullable
    public RequestCacheStats getRequestCache() {
        return requestCache;
    }

    @Nullable
    public RecoveryStats getRecoveryStats() {
        return recoveryStats;
    }

    @Nullable
    public BulkStats getBulk() {
        return bulk;
    }

    @Nullable
    public ShardCountStats getShards() {
        return shards;
    }

    /**
     * Utility method which computes total memory by adding
     * FieldData, PercolatorCache, Segments (index writer, version map)
     */
    public ByteSizeValue getTotalMemory() {
        long size = 0;
        if (this.getFieldData() != null) {
            size += this.getFieldData().getMemorySizeInBytes();
        }
        if (this.getQueryCache() != null) {
            size += this.getQueryCache().getMemorySizeInBytes();
        }
        if (this.getSegments() != null) {
            size += this.getSegments().getIndexWriterMemoryInBytes() + this.getSegments().getVersionMapMemoryInBytes();
        }

        return new ByteSizeValue(size);
    }

    // note, requires a wrapping object
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        addIfNonNull(builder, params, docs);
        addIfNonNull(builder, params, shards);
        addIfNonNull(builder, params, store);
        addIfNonNull(builder, params, indexing);
        addIfNonNull(builder, params, get);
        addIfNonNull(builder, params, search);
        addIfNonNull(builder, params, merge);
        addIfNonNull(builder, params, refresh);
        addIfNonNull(builder, params, flush);
        addIfNonNull(builder, params, warmer);
        addIfNonNull(builder, params, queryCache);
        addIfNonNull(builder, params, fieldData);
        addIfNonNull(builder, params, completion);
        addIfNonNull(builder, params, segments);
        addIfNonNull(builder, params, translog);
        addIfNonNull(builder, params, requestCache);
        addIfNonNull(builder, params, recoveryStats);
        addIfNonNull(builder, params, bulk);
        return builder;
    }

    private static void addIfNonNull(XContentBuilder builder, Params params, @Nullable ToXContent toXContent) throws IOException {
        if (toXContent != null) {
            toXContent.toXContent(builder, params);
        }
    }
}

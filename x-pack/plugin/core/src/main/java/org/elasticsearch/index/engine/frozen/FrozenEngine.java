/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.engine.frozen;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.ESCacheHelper;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * This is a stand-alone read-only engine that maintains an index reader that is opened lazily on calls to
 * {@link SearcherSupplier#acquireSearcher(String)}. The index reader opened is maintained until there are no reference to it anymore
 * and then releases itself from the engine.
 * This is necessary to for instance release all SegmentReaders after a search phase finishes and reopen them before the next search
 * phase starts.
 * This together with a throttled threadpool (search_throttled) guarantees that at most N frozen shards have a low level index reader
 * open at the same time.
 * The internal reopen of readers is treated like a refresh and refresh listeners are called up-on reopen. This allows to consume refresh
 * stats in order to obtain the number of reopens.
 */
public final class FrozenEngine extends ReadOnlyEngine {
    public static final Setting<Boolean> INDEX_FROZEN = Setting.boolSetting(
        "index.frozen",
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );
    private final SegmentsStats segmentsStats;
    private final DocsStats docsStats;
    private volatile ElasticsearchDirectoryReader lastOpenedReader;
    private final ElasticsearchDirectoryReader canMatchReader;
    private final Object cacheIdentity = new Object();
    private final Set<ESCacheHelper.ClosedListener> closedListeners = new CopyOnWriteArraySet<>();

    public FrozenEngine(EngineConfig config, boolean requireCompleteHistory, boolean lazilyLoadSoftDeletes) {
        this(config, null, null, true, Function.identity(), requireCompleteHistory, lazilyLoadSoftDeletes);
    }

    public FrozenEngine(
        EngineConfig config,
        SeqNoStats seqNoStats,
        TranslogStats translogStats,
        boolean obtainLock,
        Function<DirectoryReader, DirectoryReader> readerWrapperFunction,
        boolean requireCompleteHistory,
        boolean lazilyLoadSoftDeletes
    ) {
        super(config, seqNoStats, translogStats, obtainLock, readerWrapperFunction, requireCompleteHistory, lazilyLoadSoftDeletes);
        boolean success = false;
        Directory directory = store.directory();
        try (DirectoryReader reader = openDirectory(directory)) {
            // we record the segment stats and doc stats here - that's what the reader needs when it's open and it give the user
            // an idea of what it can save when it's closed
            this.segmentsStats = new SegmentsStats();
            for (LeafReaderContext ctx : reader.getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, true, segmentsStats);
            }
            this.docsStats = docsStats(reader);
            canMatchReader = ElasticsearchDirectoryReader.wrap(
                new RewriteCachingDirectoryReader(directory, reader.leaves(), null),
                config.getShardId()
            );
            success = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                closeNoLock("failed on construction", new CountDownLatch(1));
            }
        }
    }

    @Override
    protected DirectoryReader open(IndexCommit indexCommit) throws IOException {
        // we fake an empty DirectoryReader for the ReadOnlyEngine. this reader is only used
        // to initialize the reference manager and to make the refresh call happy which is essentially
        // a no-op now
        return new DirectoryReader(indexCommit.getDirectory(), new LeafReader[0], null) {
            @Override
            protected DirectoryReader doOpenIfChanged() {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() {
                return true; // always current
            }

            @Override
            public IndexCommit getIndexCommit() {
                return indexCommit; // TODO maybe we can return an empty commit?
            }

            @Override
            protected void doClose() {}

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    @SuppressForbidden(reason = "we manage references explicitly here")
    private synchronized void onReaderClosed(IndexReader.CacheKey key) {
        // it might look awkward that we have to check here if the keys match but if we concurrently
        // access the lastOpenedReader there might be 2 threads competing for the cached reference in
        // a way that thread 1 counts down the lastOpenedReader reference and before thread 1 can execute
        // the close listener we already open and assign a new reader to lastOpenedReader. In this case
        // the cache key doesn't match and we just ignore it since we use this method only to null out the
        // lastOpenedReader member to ensure resources can be GCed
        if (lastOpenedReader != null && key == lastOpenedReader.getReaderCacheHelper().getKey()) {
            assert lastOpenedReader.getRefCount() == 0;
            lastOpenedReader = null;
        }
    }

    @SuppressForbidden(reason = "we manage references explicitly here")
    private synchronized void closeReader(IndexReader reader) throws IOException {
        reader.decRef();
    }

    private synchronized ElasticsearchDirectoryReader getOrOpenReader() throws IOException {
        ElasticsearchDirectoryReader reader = null;
        boolean success = false;
        try {
            reader = getReader();
            if (reader == null) {
                for (ReferenceManager.RefreshListener listeners : config().getInternalRefreshListener()) {
                    listeners.beforeRefresh();
                }
                final DirectoryReader dirReader = openDirectory(engineConfig.getStore().directory());
                reader = lastOpenedReader = wrapReader(dirReader, Function.identity(), new ESCacheHelper() {
                    @Override
                    public Object getKey() {
                        return cacheIdentity;
                    }

                    @Override
                    public void addClosedListener(ClosedListener listener) {
                        closedListeners.add(Objects.requireNonNull(listener));
                    }
                });
                reader.getReaderCacheHelper().addClosedListener(this::onReaderClosed);
                for (ReferenceManager.RefreshListener listeners : config().getInternalRefreshListener()) {
                    listeners.afterRefresh(true);
                }
            }
            success = true;
            return reader;
        } finally {
            if (success == false) {
                IOUtils.close(reader);
            }
        }
    }

    @SuppressForbidden(reason = "we manage references explicitly here")
    private ElasticsearchDirectoryReader getReader() {
        final ElasticsearchDirectoryReader readerRef = lastOpenedReader; // volatile read
        if (readerRef != null && readerRef.tryIncRef()) {
            return readerRef;
        }
        return null;
    }

    @Override
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        final Store store = this.store;
        store.incRef();
        return new SearcherSupplier(wrapper) {
            @Override
            @SuppressForbidden(reason = "we manage references explicitly here")
            public Searcher acquireSearcherInternal(String source) {
                try {
                    return openSearcher(source, scope);
                } catch (IOException exc) {
                    throw new UncheckedIOException(exc);
                }
            }

            @Override
            protected void doClose() {
                store.decRef();
            }

            @Override
            public String getSearcherId() {
                return getCommitId();
            }
        };
    }

    @SuppressWarnings("fallthrough")
    @SuppressForbidden(reason = "we manage references explicitly here")
    private Engine.Searcher openSearcher(String source, SearcherScope scope) throws IOException {
        boolean maybeOpenReader;
        switch (source) {
            case "load_seq_no":
            case "load_version":
                assert false : "this is a read-only engine";
            case DOC_STATS_SOURCE:
                assert false : "doc stats are eagerly loaded";
            case "refresh_needed":
                assert false : "refresh_needed is always false";
            case "segments":
            case "segments_stats":
            case "completion_stats":
            case FIELD_RANGE_SEARCH_SOURCE: // special case for field_range - we use the cached point values reader
            case CAN_MATCH_SEARCH_SOURCE: // special case for can_match phase - we use the cached point values reader
                maybeOpenReader = false;
                break;
            default:
                maybeOpenReader = true;
        }
        ElasticsearchDirectoryReader reader = maybeOpenReader ? getOrOpenReader() : getReader();
        if (reader == null) {
            if (CAN_MATCH_SEARCH_SOURCE.equals(source) || FIELD_RANGE_SEARCH_SOURCE.equals(source)) {
                canMatchReader.incRef();
                return new Searcher(
                    source,
                    canMatchReader,
                    engineConfig.getSimilarity(),
                    engineConfig.getQueryCache(),
                    engineConfig.getQueryCachingPolicy(),
                    canMatchReader::decRef
                );
            } else {
                ReferenceManager<ElasticsearchDirectoryReader> manager = getReferenceManager(scope);
                ElasticsearchDirectoryReader acquire = manager.acquire();
                return new Searcher(
                    source,
                    acquire,
                    engineConfig.getSimilarity(),
                    engineConfig.getQueryCache(),
                    engineConfig.getQueryCachingPolicy(),
                    () -> manager.release(acquire)
                );
            }
        } else {
            return new Searcher(
                source,
                reader,
                engineConfig.getSimilarity(),
                engineConfig.getQueryCache(),
                engineConfig.getQueryCachingPolicy(),
                () -> closeReader(reader)
            );
        }
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        if (includeUnloadedSegments) {
            final SegmentsStats stats = new SegmentsStats();
            stats.add(this.segmentsStats);
            if (includeSegmentFileSizes == false) {
                stats.clearFiles();
            }
            return stats;
        } else {
            return super.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
        }
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        super.closeNoLock(reason, closedLatch);
        synchronized (closedListeners) {
            IOUtils.closeWhileHandlingException(closedListeners.stream().map(t -> (Closeable) () -> t.onClose(cacheIdentity))::iterator);
            closedListeners.clear();
        }
    }

    @Override
    public DocsStats docStats() {
        return docsStats;
    }

    synchronized boolean isReaderOpen() {
        return lastOpenedReader != null;
    } // this is mainly for tests
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * This is a stand-alone read-only engine that maintains a lazy loaded index reader that is opened on calls to
 * {@link Engine#acquireSearcher(String)}. The index reader opened is maintained until there are no reference to it anymore and then
 * releases itself from the engine. The readers returned from this engine are lazy which allows release after and reset before a search
 * phase starts. This allows releasing references as soon as possible on the search layer.
 *
 * Internally this class uses a set of wrapper abstractions to allow a reader that is used inside the {@link Engine.Searcher} returned from
 * {@link #acquireSearcher(String, SearcherScope)} to release and reset it's internal resources. This is necessary to for instance release
 * all SegmentReaders after a search phase finishes and reopen them before the next search phase starts. This together with a throttled
 * threadpool (search_throttled) guarantees that at most N frozen shards have a low level index reader open at the same time.
 *
 * In particular we have LazyDirectoryReader that wraps its LeafReaders (the actual segment readers) inside LazyLeafReaders. Each of the
 * LazyLeafReader delegates to segment LeafReader that can be reset (it's reference decremented and nulled out) on a search phase is
 * finished. Before the next search phase starts we can reopen the corresponding reader and reset the reference to execute the search phase.
 * This allows the SearchContext to hold on to the same LazyDirectoryReader across its lifecycle but under the hood resources (memory) is
 * released while the SearchContext phases are not executing.
 *
 * The internal reopen of readers is treated like a refresh and refresh listeners are called up-on reopen. This allows to consume refresh
 * stats in order to obtain the number of reopens.
 */
public final class FrozenEngine extends ReadOnlyEngine {
    public static final Setting<Boolean> INDEX_FROZEN = Setting.boolSetting("index.frozen", false, Setting.Property.IndexScope,
        Setting.Property.PrivateIndex);
    private final SegmentsStats segmentsStats;
    private final DocsStats docsStats;
    private volatile ElasticsearchDirectoryReader lastOpenedReader;
    private final ElasticsearchDirectoryReader canMatchReader;

    public FrozenEngine(EngineConfig config, boolean requireCompleteHistory) {
        super(config, null, null, true, Function.identity(), requireCompleteHistory);

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
            final DirectoryReader wrappedReader = new SoftDeletesDirectoryReaderWrapper(reader, Lucene.SOFT_DELETES_FIELD);
            canMatchReader = ElasticsearchDirectoryReader.wrap(
                new RewriteCachingDirectoryReader(directory, wrappedReader.leaves()), config.getShardId());
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
        return new DirectoryReader(indexCommit.getDirectory(), new LeafReader[0]) {
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
            protected void doClose() {
            }

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

    private synchronized ElasticsearchDirectoryReader getOrOpenReader() throws IOException {
        ElasticsearchDirectoryReader reader = null;
        boolean success = false;
        try {
            reader = getReader();
            if (reader == null) {
                for (ReferenceManager.RefreshListener listeners : config ().getInternalRefreshListener()) {
                    listeners.beforeRefresh();
                }
                final DirectoryReader dirReader = openDirectory(engineConfig.getStore().directory());
                reader = lastOpenedReader = wrapReader(dirReader, Function.identity());
                processReader(reader);
                reader.getReaderCacheHelper().addClosedListener(this::onReaderClosed);
                for (ReferenceManager.RefreshListener listeners : config ().getInternalRefreshListener()) {
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
    private synchronized ElasticsearchDirectoryReader getReader() {
        if (lastOpenedReader != null && lastOpenedReader.tryIncRef()) {
            return lastOpenedReader;
        }
        return null;
    }

    @Override
    @SuppressWarnings("fallthrough")
    @SuppressForbidden( reason = "we manage references explicitly here")
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        store.incRef();
        boolean releaseRefeference = true;
        try  {
            final boolean maybeOpenReader;
            switch (source) {
                case "load_seq_no":
                case "load_version":
                    assert false : "this is a read-only engine";
                case "doc_stats":
                    assert false : "doc_stats are overwritten";
                case "refresh_needed":
                    assert false : "refresh_needed is always false";
                case "segments":
                case "segments_stats":
                case "completion_stats":
                case "can_match": // special case for can_match phase - we use the cached point values reader
                    maybeOpenReader = false;
                    break;
                default:
                    maybeOpenReader = true;
            }
            // special case we only want to report segment stats if we have a reader open. in that case we only get a reader if we still
            // have one open at the time and can inc it's reference.
            ElasticsearchDirectoryReader reader = maybeOpenReader ? getOrOpenReader() : getReader();
            if (reader == null) {
                // we just hand out a searcher on top of an empty reader that we opened for the ReadOnlyEngine in the #open(IndexCommit)
                // method. this is the case when we don't have a reader open right now and we get a stats call any other that falls in
                // the category that doesn't trigger a reopen
                if ("can_match".equals(source)) {
                    canMatchReader.incRef();
                    return new Searcher(source, canMatchReader,
                        engineConfig.getSimilarity(), engineConfig.getQueryCache(), engineConfig.getQueryCachingPolicy(),
                        canMatchReader::decRef);
                }
                return super.acquireSearcher(source, scope);
            } else {
                try {
                    LazyDirectoryReader lazyDirectoryReader = new LazyDirectoryReader(reader, this);
                    Searcher newSearcher = new Searcher(source, lazyDirectoryReader,
                        engineConfig.getSimilarity(), engineConfig.getQueryCache(), engineConfig.getQueryCachingPolicy(),
                        () -> IOUtils.close(lazyDirectoryReader, store::decRef));
                    releaseRefeference = false;
                    return newSearcher;
                } finally {
                    if (releaseRefeference) {
                        reader.decRef(); // don't call close here we manage reference ourselves
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (releaseRefeference) {
                store.decRef();
            }
        }
    }

    static LazyDirectoryReader unwrapLazyReader(DirectoryReader reader) {
        while (reader instanceof FilterDirectoryReader) {
            if (reader instanceof LazyDirectoryReader) {
                return (LazyDirectoryReader) reader;
            }
            reader = ((FilterDirectoryReader) reader).getDelegate();
        }
        return null;
    }

    /*
     * We register this listener for a frozen index that will
     *  1. reset the reader every time the search context is validated which happens when the context is looked up ie. on a fetch phase
     *  etc.
     *  2. register a releasable resource that is cleaned after each phase that releases the reader for this searcher
     */
    public static class ReacquireEngineSearcherListener implements SearchOperationListener {

        @Override
        public void validateSearchContext(SearchContext context, TransportRequest transportRequest) {
            DirectoryReader dirReader = context.searcher().getDirectoryReader();
            LazyDirectoryReader lazyDirectoryReader = unwrapLazyReader(dirReader);
            if (lazyDirectoryReader != null) {
                try {
                    lazyDirectoryReader.reset();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                // also register a release resource in this case if we have multiple roundtrips like in DFS
                registerRelease(context, lazyDirectoryReader);
            }
        }

        private void registerRelease(SearchContext context, LazyDirectoryReader lazyDirectoryReader) {
            context.addReleasable(() -> {
                try {
                    lazyDirectoryReader.release();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, SearchContext.Lifetime.PHASE);
        }

        @Override
        public void onNewContext(SearchContext context) {
            DirectoryReader dirReader = context.searcher().getDirectoryReader();
            LazyDirectoryReader lazyDirectoryReader = unwrapLazyReader(dirReader);
            if (lazyDirectoryReader != null) {
                registerRelease(context, lazyDirectoryReader);
            }
        }
    }

    /**
     * This class allows us to use the same high level reader across multiple search phases but replace the underpinnings
     * on/after each search phase. This is really important otherwise we would hold on to multiple readers across phases.
     *
     * This reader and its leaf reader counterpart overrides FilterDirectory/LeafReader for convenience to be unwrapped but still
     * overrides all it's delegate methods. We have tests to ensure we never miss an override but we need to in order to make sure
     * the wrapper leaf readers don't register themself as close listeners on the wrapped ones otherwise we fail plugging in new readers
     * on the next search phase.
     */
    static final class LazyDirectoryReader extends FilterDirectoryReader {

        private final FrozenEngine engine;
        private volatile DirectoryReader delegate; // volatile since it might be closed concurrently

        private LazyDirectoryReader(DirectoryReader reader, FrozenEngine engine) throws IOException {
            super(reader, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new LazyLeafReader(reader);
                }
            });
            this.delegate = reader;
            this.engine = engine;
        }

        @SuppressForbidden(reason = "we manage references explicitly here")
        synchronized void release() throws IOException {
            if (delegate != null) { // we are lenient here it's ok to double close
                delegate.decRef();
                delegate = null;
                if (tryIncRef()) { // only do this if we are not closed already
                    // we end up in this case when we are not closed but in an intermediate
                    // state were we want to release all or the real leaf readers ie. in between search phases
                    // but still want to keep this Lazy reference open. In oder to let the heavy real leaf
                    // readers to be GCed we need to null our the references.
                    try {
                        for (LeafReaderContext leaf : leaves()) {
                            LazyLeafReader reader = (LazyLeafReader) leaf.reader();
                            reader.in = null;
                        }
                    } finally {
                        decRef();
                    }
                }
            }
        }

        void reset() throws IOException {
            boolean success = false;
            DirectoryReader reader = engine.getOrOpenReader();
            try {
                reset(reader);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(reader);
                }
            }
        }

        private synchronized void reset(DirectoryReader delegate) {
            if (this.delegate != null) {
                throw new AssertionError("lazy reader is not released");
            }
            assert (delegate instanceof LazyDirectoryReader) == false : "must not be a LazyDirectoryReader";
            List<LeafReaderContext> leaves = delegate.leaves();
            int ord = 0;
            for (LeafReaderContext leaf : leaves()) {
                LazyLeafReader reader = (LazyLeafReader) leaf.reader();
                LeafReader newReader = leaves.get(ord++).reader();
                assert reader.in == null;
                reader.in = newReader;
                assert reader.info.info.equals(Lucene.segmentReader(newReader).getSegmentInfo().info);
            }
            this.delegate = delegate;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
            throw new UnsupportedOperationException();
        }

        void ensureOpenOrReset() {
            // ensure we fail early and with good exceptions
            ensureOpen();
            if (delegate == null) {
                throw new AlreadyClosedException("delegate is released");
            }
        }

        @Override
        public long getVersion() {
            ensureOpenOrReset();
            return delegate.getVersion();
        }

        @Override
        public boolean isCurrent() throws IOException {
            ensureOpenOrReset();
            return delegate.isCurrent();
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            ensureOpenOrReset();
            return delegate.getIndexCommit();
        }

        @Override
        protected void doClose() throws IOException {
            release();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            ensureOpenOrReset();
            return delegate.getReaderCacheHelper();
        }

        @Override
        public DirectoryReader getDelegate() {
            ensureOpenOrReset();
            return delegate;
        }
    }

    /**
     * We basically duplicate a FilterLeafReader here since we don't want the
     * incoming reader to register with this reader as a parent reader. This would mean we barf if the incoming
     * reader is closed and that is what we actually doing on purpose.
     */
    static final class LazyLeafReader extends FilterLeafReader {

        private volatile LeafReader in;
        private final SegmentCommitInfo info;
        private final int numDocs;
        private final int maxDocs;

        private LazyLeafReader(LeafReader in) {
            super(Lucene.emptyReader(in.maxDoc())); // empty reader here to make FilterLeafReader happy
            this.info = Lucene.segmentReader(in).getSegmentInfo();
            this.in = in;
            numDocs = in.numDocs();
            maxDocs = in.maxDoc();
            // don't register in reader as a subreader here.
        }

        private void ensureOpenOrReleased() {
            ensureOpen();
            if (in == null) {
                throw new AlreadyClosedException("leaf is already released");
            }
        }

        @Override
        public Bits getLiveDocs() {
            ensureOpenOrReleased();
            return in.getLiveDocs();
        }

        @Override
        public FieldInfos getFieldInfos() {
            ensureOpenOrReleased();
            return in.getFieldInfos();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getPointValues(field);
        }

        @Override
        public Fields getTermVectors(int docID)
            throws IOException {
            ensureOpenOrReleased();
            return in.getTermVectors(docID);
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public int maxDoc() {
            return maxDocs;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            ensureOpenOrReleased();
            in.document(docID, visitor);
        }

        @Override
        protected void doClose() throws IOException {
            in.close();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            ensureOpenOrReleased();
            return in.getReaderCacheHelper();
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            ensureOpenOrReleased();
            return in.getCoreCacheHelper();
        }

        @Override
        public Terms terms(String field) throws IOException {
            ensureOpenOrReleased();
            return in.terms(field);
        }

        @Override
        public String toString() {
            final StringBuilder buffer = new StringBuilder("LazyLeafReader(");
            buffer.append(in);
            buffer.append(')');
            return buffer.toString();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getSortedSetDocValues(field);
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            ensureOpenOrReleased();
            return in.getNormValues(field);
        }

        @Override
        public LeafMetaData getMetaData() {
            ensureOpenOrReleased();
            return in.getMetaData();
        }

        @Override
        public void checkIntegrity() throws IOException {
            ensureOpenOrReleased();
            in.checkIntegrity();
        }

        @Override
        public LeafReader getDelegate() {
            return in;
        }
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        if (includeUnloadedSegments) {
            final SegmentsStats stats = new SegmentsStats();
            stats.add(this.segmentsStats);
            if (includeSegmentFileSizes == false) {
                stats.clearFileSizes();
            }
            return stats;
        } else {
            return super.segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
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

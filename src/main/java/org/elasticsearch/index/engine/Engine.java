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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.*;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public abstract class Engine implements Closeable {

    protected final ShardId shardId;
    protected final ESLogger logger;
    protected final EngineConfig engineConfig;
    protected final Store store;
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    protected final FailedEngineListener failedEngineListener;
    protected final SnapshotDeletionPolicy deletionPolicy;
    protected final ReentrantLock failEngineLock = new ReentrantLock();
    protected final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    protected final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    protected final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());

    protected volatile Throwable failedEngine = null;

    protected Engine(EngineConfig engineConfig) {
        Preconditions.checkNotNull(engineConfig.getStore(), "Store must be provided to the engine");
        Preconditions.checkNotNull(engineConfig.getDeletionPolicy(), "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(engineConfig.getTranslog(), "Translog must be provided to the engine");

        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();
        this.logger = Loggers.getLogger(Engine.class, // we use the engine class directly here to make sure all subclasses have the same logger name
                engineConfig.getIndexSettings(), engineConfig.getShardId());
        this.failedEngineListener = engineConfig.getFailedEngineListener();
        this.deletionPolicy = engineConfig.getDeletionPolicy();
    }

    /** Returns 0 in the case where accountable is null, otherwise returns {@code ramBytesUsed()} */
    protected static long guardedRamBytesUsed(Accountable a) {
        if (a == null) {
            return 0;
        }
        return a.ramBytesUsed();
    }

    /**
     * Tries to extract a segment reader from the given index reader.
     * If no SegmentReader can be extracted an {@link org.elasticsearch.ElasticsearchIllegalStateException} is thrown.
     */
    protected static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        }
        // hard fail - we can't get a SegmentReader
        throw new ElasticsearchIllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    /**
     * Returns whether a leaf reader comes from a merge (versus flush or addIndexes).
     */
    protected static boolean isMergedSegment(LeafReader reader) {
        // We expect leaves to be segment readers
        final Map<String, String> diagnostics = segmentReader(reader).getSegmentInfo().info.getDiagnostics();
        final String source = diagnostics.get(IndexWriter.SOURCE);
        assert Arrays.asList(IndexWriter.SOURCE_ADDINDEXES_READERS, IndexWriter.SOURCE_FLUSH,
                IndexWriter.SOURCE_MERGE).contains(source) : "Unknown source " + source;
        return IndexWriter.SOURCE_MERGE.equals(source);
    }

    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) {
        return new EngineSearcher(source, searcher, manager, store, logger);
    }

    public final EngineConfig config() {
        return engineConfig;
    }

    protected abstract SegmentInfos getLastCommittedSegmentInfos();

    /** A throttling class that can be activated, causing the
     * {@code acquireThrottle} method to block on a lock when throttling
     * is enabled
     */
    protected static final class IndexThrottle {

        private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());
        private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());

        private volatile ReleasableLock lock = NOOP_LOCK;

        public Releasable acquireThrottle() {
            return lock.acquire();
        }

        /** Activate throttling, which switches the lock to be a real lock */
        public void activate() {
            assert lock == NOOP_LOCK : "throttling activated while already active";
            lock = lockReference;
        }

        /** Deactivate throttling, which switches the lock to be an always-acquirable NoOpLock */
        public void deactivate() {
            assert lock != NOOP_LOCK : "throttling deactivated but not active";
            lock = NOOP_LOCK;
        }
    }

    /** A Lock implementation that always allows the lock to be acquired */
    protected static final class NoOpLock implements Lock {

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }

    public abstract void create(Create create) throws EngineException;

    public abstract boolean index(Index index) throws EngineException;

    public abstract void delete(Delete delete) throws EngineException;

    public abstract void delete(DeleteByQuery delete) throws EngineException;

    final protected GetResult getFromSearcher(Get get) throws EngineException {
        final Searcher searcher = acquireSearcher("get");
        final Versions.DocIdAndVersion docIdAndVersion;
        try {
            docIdAndVersion = Versions.loadDocIdAndVersion(searcher.reader(), get.uid());
        } catch (Throwable e) {
            Releasables.closeWhileHandlingException(searcher);
            //TODO: A better exception goes here
            throw new EngineException(shardId, "Couldn't resolve version", e);
        }

        if (docIdAndVersion != null) {
            if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) {
                Releasables.close(searcher);
                Uid uid = Uid.createUid(get.uid().text());
                throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), docIdAndVersion.version, get.version());
            }
        }

        if (docIdAndVersion != null) {
            // don't release the searcher on this path, it is the
            // responsibility of the caller to call GetResult.release
            return new GetResult(searcher, docIdAndVersion);
        } else {
            Releasables.close(searcher);
            return GetResult.NOT_EXISTS;
        }
    }

    public abstract GetResult get(Get get) throws EngineException;

    /**
     * Returns a new searcher instance. The consumer of this
     * API is responsible for releasing the returned seacher in a
     * safe manner, preferably in a try/finally block.
     *
     * @see Searcher#close()
     */
    public final Searcher acquireSearcher(String source) throws EngineException {
        boolean success = false;
         /* Acquire order here is store -> manager since we need
          * to make sure that the store is not closed before
          * the searcher is acquired. */
        store.incRef();
        try {
            final SearcherManager manager = getSearcherManager(); // can never be null
            /* This might throw NPE but that's fine we will run ensureOpen()
            *  in the catch block and throw the right exception */
            final IndexSearcher searcher = manager.acquire();
            try {
                final Searcher retVal = newSearcher(source, searcher, manager);
                success = true;
                return retVal;
            } finally {
                if (!success) {
                    manager.release(searcher);
                }
            }
        } catch (EngineClosedException ex) {
            throw ex;
        } catch (Throwable ex) {
            ensureOpen(); // throw EngineCloseException here if we are already closed
            logger.error("failed to acquire searcher, source {}", ex, source);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            if (!success) {  // release the ref in the case of an error...
                store.decRef();
            }
        }
    }

    protected void ensureOpen() {
        if (isClosed.get()) {
            throw new EngineClosedException(shardId, failedEngine);
        }
    }

    /** get commits stats for the last commit */
    public CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }



    /**
     * Global stats on segments.
     */
    public final SegmentsStats segmentsStats() {
        ensureOpen();
        try (final Searcher searcher = acquireSearcher("segments_stats")) {
            SegmentsStats stats = new SegmentsStats();
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                final SegmentReader segmentReader = segmentReader(reader.reader());
                stats.add(1, segmentReader.ramBytesUsed());
                stats.addTermsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPostingsReader()));
                stats.addStoredFieldsMemoryInBytes(guardedRamBytesUsed(segmentReader.getFieldsReader()));
                stats.addTermVectorsMemoryInBytes(guardedRamBytesUsed(segmentReader.getTermVectorsReader()));
                stats.addNormsMemoryInBytes(guardedRamBytesUsed(segmentReader.getNormsReader()));
                stats.addDocValuesMemoryInBytes(guardedRamBytesUsed(segmentReader.getDocValuesReader()));
            }
            writerSegmentStats(stats);
            return stats;
        }
    }

    protected void writerSegmentStats(SegmentsStats stats) {
        // by default we don't have a writer here... subclasses can override this
        stats.addVersionMapMemoryInBytes(0);
        stats.addIndexWriterMemoryInBytes(0);
        stats.addIndexWriterMaxMemoryInBytes(0);
    }

    protected Segment[] getSegmentInfo(SegmentInfos lastCommittedSegmentInfos, boolean verbose) {
        ensureOpen();
        Map<String, Segment> segments = new HashMap<>();

        // first, go over and compute the search ones...
        Searcher searcher = acquireSearcher("segments");
        try {
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                SegmentCommitInfo info = segmentReader(reader.reader()).getSegmentInfo();
                assert !segments.containsKey(info.info.name);
                Segment segment = new Segment(info.info.name);
                segment.search = true;
                segment.docCount = reader.reader().numDocs();
                segment.delDocCount = reader.reader().numDeletedDocs();
                segment.version = info.info.getVersion();
                segment.compound = info.info.getUseCompoundFile();
                try {
                    segment.sizeInBytes = info.sizeInBytes();
                } catch (IOException e) {
                    logger.trace("failed to get size for [{}]", e, info.info.name);
                }
                final SegmentReader segmentReader = segmentReader(reader.reader());
                segment.memoryInBytes = segmentReader.ramBytesUsed();
                if (verbose) {
                    segment.ramTree = Accountables.namedAccountable("root", segmentReader);
                }
                // TODO: add more fine grained mem stats values to per segment info here
                segments.put(info.info.name, segment);
            }
        } finally {
            searcher.close();
        }

        // now, correlate or add the committed ones...
        if (lastCommittedSegmentInfos != null) {
            SegmentInfos infos = lastCommittedSegmentInfos;
            for (SegmentCommitInfo info : infos) {
                Segment segment = segments.get(info.info.name);
                if (segment == null) {
                    segment = new Segment(info.info.name);
                    segment.search = false;
                    segment.committed = true;
                    segment.docCount = info.info.maxDoc();
                    segment.delDocCount = info.getDelCount();
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace("failed to get size for [{}]", e, info.info.name);
                    }
                    segments.put(info.info.name, segment);
                } else {
                    segment.committed = true;
                }
            }
        }

        Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
        Arrays.sort(segmentsArr, new Comparator<Segment>() {
            @Override
            public int compare(Segment o1, Segment o2) {
                return (int) (o1.getGeneration() - o2.getGeneration());
            }
        });

        return segmentsArr;
    }

    /**
     * The list of segments in the engine.
     */
    public abstract List<Segment> segments(boolean verbose);

    public final boolean refreshNeeded() {
        if (store.tryIncRef()) {
            /*
              we need to inc the store here since searcherManager.isSearcherCurrent()
              acquires a searcher internally and that might keep a file open on the
              store. this violates the assumption that all files are closed when
              the store is closed so we need to make sure we increment it here
             */
            try {
                return !getSearcherManager().isSearcherCurrent();
            } catch (IOException e) {
                logger.error("failed to access searcher manager", e);
                failEngine("failed to access searcher manager", e);
                throw new EngineException(shardId, "failed to access searcher manager", e);
            } finally {
                store.decRef();
            }
        }
        return false;
    }

    /**
     * Refreshes the engine for new search operations to reflect the latest
     * changes.
     */
    public abstract void refresh(String source) throws EngineException;

    /**
     * Flushes the state of the engine including the transaction log, clearing memory.
     * @param force if <code>true</code> a lucene commit is executed even if no changes need to be committed.
     * @param waitIfOngoing if <code>true</code> this call will block until all currently running flushes have finished.
     *                      Otherwise this call will return without blocking.
     */
    public abstract void flush(boolean force, boolean waitIfOngoing) throws EngineException;

    /**
     * Flushes the state of the engine including the transaction log, clearing memory and persisting
     * documents in the lucene index to disk including a potentially heavy and durable fsync operation.
     * This operation is not going to block if another flush operation is currently running and won't write
     * a lucene commit if nothing needs to be committed.
     */
    public abstract void flush() throws EngineException;

    /**
     * Optimizes to 1 segment
     */
    public void forceMerge(boolean flush) {
        forceMerge(flush, 1, false, false, false);
    }

    /**
     * Triggers a forced merge on this engine
     */
    public abstract void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException;

    /**
     * Snapshots the index and returns a handle to it. Will always try and "commit" the
     * lucene index to make sure we have a "fresh" copy of the files to snapshot.
     */
    public abstract SnapshotIndexCommit snapshotIndex() throws EngineException;

    public abstract void recover(RecoveryHandler recoveryHandler) throws EngineException;

    /** fail engine due to some error. the engine will also be closed. */
    public void failEngine(String reason, Throwable failure) {
        assert failure != null;
        if (failEngineLock.tryLock()) {
            store.incRef();
            try {
                try {
                    // we just go and close this engine - no way to recover
                    closeNoLock("engine failed on: [" + reason + "]");
                    // we first mark the store as corrupted before we notify any listeners
                    // this must happen first otherwise we might try to reallocate so quickly
                    // on the same node that we don't see the corrupted marker file when
                    // the shard is initializing
                    if (Lucene.isCorruptionException(failure)) {
                        try {
                            store.markStoreCorrupted(ExceptionsHelper.unwrapCorruption(failure));
                        } catch (IOException e) {
                            logger.warn("Couldn't marks store corrupted", e);
                        }
                    }
                } finally {
                    if (failedEngine != null) {
                        logger.debug("tried to fail engine but engine is already failed. ignoring. [{}]", reason, failure);
                        return;
                    }
                    logger.warn("failed engine [{}]", failure, reason);
                    // we must set a failure exception, generate one if not supplied
                    failedEngine = failure;
                    failedEngineListener.onFailedEngine(shardId, reason, failure);
                }
            } catch (Throwable t) {
                // don't bubble up these exceptions up
                logger.warn("failEngine threw exception", t);
            } finally {
                store.decRef();
            }
        } else {
            logger.debug("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason, failure);
        }
    }

    /** Check whether the engine should be failed */
    protected boolean maybeFailEngine(String source, Throwable t) {
        if (Lucene.isCorruptionException(t)) {
            failEngine("corrupt file detected source: [" + source + "]", t);
            return true;
        } else if (ExceptionsHelper.isOOM(t)) {
            failEngine("out of memory", t);
            return true;
        }
        return false;
    }

    /** Wrap a Throwable in an {@code EngineClosedException} if the engine is already closed */
    protected Throwable wrapIfClosed(Throwable t) {
        if (isClosed.get()) {
            if (t != failedEngine && failedEngine != null) {
                t.addSuppressed(failedEngine);
            }
            return new EngineClosedException(shardId, t);
        }
        return t;
    }

    public static interface FailedEngineListener {
        void onFailedEngine(ShardId shardId, String reason, @Nullable Throwable t);
    }

    /**
     * Recovery allow to start the recovery process. It is built of three phases.
     * <p/>
     * <p>The first phase allows to take a snapshot of the master index. Once this
     * is taken, no commit operations are effectively allowed on the index until the recovery
     * phases are through.
     * <p/>
     * <p>The seconds phase takes a snapshot of the current transaction log.
     * <p/>
     * <p>The last phase returns the remaining transaction log. During this phase, no dirty
     * operations are allowed on the index.
     */
    public static interface RecoveryHandler {

        void phase1(SnapshotIndexCommit snapshot) throws ElasticsearchException;

        void phase2(Translog.Snapshot snapshot) throws ElasticsearchException;

        void phase3(Translog.Snapshot snapshot) throws ElasticsearchException;
    }

    public static class Searcher implements Releasable {

        private final String source;
        private final IndexSearcher searcher;

        public Searcher(String source, IndexSearcher searcher) {
            this.source = source;
            this.searcher = searcher;
        }

        /**
         * The source that caused this searcher to be acquired.
         */
        public String source() {
            return source;
        }

        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public void close() throws ElasticsearchException {
            // Nothing to close here
        }
    }

    public static interface Operation {
        static enum Type {
            CREATE,
            INDEX,
            DELETE
        }

        static enum Origin {
            PRIMARY,
            REPLICA,
            RECOVERY
        }

        Type opType();

        Origin origin();
    }

    public static abstract class IndexingOperation implements Operation {

        private final DocumentMapper docMapper;
        private final Term uid;
        private final ParsedDocument doc;
        private long version;
        private final VersionType versionType;
        private final Origin origin;
        private final boolean canHaveDuplicates;

        private final long startTime;
        private long endTime;

        public IndexingOperation(DocumentMapper docMapper, Term uid, ParsedDocument doc, long version, VersionType versionType, Origin origin, long startTime, boolean canHaveDuplicates) {
            this.docMapper = docMapper;
            this.uid = uid;
            this.doc = doc;
            this.version = version;
            this.versionType = versionType;
            this.origin = origin;
            this.startTime = startTime;
            this.canHaveDuplicates = canHaveDuplicates;
        }

        public IndexingOperation(DocumentMapper docMapper, Term uid, ParsedDocument doc) {
            this(docMapper, uid, doc, Versions.MATCH_ANY, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime(), true);
        }

        public DocumentMapper docMapper() {
            return this.docMapper;
        }

        @Override
        public Origin origin() {
            return this.origin;
        }

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        public Term uid() {
            return this.uid;
        }

        public String type() {
            return this.doc.type();
        }

        public String id() {
            return this.doc.id();
        }

        public String routing() {
            return this.doc.routing();
        }

        public long timestamp() {
            return this.doc.timestamp();
        }

        public long ttl() {
            return this.doc.ttl();
        }

        public long version() {
            return this.version;
        }

        public void updateVersion(long version) {
            this.version = version;
            this.doc.version().setLongValue(version);
        }

        public VersionType versionType() {
            return this.versionType;
        }

        public boolean canHaveDuplicates() {
            return this.canHaveDuplicates;
        }

        public String parent() {
            return this.doc.parent();
        }

        public List<Document> docs() {
            return this.doc.docs();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public void endTime(long endTime) {
            this.endTime = endTime;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }

        /**
         * Execute this operation against the provided {@link IndexShard} and
         * return whether the document was created.
         */
        public abstract boolean execute(IndexShard shard);
    }

    public static final class Create extends IndexingOperation {
        private final boolean autoGeneratedId;

        public Create(DocumentMapper docMapper, Term uid, ParsedDocument doc, long version, VersionType versionType, Origin origin, long startTime, boolean canHaveDuplicates, boolean autoGeneratedId) {
            super(docMapper, uid, doc, version, versionType, origin, startTime, canHaveDuplicates);
            this.autoGeneratedId = autoGeneratedId;
        }

        public Create(DocumentMapper docMapper, Term uid, ParsedDocument doc, long version, VersionType versionType, Origin origin, long startTime) {
            this(docMapper, uid, doc, version, versionType, origin, startTime, true, false);
        }

        public Create(DocumentMapper docMapper, Term uid, ParsedDocument doc) {
            super(docMapper, uid, doc);
            autoGeneratedId = false;
        }

        @Override
        public Type opType() {
            return Type.CREATE;
        }


        public boolean autoGeneratedId() {
            return this.autoGeneratedId;
        }

        @Override
        public boolean execute(IndexShard shard) {
            shard.create(this);
            return true;
        }
    }

    public static final class Index extends IndexingOperation {

        public Index(DocumentMapper docMapper, Term uid, ParsedDocument doc, long version, VersionType versionType, Origin origin, long startTime, boolean canHaveDuplicates) {
            super(docMapper, uid, doc, version, versionType, origin, startTime, canHaveDuplicates);
        }

        public Index(DocumentMapper docMapper, Term uid, ParsedDocument doc, long version, VersionType versionType, Origin origin, long startTime) {
            super(docMapper, uid, doc, version, versionType, origin, startTime, true);
        }

        public Index(DocumentMapper docMapper, Term uid, ParsedDocument doc) {
            super(docMapper, uid, doc);
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        @Override
        public boolean execute(IndexShard shard) {
            return shard.index(this);
        }
    }

    public static class Delete implements Operation {
        private final String type;
        private final String id;
        private final Term uid;
        private long version;
        private final VersionType versionType;
        private final Origin origin;
        private boolean found;

        private final long startTime;
        private long endTime;

        public Delete(String type, String id, Term uid, long version, VersionType versionType, Origin origin, long startTime, boolean found) {
            this.type = type;
            this.id = id;
            this.uid = uid;
            this.version = version;
            this.versionType = versionType;
            this.origin = origin;
            this.startTime = startTime;
            this.found = found;
        }

        public Delete(String type, String id, Term uid) {
            this(type, id, uid, Versions.MATCH_ANY, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime(), false);
        }

        public Delete(Delete template, VersionType versionType) {
            this(template.type(), template.id(), template.uid(), template.version(), versionType, template.origin(), template.startTime(), template.found());
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public Origin origin() {
            return this.origin;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public Term uid() {
            return this.uid;
        }

        public void updateVersion(long version, boolean found) {
            this.version = version;
            this.found = found;
        }

        /**
         * before delete execution this is the version to be deleted. After this is the version of the "delete" transaction record.
         */
        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        public boolean found() {
            return this.found;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public void endTime(long endTime) {
            this.endTime = endTime;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }
    }

    public static class DeleteByQuery {
        private final Query query;
        private final BytesReference source;
        private final String[] filteringAliases;
        private final Filter aliasFilter;
        private final String[] types;
        private final BitDocIdSetFilter parentFilter;
        private final Operation.Origin origin;

        private final long startTime;
        private long endTime;

        public DeleteByQuery(Query query, BytesReference source, @Nullable String[] filteringAliases, @Nullable Filter aliasFilter, BitDocIdSetFilter parentFilter, Operation.Origin origin, long startTime, String... types) {
            this.query = query;
            this.source = source;
            this.types = types;
            this.filteringAliases = filteringAliases;
            this.aliasFilter = aliasFilter;
            this.parentFilter = parentFilter;
            this.startTime = startTime;
            this.origin = origin;
        }

        public Query query() {
            return this.query;
        }

        public BytesReference source() {
            return this.source;
        }

        public String[] types() {
            return this.types;
        }

        public String[] filteringAliases() {
            return filteringAliases;
        }

        public Filter aliasFilter() {
            return aliasFilter;
        }

        public boolean nested() {
            return parentFilter != null;
        }

        public BitDocIdSetFilter parentFilter() {
            return parentFilter;
        }

        public Operation.Origin origin() {
            return this.origin;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        public DeleteByQuery endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Returns operation end time in nanoseconds.
         */
        public long endTime() {
            return this.endTime;
        }
    }


    public static class Get {
        private final boolean realtime;
        private final Term uid;
        private boolean loadSource = true;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Get(boolean realtime, Term uid) {
            this.realtime = realtime;
            this.uid = uid;
        }

        public boolean realtime() {
            return this.realtime;
        }

        public Term uid() {
            return uid;
        }

        public boolean loadSource() {
            return this.loadSource;
        }

        public Get loadSource(boolean loadSource) {
            this.loadSource = loadSource;
            return this;
        }

        public long version() {
            return version;
        }

        public Get version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Get versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }
    }

    public static class GetResult {
        private final boolean exists;
        private final long version;
        private final Translog.Source source;
        private final Versions.DocIdAndVersion docIdAndVersion;
        private final Searcher searcher;

        public static final GetResult NOT_EXISTS = new GetResult(false, Versions.NOT_FOUND, null);

        public GetResult(boolean exists, long version, @Nullable Translog.Source source) {
            this.source = source;
            this.exists = exists;
            this.version = version;
            this.docIdAndVersion = null;
            this.searcher = null;
        }

        public GetResult(Searcher searcher, Versions.DocIdAndVersion docIdAndVersion) {
            this.exists = true;
            this.source = null;
            this.version = docIdAndVersion.version;
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        public boolean exists() {
            return exists;
        }

        public long version() {
            return this.version;
        }

        @Nullable
        public Translog.Source source() {
            return source;
        }

        public Searcher searcher() {
            return this.searcher;
        }

        public Versions.DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        public void release() {
            if (searcher != null) {
                searcher.close();
            }
        }
    }

    protected abstract SearcherManager getSearcherManager();

    protected abstract void closeNoLock(String reason) throws ElasticsearchException;

    public void flushAndClose() throws IOException {
        if (isClosed.get() == false) {
            logger.trace("flushAndClose now acquire writeLock");
            try (ReleasableLock _ = writeLock.acquire()) {
                logger.trace("flushAndClose now acquired writeLock");
                try {
                    logger.debug("flushing shard on close - this might take some time to sync files to disk");
                    try {
                        flush(); // TODO we might force a flush in the future since we have the write lock already even though recoveries are running.
                    } catch (FlushNotAllowedEngineException ex) {
                        logger.debug("flush not allowed during flushAndClose - skipping");
                    } catch (EngineClosedException ex) {
                        logger.debug("engine already closed - skipping flushAndClose");
                    }
                } finally {
                    close(); // double close is not a problem
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) { // don't acquire the write lock if we are already closed
            logger.debug("close now acquiring writeLock");
            try (ReleasableLock _ = writeLock.acquire()) {
                logger.debug("close acquired writeLock");
                closeNoLock("api");
            }
        }
    }

    /**
     * Returns <code>true</code> the internal writer has any uncommitted changes. Otherwise <code>false</code>
     * @return
     */
    public abstract boolean hasUncommittedChanges();
}

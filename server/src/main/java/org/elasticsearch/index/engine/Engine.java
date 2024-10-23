/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.UnsafePlainActionFuture;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public abstract class Engine implements Closeable {

    @UpdateForV9(owner = UpdateForV9.Owner.DISTRIBUTED_INDEXING) // TODO: Remove sync_id in 9.0
    public static final String SYNC_COMMIT_ID = "sync_id";
    public static final String HISTORY_UUID_KEY = "history_uuid";
    public static final String FORCE_MERGE_UUID_KEY = "force_merge_uuid";
    public static final String MIN_RETAINED_SEQNO = "min_retained_seq_no";
    public static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID = "max_unsafe_auto_id_timestamp";
    // Field name that stores the Elasticsearch version in Lucene commit user data, representing
    // the version that was used to write the commit (and thus a max version for the underlying segments).
    public static final String ES_VERSION = "es_version";
    public static final String SEARCH_SOURCE = "search"; // TODO: Make source of search enum?
    public static final String CAN_MATCH_SEARCH_SOURCE = "can_match";
    protected static final String DOC_STATS_SOURCE = "doc_stats";
    public static final long UNKNOWN_PRIMARY_TERM = -1L;
    public static final String ROOT_DOC_FIELD_NAME = "__root_doc_for_nested";

    protected final ShardId shardId;
    protected final Logger logger;
    protected final EngineConfig engineConfig;
    protected final Store store;
    protected final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    protected final EventListener eventListener;
    protected final ReentrantLock failEngineLock = new ReentrantLock();
    protected final SetOnce<Exception> failedEngine = new SetOnce<>();
    protected final boolean enableRecoverySource;

    private final AtomicBoolean isClosing = new AtomicBoolean();
    private final SubscribableListener<Void> drainOnCloseListener = new SubscribableListener<>();
    private final RefCounted ensureOpenRefs = AbstractRefCounted.of(() -> drainOnCloseListener.onResponse(null));
    private final Releasable releaseEnsureOpenRef = ensureOpenRefs::decRef; // reuse this to avoid allocation for each op

    /*
     * on {@code lastWriteNanos} we use System.nanoTime() to initialize this since:
     *  - we use the value for figuring out if the shard / engine is active so if we startup and no write has happened yet we still
     *    consider it active for the duration of the configured active to inactive period. If we initialize to 0 or Long.MAX_VALUE we
     *    either immediately or never mark it inactive if no writes at all happen to the shard.
     *  - we also use this to flush big-ass merges on an inactive engine / shard but if we we initialize 0 or Long.MAX_VALUE we either
     *    immediately or never commit merges even though we shouldn't from a user perspective (this can also have funky side effects in
     *    tests when we open indices with lots of segments and suddenly merges kick in.
     *  NOTE: don't use this value for anything accurate it's a best effort for freeing up diskspace after merges and on a shard level to
     *  reduce index buffer sizes on inactive shards.
     */
    protected volatile long lastWriteNanos = System.nanoTime();

    protected Engine(EngineConfig engineConfig) {
        Objects.requireNonNull(engineConfig.getStore(), "Store must be provided to the engine");

        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();
        // we use the engine class directly here to make sure all subclasses have the same logger name
        this.logger = Loggers.getLogger(Engine.class, engineConfig.getShardId());
        this.eventListener = engineConfig.getEventListener();
        this.enableRecoverySource = RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.get(
            engineConfig.getIndexSettings().getSettings()
        );
    }

    /**
     * Reads an {@code IndexVersion} from an {@code es_version} metadata string
     */
    public static IndexVersion readIndexVersion(String esVersion) {
        if (esVersion.contains(".")) {
            // backwards-compatible Version-style
            org.elasticsearch.Version v = org.elasticsearch.Version.fromString(esVersion);
            assert v.onOrBefore(org.elasticsearch.Version.V_8_11_0);
            return IndexVersion.fromId(v.id);
        } else {
            return IndexVersion.fromId(Integer.parseInt(esVersion));
        }
    }

    public final EngineConfig config() {
        return engineConfig;
    }

    public abstract SegmentInfos getLastCommittedSegmentInfos();

    public MergeStats getMergeStats() {
        return new MergeStats();
    }

    /** returns the history uuid for the engine */
    public abstract String getHistoryUUID();

    /** Returns how many bytes we are currently moving from heap to disk */
    public abstract long getWritingBytes();

    /**
     * Returns the {@link CompletionStats} for this engine
     */
    public abstract CompletionStats completionStats(String... fieldNamePatterns);

    /**
     * Returns the {@link DocsStats} for this engine
     */
    public DocsStats docStats() {
        // we calculate the doc stats based on the internal searcher that is more up-to-date and not subject
        // to external refreshes. For instance we don't refresh an external searcher if we flush and indices with
        // index.refresh_interval=-1 won't see any doc stats updates at all. This change will give more accurate statistics
        // when indexing but not refreshing in general. Yet, if a refresh happens the internal searcher is refresh as well so we are
        // safe here.
        try (Searcher searcher = acquireSearcher(DOC_STATS_SOURCE, SearcherScope.INTERNAL)) {
            return docsStats(searcher.getIndexReader());
        }
    }

    protected final DocsStats docsStats(IndexReader indexReader) {
        long numDocs = 0;
        long numDeletedDocs = 0;
        long sizeInBytes = 0;
        // we don't wait for a pending refreshes here since it's a stats call instead we mark it as accessed only which will cause
        // the next scheduled refresh to go through and refresh the stats as well
        for (LeafReaderContext readerContext : indexReader.leaves()) {
            // we go on the segment level here to get accurate numbers
            final SegmentReader segmentReader = Lucene.segmentReader(readerContext.reader());
            SegmentCommitInfo info = segmentReader.getSegmentInfo();
            numDocs += readerContext.reader().numDocs();
            numDeletedDocs += readerContext.reader().numDeletedDocs();
            try {
                sizeInBytes += info.sizeInBytes();
            } catch (IOException e) {
                logger.trace(() -> "failed to get size for [" + info.info.name + "]", e);
            }
        }
        return new DocsStats(numDocs, numDeletedDocs, sizeInBytes);
    }

    /**
     * Returns the {@link DenseVectorStats} for this engine
     */
    public DenseVectorStats denseVectorStats(MappingLookup mappingLookup) {
        if (mappingLookup == null) {
            return new DenseVectorStats(0);
        }

        List<String> fields = new ArrayList<>();
        for (Mapper mapper : mappingLookup.fieldMappers()) {
            if (mapper instanceof DenseVectorFieldMapper) {
                fields.add(mapper.fullPath());
            }
        }
        if (fields.isEmpty()) {
            return new DenseVectorStats(0);
        }
        try (Searcher searcher = acquireSearcher(DOC_STATS_SOURCE, SearcherScope.INTERNAL)) {
            return denseVectorStats(searcher.getIndexReader(), fields);
        }
    }

    protected final DenseVectorStats denseVectorStats(IndexReader indexReader, List<String> fields) {
        long valueCount = 0;
        // we don't wait for a pending refreshes here since it's a stats call instead we mark it as accessed only which will cause
        // the next scheduled refresh to go through and refresh the stats as well
        for (LeafReaderContext readerContext : indexReader.leaves()) {
            try {
                valueCount += getDenseVectorValueCount(readerContext.reader(), fields);
            } catch (IOException e) {
                logger.trace(() -> "failed to get dense vector stats for [" + readerContext + "]", e);
            }
        }
        return new DenseVectorStats(valueCount);
    }

    private long getDenseVectorValueCount(final LeafReader atomicReader, List<String> fields) throws IOException {
        long count = 0;
        for (var field : fields) {
            var info = atomicReader.getFieldInfos().fieldInfo(field);
            if (info != null && info.getVectorDimension() > 0) {
                switch (info.getVectorEncoding()) {
                    case FLOAT32 -> {
                        FloatVectorValues values = atomicReader.getFloatVectorValues(info.name);
                        count += values != null ? values.size() : 0;
                    }
                    case BYTE -> {
                        ByteVectorValues values = atomicReader.getByteVectorValues(info.name);
                        count += values != null ? values.size() : 0;
                    }
                }
            }
        }
        return count;
    }

    /**
     * Returns the {@link SparseVectorStats} for this engine
     */
    public SparseVectorStats sparseVectorStats(MappingLookup mappingLookup) {
        if (mappingLookup == null) {
            return new SparseVectorStats(0);
        }
        List<BytesRef> fields = new ArrayList<>();
        for (Mapper mapper : mappingLookup.fieldMappers()) {
            if (mapper instanceof SparseVectorFieldMapper) {
                fields.add(new BytesRef(mapper.fullPath()));
            }
        }
        if (fields.isEmpty()) {
            return new SparseVectorStats(0);
        }
        Collections.sort(fields);
        try (Searcher searcher = acquireSearcher(DOC_STATS_SOURCE, SearcherScope.INTERNAL)) {
            return sparseVectorStats(searcher.getIndexReader(), fields);
        }
    }

    protected final SparseVectorStats sparseVectorStats(IndexReader indexReader, List<BytesRef> fields) {
        long valueCount = 0;
        // we don't wait for a pending refreshes here since it's a stats call instead we mark it as accessed only which will cause
        // the next scheduled refresh to go through and refresh the stats as well
        for (LeafReaderContext readerContext : indexReader.leaves()) {
            try {
                valueCount += getSparseVectorValueCount(readerContext.reader(), fields);
            } catch (IOException e) {
                logger.trace(() -> "failed to get sparse vector stats for [" + readerContext + "]", e);
            }
        }
        return new SparseVectorStats(valueCount);
    }

    private long getSparseVectorValueCount(final LeafReader atomicReader, List<BytesRef> fields) throws IOException {
        long count = 0;
        Terms terms = atomicReader.terms(FieldNamesFieldMapper.NAME);
        if (terms == null) {
            return count;
        }
        TermsEnum termsEnum = terms.iterator();
        for (var fieldName : fields) {
            if (termsEnum.seekExact(fieldName)) {
                count += termsEnum.docFreq();
            }
        }
        return count;
    }

    /**
     * Performs the pre-closing checks on the {@link Engine}.
     *
     * @throws IllegalStateException if the sanity checks failed
     */
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        final long globalCheckpoint = engineConfig.getGlobalCheckpointSupplier().getAsLong();
        final long maxSeqNo = getSeqNoStats(globalCheckpoint).getMaxSeqNo();
        if (globalCheckpoint != maxSeqNo) {
            throw new IllegalStateException(
                "Global checkpoint ["
                    + globalCheckpoint
                    + "] mismatches maximum sequence number ["
                    + maxSeqNo
                    + "] on index shard "
                    + shardId
            );
        }
    }

    public interface IndexCommitListener {

        /**
         * This method is invoked each time a new Lucene commit is created through this engine. Note that commits are notified in order. The
         * {@link IndexCommitRef} prevents the {@link IndexCommitRef} files to be deleted from disk until the reference is closed. As such,
         * the listener must close the reference as soon as it is done with it.
         *
         * @param shardId         the {@link ShardId} of shard
         * @param store           the index shard store
         * @param primaryTerm     the shard's primary term value
         * @param indexCommitRef  a reference on the newly created index commit
         * @param additionalFiles the set of filenames that are added by the new commit
         */
        void onNewCommit(ShardId shardId, Store store, long primaryTerm, IndexCommitRef indexCommitRef, Set<String> additionalFiles);

        /**
         * This method is invoked after the policy deleted the given {@link IndexCommit}. A listener is never notified of a deleted commit
         * until the corresponding {@link Engine.IndexCommitRef} received through {@link #onNewCommit} has been closed; closing which in
         * turn can call this method directly.
         *
         * @param shardId the {@link ShardId} of shard
         * @param deletedCommit the deleted {@link IndexCommit}
         */
        void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit);
    }

    /**
     * A throttling class that can be activated, causing the
     * {@code acquireThrottle} method to block on a lock when throttling
     * is enabled
     */
    protected static final class IndexThrottle {
        private final CounterMetric throttleTimeMillisMetric = new CounterMetric();
        private volatile long startOfThrottleNS;
        private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());
        private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());
        private volatile ReleasableLock lock = NOOP_LOCK;

        public Releasable acquireThrottle() {
            return lock.acquire();
        }

        /** Activate throttling, which switches the lock to be a real lock */
        public void activate() {
            assert lock == NOOP_LOCK : "throttling activated while already active";
            startOfThrottleNS = System.nanoTime();
            lock = lockReference;
        }

        /** Deactivate throttling, which switches the lock to be an always-acquirable NoOpLock */
        public void deactivate() {
            assert lock != NOOP_LOCK : "throttling deactivated but not active";
            lock = NOOP_LOCK;

            assert startOfThrottleNS > 0 : "Bad state of startOfThrottleNS";
            long throttleTimeNS = System.nanoTime() - startOfThrottleNS;
            if (throttleTimeNS >= 0) {
                // Paranoia (System.nanoTime() is supposed to be monotonic): time slip may have occurred but never want
                // to add a negative number
                throttleTimeMillisMetric.inc(TimeValue.nsecToMSec(throttleTimeNS));
            }
        }

        long getThrottleTimeInMillis() {
            long currentThrottleNS = 0;
            if (isThrottled() && startOfThrottleNS != 0) {
                currentThrottleNS += System.nanoTime() - startOfThrottleNS;
                if (currentThrottleNS < 0) {
                    // Paranoia (System.nanoTime() is supposed to be monotonic): time slip must have happened, have to ignore this value
                    currentThrottleNS = 0;
                }
            }
            return throttleTimeMillisMetric.count() + TimeValue.nsecToMSec(currentThrottleNS);
        }

        boolean isThrottled() {
            return lock != NOOP_LOCK;
        }

        boolean throttleLockIsHeldByCurrentThread() { // to be used in assertions and tests only
            if (isThrottled()) {
                return lock.isHeldByCurrentThread();
            }
            return false;
        }
    }

    /**
     * Returns the number of milliseconds this engine was under index throttling.
     */
    public abstract long getIndexThrottleTimeInMillis();

    /**
     * Returns the <code>true</code> iff this engine is currently under index throttling.
     *
     * @see #getIndexThrottleTimeInMillis()
     */
    public abstract boolean isThrottled();

    /**
     * Trims translog for terms below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>
     *
     * @see Translog#trimOperations(long, long)
     */
    public abstract void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException;

    /**
     * Returns the total time flushes have been executed excluding waiting on locks.
     */
    public long getTotalFlushTimeExcludingWaitingOnLockInMillis() {
        return 0;
    }

    /** A Lock implementation that always allows the lock to be acquired */
    protected static final class NoOpLock implements Lock {

        @Override
        public void lock() {}

        @Override
        public void lockInterruptibly() throws InterruptedException {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }

    /**
     * Perform document index operation on the engine
     * @param index operation to perform
     * @return {@link IndexResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    public abstract IndexResult index(Index index) throws IOException;

    /**
     * Perform document delete operation on the engine
     * @param delete operation to perform
     * @return {@link DeleteResult} containing updated translog location, version and
     * document specific failures
     *
     * Note: engine level failures (i.e. persistent engine failures) are thrown
     */
    public abstract DeleteResult delete(Delete delete) throws IOException;

    public abstract NoOpResult noOp(NoOp noOp) throws IOException;

    /**
     * Base class for index and delete operation results
     * Holds result meta data (e.g. translog location, updated version)
     * for an executed write {@link Operation}
     **/
    public abstract static class Result {
        private final Operation.TYPE operationType;
        private final Result.Type resultType;
        private final long version;
        private final long term;
        private final long seqNo;
        private final Exception failure;
        private final SetOnce<Boolean> freeze = new SetOnce<>();
        private final Mapping requiredMappingUpdate;
        private final String id;
        private Translog.Location translogLocation;
        private long took;

        protected Result(Operation.TYPE operationType, Exception failure, long version, long term, long seqNo, String id) {
            this.operationType = operationType;
            this.failure = Objects.requireNonNull(failure);
            this.version = version;
            this.term = term;
            this.seqNo = seqNo;
            this.requiredMappingUpdate = null;
            this.resultType = Type.FAILURE;
            this.id = id;
        }

        protected Result(Operation.TYPE operationType, long version, long term, long seqNo, String id) {
            this.operationType = operationType;
            this.version = version;
            this.seqNo = seqNo;
            this.term = term;
            this.failure = null;
            this.requiredMappingUpdate = null;
            this.resultType = Type.SUCCESS;
            this.id = id;
        }

        protected Result(Operation.TYPE operationType, Mapping requiredMappingUpdate, String id) {
            this.operationType = operationType;
            this.version = Versions.NOT_FOUND;
            this.seqNo = UNASSIGNED_SEQ_NO;
            this.term = UNASSIGNED_PRIMARY_TERM;
            this.failure = null;
            this.requiredMappingUpdate = requiredMappingUpdate;
            this.resultType = Type.MAPPING_UPDATE_REQUIRED;
            this.id = id;
        }

        /** whether the operation was successful, has failed or was aborted due to a mapping update */
        public Type getResultType() {
            return resultType;
        }

        /** get the updated document version */
        public long getVersion() {
            return version;
        }

        /**
         * Get the sequence number on the primary.
         *
         * @return the sequence number
         */
        public long getSeqNo() {
            return seqNo;
        }

        public long getTerm() {
            return term;
        }

        /**
         * If the operation was aborted due to missing mappings, this method will return the mappings
         * that are required to complete the operation.
         */
        public Mapping getRequiredMappingUpdate() {
            return requiredMappingUpdate;
        }

        /** get the translog location after executing the operation */
        public Translog.Location getTranslogLocation() {
            return translogLocation;
        }

        /** get document failure while executing the operation {@code null} in case of no failure */
        public Exception getFailure() {
            return failure;
        }

        /** get total time in nanoseconds */
        public long getTook() {
            return took;
        }

        public Operation.TYPE getOperationType() {
            return operationType;
        }

        public String getId() {
            return id;
        }

        void setTranslogLocation(Translog.Location translogLocation) {
            if (freeze.get() == null) {
                this.translogLocation = translogLocation;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void setTook(long took) {
            if (freeze.get() == null) {
                this.took = took;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void freeze() {
            freeze.set(true);
        }

        public enum Type {
            SUCCESS,
            FAILURE,
            MAPPING_UPDATE_REQUIRED
        }
    }

    public static class IndexResult extends Result {

        private final boolean created;

        public IndexResult(long version, long term, long seqNo, boolean created, String id) {
            super(Operation.TYPE.INDEX, version, term, seqNo, id);
            this.created = created;
        }

        /**
         * use in case of the index operation failed before getting to internal engine
         **/
        public IndexResult(Exception failure, long version, String id) {
            this(failure, version, UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO, id);
        }

        public IndexResult(Exception failure, long version, long term, long seqNo, String id) {
            super(Operation.TYPE.INDEX, failure, version, term, seqNo, id);
            this.created = false;
        }

        public IndexResult(Mapping requiredMappingUpdate, String id) {
            super(Operation.TYPE.INDEX, requiredMappingUpdate, id);
            this.created = false;
        }

        public boolean isCreated() {
            return created;
        }
    }

    public static class DeleteResult extends Result {

        private final boolean found;

        public DeleteResult(long version, long term, long seqNo, boolean found, String id) {
            super(Operation.TYPE.DELETE, version, term, seqNo, id);
            this.found = found;
        }

        /**
         * use in case of the delete operation failed before getting to internal engine
         **/
        public DeleteResult(Exception failure, long version, long term, String id) {
            this(failure, version, term, UNASSIGNED_SEQ_NO, false, id);
        }

        public DeleteResult(Exception failure, long version, long term, long seqNo, boolean found, String id) {
            super(Operation.TYPE.DELETE, failure, version, term, seqNo, id);
            this.found = found;
        }

        public boolean isFound() {
            return found;
        }

    }

    public static class NoOpResult extends Result {

        public NoOpResult(long term, long seqNo) {
            super(Operation.TYPE.NO_OP, 0, term, seqNo, null);
        }

        NoOpResult(long term, long seqNo, Exception failure) {
            super(Operation.TYPE.NO_OP, failure, 0, term, seqNo, null);
        }

    }

    protected final GetResult getFromSearcher(Get get, Engine.Searcher searcher, boolean uncachedLookup) throws EngineException {
        final DocIdAndVersion docIdAndVersion;
        try {
            if (uncachedLookup) {
                docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersionUncached(searcher.getIndexReader(), get.uid(), true);
            } else {
                docIdAndVersion = VersionsAndSeqNoResolver.timeSeriesLoadDocIdAndVersion(searcher.getIndexReader(), get.uid(), true);
            }
        } catch (Exception e) {
            Releasables.closeWhileHandlingException(searcher);
            // TODO: A better exception goes here
            throw new EngineException(shardId, "Couldn't resolve version", e);
        }

        if (docIdAndVersion != null) {
            if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) {
                Releasables.close(searcher);
                throw new VersionConflictEngineException(
                    shardId,
                    "[" + get.id() + "]",
                    get.versionType().explainConflictForReads(docIdAndVersion.version, get.version())
                );
            }
            if (get.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                && (get.getIfSeqNo() != docIdAndVersion.seqNo || get.getIfPrimaryTerm() != docIdAndVersion.primaryTerm)) {
                Releasables.close(searcher);
                throw new VersionConflictEngineException(
                    shardId,
                    get.id(),
                    get.getIfSeqNo(),
                    get.getIfPrimaryTerm(),
                    docIdAndVersion.seqNo,
                    docIdAndVersion.primaryTerm
                );
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

    public abstract GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Engine.Searcher, Engine.Searcher> searcherWrapper
    );

    /**
     * Similar to {@link Engine#get}, but it only attempts to serve the get from the translog.
     * If not found in translog, it returns null, as {@link GetResult#NOT_EXISTS} could mean deletion.
     */
    public GetResult getFromTranslog(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Engine.Searcher, Engine.Searcher> searcherWrapper
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    public final SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper) throws EngineException {
        return acquireSearcherSupplier(wrapper, SearcherScope.EXTERNAL);
    }

    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        /* Acquire order here is store -> manager since we need
         * to make sure that the store is not closed before
         * the searcher is acquired. */
        if (store.tryIncRef() == false) {
            throw new AlreadyClosedException(shardId + " store is closed", failedEngine.get());
        }
        Releasable releasable = store::decRef;
        try {
            ReferenceManager<ElasticsearchDirectoryReader> referenceManager = getReferenceManager(scope);
            ElasticsearchDirectoryReader acquire = referenceManager.acquire();
            SearcherSupplier reader = new SearcherSupplier(wrapper) {
                @Override
                public Searcher acquireSearcherInternal(String source) {
                    assert assertSearcherIsWarmedUp(source, scope);
                    return new Searcher(
                        source,
                        acquire,
                        engineConfig.getSimilarity(),
                        engineConfig.getQueryCache(),
                        engineConfig.getQueryCachingPolicy(),
                        () -> {}
                    );
                }

                @Override
                protected void doClose() {
                    try {
                        referenceManager.release(acquire);
                    } catch (IOException e) {
                        throw new UncheckedIOException("failed to close", e);
                    } catch (AlreadyClosedException e) {
                        // This means there's a bug somewhere: don't suppress it
                        throw new AssertionError(e);
                    } finally {
                        store.decRef();
                    }
                }
            };
            releasable = null; // success - hand over the reference to the engine reader
            return reader;
        } catch (AlreadyClosedException ex) {
            throw ex;
        } catch (Exception ex) {
            maybeFailEngine("acquire_reader", ex);
            ensureOpen(ex); // throw EngineCloseException here if we are already closed
            logger.error("failed to acquire reader", ex);
            throw new EngineException(shardId, "failed to acquire reader", ex);
        } finally {
            Releasables.close(releasable);
        }
    }

    public final Searcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, SearcherScope.EXTERNAL);
    }

    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        return acquireSearcher(source, scope, Function.identity());
    }

    public Searcher acquireSearcher(String source, SearcherScope scope, Function<Searcher, Searcher> wrapper) throws EngineException {
        SearcherSupplier releasable = null;
        try {
            SearcherSupplier reader = releasable = acquireSearcherSupplier(wrapper, scope);
            Searcher searcher = reader.acquireSearcher(source);
            releasable = null;
            return new Searcher(
                source,
                searcher.getDirectoryReader(),
                searcher.getSimilarity(),
                searcher.getQueryCache(),
                searcher.getQueryCachingPolicy(),
                () -> Releasables.close(searcher, reader)
            );
        } finally {
            Releasables.close(releasable);
        }
    }

    protected abstract ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope);

    boolean assertSearcherIsWarmedUp(String source, SearcherScope scope) {
        return true;
    }

    public enum SearcherScope {
        EXTERNAL,
        INTERNAL
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public abstract boolean isTranslogSyncNeeded();

    /**
     * Whether search idleness may be allowed to be considered for skipping a scheduled refresh.
     */
    public boolean allowSearchIdleOptimization() {
        return true;
    }

    /**
     * Ensures that the location has been written to the underlying storage.
     */
    public abstract void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener);

    /**
     * Ensures that the global checkpoint has been persisted to the underlying storage.
     */
    public abstract void asyncEnsureGlobalCheckpointSynced(long globalCheckpoint, Consumer<Exception> listener);

    public abstract void syncTranslog() throws IOException;

    /**
     * Acquires a lock on the translog files and Lucene soft-deleted documents to prevent them from being trimmed
     */
    public abstract Closeable acquireHistoryRetentionLock();

    /**
     * Counts the number of operations in the range of the given sequence numbers.
     *
     * @param source    the source of the request
     * @param fromSeqNo the start sequence number (inclusive)
     * @param toSeqNo   the end sequence number (inclusive)
     * @see #newChangesSnapshot(String, long, long, boolean, boolean, boolean)
     */
    public abstract int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException;

    /**
     * Creates a new history snapshot from Lucene for reading operations whose seqno in the requesting seqno range (both inclusive).
     * This feature requires soft-deletes enabled. If soft-deletes are disabled, this method will throw an {@link IllegalStateException}.
     */
    public abstract Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats
    ) throws IOException;

    /**
     * Checks if this engine has every operations since  {@code startingSeqNo}(inclusive) in its history (either Lucene or translog)
     */
    public abstract boolean hasCompleteOperationHistory(String reason, long startingSeqNo);

    /**
     * Gets the minimum retained sequence number for this engine.
     *
     * @return the minimum retained sequence number
     */
    public abstract long getMinRetainedSeqNo();

    public abstract TranslogStats getTranslogStats();

    /**
     * Returns the last location that the translog of this engine has written into.
     */
    public abstract Translog.Location getTranslogLastWriteLocation();

    protected final void ensureOpen(Exception suppressed) {
        if (isClosed.get()) {
            AlreadyClosedException ace = new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
            if (suppressed != null) {
                ace.addSuppressed(suppressed);
            }
            throw ace;
        }
    }

    protected final void ensureOpen() {
        ensureOpen(null);
    }

    /** get commits stats for the last commit */
    public final CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }

    /**
     * @return the max issued or seen seqNo for this Engine
     */
    public abstract long getMaxSeqNo();

    /**
     * @return the processed local checkpoint for this Engine
     */
    public abstract long getProcessedLocalCheckpoint();

    /**
     * @return the persisted local checkpoint for this Engine
     */
    public abstract long getPersistedLocalCheckpoint();

    /**
     * @return a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     */
    public abstract SeqNoStats getSeqNoStats(long globalCheckpoint);

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public abstract long getLastSyncedGlobalCheckpoint();

    /**
     * Global stats on segments.
     */
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        ensureOpen();
        Set<String> segmentName = new HashSet<>();
        SegmentsStats stats = new SegmentsStats();
        try (Searcher searcher = acquireSearcher("segments_stats", SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
                segmentName.add(segmentReader.getSegmentName());
            }
        }

        try (Searcher searcher = acquireSearcher("segments_stats", SearcherScope.EXTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segmentName.contains(segmentReader.getSegmentName()) == false) {
                    fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
                }
            }
        }
        writerSegmentStats(stats);
        return stats;
    }

    protected void fillSegmentStats(SegmentReader segmentReader, boolean includeSegmentFileSizes, SegmentsStats stats) {
        stats.add(1);
        if (includeSegmentFileSizes) {
            stats.addFiles(getSegmentFileSizes(segmentReader));
        }
    }

    private Map<String, SegmentsStats.FileStats> getSegmentFileSizes(SegmentReader segmentReader) {
        try {
            Map<String, SegmentsStats.FileStats> files = new HashMap<>();
            final SegmentCommitInfo segmentCommitInfo = segmentReader.getSegmentInfo();
            for (String fileName : segmentCommitInfo.files()) {
                String fileExtension = IndexFileNames.getExtension(fileName);
                if (fileExtension != null) {
                    try {
                        long fileLength = segmentReader.directory().fileLength(fileName);
                        files.put(fileExtension, new SegmentsStats.FileStats(fileExtension, fileLength, 1L, fileLength, fileLength));
                    } catch (IOException ioe) {
                        logger.warn(() -> "Error when retrieving file length for [" + fileName + "]", ioe);
                    } catch (AlreadyClosedException ace) {
                        logger.warn(() -> "Error when retrieving file length for [" + fileName + "], directory is closed", ace);
                        return Map.of();
                    }
                }
            }
            return Collections.unmodifiableMap(files);
        } catch (IOException e) {
            logger.warn(
                () -> format(
                    "Error when listing files for segment reader [%s] and segment info [%s]",
                    segmentReader,
                    segmentReader.getSegmentInfo()
                ),
                e
            );
            return Map.of();
        }
    }

    protected void writerSegmentStats(SegmentsStats stats) {
        // by default we don't have a writer here... subclasses can override this
        stats.addVersionMapMemoryInBytes(0);
        stats.addIndexWriterMemoryInBytes(0);
    }

    /** How much heap is used that would be freed by a refresh. This includes both the current memory being freed and any remaining
     * memory usage that could be freed, e.g., by refreshing. Note that this may throw {@link AlreadyClosedException}. */
    public abstract long getIndexBufferRAMBytesUsed();

    final Segment[] getSegmentInfo(SegmentInfos lastCommittedSegmentInfos) {
        return getSegmentInfo(lastCommittedSegmentInfos, false);
    }

    final Segment[] getSegmentInfo(SegmentInfos lastCommittedSegmentInfos, boolean includeVectorFormatsInfo) {
        ensureOpen();
        Map<String, Segment> segments = new HashMap<>();
        // first, go over and compute the search ones...
        try (Searcher searcher = acquireSearcher("segments", SearcherScope.EXTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                fillSegmentInfo(Lucene.segmentReader(ctx.reader()), true, segments, includeVectorFormatsInfo);
            }
        }

        try (Searcher searcher = acquireSearcher("segments", SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segments.containsKey(segmentReader.getSegmentName()) == false) {
                    fillSegmentInfo(segmentReader, false, segments, includeVectorFormatsInfo);
                }
            }
        }

        // now, correlate or add the committed ones...
        if (lastCommittedSegmentInfos != null) {
            for (SegmentCommitInfo info : lastCommittedSegmentInfos) {
                Segment segment = segments.get(info.info.name);
                if (segment == null) {
                    segment = new Segment(info.info.name);
                    segment.search = false;
                    segment.committed = true;
                    segment.delDocCount = info.getDelCount() + info.getSoftDelCount();
                    segment.docCount = info.info.maxDoc() - segment.delDocCount;
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace(() -> "failed to get size for [" + info.info.name + "]", e);
                    }
                    segment.segmentSort = info.info.getIndexSort();
                    segment.attributes = info.info.getAttributes();
                    segments.put(info.info.name, segment);
                } else {
                    segment.committed = true;
                }
            }
        }

        Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
        Arrays.sort(segmentsArr, Comparator.comparingLong(Segment::getGeneration));
        return segmentsArr;
    }

    private void fillSegmentInfo(
        SegmentReader segmentReader,
        boolean search,
        Map<String, Segment> segments,
        boolean includeVectorFormatsInfo
    ) {
        SegmentCommitInfo info = segmentReader.getSegmentInfo();
        assert segments.containsKey(info.info.name) == false;
        Segment segment = new Segment(info.info.name);
        segment.search = search;
        segment.docCount = segmentReader.numDocs();
        segment.delDocCount = segmentReader.numDeletedDocs();
        segment.version = info.info.getVersion();
        segment.compound = info.info.getUseCompoundFile();
        try {
            segment.sizeInBytes = info.sizeInBytes();
        } catch (IOException e) {
            logger.trace(() -> "failed to get size for [" + info.info.name + "]", e);
        }
        segment.segmentSort = info.info.getIndexSort();
        segment.attributes = new HashMap<>();
        segment.attributes.putAll(info.info.getAttributes());
        Map<String, List<String>> knnFormats = null;
        if (includeVectorFormatsInfo) {
            try {
                FieldInfos fieldInfos = segmentReader.getFieldInfos();
                if (fieldInfos.hasVectorValues()) {
                    for (FieldInfo fieldInfo : fieldInfos) {
                        String name = fieldInfo.getName();
                        if (fieldInfo.hasVectorValues()) {
                            if (knnFormats == null) {
                                knnFormats = new HashMap<>();
                            }
                            String key = fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
                            knnFormats.compute(key, (s, a) -> {
                                if (a == null) {
                                    a = new ArrayList<>();
                                }
                                a.add(name);
                                return a;
                            });
                        }
                    }
                }
            } catch (AlreadyClosedException ace) {
                // silently ignore
            }
        }
        if (knnFormats != null) {
            for (Map.Entry<String, List<String>> entry : knnFormats.entrySet()) {
                segment.attributes.put(entry.getKey(), entry.getValue().toString());
            }
        }
        // TODO: add more fine grained mem stats values to per segment info here
        segments.put(info.info.name, segment);
    }

    /**
     * The list of segments in the engine.
     */
    public abstract List<Segment> segments();

    public abstract List<Segment> segments(boolean includeVectorFormatsInfo);

    public boolean refreshNeeded() {
        if (store.tryIncRef()) {
            /*
              we need to inc the store here since we acquire a searcher and that might keep a file open on the
              store. this violates the assumption that all files are closed when
              the store is closed so we need to make sure we increment it here
             */
            try {
                try (Searcher searcher = acquireSearcher("refresh_needed", SearcherScope.EXTERNAL)) {
                    return searcher.getDirectoryReader().isCurrent() == false;
                }
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
     * Synchronously refreshes the engine for new search operations to reflect the latest
     * changes.
     */
    @Nullable
    public abstract RefreshResult refresh(String source) throws EngineException;

    /**
     * An async variant of {@link Engine#refresh(String)} that may apply some rate-limiting.
     */
    public void externalRefresh(String source, ActionListener<Engine.RefreshResult> listener) {
        ActionListener.completeWith(listener, () -> {
            logger.trace("external refresh with source [{}]", source);
            return refresh(source);
        });
    }

    /**
     * Asynchronously refreshes the engine for new search operations to reflect the latest
     * changes unless another thread is already refreshing the engine concurrently.
     */
    @Nullable
    public abstract void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException;

    /**
     * Called when our engine is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    // NOTE: do NOT rename this to something containing flush or refresh!
    public abstract void writeIndexingBuffer() throws IOException;

    /**
     * Checks if this engine should be flushed periodically.
     * This check is mainly based on the uncommitted translog size and the translog flush threshold setting.
     */
    public abstract boolean shouldPeriodicallyFlush();

    /**
     * A Blocking helper method for calling the async flush method.
     */
    // TODO: Remove or rename for increased clarity
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        PlainActionFuture<FlushResult> future = new PlainActionFuture<>();
        flush(force, waitIfOngoing, future);
        future.actionGet();
    }

    /**
     * Flushes the state of the engine including the transaction log, clearing memory, and writing the
     * documents in the Lucene index to disk. This method will synchronously flush on the calling thread.
     * However, depending on engine implementation, full durability will not be guaranteed until the listener
     * is triggered.
     *
     * @param force         if <code>true</code> a lucene commit is executed even if no changes need to be committed.
     * @param waitIfOngoing if <code>true</code> this call will block until all currently running flushes have finished.
     *                      Otherwise this call will return without blocking.
     * @param listener      to notify after fully durability has been achieved. if <code>waitIfOngoing==false</code> and ongoing
     *                      request is detected, no flush will have occurred and the listener will be completed with a marker
     *                      indicating no flush and unknown generation.
     */
    public final void flush(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        try (var ignored = acquireEnsureOpenRef()) {
            flushHoldingLock(force, waitIfOngoing, listener);
        }
    }

    /**
     * The actual implementation of {@link #flush(boolean, boolean, ActionListener)}, to be called either when holding a ref that ensures
     * the engine remains open, or holding {@code IndexShard#engineMutex} while closing the engine.
     *
     */
    protected abstract void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener)
        throws EngineException;

    /**
     * Flushes the state of the engine including the transaction log, clearing memory and persisting
     * documents in the lucene index to disk including a potentially heavy and durable fsync operation.
     * This operation is not going to block if another flush operation is currently running and won't write
     * a lucene commit if nothing needs to be committed.
     */
    public final void flush() throws EngineException {
        PlainActionFuture<FlushResult> future = new PlainActionFuture<>();
        flush(false, false, future);
        future.actionGet();
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public abstract void trimUnreferencedTranslogFiles() throws EngineException;

    /**
     * Tests whether or not the translog generation should be rolled to a new generation.
     * This test is based on the size of the current generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    public abstract boolean shouldRollTranslogGeneration();

    /**
     * Rolls the translog generation and cleans unneeded.
     */
    public abstract void rollTranslogGeneration() throws EngineException;

    /**
     * Triggers a forced merge on this engine
     */
    public abstract void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID)
        throws EngineException, IOException;

    /**
     * Snapshots the most recent index and returns a handle to it. If needed will try and "commit" the
     * lucene index to make sure we have a "fresh" copy of the files to snapshot.
     *
     * @param flushFirst indicates whether the engine should flush before returning the snapshot
     */
    public abstract IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException;

    /**
     * Snapshots the most recent safe index commit from the engine.
     */
    public abstract IndexCommitRef acquireSafeIndexCommit() throws EngineException;

    /**
     * Acquires the index commit that should be included in a snapshot.
     */
    public final IndexCommitRef acquireIndexCommitForSnapshot() throws EngineException {
        return engineConfig.getSnapshotCommitSupplier().acquireIndexCommitForSnapshot(this);
    }

    /**
     * @return a summary of the contents of the current safe commit
     */
    public abstract SafeCommitInfo getSafeCommitInfo();

    /**
     * If the specified throwable contains a fatal error in the throwable graph, such a fatal error will be thrown. Callers should ensure
     * that there are no catch statements that would catch an error in the stack as the fatal error here should go uncaught and be handled
     * by the uncaught exception handler that we install during bootstrap. If the specified throwable does indeed contain a fatal error,
     * the specified message will attempt to be logged before throwing the fatal error. If the specified throwable does not contain a fatal
     * error, this method is a no-op.
     *
     * @param maybeMessage the message to maybe log
     * @param maybeFatal   the throwable that maybe contains a fatal error
     */
    @SuppressWarnings("finally")
    private void maybeDie(final String maybeMessage, final Throwable maybeFatal) {
        ExceptionsHelper.maybeError(maybeFatal).ifPresent(error -> {
            try {
                logger.error(maybeMessage, error);
            } finally {
                throw error;
            }
        });
    }

    /**
     * fail engine due to some error. the engine will also be closed.
     * The underlying store is marked corrupted iff failure is caused by index corruption
     */
    public void failEngine(String reason, @Nullable Exception failure) {
        assert Transports.assertNotTransportThread("failEngine can block on IO");
        if (failure != null) {
            maybeDie(reason, failure);
        }
        if (failEngineLock.tryLock()) {
            try {
                if (failedEngine.get() != null) {
                    logger.warn(() -> "tried to fail engine but engine is already failed. ignoring. [" + reason + "]", failure);
                    return;
                }
                // this must happen before we close IW or Translog such that we can check this state to opt out of failing the engine
                // again on any caught AlreadyClosedException
                failedEngine.set((failure != null) ? failure : new IllegalStateException(reason));
                try {
                    // we just go and close this engine - no way to recover
                    closeNoLock("engine failed on: [" + reason + "]", closedLatch);
                } finally {
                    logger.warn(() -> "failed engine [" + reason + "]", failure);
                    // we must set a failure exception, generate one if not supplied
                    // we first mark the store as corrupted before we notify any listeners
                    // this must happen first otherwise we might try to reallocate so quickly
                    // on the same node that we don't see the corrupted marker file when
                    // the shard is initializing
                    if (Lucene.isCorruptionException(failure)) {
                        if (store.tryIncRef()) {
                            try {
                                store.markStoreCorrupted(
                                    new IOException("failed engine (reason: [" + reason + "])", ExceptionsHelper.unwrapCorruption(failure))
                                );
                            } catch (IOException e) {
                                logger.warn("Couldn't mark store corrupted", e);
                            } finally {
                                store.decRef();
                            }
                        } else {
                            logger.warn(
                                () -> format("tried to mark store as corrupted but store is already closed. [%s]", reason),
                                failure
                            );
                        }
                    }
                    eventListener.onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null) inner.addSuppressed(failure);
                // don't bubble up these exceptions up
                logger.warn("failEngine threw exception", inner);
            }
        } else {
            logger.debug(
                () -> format("tried to fail engine but could not acquire lock - engine should be failed by now [%s]", reason),
                failure
            );
        }
    }

    /** Check whether the engine should be failed */
    protected boolean maybeFailEngine(String source, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            failEngine("corrupt file (source: [" + source + "])", e);
            return true;
        }
        return false;
    }

    public interface EventListener {
        /**
         * Called when a fatal exception occurred
         */
        default void onFailedEngine(String reason, @Nullable Exception e) {}
    }

    public abstract static class SearcherSupplier implements Releasable {
        private final Function<Searcher, Searcher> wrapper;
        private final AtomicBoolean released = new AtomicBoolean(false);

        public SearcherSupplier(Function<Searcher, Searcher> wrapper) {
            this.wrapper = wrapper;
        }

        public final Searcher acquireSearcher(String source) {
            if (released.get()) {
                throw new AlreadyClosedException("SearcherSupplier was closed");
            }
            final Searcher searcher = acquireSearcherInternal(source);
            return wrapper.apply(searcher);
        }

        @Override
        public final void close() {
            if (released.compareAndSet(false, true)) {
                doClose();
            } else {
                assert false : "SearchSupplier was released twice";
            }
        }

        protected abstract void doClose();

        protected abstract Searcher acquireSearcherInternal(String source);

        /**
         * Returns an id associated with this searcher if exists. Two searchers with the same searcher id must have
         * identical Lucene level indices (i.e., identical segments with same docs using same doc-ids).
         */
        @Nullable
        public String getSearcherId() {
            return null;
        }
    }

    public static final class Searcher extends IndexSearcher implements Releasable {
        private final String source;
        private final Closeable onClose;

        public Searcher(
            String source,
            IndexReader reader,
            Similarity similarity,
            QueryCache queryCache,
            QueryCachingPolicy queryCachingPolicy,
            Closeable onClose
        ) {
            super(reader);
            setSimilarity(similarity);
            setQueryCache(queryCache);
            setQueryCachingPolicy(queryCachingPolicy);
            this.source = source;
            this.onClose = onClose;
        }

        /**
         * The source that caused this searcher to be acquired.
         */
        public String source() {
            return source;
        }

        public DirectoryReader getDirectoryReader() {
            if (getIndexReader() instanceof DirectoryReader) {
                return (DirectoryReader) getIndexReader();
            }
            throw new IllegalStateException("Can't use " + getIndexReader().getClass() + " as a directory reader");
        }

        @Override
        public void close() {
            try {
                onClose.close();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to close", e);
            } catch (AlreadyClosedException e) {
                // This means there's a bug somewhere: don't suppress it
                throw new AssertionError(e);
            }
        }
    }

    public abstract static class Operation {

        /** type of operation (index, delete), subclasses use static types */
        public enum TYPE {
            INDEX,
            DELETE,
            NO_OP;

            private final String lowercase;

            TYPE() {
                this.lowercase = this.toString().toLowerCase(Locale.ROOT);
            }

            public String getLowercase() {
                return lowercase;
            }
        }

        private final BytesRef uid;
        private final long version;
        private final long seqNo;
        private final long primaryTerm;
        private final VersionType versionType;
        private final Origin origin;
        private final long startTime;

        public Operation(BytesRef uid, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin, long startTime) {
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
            this.origin = origin;
            this.startTime = startTime;
        }

        public enum Origin {
            PRIMARY,
            REPLICA,
            PEER_RECOVERY,
            LOCAL_TRANSLOG_RECOVERY,
            LOCAL_RESET;

            public boolean isRecovery() {
                return this == PEER_RECOVERY || this == LOCAL_TRANSLOG_RECOVERY;
            }

            boolean isFromTranslog() {
                return this == LOCAL_TRANSLOG_RECOVERY || this == LOCAL_RESET;
            }
        }

        public Origin origin() {
            return this.origin;
        }

        public BytesRef uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public abstract int estimatedSizeInBytes();

        public VersionType versionType() {
            return this.versionType;
        }

        /**
         * Returns operation start time in nanoseconds.
         */
        public long startTime() {
            return this.startTime;
        }

        abstract String id();

        public abstract TYPE operationType();
    }

    public static class Index extends Operation {

        private final ParsedDocument doc;
        private final long autoGeneratedIdTimestamp;
        private final boolean isRetry;
        private final long ifSeqNo;
        private final long ifPrimaryTerm;

        public Index(
            BytesRef uid,
            ParsedDocument doc,
            long seqNo,
            long primaryTerm,
            long version,
            VersionType versionType,
            Origin origin,
            long startTime,
            long autoGeneratedIdTimestamp,
            boolean isRetry,
            long ifSeqNo,
            long ifPrimaryTerm
        ) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            assert (origin == Origin.PRIMARY) == (versionType != null) : "invalid version_type=" + versionType + " for origin=" + origin;
            assert ifPrimaryTerm >= 0 : "ifPrimaryTerm [" + ifPrimaryTerm + "] must be non negative";
            assert ifSeqNo == UNASSIGNED_SEQ_NO || ifSeqNo >= 0 : "ifSeqNo [" + ifSeqNo + "] must be non negative or unset";
            assert (origin == Origin.PRIMARY) || (ifSeqNo == UNASSIGNED_SEQ_NO && ifPrimaryTerm == UNASSIGNED_PRIMARY_TERM)
                : "cas operations are only allowed if origin is primary. get [" + origin + "]";
            this.doc = doc;
            this.isRetry = isRetry;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
            this.ifSeqNo = ifSeqNo;
            this.ifPrimaryTerm = ifPrimaryTerm;
        }

        public Index(BytesRef uid, long primaryTerm, ParsedDocument doc) {
            this(uid, primaryTerm, doc, Versions.MATCH_ANY);
        } // TEST ONLY

        Index(BytesRef uid, long primaryTerm, ParsedDocument doc, long version) {
            this(
                uid,
                doc,
                UNASSIGNED_SEQ_NO,
                primaryTerm,
                version,
                VersionType.INTERNAL,
                Origin.PRIMARY,
                System.nanoTime(),
                -1,
                false,
                UNASSIGNED_SEQ_NO,
                0
            );
        } // TEST ONLY

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        @Override
        public String id() {
            return this.doc.id();
        }

        @Override
        public TYPE operationType() {
            return TYPE.INDEX;
        }

        public String routing() {
            return this.doc.routing();
        }

        public List<LuceneDocument> docs() {
            return this.doc.docs();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        @Override
        public int estimatedSizeInBytes() {
            return (id().length() * 2) + source().length() + 12;
        }

        /**
         * Returns a positive timestamp if the ID of this document is auto-generated by elasticsearch.
         * if this property is non-negative indexing code might optimize the addition of this document
         * due to it's append only nature.
         */
        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

        /**
         * Returns <code>true</code> if this index requests has been retried on the coordinating node and can therefor be delivered
         * multiple times. Note: this might also be set to true if an equivalent event occurred like the replay of the transaction log
         */
        public boolean isRetry() {
            return isRetry;
        }

        public long getIfSeqNo() {
            return ifSeqNo;
        }

        public long getIfPrimaryTerm() {
            return ifPrimaryTerm;
        }
    }

    public static class Delete extends Operation {

        private final String id;
        private final long ifSeqNo;
        private final long ifPrimaryTerm;

        public Delete(
            String id,
            BytesRef uid,
            long seqNo,
            long primaryTerm,
            long version,
            VersionType versionType,
            Origin origin,
            long startTime,
            long ifSeqNo,
            long ifPrimaryTerm
        ) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            assert (origin == Origin.PRIMARY) == (versionType != null) : "invalid version_type=" + versionType + " for origin=" + origin;
            assert ifPrimaryTerm >= 0 : "ifPrimaryTerm [" + ifPrimaryTerm + "] must be non negative";
            assert ifSeqNo == UNASSIGNED_SEQ_NO || ifSeqNo >= 0 : "ifSeqNo [" + ifSeqNo + "] must be non negative or unset";
            assert (origin == Origin.PRIMARY) || (ifSeqNo == UNASSIGNED_SEQ_NO && ifPrimaryTerm == UNASSIGNED_PRIMARY_TERM)
                : "cas operations are only allowed if origin is primary. get [" + origin + "]";
            this.id = Objects.requireNonNull(id);
            this.ifSeqNo = ifSeqNo;
            this.ifPrimaryTerm = ifPrimaryTerm;
        }

        public Delete(String id, BytesRef uid, long primaryTerm) {
            this(
                id,
                uid,
                UNASSIGNED_SEQ_NO,
                primaryTerm,
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Origin.PRIMARY,
                System.nanoTime(),
                UNASSIGNED_SEQ_NO,
                0
            );
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public TYPE operationType() {
            return TYPE.DELETE;
        }

        @Override
        public int estimatedSizeInBytes() {
            return uid().length * 2 + 20;
        }

        public long getIfSeqNo() {
            return ifSeqNo;
        }

        public long getIfPrimaryTerm() {
            return ifPrimaryTerm;
        }
    }

    public static class NoOp extends Operation {

        private final String reason;

        public String reason() {
            return reason;
        }

        public NoOp(final long seqNo, final long primaryTerm, final Origin origin, final long startTime, final String reason) {
            super(null, seqNo, primaryTerm, Versions.NOT_FOUND, null, origin, startTime);
            this.reason = reason;
        }

        @Override
        public BytesRef uid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long version() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VersionType versionType() {
            throw new UnsupportedOperationException();
        }

        @Override
        String id() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TYPE operationType() {
            return TYPE.NO_OP;
        }

        @Override
        public int estimatedSizeInBytes() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

    }

    public static class Get {
        private final boolean realtime;
        private final BytesRef uid;
        private final String id;
        private final boolean readFromTranslog;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private long ifSeqNo = UNASSIGNED_SEQ_NO;
        private long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;

        public Get(boolean realtime, boolean readFromTranslog, String id) {
            this.realtime = realtime;
            this.id = id;
            this.uid = Uid.encodeId(id);
            this.readFromTranslog = readFromTranslog;
        }

        public boolean realtime() {
            return this.realtime;
        }

        public String id() {
            return id;
        }

        public BytesRef uid() {
            return uid;
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

        public boolean isReadFromTranslog() {
            return readFromTranslog;
        }

        public Get setIfSeqNo(long seqNo) {
            this.ifSeqNo = seqNo;
            return this;
        }

        public long getIfSeqNo() {
            return ifSeqNo;
        }

        public Get setIfPrimaryTerm(long primaryTerm) {
            this.ifPrimaryTerm = primaryTerm;
            return this;
        }

        public long getIfPrimaryTerm() {
            return ifPrimaryTerm;
        }

    }

    public static class GetResult implements Releasable {
        private final boolean exists;
        private final long version;
        private final DocIdAndVersion docIdAndVersion;
        private final Engine.Searcher searcher;

        public static final GetResult NOT_EXISTS = new GetResult(false, Versions.NOT_FOUND, null, null);

        private GetResult(boolean exists, long version, DocIdAndVersion docIdAndVersion, Engine.Searcher searcher) {
            this.exists = exists;
            this.version = version;
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        public GetResult(Engine.Searcher searcher, DocIdAndVersion docIdAndVersion) {
            this(true, docIdAndVersion.version, docIdAndVersion, searcher);
        }

        public boolean exists() {
            return exists;
        }

        public long version() {
            return this.version;
        }

        public Engine.Searcher searcher() {
            return this.searcher;
        }

        public DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        @Override
        public void close() {
            Releasables.close(searcher);
        }
    }

    /**
     * Closes the engine without acquiring any refs or locks. The caller should either have changed {@link #isClosing} from {@code false} to
     * {@code true} or else must hold the {@link #failEngineLock}. The implementation must decrement the supplied latch when done.
     */
    protected abstract void closeNoLock(String reason, CountDownLatch closedLatch);

    protected final boolean isDrainedForClose() {
        return ensureOpenRefs.hasReferences() == false;
    }

    protected final boolean isClosing() {
        return isClosing.get();
    }

    protected final Releasable acquireEnsureOpenRef() {
        if (isClosing() || ensureOpenRefs.tryIncRef() == false) {
            ensureOpen(); // throws "engine is closed" exception if we're actually closed, otherwise ...
            throw new AlreadyClosedException(shardId + " engine is closing", failedEngine.get());
        }
        return Releasables.assertOnce(releaseEnsureOpenRef);
    }

    /**
     * When called for the first time, puts the engine into a closing state in which further calls to {@link #acquireEnsureOpenRef()} will
     * fail with an {@link AlreadyClosedException} and waits for all outstanding ensure-open refs to be released, before returning {@code
     * true}. If called again, returns {@code false} without waiting.
     *
     * @return a flag indicating whether this was the first call or not.
     */
    private boolean drainForClose() {
        if (isClosing.compareAndSet(false, true) == false) {
            logger.trace("drainForClose(): already closing");
            return false;
        }

        logger.debug("drainForClose(): draining ops");
        releaseEnsureOpenRef.close();
        final var future = new UnsafePlainActionFuture<Void>(ThreadPool.Names.GENERIC) {
            @Override
            protected boolean blockingAllowed() {
                // TODO remove this blocking, or at least do it elsewhere, see https://github.com/elastic/elasticsearch/issues/89821
                return Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME)
                    || super.blockingAllowed();
            }
        };
        drainOnCloseListener.addListener(future);
        try {
            future.get();
            return true;
        } catch (ExecutionException e) {
            logger.error("failure while draining operations on close", e);
            assert false : e;
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted while draining operations on close");
            throw new IllegalStateException(e);
        }
    }

    /**
     * Flush the engine (committing segments to disk and truncating the translog) and close it.
     */
    public void flushAndClose() throws IOException {
        logger.trace("flushAndClose() maybe draining ops");
        if (isClosed.get() == false && drainForClose()) {
            logger.trace("flushAndClose drained ops");
            try {
                logger.debug("flushing shard on close - this might take some time to sync files to disk");
                try {
                    // TODO: We are not waiting for full durability here atm because we are on the cluster state update thread
                    flushHoldingLock(false, false, ActionListener.noop());
                } catch (AlreadyClosedException ex) {
                    logger.debug("engine already closed - skipping flushAndClose");
                }
            } finally {
                closeNoLock("flushAndClose", closedLatch);
            }
        }
        awaitPendingClose();
    }

    @Override
    public void close() throws IOException {
        logger.debug("close() maybe draining ops");
        if (isClosed.get() == false && drainForClose()) {
            logger.debug("close drained ops");
            closeNoLock("api", closedLatch);
        }
        awaitPendingClose();
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class IndexCommitRef implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CheckedRunnable<IOException> onClose;
        private final IndexCommit indexCommit;

        public IndexCommitRef(IndexCommit indexCommit, CheckedRunnable<IOException> onClose) {
            this.indexCommit = indexCommit;
            this.onClose = onClose;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }

        public IndexCommit getIndexCommit() {
            return indexCommit;
        }
    }

    public void onSettingsChanged() {

    }

    /**
     * Returns the timestamp of the last write in nanoseconds.
     * Note: this time might not be absolutely accurate since the {@link Operation#startTime()} is used which might be
     * slightly inaccurate.
     *
     * @see System#nanoTime()
     * @see Operation#startTime()
     */
    public long getLastWriteNanos() {
        return this.lastWriteNanos;
    }

    /**
     * Called for each new opened engine reader to warm new segments
     *
     * @see EngineConfig#getWarmer()
     */
    public interface Warmer {
        /**
         * Called once a new top-level reader is opened.
         */
        void warm(ElasticsearchDirectoryReader reader);
    }

    /**
     * Request that this engine throttle incoming indexing requests to one thread.
     * Must be matched by a later call to {@link #deactivateThrottling()}.
     */
    public abstract void activateThrottling();

    /**
     * Reverses a previous {@link #activateThrottling} call.
     */
    public abstract void deactivateThrottling();

    /**
     * This method replays translog to restore the Lucene index which might be reverted previously.
     * This ensures that all acknowledged writes are restored correctly when this engine is promoted.
     *
     * @return the number of translog operations have been recovered
     */
    public abstract int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException;

    /**
     * Fills up the local checkpoints history with no-ops until the local checkpoint
     * and the max seen sequence ID are identical.
     * @param primaryTerm the shards primary term this engine was created for
     * @return the number of no-ops added
     */
    public abstract int fillSeqNoGaps(long primaryTerm) throws IOException;

    /**
     * Performs recovery from the transaction log up to {@code recoverUpToSeqNo} (inclusive).
     * This operation will close the engine if the recovery fails. Use EngineTestCase#recoverFromTranslog for test usages
     *
     * @param translogRecoveryRunner the translog recovery runner
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered
     */
    // TODO make all the production usages fully async
    public final void recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        final var future = new PlainActionFuture<Void>();
        recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo, future);
        try {
            future.get();
        } catch (ExecutionException e) {
            // This is a (temporary) adapter between the older synchronous (blocking) code and the newer (async) API. Callers expect
            // exceptions to be thrown directly, but Future#get adds an ExecutionException wrapper which we must remove to preserve the
            // expected exception semantics.
            if (e.getCause() instanceof IOException ioException) {
                throw ioException;
            } else if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            } else {
                // the old code was "throws IOException" so we shouldn't see any other exception types here
                logger.error("checked non-IOException unexpectedly thrown", e);
                assert false : e;
                throw new UncategorizedExecutionException("recoverFromTranslog", e);
            }
        } catch (InterruptedException e) {
            // We don't really use interrupts in this area so this is somewhat unexpected (unless perhaps we're shutting down), just treat
            // it like any other exception.
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs recovery from the transaction log up to {@code recoverUpToSeqNo} (inclusive).
     * This operation will close the engine if the recovery fails.
     *
     * @param translogRecoveryRunner the translog recovery runner
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered
     * @param listener               listener notified on completion of the recovery, whether successful or otherwise
     */
    public abstract void recoverFromTranslog(
        TranslogRecoveryRunner translogRecoveryRunner,
        long recoverUpToSeqNo,
        ActionListener<Void> listener
    );

    /**
     * Do not replay translog operations, but make the engine be ready.
     */
    public abstract void skipTranslogRecovery();

    /**
     * Tries to prune buffered deletes from the version map.
     */
    public abstract void maybePruneDeletes();

    /**
     * Returns the maximum auto_id_timestamp of all append-only index requests have been processed by this engine
     * or the auto_id_timestamp received from its primary shard via {@link #updateMaxUnsafeAutoIdTimestamp(long)}.
     * Notes this method returns the auto_id_timestamp of all append-only requests, not max_unsafe_auto_id_timestamp.
     */
    public long getMaxSeenAutoIdTimestamp() {
        return IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
    }

    /**
     * Forces this engine to advance its max_unsafe_auto_id_timestamp marker to at least the given timestamp.
     * The engine will disable optimization for all append-only whose timestamp at most {@code newTimestamp}.
     */
    public abstract void updateMaxUnsafeAutoIdTimestamp(long newTimestamp);

    @FunctionalInterface
    public interface TranslogRecoveryRunner {
        int run(Engine engine, Translog.Snapshot snapshot) throws IOException;
    }

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this engine
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in Lucene index with the same document id.
     * <p>
     * A note on the optimization using max_seq_no_of_updates_or_deletes:
     * For each operation O, the key invariants are:
     * <ol>
     *     <li> I1: There is no operation on docID(O) with seqno that is {@literal > MSU(O) and < seqno(O)} </li>
     *     <li> I2: If {@literal MSU(O) < seqno(O)} then docID(O) did not exist when O was applied; more precisely, if there is any O'
     *              with {@literal seqno(O') < seqno(O) and docID(O') = docID(O)} then the one with the greatest seqno is a delete.</li>
     * </ol>
     * <p>
     * When a receiving shard (either a replica or a follower) receives an operation O, it must first ensure its own MSU at least MSU(O),
     * and then compares its MSU to its local checkpoint (LCP). If {@literal LCP < MSU} then there's a gap: there may be some operations
     * that act on docID(O) about which we do not yet know, so we cannot perform an add. Note this also covers the case where a future
     * operation O' with {@literal seqNo(O') > seqNo(O) and docId(O') = docID(O)} is processed before O. In that case MSU(O') is at least
     * seqno(O') and this means {@literal MSU >= seqNo(O') > seqNo(O) > LCP} (because O wasn't processed yet).
     * <p>
     * However, if {@literal MSU <= LCP} then there is no gap: we have processed every {@literal operation <= LCP}, and no operation O'
     * with {@literal seqno(O') > LCP and seqno(O') < seqno(O) also has docID(O') = docID(O)}, because such an operation would have
     * {@literal seqno(O') > LCP >= MSU >= MSU(O)} which contradicts the first invariant. Furthermore in this case we immediately know
     * that docID(O) has been deleted (or never existed) without needing to check Lucene for the following reason. If there's no earlier
     * operation on docID(O) then this is clear, so suppose instead that the preceding operation on docID(O) is O':
     * 1. The first invariant above tells us that {@literal seqno(O') <= MSU(O) <= LCP} so we have already applied O' to Lucene.
     * 2. Also {@literal MSU(O) <= MSU <= LCP < seqno(O)} (we discard O if {@literal seqno(O) <= LCP}) so the second invariant applies,
     *    meaning that the O' was a delete.
     * <p>
     * Therefore, if {@literal MSU <= LCP < seqno(O)} we know that O can safely be optimized with and added to lucene with addDocument.
     * Moreover, operations that are optimized using the MSU optimization must not be processed twice as this will create duplicates
     * in Lucene. To avoid this we check the local checkpoint tracker to see if an operation was already processed.
     *
     * @see #advanceMaxSeqNoOfUpdatesOrDeletes(long)
     */
    public abstract long getMaxSeqNoOfUpdatesOrDeletes();

    /**
     * A replica shard receives a new max_seq_no_of_updates from its primary shard, then calls this method
     * to advance this marker to at least the given sequence number.
     */
    public abstract void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary);

    /**
     * @return a {@link ShardLongFieldRange} containing the min and max raw values of the given field for this shard if the engine
     * guarantees these values never to change, or {@link ShardLongFieldRange#EMPTY} if this field is empty, or
     * {@link ShardLongFieldRange#UNKNOWN} if this field's value range may change in future.
     */
    public abstract ShardLongFieldRange getRawFieldRange(String field) throws IOException;

    public final EngineConfig getEngineConfig() {
        return engineConfig;
    }

    /**
     * Allows registering a listener for when the index shard is on a segment generation >= minGeneration.
     *
     * @deprecated use {@link #addPrimaryTermAndGenerationListener(long, long, ActionListener)} instead.
     */
    @Deprecated
    public void addSegmentGenerationListener(long minGeneration, ActionListener<Long> listener) {
        addPrimaryTermAndGenerationListener(UNKNOWN_PRIMARY_TERM, minGeneration, listener);
    }

    /**
     * Allows registering a listener for when the index shard is on a primary term >= minPrimaryTerm
     * and a segment generation >= minGeneration.
     */
    public void addPrimaryTermAndGenerationListener(long minPrimaryTerm, long minGeneration, ActionListener<Long> listener) {
        throw new UnsupportedOperationException();
    }

    public void addFlushListener(Translog.Location location, ActionListener<Long> listener) {
        listener.onFailure(new UnsupportedOperationException("Engine type " + this.getClass() + " does not support flush listeners."));
    }

    /**
     * Captures the result of a refresh operation on the index shard.
     * <p>
     * <code>refreshed</code> is true if a refresh happened. If refreshed, <code>generation</code>
     * contains the generation of the index commit that the reader has opened upon refresh.
     */
    public record RefreshResult(boolean refreshed, long primaryTerm, long generation) {

        public static final long UNKNOWN_GENERATION = -1L;
        public static final RefreshResult NO_REFRESH = new RefreshResult(false);

        public RefreshResult(boolean refreshed) {
            this(refreshed, UNKNOWN_PRIMARY_TERM, UNKNOWN_GENERATION);
        }
    }

    public record FlushResult(boolean flushPerformed, long generation) {

        public static final long UNKNOWN_GENERATION = -1L;
        public static final FlushResult NO_FLUSH = new FlushResult(false, UNKNOWN_GENERATION);
    }
}

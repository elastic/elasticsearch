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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader.FSTLoadMode;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.fieldvisitor.IdOnlyFieldVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalEngine extends Engine {

    /**
     * When we last pruned expired tombstones from versionMap.deletes:
     */
    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;
    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final ExternalReaderManager externalReaderManager;
    private final ElasticsearchReaderManager internalReaderManager;

    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();

    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final LiveVersionMap versionMap = new LiveVersionMap();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final LocalCheckpointTracker localCheckpointTracker;

    private final CombinedDeletionPolicy combinedDeletionPolicy;

    // How many callers are currently requesting index throttling.  Currently there are only two situations where we do this: when merges
    // are falling behind and when writing indexing buffer to disk is too slow.  When this is 0, there is no throttling, else we throttling
    // incoming indexing ops to a single thread:
    private final AtomicInteger throttleRequestCount = new AtomicInteger();
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    // max_seq_no_of_updates_or_deletes tracks the max seq_no of update or delete operations that have been processed in this engine.
    // An index request is considered as an update if it overwrites existing documents with the same docId in the Lucene index.
    // The value of this marker never goes backwards, and is tracked/updated differently on primary and replica.
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes;
    private final CounterMetric numVersionLookups = new CounterMetric();
    private final CounterMetric numIndexVersionsLookups = new CounterMetric();
    // Lucene operations since this engine was opened - not include operations from existing segments.
    private final CounterMetric numDocDeletes = new CounterMetric();
    private final CounterMetric numDocAppends = new CounterMetric();
    private final CounterMetric numDocUpdates = new CounterMetric();
    private final NumericDocValuesField softDeletesField = Lucene.newSoftDeletesField();
    private final boolean softDeleteEnabled;
    private final SoftDeletesPolicy softDeletesPolicy;
    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;

    private final AtomicBoolean trackTranslogLocation = new AtomicBoolean(false);
    private final KeyedLock<Long> noOpKeyedLock = new KeyedLock<>();
    private final AtomicBoolean shouldPeriodicallyFlushAfterBigMerge = new AtomicBoolean(false);

    @Nullable
    private final String historyUUID;

    public InternalEngine(EngineConfig engineConfig) {
        this(engineConfig, LocalCheckpointTracker::new);
    }

    InternalEngine(
            final EngineConfig engineConfig,
            final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        super(engineConfig);
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis()
        );
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        ExternalReaderManager externalReaderManager = null;
        ElasticsearchReaderManager internalReaderManager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            try {
                trimUnsafeCommits(engineConfig);
                translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier(),
                    seqNo -> {
                        final LocalCheckpointTracker tracker = getLocalCheckpointTracker();
                        assert tracker != null || getTranslog().isOpen() == false;
                        if (tracker != null) {
                            tracker.markSeqNoAsPersisted(seqNo);
                        }
                    });
                assert translog.getGeneration() != null;
                this.translog = translog;
                this.softDeleteEnabled = engineConfig.getIndexSettings().isSoftDeleteEnabled();
                this.softDeletesPolicy = newSoftDeletesPolicy();
                this.combinedDeletionPolicy =
                    new CombinedDeletionPolicy(logger, translogDeletionPolicy, softDeletesPolicy, translog::getLastSyncedGlobalCheckpoint);
                this.localCheckpointTracker = createLocalCheckpointTracker(localCheckpointTrackerSupplier);
                writer = createWriter();
                bootstrapAppendOnlyInfoFromWriter(writer);
                historyUUID = loadHistoryUUID(writer);
                indexWriter = writer;
            } catch (IOException | TranslogCorruptedException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } catch (AssertionError e) {
                // IndexWriter throws AssertionError on init, if asserts are enabled, if any files don't exist, but tests that
                // randomly throw FNFE/NSFE can also hit this:
                if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                    throw new EngineCreationFailureException(shardId, "failed to create engine", e);
                } else {
                    throw e;
                }
            }
            externalReaderManager = createReaderManager(new RefreshWarmerListener(logger, isClosed, engineConfig));
            internalReaderManager = externalReaderManager.internalReaderManager;
            this.internalReaderManager = internalReaderManager;
            this.externalReaderManager = externalReaderManager;
            internalReaderManager.addListener(versionMap);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            // don't allow commits until we are done with recovering
            pendingTranslogRecovery.set(true);
            for (ReferenceManager.RefreshListener listener: engineConfig.getExternalRefreshListener()) {
                this.externalReaderManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener: engineConfig.getInternalRefreshListener()) {
                this.internalReaderManager.addListener(listener);
            }
            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker.getProcessedCheckpoint());
            this.internalReaderManager.addListener(lastRefreshedCheckpointListener);
            maxSeqNoOfUpdatesOrDeletes = new AtomicLong(SequenceNumbers.max(localCheckpointTracker.getMaxSeqNo(), translog.getMaxSeqNo()));
            if (softDeleteEnabled && localCheckpointTracker.getPersistedCheckpoint() < localCheckpointTracker.getMaxSeqNo()) {
                try (Searcher searcher =
                         acquireSearcher("restore_version_map_and_checkpoint_tracker", SearcherScope.INTERNAL)) {
                    restoreVersionMapAndCheckpointTracker(Lucene.wrapAllDocsLive(searcher.getDirectoryReader()));
                } catch (IOException e) {
                    throw new EngineCreationFailureException(config().getShardId(),
                        "failed to restore version map and local checkpoint tracker", e);
                }
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, internalReaderManager, externalReaderManager, scheduler);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    private LocalCheckpointTracker createLocalCheckpointTracker(
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(store.readLastCommittedSegmentsInfo().userData.entrySet());
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;
        logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        return localCheckpointTrackerSupplier.apply(maxSeqNo, localCheckpoint);
    }

    private SoftDeletesPolicy newSoftDeletesPolicy() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().userData;
        final long lastMinRetainedSeqNo;
        if (commitUserData.containsKey(Engine.MIN_RETAINED_SEQNO)) {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(Engine.MIN_RETAINED_SEQNO));
        } else {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO)) + 1;
        }
        return new SoftDeletesPolicy(
                translog::getLastSyncedGlobalCheckpoint,
                lastMinRetainedSeqNo,
                engineConfig.getIndexSettings().getSoftDeleteRetentionOperations(),
                engineConfig.retentionLeasesSupplier());
    }

    /**
     * This reference manager delegates all it's refresh calls to another (internal) ReaderManager
     * The main purpose for this is that if we have external refreshes happening we don't issue extra
     * refreshes to clear version map memory etc. this can cause excessive segment creation if heavy indexing
     * is happening and the refresh interval is low (ie. 1 sec)
     *
     * This also prevents segment starvation where an internal reader holds on to old segments literally forever
     * since no indexing is happening and refreshes are only happening to the external reader manager, while with
     * this specialized implementation an external refresh will immediately be reflected on the internal reader
     * and old segments can be released in the same way previous version did this (as a side-effect of _refresh)
     */
    @SuppressForbidden(reason = "reference counting is required here")
    private static final class ExternalReaderManager extends ReferenceManager<ElasticsearchDirectoryReader> {
        private final BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener;
        private final ElasticsearchReaderManager internalReaderManager;

        ExternalReaderManager(ElasticsearchReaderManager internalReaderManager,
                              BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener) throws IOException {
            this.refreshListener = refreshListener;
            this.internalReaderManager = internalReaderManager;
            ElasticsearchDirectoryReader acquire = internalReaderManager.acquire();
            try {
                incrementAndNotify(acquire, null);
                current = acquire;
            } finally {
                internalReaderManager.release(acquire);
            }
        }

        @Override
        protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
            // we simply run a blocking refresh on the internal reference manager and then steal it's reader
            // it's a save operation since we acquire the reader which incs it's reference but then down the road
            // steal it by calling incRef on the "stolen" reader
            internalReaderManager.maybeRefreshBlocking();
            ElasticsearchDirectoryReader acquire = internalReaderManager.acquire();
            try {
                if (acquire == referenceToRefresh) {
                    // nothing has changed - both ref managers share the same instance so we can use reference equality
                    return null;
                } else {
                    incrementAndNotify(acquire, referenceToRefresh);
                    return acquire;
                }
            } finally {
                internalReaderManager.release(acquire);
            }
        }

        private void incrementAndNotify(ElasticsearchDirectoryReader reader,
                                            ElasticsearchDirectoryReader previousReader) throws IOException {
            reader.incRef(); // steal the reference
            try (Closeable c = reader::decRef) {
                refreshListener.accept(reader, previousReader);
                reader.incRef(); // double inc-ref if we were successful
            }
        }

        @Override
        protected boolean tryIncRef(ElasticsearchDirectoryReader reference) {
            return reference.tryIncRef();
        }

        @Override
        protected int getRefCount(ElasticsearchDirectoryReader reference) {
            return reference.getRefCount();
        }

        @Override
        protected void decRef(ElasticsearchDirectoryReader reference) throws IOException { reference.decRef(); }
    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(localCheckpoint + 1)) {
                return translogRecoveryRunner.run(this, snapshot);
            }
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            final long maxSeqNo = localCheckpointTracker.getMaxSeqNo();
            int numNoOpsAdded = 0;
            for (
                    long seqNo = localCheckpoint + 1;
                    seqNo <= maxSeqNo;
                    seqNo = localCheckpointTracker.getProcessedCheckpoint() + 1 /* leap-frog the local checkpoint */) {
                innerNoOp(new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
                numNoOpsAdded++;
                assert seqNo <= localCheckpointTracker.getProcessedCheckpoint() :
                    "local checkpoint did not advance; was [" + seqNo + "], now [" + localCheckpointTracker.getProcessedCheckpoint() + "]";

            }
            syncTranslog(); // to persist noops associated with the advancement of the local checkpoint
            assert localCheckpointTracker.getPersistedCheckpoint() == maxSeqNo
                : "persisted local checkpoint did not advance to max seq no; is [" + localCheckpointTracker.getPersistedCheckpoint() +
                "], max seq no [" + maxSeqNo + "]";
            return numNoOpsAdded;
        }
    }

    private void bootstrapAppendOnlyInfoFromWriter(IndexWriter writer) {
        for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
            if (MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID.equals(entry.getKey())) {
                assert maxUnsafeAutoIdTimestamp.get() == -1 :
                    "max unsafe timestamp was assigned already [" + maxUnsafeAutoIdTimestamp.get() + "]";
                updateAutoIdTimestamp(Long.parseLong(entry.getValue()), true);
            }
        }
    }

    @Override
    public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslogInternal(translogRecoveryRunner, recoverUpToSeqNo);
            } catch (Exception e) {
                try {
                    pendingTranslogRecovery.set(true); // just play safe and never allow commits on this see #ensureCanFlush
                    failEngine("failed to recover from translog", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                throw e;
            }
        } finally {
            flushLock.unlock();
        }
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
    }

    private void recoverFromTranslogInternal(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        final long translogFileGen = Long.parseLong(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(
            new Translog.TranslogGeneration(translog.getTranslogUUID(), translogFileGen), recoverUpToSeqNo)) {
            opsRecovered = translogRecoveryRunner.run(this, snapshot);
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
        // flush if we recovered something or if we have references to older translogs
        // note: if opsRecovered == 0 and we have older translogs it means they are corrupted or 0 length.
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog. ops recovered [{}]. committed translog id [{}]. current id [{}]",
                opsRecovered, translogGeneration == null ? null :
                    translogGeneration.translogFileGeneration, translog.currentFileGeneration());
            commitIndexWriter(indexWriter, translog, null);
            refreshLastCommittedSegmentInfos();
            refresh("translog_recovery");
        }
        translog.trimUnreferencedReaders();
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy,
                                  LongSupplier globalCheckpointSupplier, LongConsumer persistedSequenceNumberConsumer) throws IOException {

        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier,
            engineConfig.getPrimaryTermSupplier(), persistedSequenceNumberConsumer);
    }

    // Package private for testing purposes only
    Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    // Package private for testing purposes only
    boolean hasSnapshottedCommits() {
        return combinedDeletionPolicy.hasSnapshottedCommits();
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return getTranslog().syncNeeded();
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        final boolean synced = translog.ensureSynced(locations);
        if (synced) {
            revisitIndexDeletionPolicyOnTranslogSynced();
        }
        return synced;
    }

    @Override
    public void syncTranslog() throws IOException {
        translog.sync();
        revisitIndexDeletionPolicyOnTranslogSynced();
    }

    /**
     * Creates a new history snapshot for reading operations since the provided seqno.
     * The returned snapshot can be retrieved from either Lucene index or translog files.
     */
    @Override
    public Translog.Snapshot readHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            return newChangesSnapshot(source, mapperService, Math.max(0, startingSeqNo), Long.MAX_VALUE, false);
        }

        return getTranslog().newSnapshotFromMinSeqNo(startingSeqNo);
    }

    /**
     * Returns the estimated number of history operations whose seq# at least the provided seq# in this engine.
     */
    @Override
    public int estimateNumberOfHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            try (Translog.Snapshot snapshot = newChangesSnapshot(source, mapperService, Math.max(0, startingSeqNo),
                Long.MAX_VALUE, false)) {
                return snapshot.totalOperations();
            }
        }

        return getTranslog().estimateTotalOperationsFromMinSeq(startingSeqNo);
    }

    @Override
    public TranslogStats getTranslogStats() {
        return getTranslog().stats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog().getLastWriteLocation();
    }

    private void revisitIndexDeletionPolicyOnTranslogSynced() throws IOException {
        if (combinedDeletionPolicy.hasUnreferencedCommits()) {
            indexWriter.deleteUnusedFiles();
            translog.trimUnreferencedReaders();
        }
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /** Returns how many bytes we are currently moving from indexing buffer to segments on disk */
    @Override
    public long getWritingBytes() {
        return indexWriter.getFlushingBytes() + versionMap.getRefreshingBytes();
    }

    /**
     * Reads the current stored translog ID from the last commit data.
     */
    @Nullable
    private String loadTranslogUUIDFromLastCommit() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    /**
     * Reads the current stored history ID from the IW commit data.
     */
    private String loadHistoryUUID(final IndexWriter writer) {
        final String uuid = commitDataAsMap(writer).get(HISTORY_UUID_KEY);
        if (uuid == null) {
            throw new IllegalStateException("commit doesn't contain history uuid");
        }
        return uuid;
    }

    private ExternalReaderManager createReaderManager(RefreshWarmerListener externalRefreshListener) throws EngineException {
        boolean success = false;
        ElasticsearchReaderManager internalReaderManager = null;
        try {
            try {
                final ElasticsearchDirectoryReader directoryReader =
                    ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                internalReaderManager = new ElasticsearchReaderManager(directoryReader,
                       new RamAccountingRefreshListener(engineConfig.getCircuitBreakerService()));
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                ExternalReaderManager externalReaderManager = new ExternalReaderManager(internalReaderManager, externalRefreshListener);
                success = true;
                return externalReaderManager;
            } catch (IOException e) {
                maybeFailEngine("start", e);
                try {
                    indexWriter.rollback();
                } catch (IOException inner) { // iw is closed below
                    e.addSuppressed(inner);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            if (success == false) { // release everything we created on a failure
                IOUtils.closeWhileHandlingException(internalReaderManager, indexWriter);
            }
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Engine.Searcher> searcherFactory) throws EngineException {
        assert Objects.equals(get.uid().field(), IdFieldMapper.NAME) : get.uid().field();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            SearcherScope scope;
            if (get.realtime()) {
                VersionValue versionValue = null;
                try (Releasable ignore = versionMap.acquireLock(get.uid().bytes())) {
                    // we need to lock here to access the version map to do this truly in RT
                    versionValue = getVersionFromMap(get.uid().bytes());
                }
                if (versionValue != null) {
                    if (versionValue.isDelete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(shardId, get.id(),
                            get.versionType().explainConflictForReads(versionValue.version, get.version()));
                    }
                    if (get.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
                        get.getIfSeqNo() != versionValue.seqNo || get.getIfPrimaryTerm() != versionValue.term
                        )) {
                        throw new VersionConflictEngineException(shardId, get.id(),
                            get.getIfSeqNo(), get.getIfPrimaryTerm(), versionValue.seqNo, versionValue.term);
                    }
                    if (get.isReadFromTranslog()) {
                        // this is only used for updates - API _GET calls will always read form a reader for consistency
                        // the update call doesn't need the consistency since it's source only + _parent but parent can go away in 7.0
                        if (versionValue.getLocation() != null) {
                            try {
                                Translog.Operation operation = translog.readOperation(versionValue.getLocation());
                                if (operation != null) {
                                    // in the case of a already pruned translog generation we might get null here - yet very unlikely
                                    final Translog.Index index = (Translog.Index) operation;
                                    TranslogLeafReader reader = new TranslogLeafReader(index);
                                    return new GetResult(new Engine.Searcher("realtime_get", reader,
                                        IndexSearcher.getDefaultSimilarity(), null, IndexSearcher.getDefaultQueryCachingPolicy(), reader),
                                        new VersionsAndSeqNoResolver.DocIdAndVersion(0, index.version(), index.seqNo(), index.primaryTerm(),
                                            reader, 0));
                                }
                            } catch (IOException e) {
                                maybeFailEngine("realtime_get", e); // lets check if the translog has failed with a tragic event
                                throw new EngineException(shardId, "failed to read operation from translog", e);
                            }
                        } else {
                            trackTranslogLocation.set(true);
                        }
                    }
                    refresh("realtime_get", SearcherScope.INTERNAL, true);
                }
                scope = SearcherScope.INTERNAL;
            } else {
                // we expose what has been externally expose in a point in time snapshot via an explicit refresh
                scope = SearcherScope.EXTERNAL;
            }

            // no version, get the version from the index, we know that we refresh on flush
            return getFromSearcher(get, searcherFactory, scope);
        }
    }

    /**
     * the status of the current doc version in lucene, compared to the version in an incoming
     * operation
     */
    enum OpVsLuceneDocStatus {
        /** the op is more recent than the one that last modified the doc found in lucene*/
        OP_NEWER,
        /** the op is older or the same as the one that last modified the doc found in lucene*/
        OP_STALE_OR_EQUAL,
        /** no doc was found in lucene */
        LUCENE_DOC_NOT_FOUND
    }

    private static OpVsLuceneDocStatus compareOpToVersionMapOnSeqNo(String id, long seqNo, long primaryTerm, VersionValue versionValue) {
        Objects.requireNonNull(versionValue);
        if (seqNo > versionValue.seqNo) {
            return OpVsLuceneDocStatus.OP_NEWER;
        } else if (seqNo == versionValue.seqNo) {
            assert versionValue.term == primaryTerm : "primary term not matched; id=" + id + " seq_no=" + seqNo
                + " op_term=" + primaryTerm + " existing_term=" + versionValue.term;
            return OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        } else {
            return OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        }
    }

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnSeqNo(final Operation op) throws IOException {
        assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "resolving ops based on seq# but no seqNo is found";
        final OpVsLuceneDocStatus status;
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        assert incrementVersionLookup();
        if (versionValue != null) {
            status = compareOpToVersionMapOnSeqNo(op.id(), op.seqNo(), op.primaryTerm(), versionValue);
        } else {
            // load from index
            assert incrementIndexVersionLookup();
            try (Searcher searcher = acquireSearcher("load_seq_no", SearcherScope.INTERNAL)) {
                final DocIdAndSeqNo docAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.getIndexReader(), op.uid());
                if (docAndSeqNo == null) {
                    status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
                } else if (op.seqNo() > docAndSeqNo.seqNo) {
                    status = OpVsLuceneDocStatus.OP_NEWER;
                } else if (op.seqNo() == docAndSeqNo.seqNo) {
                    assert localCheckpointTracker.hasProcessed(op.seqNo()) || softDeleteEnabled == false :
                        "local checkpoint tracker is not updated seq_no=" + op.seqNo() + " id=" + op.id();
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                } else {
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                }
            }
        }
        return status;
    }

    /** resolves the current version of the document, returning null if not found */
    private VersionValue resolveDocVersion(final Operation op, boolean loadSeqNo) throws IOException {
        assert incrementVersionLookup(); // used for asserting in tests
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        if (versionValue == null) {
            assert incrementIndexVersionLookup(); // used for asserting in tests
            final VersionsAndSeqNoResolver.DocIdAndVersion docIdAndVersion;
            try (Searcher searcher = acquireSearcher("load_version", SearcherScope.INTERNAL)) {
                 docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(searcher.getIndexReader(), op.uid(), loadSeqNo);
            }
            if (docIdAndVersion != null) {
                versionValue = new IndexVersionValue(null, docIdAndVersion.version, docIdAndVersion.seqNo, docIdAndVersion.primaryTerm);
            }
        } else if (engineConfig.isEnableGcDeletes() && versionValue.isDelete() &&
            (engineConfig.getThreadPool().relativeTimeInMillis() - ((DeleteVersionValue)versionValue).time) > getGcDeletesInMillis()) {
            versionValue = null;
        }
        return versionValue;
    }

    private VersionValue getVersionFromMap(BytesRef id) {
        if (versionMap.isUnsafe()) {
            synchronized (versionMap) {
                // we are switching from an unsafe map to a safe map. This might happen concurrently
                // but we only need to do this once since the last operation per ID is to add to the version
                // map so once we pass this point we can safely lookup from the version map.
                if (versionMap.isUnsafe()) {
                    refresh("unsafe_version_map", SearcherScope.INTERNAL, true);
                }
                versionMap.enforceSafeAccess();
            }
        }
        return versionMap.getUnderLock(id);
    }

    private boolean canOptimizeAddDocument(Index index) {
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert index.getAutoGeneratedIdTimestamp() >= 0 : "autoGeneratedIdTimestamp must be positive but was: "
                + index.getAutoGeneratedIdTimestamp();
            switch (index.origin()) {
                case PRIMARY:
                    assert assertPrimaryCanOptimizeAddDocument(index);
                    return true;
                case PEER_RECOVERY:
                case REPLICA:
                    assert index.version() == 1 && index.versionType() == null
                        : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                case LOCAL_TRANSLOG_RECOVERY:
                case LOCAL_RESET:
                    assert index.isRetry();
                    return true; // allow to optimize in order to update the max safe time stamp
                default:
                    throw new IllegalArgumentException("unknown origin " + index.origin());
            }
        }
        return false;
    }

    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert (index.version() == Versions.MATCH_ANY && index.versionType() == VersionType.INTERNAL)
            : "version: " + index.version() + " type: " + index.versionType();
        return true;
    }

    private boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (origin == Operation.Origin.PRIMARY) {
            assert assertPrimaryIncomingSequenceNumber(origin, seqNo);
        } else {
            // sequence number should be set when operation origin is not primary
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    protected boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        // sequence number should not be set when operation origin is primary
        assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    protected long generateSeqNoForOperationOnPrimary(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        assert operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "ops should not have an assigned seq no. but was: " + operation.seqNo();
        return doGenerateSeqNoForOperation(operation);
    }

    protected void advanceMaxSeqNoOfUpdatesOrDeletesOnPrimary(long seqNo) {
        advanceMaxSeqNoOfUpdatesOrDeletes(seqNo);
    }

    /**
     * Generate the sequence number for the specified operation.
     *
     * @param operation the operation
     * @return the sequence number
     */
    long doGenerateSeqNoForOperation(final Operation operation) {
        return localCheckpointTracker.generateSeqNo();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            try (Releasable ignored = versionMap.acquireLock(index.uid().bytes());
                Releasable indexThrottle = doThrottle ? () -> {} : throttle.acquireThrottle()) {
                lastWriteNanos = index.startTime();
                /* A NOTE ABOUT APPEND ONLY OPTIMIZATIONS:
                 * if we have an autoGeneratedID that comes into the engine we can potentially optimize
                 * and just use addDocument instead of updateDocument and skip the entire version and index lookupVersion across the board.
                 * Yet, we have to deal with multiple document delivery, for this we use a property of the document that is added
                 * to detect if it has potentially been added before. We use the documents timestamp for this since it's something
                 * that:
                 *  - doesn't change per document
                 *  - is preserved in the transaction log
                 *  - and is assigned before we start to index / replicate
                 * NOTE: it's not important for this timestamp to be consistent across nodes etc. it's just a number that is in the common
                 * case increasing and can be used in the failure case when we retry and resent documents to establish a happens before
                 * relationship. For instance:
                 *  - doc A has autoGeneratedIdTimestamp = 10, isRetry = false
                 *  - doc B has autoGeneratedIdTimestamp = 9, isRetry = false
                 *
                 *  while both docs are in in flight, we disconnect on one node, reconnect and send doc A again
                 *  - now doc A' has autoGeneratedIdTimestamp = 10, isRetry = true
                 *
                 *  if A' arrives on the shard first we update maxUnsafeAutoIdTimestamp to 10 and use update document. All subsequent
                 *  documents that arrive (A and B) will also use updateDocument since their timestamps are less than
                 *  maxUnsafeAutoIdTimestamp. While this is not strictly needed for doc B it is just much simpler to implement since it
                 *  will just de-optimize some doc in the worst case.
                 *
                 *  if A arrives on the shard first we use addDocument since maxUnsafeAutoIdTimestamp is < 10. A` will then just be skipped
                 *  or calls updateDocument.
                 */
                final IndexingStrategy plan = indexingStrategyForOperation(index);

                final IndexResult indexResult;
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    indexResult = plan.earlyResultOnPreFlightError.get();
                    assert indexResult.getResultType() == Result.Type.FAILURE : indexResult.getResultType();
                } else {
                    // generate or register sequence number
                    if (index.origin() == Operation.Origin.PRIMARY) {
                        index = new Index(index.uid(), index.parsedDoc(), generateSeqNoForOperationOnPrimary(index), index.primaryTerm(),
                            index.version(), index.versionType(), index.origin(), index.startTime(), index.getAutoGeneratedIdTimestamp(),
                            index.isRetry(), index.getIfSeqNo(), index.getIfPrimaryTerm());

                        final boolean toAppend = plan.indexIntoLucene && plan.useLuceneUpdateDocument == false;
                        if (toAppend == false) {
                            advanceMaxSeqNoOfUpdatesOrDeletesOnPrimary(index.seqNo());
                        }
                    } else {
                        markSeqNoAsSeen(index.seqNo());
                    }

                    assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();

                    if (plan.indexIntoLucene || plan.addStaleOpToLucene) {
                        indexResult = indexIntoLucene(index, plan);
                    } else {
                        indexResult = new IndexResult(
                            plan.versionForIndexing, index.primaryTerm(), index.seqNo(), plan.currentNotFoundOrDeleted);
                    }
                }
                if (index.origin().isFromTranslog() == false) {
                    final Translog.Location location;
                    if (indexResult.getResultType() == Result.Type.SUCCESS) {
                        location = translog.add(new Translog.Index(index, indexResult));
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        // if we have document failure, record it as a no-op in the translog and Lucene with the generated seq_no
                        final NoOp noOp = new NoOp(indexResult.getSeqNo(), index.primaryTerm(), index.origin(),
                            index.startTime(), indexResult.getFailure().toString());
                        location = innerNoOp(noOp).getTranslogLocation();
                    } else {
                        location = null;
                    }
                    indexResult.setTranslogLocation(location);
                }
                if (plan.indexIntoLucene && indexResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location translogLocation = trackTranslogLocation.get() ? indexResult.getTranslogLocation() : null;
                    versionMap.maybePutIndexUnderLock(index.uid().bytes(),
                        new IndexVersionValue(translogLocation, plan.versionForIndexing, index.seqNo(), index.primaryTerm()));
                }
                localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
                if (indexResult.getTranslogLocation() == null) {
                    // the op is coming from the translog (and is hence persisted already) or it does not have a sequence number
                    assert index.origin().isFromTranslog() || indexResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                    localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
                }
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
            }
        } catch (RuntimeException | IOException e) {
            try {
                if (e instanceof AlreadyClosedException == false && treatDocumentFailureAsTragicError(index)) {
                    failEngine("index id[" + index.id() + "] origin[" + index.origin() + "] seq#[" + index.seqNo() + "]", e);
                } else {
                    maybeFailEngine("index id[" + index.id() + "] origin[" + index.origin() + "] seq#[" + index.seqNo() + "]", e);
                }
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    protected final IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
        assert assertNonPrimaryOrigin(index);
        // needs to maintain the auto_id timestamp in case this replica becomes primary
        if (canOptimizeAddDocument(index)) {
            mayHaveBeenIndexedBefore(index);
        }
        final IndexingStrategy plan;
        // unlike the primary, replicas don't really care to about creation status of documents
        // this allows to ignore the case where a document was found in the live version maps in
        // a delete state and return false for the created flag in favor of code simplicity
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        if (hasBeenProcessedBefore(index)) {
            // the operation seq# was processed and thus the same operation was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            plan = IndexingStrategy.processButSkipLucene(false, index.version());
        } else if (maxSeqNoOfUpdatesOrDeletes <= localCheckpointTracker.getProcessedCheckpoint()) {
            // see Engine#getMaxSeqNoOfUpdatesOrDeletes for the explanation of the optimization using sequence numbers
            assert maxSeqNoOfUpdatesOrDeletes < index.seqNo() : index.seqNo() + ">=" + maxSeqNoOfUpdatesOrDeletes;
            plan = IndexingStrategy.optimizedAppendOnly(index.version());
        } else {
            versionMap.enforceSafeAccess();
            final OpVsLuceneDocStatus opVsLucene = compareOpToLuceneDocBasedOnSeqNo(index);
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = IndexingStrategy.processAsStaleOp(softDeleteEnabled, index.version());
            } else {
                plan = IndexingStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, index.version());
            }
        }
        return plan;
    }

    protected IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        if (index.origin() == Operation.Origin.PRIMARY) {
            return planIndexingAsPrimary(index);
        } else {
            // non-primary mode (i.e., replica or recovery)
            return planIndexingAsNonPrimary(index);
        }
    }

    private IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
        assert index.origin() == Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        final IndexingStrategy plan;
        // resolve an external operation into an internal one which is safe to replay
        if (canOptimizeAddDocument(index)) {
            if (mayHaveBeenIndexedBefore(index)) {
                plan = IndexingStrategy.overrideExistingAsIfNotThere();
                versionMap.enforceSafeAccess();
            } else {
                plan = IndexingStrategy.optimizedAppendOnly(1L);
            }
        } else {
            versionMap.enforceSafeAccess();
            // resolves incoming version
            final VersionValue versionValue =
                resolveDocVersion(index, index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO);
            final long currentVersion;
            final boolean currentNotFoundOrDeleted;
            if (versionValue == null) {
                currentVersion = Versions.NOT_FOUND;
                currentNotFoundOrDeleted = true;
            } else {
                currentVersion = versionValue.version;
                currentNotFoundOrDeleted = versionValue.isDelete();
            }
            if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && versionValue == null) {
                final VersionConflictEngineException e = new VersionConflictEngineException(shardId, index.id(),
                    index.getIfSeqNo(), index.getIfPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
                plan = IndexingStrategy.skipDueToVersionConflict(e, true, currentVersion);
            } else if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
                versionValue.seqNo != index.getIfSeqNo() || versionValue.term != index.getIfPrimaryTerm()
            )) {
                final VersionConflictEngineException e = new VersionConflictEngineException(shardId, index.id(),
                    index.getIfSeqNo(), index.getIfPrimaryTerm(), versionValue.seqNo, versionValue.term);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else if (index.versionType().isVersionConflictForWrites(
                currentVersion, index.version(), currentNotFoundOrDeleted)) {
                final VersionConflictEngineException e =
                        new VersionConflictEngineException(shardId, index, currentVersion, currentNotFoundOrDeleted);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else {
                plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted,
                    index.versionType().updateVersion(currentVersion, index.version())
                );
            }
        }
        return plan;
    }

    private IndexResult indexIntoLucene(Index index, IndexingStrategy plan)
        throws IOException {
        assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();
        assert plan.versionForIndexing >= 0 : "version must be set. got " + plan.versionForIndexing;
        assert plan.indexIntoLucene || plan.addStaleOpToLucene;
        /* Update the document's sequence number and primary term; the sequence number here is derived here from either the sequence
         * number service if this is on the primary, or the existing document's sequence number if this is on the replica. The
         * primary term here has already been set, see IndexShard#prepareIndex where the Engine$Index operation is created.
         */
        index.parsedDoc().updateSeqID(index.seqNo(), index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        try {
            if (plan.addStaleOpToLucene) {
                addStaleDocs(index.docs(), indexWriter);
            } else if (plan.useLuceneUpdateDocument) {
                assert assertMaxSeqNoOfUpdatesIsAdvanced(index.uid(), index.seqNo(), true, true);
                updateDocs(index.uid(), index.docs(), indexWriter);
            } else {
                // document does not exists, we can optimize for create, but double check if assertions are running
                assert assertDocDoesNotExist(index, canOptimizeAddDocument(index) == false);
                addDocs(index.docs(), indexWriter);
            }
            return new IndexResult(plan.versionForIndexing, index.primaryTerm(), index.seqNo(), plan.currentNotFoundOrDeleted);
        } catch (Exception ex) {
            if (ex instanceof AlreadyClosedException == false &&
                indexWriter.getTragicException() == null && treatDocumentFailureAsTragicError(index) == false) {
                /* There is no tragic event recorded so this must be a document failure.
                 *
                 * The handling inside IW doesn't guarantee that an tragic / aborting exception
                 * will be used as THE tragicEventException since if there are multiple exceptions causing an abort in IW
                 * only one wins. Yet, only the one that wins will also close the IW and in turn fail the engine such that
                 * we can potentially handle the exception before the engine is failed.
                 * Bottom line is that we can only rely on the fact that if it's a document failure then
                 * `indexWriter.getTragicException()` will be null otherwise we have to rethrow and treat it as fatal or rather
                 * non-document failure
                 *
                 * we return a `MATCH_ANY` version to indicate no document was index. The value is
                 * not used anyway
                 */
                return new IndexResult(ex, Versions.MATCH_ANY, index.primaryTerm(), index.seqNo());
            } else {
                throw ex;
            }
        }
    }

    /**
     * Whether we should treat any document failure as tragic error.
     * If we hit any failure while processing an indexing on a replica, we should treat that error as tragic and fail the engine.
     * However, we prefer to fail a request individually (instead of a shard) if we hit a document failure on the primary.
     */
    private boolean treatDocumentFailureAsTragicError(Index index) {
        // TODO: can we enable this check for all origins except primary on the leader?
        return index.origin() == Operation.Origin.REPLICA
            || index.origin() == Operation.Origin.PEER_RECOVERY
            || index.origin() == Operation.Origin.LOCAL_RESET;
    }

    /**
     * returns true if the indexing operation may have already be processed by this engine.
     * Note that it is OK to rarely return true even if this is not the case. However a `false`
     * return value must always be correct.
     *
     */
    private boolean mayHaveBeenIndexedBefore(Index index) {
        assert canOptimizeAddDocument(index);
        final boolean mayHaveBeenIndexBefore;
        if (index.isRetry()) {
            mayHaveBeenIndexBefore = true;
            updateAutoIdTimestamp(index.getAutoGeneratedIdTimestamp(), true);
            assert maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            // in this case we force
            mayHaveBeenIndexBefore = maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
            updateAutoIdTimestamp(index.getAutoGeneratedIdTimestamp(), false);
        }
        return mayHaveBeenIndexBefore;
    }

    private void addDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
        numDocAppends.inc(docs.size());
    }

    private void addStaleDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        assert softDeleteEnabled : "Add history documents but soft-deletes is disabled";
        for (ParseContext.Document doc : docs) {
            doc.add(softDeletesField); // soft-deleted every document before adding to Lucene
        }
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    protected static final class IndexingStrategy {
        final boolean currentNotFoundOrDeleted;
        final boolean useLuceneUpdateDocument;
        final long versionForIndexing;
        final boolean indexIntoLucene;
        final boolean addStaleOpToLucene;
        final Optional<IndexResult> earlyResultOnPreFlightError;

        private IndexingStrategy(boolean currentNotFoundOrDeleted, boolean useLuceneUpdateDocument,
                                 boolean indexIntoLucene, boolean addStaleOpToLucene,
                                 long versionForIndexing, IndexResult earlyResultOnPreFlightError) {
            assert useLuceneUpdateDocument == false || indexIntoLucene :
                "use lucene update is set to true, but we're not indexing into lucene";
            assert (indexIntoLucene && earlyResultOnPreFlightError != null) == false :
                "can only index into lucene or have a preflight result but not both." +
                    "indexIntoLucene: " + indexIntoLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
            this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
            this.useLuceneUpdateDocument = useLuceneUpdateDocument;
            this.versionForIndexing = versionForIndexing;
            this.indexIntoLucene = indexIntoLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.earlyResultOnPreFlightError =
                earlyResultOnPreFlightError == null ? Optional.empty() :
                    Optional.of(earlyResultOnPreFlightError);
        }

        static IndexingStrategy optimizedAppendOnly(long versionForIndexing) {
            return new IndexingStrategy(true, false, true, false, versionForIndexing, null);
        }

        public static IndexingStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, boolean currentNotFoundOrDeleted, long currentVersion) {
            final IndexResult result = new IndexResult(e, currentVersion);
            return new IndexingStrategy(
                    currentNotFoundOrDeleted, false, false, false,
                Versions.NOT_FOUND, result);
        }

        static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted,
                                                long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, currentNotFoundOrDeleted == false,
                true, false, versionForIndexing, null);
        }

        static IndexingStrategy overrideExistingAsIfNotThere() {
            return new IndexingStrategy(true, true, true,
                false, 1L, null);
        }

        public static IndexingStrategy processButSkipLucene(boolean currentNotFoundOrDeleted, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false,
                false, versionForIndexing, null);
        }

        static IndexingStrategy processAsStaleOp(boolean addStaleOpToLucene, long versionForIndexing) {
            return new IndexingStrategy(false, false, false,
                addStaleOpToLucene, versionForIndexing, null);
        }
    }

    /**
     * Asserts that the doc in the index operation really doesn't exist
     */
    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        // NOTE this uses direct access to the version map since we are in the assertion code where we maintain a secondary
        // map in the version map such that we don't need to refresh if we are unsafe;
        final VersionValue versionValue = versionMap.getVersionForAssert(index.uid().bytes());
        if (versionValue != null) {
            if (versionValue.isDelete() == false || allowDeleted == false) {
                throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists in version map (version " +
                    versionValue + ")");
            }
        } else {
            try (Searcher searcher = acquireSearcher("assert doc doesn't exist", SearcherScope.INTERNAL)) {
                final long docsWithId = searcher.count(new TermQuery(index.uid()));
                if (docsWithId > 0) {
                    throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists [" + docsWithId +
                        "] times in index");
                }
            }
        }
        return true;
    }

    private void updateDocs(final Term uid, final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (softDeleteEnabled) {
            if (docs.size() > 1) {
                indexWriter.softUpdateDocuments(uid, docs, softDeletesField);
            } else {
                indexWriter.softUpdateDocument(uid, docs.get(0), softDeletesField);
            }
        } else {
            if (docs.size() > 1) {
                indexWriter.updateDocuments(uid, docs);
            } else {
                indexWriter.updateDocument(uid, docs.get(0));
            }
        }
        numDocUpdates.inc(docs.size());
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        versionMap.enforceSafeAccess();
        assert Objects.equals(delete.uid().field(), IdFieldMapper.NAME) : delete.uid().field();
        assert assertIncomingSequenceNumber(delete.origin(), delete.seqNo());
        final DeleteResult deleteResult;
        // NOTE: we don't throttle this when merges fall behind because delete-by-id does not create new segments:
        try (ReleasableLock ignored = readLock.acquire(); Releasable ignored2 = versionMap.acquireLock(delete.uid().bytes())) {
            ensureOpen();
            lastWriteNanos = delete.startTime();
            final DeletionStrategy plan = deletionStrategyForOperation(delete);

            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else {
                // generate or register sequence number
                if (delete.origin() == Operation.Origin.PRIMARY) {
                    delete = new Delete(delete.type(), delete.id(), delete.uid(), generateSeqNoForOperationOnPrimary(delete),
                        delete.primaryTerm(), delete.version(), delete.versionType(), delete.origin(), delete.startTime(),
                        delete.getIfSeqNo(), delete.getIfPrimaryTerm());

                    advanceMaxSeqNoOfUpdatesOrDeletesOnPrimary(delete.seqNo());
                } else {
                    markSeqNoAsSeen(delete.seqNo());
                }

                assert delete.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + delete.origin();

                if (plan.deleteFromLucene || plan.addStaleOpToLucene) {
                    deleteResult = deleteInLucene(delete, plan);
                } else {
                    deleteResult = new DeleteResult(
                        plan.versionOfDeletion, delete.primaryTerm(), delete.seqNo(), plan.currentlyDeleted == false);
                }
            }
            if (delete.origin().isFromTranslog() == false && deleteResult.getResultType() == Result.Type.SUCCESS) {
                final Translog.Location location = translog.add(new Translog.Delete(delete, deleteResult));
                deleteResult.setTranslogLocation(location);
            }
            localCheckpointTracker.markSeqNoAsProcessed(deleteResult.getSeqNo());
            if (deleteResult.getTranslogLocation() == null) {
                // the op is coming from the translog (and is hence persisted already) or does not have a sequence number (version conflict)
                assert delete.origin().isFromTranslog() || deleteResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                localCheckpointTracker.markSeqNoAsPersisted(deleteResult.getSeqNo());
            }
            deleteResult.setTook(System.nanoTime() - delete.startTime());
            deleteResult.freeze();
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("delete", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        maybePruneDeletes();
        return deleteResult;
    }

    protected DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        if (delete.origin() == Operation.Origin.PRIMARY) {
            return planDeletionAsPrimary(delete);
        } else {
            // non-primary mode (i.e., replica or recovery)
            return planDeletionAsNonPrimary(delete);
        }
    }

    protected final DeletionStrategy planDeletionAsNonPrimary(Delete delete) throws IOException {
        assert assertNonPrimaryOrigin(delete);
        final DeletionStrategy plan;
        if (hasBeenProcessedBefore(delete)) {
            // the operation seq# was processed thus this operation was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            plan = DeletionStrategy.processButSkipLucene(false, delete.version());
        } else {
            final OpVsLuceneDocStatus opVsLucene = compareOpToLuceneDocBasedOnSeqNo(delete);
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = DeletionStrategy.processAsStaleOp(softDeleteEnabled, delete.version());
            } else {
                plan = DeletionStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, delete.version());
            }
        }
        return plan;
    }

    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        assert operation.origin() != Operation.Origin.PRIMARY : "planing as primary but got " + operation.origin();
        return true;
    }

    private DeletionStrategy planDeletionAsPrimary(Delete delete) throws IOException {
        assert delete.origin() == Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        // resolve operation from external to internal
        final VersionValue versionValue = resolveDocVersion(delete, delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO);
        assert incrementVersionLookup();
        final long currentVersion;
        final boolean currentlyDeleted;
        if (versionValue == null) {
            currentVersion = Versions.NOT_FOUND;
            currentlyDeleted = true;
        } else {
            currentVersion = versionValue.version;
            currentlyDeleted = versionValue.isDelete();
        }
        final DeletionStrategy plan;
        if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && versionValue == null) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete.id(),
                delete.getIfSeqNo(), delete.getIfPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, true);
        } else if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
            versionValue.seqNo != delete.getIfSeqNo() || versionValue.term != delete.getIfPrimaryTerm()
        )) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete.id(),
                delete.getIfSeqNo(), delete.getIfPrimaryTerm(), versionValue.seqNo, versionValue.term);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
        } else if (delete.versionType().isVersionConflictForWrites(currentVersion, delete.version(), currentlyDeleted)) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete, currentVersion, currentlyDeleted);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
        } else {
            plan = DeletionStrategy.processNormally(currentlyDeleted, delete.versionType().updateVersion(currentVersion, delete.version()));
        }
        return plan;
    }

    private DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan) throws IOException {
        assert assertMaxSeqNoOfUpdatesIsAdvanced(delete.uid(), delete.seqNo(), false, false);
        try {
            if (softDeleteEnabled) {
                final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newDeleteTombstoneDoc(delete.type(), delete.id());
                assert tombstone.docs().size() == 1 : "Tombstone doc should have single doc [" + tombstone + "]";
                tombstone.updateSeqID(delete.seqNo(), delete.primaryTerm());
                tombstone.version().setLongValue(plan.versionOfDeletion);
                final ParseContext.Document doc = tombstone.docs().get(0);
                assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null :
                    "Delete tombstone document but _tombstone field is not set [" + doc + " ]";
                doc.add(softDeletesField);
                if (plan.addStaleOpToLucene || plan.currentlyDeleted) {
                    indexWriter.addDocument(doc);
                } else {
                    indexWriter.softUpdateDocument(delete.uid(), doc, softDeletesField);
                }
            } else if (plan.currentlyDeleted == false) {
                // any exception that comes from this is a either an ACE or a fatal exception there
                // can't be any document failures  coming from this
                indexWriter.deleteDocuments(delete.uid());
            }
            if (plan.deleteFromLucene) {
                numDocDeletes.inc();
                versionMap.putDeleteUnderLock(delete.uid().bytes(),
                    new DeleteVersionValue(plan.versionOfDeletion, delete.seqNo(), delete.primaryTerm(),
                        engineConfig.getThreadPool().relativeTimeInMillis()));
            }
            return new DeleteResult(
                plan.versionOfDeletion, delete.primaryTerm(), delete.seqNo(), plan.currentlyDeleted == false);
        } catch (final Exception ex) {
            /*
             * Document level failures when deleting are unexpected, we likely hit something fatal such as the Lucene index being corrupt,
             * or the Lucene document limit. We have already issued a sequence number here so this is fatal, fail the engine.
             */
            if (ex instanceof AlreadyClosedException == false && indexWriter.getTragicException() == null) {
                final String reason = String.format(
                    Locale.ROOT,
                    "delete id[%s] origin [%s] seq#[%d] failed at the document level",
                    delete.id(),
                    delete.origin(),
                    delete.seqNo());
                failEngine(reason, ex);
            }
            throw ex;
        }
    }

    protected static final class DeletionStrategy {
        // of a rare double delete
        final boolean deleteFromLucene;
        final boolean addStaleOpToLucene;
        final boolean currentlyDeleted;
        final long versionOfDeletion;
        final Optional<DeleteResult> earlyResultOnPreflightError;

        private DeletionStrategy(boolean deleteFromLucene, boolean addStaleOpToLucene, boolean currentlyDeleted,
                                 long versionOfDeletion, DeleteResult earlyResultOnPreflightError) {
            assert (deleteFromLucene && earlyResultOnPreflightError != null) == false :
                "can only delete from lucene or have a preflight result but not both." +
                    "deleteFromLucene: " + deleteFromLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
            this.deleteFromLucene = deleteFromLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.currentlyDeleted = currentlyDeleted;
            this.versionOfDeletion = versionOfDeletion;
            this.earlyResultOnPreflightError = earlyResultOnPreflightError == null ?
                Optional.empty() : Optional.of(earlyResultOnPreflightError);
        }

        public static DeletionStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, long currentVersion, boolean currentlyDeleted) {
            final DeleteResult deleteResult = new DeleteResult(e, currentVersion, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                SequenceNumbers.UNASSIGNED_SEQ_NO, currentlyDeleted == false);
            return new DeletionStrategy(false, false, currentlyDeleted, Versions.NOT_FOUND, deleteResult);
        }

        static DeletionStrategy processNormally(boolean currentlyDeleted, long versionOfDeletion) {
            return new DeletionStrategy(true, false, currentlyDeleted, versionOfDeletion, null);

        }

        public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted, long versionOfDeletion) {
            return new DeletionStrategy(false, false, currentlyDeleted, versionOfDeletion, null);
        }

        static DeletionStrategy processAsStaleOp(boolean addStaleOpToLucene, long versionOfDeletion) {
            return new DeletionStrategy(false, addStaleOpToLucene, false, versionOfDeletion, null);
        }
    }

    @Override
    public void maybePruneDeletes() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        if (engineConfig.isEnableGcDeletes() &&
                engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    @Override
    public NoOpResult noOp(final NoOp noOp) throws IOException {
        final NoOpResult noOpResult;
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            noOpResult = innerNoOp(noOp);
        } catch (final Exception e) {
            try {
                maybeFailEngine("noop", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        return noOpResult;
    }

    private NoOpResult innerNoOp(final NoOp noOp) throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        assert noOp.seqNo() > SequenceNumbers.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();
        try (Releasable ignored = noOpKeyedLock.acquire(seqNo)) {
            final NoOpResult noOpResult;
            final Optional<Exception> preFlightError = preFlightCheckForNoOp(noOp);
            if (preFlightError.isPresent()) {
                noOpResult = new NoOpResult(SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    SequenceNumbers.UNASSIGNED_SEQ_NO, preFlightError.get());
            } else {
                markSeqNoAsSeen(noOp.seqNo());
                if (softDeleteEnabled && hasBeenProcessedBefore(noOp) == false) {
                    try {
                        final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newNoopTombstoneDoc(noOp.reason());
                        tombstone.updateSeqID(noOp.seqNo(), noOp.primaryTerm());
                        // A noop tombstone does not require a _version but it's added to have a fully dense docvalues for the version
                        // field. 1L is selected to optimize the compression because it might probably be the most common value in
                        // version field.
                        tombstone.version().setLongValue(1L);
                        assert tombstone.docs().size() == 1 : "Tombstone should have a single doc [" + tombstone + "]";
                        final ParseContext.Document doc = tombstone.docs().get(0);
                        assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null
                            : "Noop tombstone document but _tombstone field is not set [" + doc + " ]";
                        doc.add(softDeletesField);
                        indexWriter.addDocument(doc);
                    } catch (final Exception ex) {
                        /*
                         * Document level failures when adding a no-op are unexpected, we likely hit something fatal such as the Lucene
                         * index being corrupt, or the Lucene document limit. We have already issued a sequence number here so this is
                         * fatal, fail the engine.
                         */
                        if (ex instanceof AlreadyClosedException == false && indexWriter.getTragicException() == null) {
                            failEngine("no-op origin[" + noOp.origin() + "] seq#[" + noOp.seqNo() + "] failed at document level", ex);
                        }
                        throw ex;
                    }
                }
                noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
                if (noOp.origin().isFromTranslog() == false && noOpResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                    noOpResult.setTranslogLocation(location);
                }
            }
            localCheckpointTracker.markSeqNoAsProcessed(noOpResult.getSeqNo());
            if (noOpResult.getTranslogLocation() == null) {
                // the op is coming from the translog (and is hence persisted already) or it does not have a sequence number
                assert noOp.origin().isFromTranslog() || noOpResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                localCheckpointTracker.markSeqNoAsPersisted(noOpResult.getSeqNo());
            }
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        }
    }

    /**
     * Executes a pre-flight check for a given NoOp.
     * If this method returns a non-empty result, the engine won't process this NoOp and returns a failure.
     */
    protected Optional<Exception> preFlightCheckForNoOp(final NoOp noOp) throws IOException {
        return Optional.empty();
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL, true);
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return refresh(source, SearcherScope.EXTERNAL, false);
    }

    final boolean refresh(String source, SearcherScope scope, boolean block) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        // both refresh types will result in an internal refresh but only the external will also
        // pass the new reader reference to the external reader manager.
        final long localCheckpointBeforeRefresh = localCheckpointTracker.getProcessedCheckpoint();
        boolean refreshed;
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (store.tryIncRef()) {
                // increment the ref just to ensure nobody closes the store during a refresh
                try {
                    // even though we maintain 2 managers we really do the heavy-lifting only once.
                    // the second refresh will only do the extra work we have to do for warming caches etc.
                    ReferenceManager<ElasticsearchDirectoryReader> referenceManager = getReferenceManager(scope);
                    // it is intentional that we never refresh both internal / external together
                    if (block) {
                        referenceManager.maybeRefreshBlocking();
                        refreshed = true;
                    } else {
                        refreshed = referenceManager.maybeRefresh();
                    }
                } finally {
                    store.decRef();
                }
                if (refreshed) {
                    lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
                }
            } else {
                refreshed = false;
            }
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed source[" + source + "]", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
        assert refreshed == false || lastRefreshedCheckpoint() >= localCheckpointBeforeRefresh : "refresh checkpoint was not advanced; " +
            "local_checkpoint=" + localCheckpointBeforeRefresh + " refresh_checkpoint=" + lastRefreshedCheckpoint();
        // TODO: maybe we should just put a scheduled job in threadPool?
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        maybePruneDeletes();
        mergeScheduler.refreshConfig();
        return refreshed;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are writing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        refresh("write indexing buffer", SearcherScope.INTERNAL, true);
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        // best effort attempt before we acquire locks
        ensureOpen();
        if (indexWriter.hasUncommittedChanges()) {
            logger.trace("can't sync commit [{}]. have pending changes", syncId);
            return SyncedFlushResult.PENDING_OPERATIONS;
        }
        if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
            logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
            return SyncedFlushResult.COMMIT_MISMATCH;
        }
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            ensureCanFlush();
            // lets do a refresh to make sure we shrink the version map. This refresh will be either a no-op (just shrink the version map)
            // or we also have uncommitted changes and that causes this syncFlush to fail.
            refresh("sync_flush", SearcherScope.INTERNAL, true);
            if (indexWriter.hasUncommittedChanges()) {
                logger.trace("can't sync commit [{}]. have pending changes", syncId);
                return SyncedFlushResult.PENDING_OPERATIONS;
            }
            if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
                logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
                return SyncedFlushResult.COMMIT_MISMATCH;
            }
            logger.trace("starting sync commit [{}]", syncId);
            commitIndexWriter(indexWriter, translog, syncId);
            logger.debug("successfully sync committed. sync id [{}].", syncId);
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            return SyncedFlushResult.SUCCESS;
        } catch (IOException ex) {
            maybeFailEngine("sync commit", ex);
            throw new EngineException(shardId, "failed to sync commit", ex);
        }
    }

    final boolean tryRenewSyncCommit() {
        boolean renewed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            ensureCanFlush();
            String syncId = lastCommittedSegmentInfos.getUserData().get(SYNC_COMMIT_ID);
            long translogGenOfLastCommit = Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));
            if (syncId != null && indexWriter.hasUncommittedChanges() && translog.totalOperationsByMinGen(translogGenOfLastCommit) == 0) {
                logger.trace("start renewing sync commit [{}]", syncId);
                commitIndexWriter(indexWriter, translog, syncId);
                logger.debug("successfully sync committed. sync id [{}].", syncId);
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                renewed = true;
            }
        } catch (IOException ex) {
            maybeFailEngine("renew sync commit", ex);
            throw new EngineException(shardId, "failed to renew sync commit", ex);
        }
        if (renewed) {
            // refresh outside of the write lock
            // we have to refresh internal reader here to ensure we release unreferenced segments.
            refresh("renew sync commit", SearcherScope.INTERNAL, true);
        }
        return renewed;
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        if (shouldPeriodicallyFlushAfterBigMerge.get()) {
            return true;
        }
        final long translogGenerationOfLastCommit =
            Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));
        final long flushThreshold = config().getIndexSettings().getFlushThresholdSize().getBytes();
        if (translog.sizeInBytesByMinGen(translogGenerationOfLastCommit) < flushThreshold) {
            return false;
        }
        /*
         * We flush to reduce the size of uncommitted translog but strictly speaking the uncommitted size won't always be
         * below the flush-threshold after a flush. To avoid getting into an endless loop of flushing, we only enable the
         * periodically flush condition if this condition is disabled after a flush. The condition will change if the new
         * commit points to the later generation the last commit's(eg. gen-of-last-commit < gen-of-new-commit)[1].
         *
         * When the local checkpoint equals to max_seqno, and translog-gen of the last commit equals to translog-gen of
         * the new commit, we know that the last generation must contain operations because its size is above the flush
         * threshold and the flush-threshold is guaranteed to be higher than an empty translog by the setting validation.
         * This guarantees that the new commit will point to the newly rolled generation. In fact, this scenario only
         * happens when the generation-threshold is close to or above the flush-threshold; otherwise we have rolled
         * generations as the generation-threshold was reached, then the first condition (eg. [1]) is already satisfied.
         *
         * This method is to maintain translog only, thus IndexWriter#hasUncommittedChanges condition is not considered.
         */
        final long translogGenerationOfNewCommit =
            translog.getMinGenerationForSeqNo(localCheckpointTracker.getProcessedCheckpoint() + 1).translogFileGeneration;
        return translogGenerationOfLastCommit < translogGenerationOfNewCommit
            || localCheckpointTracker.getProcessedCheckpoint() == localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        if (force && waitIfOngoing == false) {
            assert false : "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing;
            throw new IllegalArgumentException(
                "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing);
        }
        final byte[] newCommitId;
        /*
         * Unfortunately the lock order is important here. We have to acquire the readlock first otherwise
         * if we are flushing at the end of the recovery while holding the write lock we can deadlock if:
         *  Thread 1: flushes via API and gets the flush lock but blocks on the readlock since Thread 2 has the writeLock
         *  Thread 2: flushes at the end of the recovery holding the writeLock and blocks on the flushLock owned by Thread 1
         */
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                // if we can't get the lock right away we block if needed otherwise barf
                if (waitIfOngoing) {
                    logger.trace("waiting for in-flight flush to finish");
                    flushLock.lock();
                    logger.trace("acquired flush lock after blocking");
                } else {
                    return new CommitId(lastCommittedSegmentInfos.getId());
                }
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                // Only flush if (1) Lucene has uncommitted docs, or (2) forced by caller, or (3) the
                // newly created commit points to a different translog generation (can free translog)
                boolean hasUncommittedChanges = indexWriter.hasUncommittedChanges();
                boolean shouldPeriodicallyFlush = shouldPeriodicallyFlush();
                if (hasUncommittedChanges || force || shouldPeriodicallyFlush) {
                    ensureCanFlush();
                    try {
                        translog.rollGeneration();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");

                        // a temporary debugging to investigate test failure - issue#32827. Remove when the issue is resolved
                        logger.debug("new commit on flush, hasUncommittedChanges:{}, force:{}, shouldPeriodicallyFlush:{}",
                            hasUncommittedChanges, force, shouldPeriodicallyFlush);

                        // we need to refresh in order to clear older version values
                        refresh("version_table_flush", SearcherScope.INTERNAL, true);
                        translog.trimUnreferencedReaders();
                    } catch (AlreadyClosedException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                    refreshLastCommittedSegmentInfos();

                }
                newCommitId = lastCommittedSegmentInfos.getId();
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } finally {
                flushLock.unlock();
            }
        }
        // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
        // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
        if (engineConfig.isEnableGcDeletes()) {
            pruneDeletedTombstones();
        }
        return new CommitId(newCommitId);
    }

    private void refreshLastCommittedSegmentInfos() {
    /*
     * we have to inc-ref the store here since if the engine is closed by a tragic event
     * we don't acquire the write lock and wait until we have exclusive access. This might also
     * dec the store reference which can essentially close the store and unless we can inc the reference
     * we can't use it.
     */
        store.incRef();
        try {
            // reread the last committed segment infos
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Exception e) {
            if (isClosed.get() == false) {
                try {
                    logger.warn("failed to read latest segment infos on flush", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                if (Lucene.isCorruptionException(e)) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        } finally {
            store.decRef();
        }
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to roll translog", e);
        }
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog", e);
        }
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return getTranslog().shouldRollGeneration();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog operations trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog operations", e);
        }
    }

    private void pruneDeletedTombstones() {
        /*
         * We need to deploy two different trimming strategies for GC deletes on primary and replicas. Delete operations on primary
         * are remembered for at least one GC delete cycle and trimmed periodically. This is, at the moment, the best we can do on
         * primary for user facing APIs but this arbitrary time limit is problematic for replicas. On replicas however we should
         * trim only deletes whose seqno at most the local checkpoint. This requirement is explained as follows.
         *
         * Suppose o1 and o2 are two operations on the same document with seq#(o1) < seq#(o2), and o2 arrives before o1 on the replica.
         * o2 is processed normally since it arrives first; when o1 arrives it should be discarded:
         * - If seq#(o1) <= LCP, then it will be not be added to Lucene, as it was already previously added.
         * - If seq#(o1)  > LCP, then it depends on the nature of o2:
         *   *) If o2 is a delete then its seq# is recorded in the VersionMap, since seq#(o2) > seq#(o1) > LCP,
         *      so a lookup can find it and determine that o1 is stale.
         *   *) If o2 is an indexing then its seq# is either in Lucene (if refreshed) or the VersionMap (if not refreshed yet),
         *      so a real-time lookup can find it and determine that o1 is stale.
         *
         * Here we prefer to deploy a single trimming strategy, which satisfies two constraints, on both primary and replicas because:
         * - It's simpler - no need to distinguish if an engine is running at primary mode or replica mode or being promoted.
         * - If a replica subsequently is promoted, user experience is maintained as that replica remembers deletes for the last GC cycle.
         *
         * However, the version map may consume less memory if we deploy two different trimming strategies for primary and replicas.
         */
        final long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        final long maxTimestampToPrune = timeMSec - engineConfig.getIndexSettings().getGcDeletesInMillis();
        versionMap.pruneTombstones(maxTimestampToPrune, localCheckpointTracker.getProcessedCheckpoint());
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    // testing
    void clearDeletedTombstones() {
        versionMap.pruneTombstones(Long.MAX_VALUE, localCheckpointTracker.getMaxSeqNo());
    }

    // for testing
    final Map<BytesRef, VersionValue> getVersionMap() {
        return Stream.concat(versionMap.getAllCurrent().entrySet().stream(), versionMap.getAllTombstones().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           final boolean upgrade, final boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
        if (onlyExpungeDeletes && maxNumSegments >= 0) {
            throw new IllegalArgumentException("only_expunge_deletes and max_num_segments are mutually exclusive");
        }
        /*
         * We do NOT acquire the readlock here since we are waiting on the merges to finish
         * that's fine since the IW.rollback should stop all the threads and trigger an IOException
         * causing us to fail the forceMerge
         *
         * The way we implement upgrades is a bit hackish in the sense that we set an instance
         * variable and that this setting will thus apply to the next forced merge that will be run.
         * This is ok because (1) this is the only place we call forceMerge, (2) we have a single
         * thread for optimize, and the 'optimizeLock' guarding this code, and (3) ConcurrentMergeScheduler
         * syncs calls to findForcedMerges.
         */
        assert indexWriter.getConfig().getMergePolicy() instanceof ElasticsearchMergePolicy : "MergePolicy is " +
            indexWriter.getConfig().getMergePolicy().getClass().getName();
        ElasticsearchMergePolicy mp = (ElasticsearchMergePolicy) indexWriter.getConfig().getMergePolicy();
        optimizeLock.lock();
        try {
            ensureOpen();
            if (upgrade) {
                logger.info("starting segment upgrade upgradeOnlyAncientSegments={}", upgradeOnlyAncientSegments);
                mp.setUpgradeInProgress(true, upgradeOnlyAncientSegments);
            }
            store.incRef(); // increment the ref just to ensure nobody closes the store while we optimize
            try {
                if (onlyExpungeDeletes) {
                    assert upgrade == false;
                    indexWriter.forceMergeDeletes(true /* blocks and waits for merges*/);
                } else if (maxNumSegments <= 0) {
                    assert upgrade == false;
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.forceMerge(maxNumSegments, true /* blocks and waits for merges*/);
                }
                if (flush) {
                    if (tryRenewSyncCommit() == false) {
                        flush(false, true);
                    }
                }
                if (upgrade) {
                    logger.info("finished segment upgrade");
                }
            } finally {
                store.decRef();
            }
        } catch (AlreadyClosedException ex) {
            /* in this case we first check if the engine is still open. If so this exception is just fine
             * and expected. We don't hold any locks while we block on forceMerge otherwise it would block
             * closing the engine as well. If we are not closed we pass it on to failOnTragicEvent which ensures
             * we are handling a tragic even exception here */
            ensureOpen(ex);
            failOnTragicEvent(ex);
            throw ex;
        } catch (Exception e) {
            try {
                maybeFailEngine("force merge", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            try {
                // reset it just to make sure we reset it in a case of an error
                mp.setUpgradeInProgress(false, false);
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(final boolean flushFirst) throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        final IndexCommit lastCommit = combinedDeletionPolicy.acquireIndexCommit(false);
        return new Engine.IndexCommitRef(lastCommit, () -> releaseIndexCommit(lastCommit));
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
        return new Engine.IndexCommitRef(safeCommit, () -> releaseIndexCommit(safeCommit));
    }

    private void releaseIndexCommit(IndexCommit snapshot) throws IOException {
        // Revisit the deletion policy if we can clean up the snapshotting commit.
        if (combinedDeletionPolicy.releaseCommit(snapshot)) {
            ensureOpen();
            // Here we don't have to trim translog because snapshotting an index commit
            // does not lock translog or prevents unreferenced files from trimming.
            indexWriter.deleteUnusedFiles();
        }
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return combinedDeletionPolicy.getSafeCommitInfo();
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        // if we are already closed due to some tragic exception
        // we need to fail the engine. it might have already been failed before
        // but we are double-checking it's failed and closed
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            final Exception tragicException;
            if (indexWriter.getTragicException() instanceof Exception) {
                tragicException = (Exception) indexWriter.getTragicException();
            } else {
                tragicException = new RuntimeException(indexWriter.getTragicException());
            }
            failEngine("already closed by tragic event on the index writer", tragicException);
            engineFailed = true;
        } else if (translog.isOpen() == false && translog.getTragicException() != null) {
            failEngine("already closed by tragic event on the translog", translog.getTragicException());
            engineFailed = true;
        } else if (failedEngine.get() == null && isClosed.get() == false) { // we are closed but the engine is not failed yet?
            // this smells like a bug - we only expect ACE if we are in a fatal case ie. either translog or IW is closed by
            // a tragic event or has closed itself. if that is not the case we are in a buggy state and raise an assertion error
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        } else {
            engineFailed = false;
        }
        return engineFailed;
    }

    @Override
    protected boolean maybeFailEngine(String source, Exception e) {
        boolean shouldFail = super.maybeFailEngine(source, e);
        if (shouldFail) {
            return true;
        }
        // Check for AlreadyClosedException -- ACE is a very special
        // exception that should only be thrown in a tragic event. we pass on the checks to failOnTragicEvent which will
        // throw and AssertionError if the tragic event condition is not met.
        if (e instanceof AlreadyClosedException) {
            return failOnTragicEvent((AlreadyClosedException)e);
        } else if (e != null &&
                ((indexWriter.isOpen() == false && indexWriter.getTragicException() == e)
                        || (translog.isOpen() == false && translog.getTragicException() == e))) {
            // this spot on - we are handling the tragic event exception here so we have to fail the engine
            // right away
            failEngine(source, e);
            return true;
        }
        return false;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected final void writerSegmentStats(SegmentsStats stats) {
        stats.addVersionMapMemoryInBytes(versionMap.ramBytesUsed());
        stats.addIndexWriterMemoryInBytes(indexWriter.ramBytesUsed());
        stats.updateMaxUnsafeAutoIdTimestamp(maxUnsafeAutoIdTimestamp.get());
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        // We don't guard w/ readLock here, so we could throw AlreadyClosedException
        return indexWriter.ramBytesUsed() + versionMap.ramBytesUsedForRefresh();
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);

            // fill in the merges flag
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }
            return Arrays.asList(segmentsArr);
        }
    }

    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() :
                "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                if (internalReaderManager != null) {
                    internalReaderManager.removeListener(versionMap);
                }
                try {
                    IOUtils.close(externalReaderManager, internalReaderManager);
                } catch (Exception e) {
                    logger.warn("Failed to close ReaderManager", e);
                }
                try {
                    IOUtils.close(translog);
                } catch (Exception e) {
                    logger.warn("Failed to close translog", e);
                }
                // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
                logger.trace("rollback indexWriter");
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException ex) {
                    failOnTragicEvent(ex);
                    throw ex;
                }
                logger.trace("rollback indexWriter done");
            } catch (Exception e) {
                logger.warn("failed to rollback writer on close", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    @Override
    protected final ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        switch (scope) {
            case INTERNAL:
                return internalReaderManager;
            case EXTERNAL:
                return externalReaderManager;
            default:
                throw new IllegalStateException("unknown scope: " + scope);
        }
    }

    private IndexWriter createWriter() throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig();
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    // pkg-private for testing
    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        if (Assertions.ENABLED) {
            return new AssertingIndexWriter(directory, iwc);
        } else {
            return new IndexWriter(directory, iwc);
        }
    }

    static Map<String, String> getReaderAttributes(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        boolean defaultOffHeap = FsDirectoryFactory.isHybridFs(unwrap) || unwrap instanceof MMapDirectory;
        return Map.of(
            BlockTreeTermsReader.FST_MODE_KEY, // if we are using MMAP for term dics we force all off heap unless it's the ID field
            defaultOffHeap ? FSTLoadMode.OFF_HEAP.name() : FSTLoadMode.ON_HEAP.name()
            , BlockTreeTermsReader.FST_MODE_KEY + "." + IdFieldMapper.NAME, // always force ID field on-heap for fast updates
            FSTLoadMode.ON_HEAP.name());
    }

    private IndexWriterConfig getIndexWriterConfig() {
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCommitOnClose(false); // we by default don't commit on close
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        iwc.setReaderAttributes(getReaderAttributes(store.directory()));
        iwc.setIndexDeletionPolicy(combinedDeletionPolicy);
        // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {
        }
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        iwc.setMergeScheduler(mergeScheduler);
        // Give us the opportunity to upgrade old segments while performing
        // background merges
        MergePolicy mergePolicy = config().getMergePolicy();
        // always configure soft-deletes field so an engine with soft-deletes disabled can open a Lucene index with soft-deletes.
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        if (softDeleteEnabled) {
            mergePolicy = new RecoverySourcePruneMergePolicy(SourceFieldMapper.RECOVERY_SOURCE_NAME, softDeletesPolicy::getRetentionQuery,
                new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, softDeletesPolicy::getRetentionQuery,
                    new PrunePostingsMergePolicy(mergePolicy, IdFieldMapper.NAME)));
        }
        iwc.setMergePolicy(new ElasticsearchMergePolicy(mergePolicy));
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setCodec(engineConfig.getCodec());
        iwc.setUseCompoundFile(true); // always use compound on flush - reduces # of file-handles on refresh
        if (config().getIndexSort() != null) {
            iwc.setIndexSort(config().getIndexSort());
        }
        return iwc;
    }

    /** A listener that warms the segments if needed when acquiring a new reader */
    static final class RefreshWarmerListener implements BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> {
        private final Engine.Warmer warmer;
        private final Logger logger;
        private final AtomicBoolean isEngineClosed;

        RefreshWarmerListener(Logger logger, AtomicBoolean isEngineClosed, EngineConfig engineConfig) {
            warmer = engineConfig.getWarmer();
            this.logger = logger;
            this.isEngineClosed = isEngineClosed;
        }

        @Override
        public void accept(ElasticsearchDirectoryReader reader, ElasticsearchDirectoryReader previousReader) {
            if (warmer != null) {
                try {
                    warmer.warm(reader);
                } catch (Exception e) {
                    if (isEngineClosed.get() == false) {
                        logger.warn("failed to prepare/warm", e);
                    }
                }
            }
        }
    }

    @Override
    public void activateThrottling() {
        int count = throttleRequestCount.incrementAndGet();
        assert count >= 1 : "invalid post-increment throttleRequestCount=" + count;
        if (count == 1) {
            throttle.activate();
        }
    }

    @Override
    public void deactivateThrottling() {
        int count = throttleRequestCount.decrementAndGet();
        assert count >= 0 : "invalid post-decrement throttleRequestCount=" + count;
        if (count == 0) {
            throttle.deactivate();
        }
    }

    @Override
    public boolean isThrottled() {
        return throttle.isThrottled();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttle.getThrottleTimeInMillis();
    }

    long getGcDeletesInMillis() {
        return engineConfig.getIndexSettings().getGcDeletesInMillis();
    }

    LiveIndexWriterConfig getCurrentIndexWriterConfig() {
        return indexWriter.getConfig();
    }

    private final class EngineMergeScheduler extends ElasticsearchConcurrentMergeScheduler {
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        private final AtomicBoolean isThrottling = new AtomicBoolean();

        EngineMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
            super(shardId, indexSettings);
        }

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    activateThrottling();
                }
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}",
                        numMergesInFlight, maxNumMerges);
                    deactivateThrottling();
                }
            }
            if (indexWriter.hasPendingMerges() == false &&
                    System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
                // NEVER do this on a merge thread since we acquire some locks blocking here and if we concurrently rollback the writer
                // we deadlock on engine#close for instance.
                engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        if (isClosed.get() == false) {
                            logger.warn("failed to flush after merge has finished");
                        }
                    }

                    @Override
                    protected void doRun() {
                        // if we have no pending merges and we are supposed to flush once merges have finished
                        // we try to renew a sync commit which is the case when we are having a big merge after we
                        // are inactive. If that didn't work we go and do a real flush which is ok since it only doesn't work
                        // if we either have records in the translog or if we don't have a sync ID at all...
                        // maybe even more important, we flush after all merges finish and we are inactive indexing-wise to
                        // free up transient disk usage of the (presumably biggish) segments that were just merged
                        if (tryRenewSyncCommit() == false) {
                            flush();
                        }
                    }
                });
            } else if (merge.getTotalBytesSize() >= engineConfig.getIndexSettings().getFlushAfterMergeThresholdSize().getBytes()) {
                // we hit a significant merge which would allow us to free up memory if we'd commit it hence on the next change
                // we should execute a flush on the next operation if that's a flush after inactive or indexing a document.
                // we could fork a thread and do it right away but we try to minimize forking and piggyback on outside events.
                shouldPeriodicallyFlushAfterBigMerge.set(true);
            }
        }

        @Override
        protected void handleMergeException(final Directory dir, final Throwable exc) {
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    /*
                     * We do this on another thread rather than the merge thread that we are initially called on so that we have complete
                     * confidence that the call stack does not contain catch statements that would cause the error that might be thrown
                     * here from being caught and never reaching the uncaught exception handler.
                     */
                    failEngine("merge failed", new MergePolicy.MergeException(exc, dir));
                }
            });
        }
    }

    /**
     * Commits the specified index writer.
     *
     * @param writer   the index writer to commit
     * @param translog the translog
     * @param syncId   the sync flush ID ({@code null} if not committing a synced flush)
     * @throws IOException if an I/O exception occurs committing the specfied writer
     */
    protected void commitIndexWriter(final IndexWriter writer, final Translog translog, @Nullable final String syncId) throws IOException {
        ensureCanFlush();
        try {
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            final Translog.TranslogGeneration translogGeneration = translog.getMinGenerationForSeqNo(localCheckpoint + 1);
            final String translogFileGeneration = Long.toString(translogGeneration.translogFileGeneration);
            final String translogUUID = translogGeneration.translogUUID;
            final String localCheckpointValue = Long.toString(localCheckpoint);

            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different, we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index. Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the maximum sequence number to the time
                 * of invocation of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
                final Map<String, String> commitData = new HashMap<>(8);
                commitData.put(Translog.TRANSLOG_GENERATION_KEY, translogFileGeneration);
                commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpointValue);
                if (syncId != null) {
                    commitData.put(Engine.SYNC_COMMIT_ID, syncId);
                }
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                commitData.put(HISTORY_UUID_KEY, historyUUID);
                if (softDeleteEnabled) {
                    commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo()));
                }
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });
            shouldPeriodicallyFlushAfterBigMerge.set(false);
            writer.commit();
        } catch (final Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final AssertionError e) {
            /*
             * If assertions are enabled, IndexWriter throws AssertionError on commit if any files don't exist, but tests that randomly
             * throw FileNotFoundException or NoSuchFileException can also hit this.
             */
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                final EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (final Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    private void ensureCanFlush() {
        // translog recover happens after the engine is fully constructed
        // if we are in this stage we have to prevent flushes from this
        // engine otherwise we might loose documents if the flush succeeds
        // and the translog recover fails we we "commit" the translog on flush.
        if (pendingTranslogRecovery.get()) {
            throw new IllegalStateException(shardId.toString() + " flushes are disabled - pending translog recovery");
        }
    }

    public void onSettingsChanged() {
        mergeScheduler.refreshConfig();
        // config().isEnableGcDeletes() or config.getGcDeletesInMillis() may have changed:
        maybePruneDeletes();
        final TranslogDeletionPolicy translogDeletionPolicy = translog.getDeletionPolicy();
        final IndexSettings indexSettings = engineConfig.getIndexSettings();
        translogDeletionPolicy.setRetentionAgeInMillis(indexSettings.getTranslogRetentionAge().getMillis());
        translogDeletionPolicy.setRetentionSizeInBytes(indexSettings.getTranslogRetentionSize().getBytes());
        softDeletesPolicy.setRetentionOperations(indexSettings.getSoftDeleteRetentionOperations());
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return getTranslog().getLastSyncedGlobalCheckpoint();
    }

    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return localCheckpointTracker.getPersistedCheckpoint();
    }

    /**
     * Marks the given seq_no as seen and advances the max_seq_no of this engine to at least that value.
     */
    protected final void markSeqNoAsSeen(long seqNo) {
        localCheckpointTracker.advanceMaxSeqNo(seqNo);
    }

    /**
     * Checks if the given operation has been processed in this engine or not.
     * @return true if the given operation was processed; otherwise false.
     */
    protected final boolean hasBeenProcessedBefore(Operation op) {
        if (Assertions.ENABLED) {
            assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "operation is not assigned seq_no";
            if (op.operationType() == Operation.TYPE.NO_OP) {
                assert noOpKeyedLock.isHeldByCurrentThread(op.seqNo());
            } else {
                assert versionMap.assertKeyedLockHeldByCurrentThread(op.uid().bytes());
            }
        }
        return localCheckpointTracker.hasProcessed(op.seqNo());
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    /**
     * Returns the number of times a version was looked up either from the index.
     * Note this is only available if assertions are enabled
     */
    long getNumIndexVersionsLookups() { // for testing
        return numIndexVersionsLookups.count();
    }

    /**
     * Returns the number of times a version was looked up either from memory or from the index.
     * Note this is only available if assertions are enabled
     */
    long getNumVersionLookups() { // for testing
        return numVersionLookups.count();
    }

    private boolean incrementVersionLookup() { // only used by asserts
        numVersionLookups.inc();
        return true;
    }

    private boolean incrementIndexVersionLookup() {
        numIndexVersionsLookups.inc();
        return true;
    }

    boolean isSafeAccessRequired() {
        return versionMap.isSafeAccessRequired();
    }

    /**
     * Returns the number of documents have been deleted since this engine was opened.
     * This count does not include the deletions from the existing segments before opening engine.
     */
    long getNumDocDeletes() {
        return numDocDeletes.count();
    }

    /**
     * Returns the number of documents have been appended since this engine was opened.
     * This count does not include the appends from the existing segments before opening engine.
     */
    long getNumDocAppends() {
        return numDocAppends.count();
    }

    /**
     * Returns the number of documents have been updated since this engine was opened.
     * This count does not include the updates from the existing segments before opening engine.
     */
    long getNumDocUpdates() {
        return numDocUpdates.count();
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, MapperService mapperService,
                                                long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        if (softDeleteEnabled == false) {
            throw new IllegalStateException("accessing changes snapshot requires soft-deletes enabled");
        }
        ensureOpen();
        refreshIfNeeded(source, toSeqNo);
        Searcher searcher = acquireSearcher(source, SearcherScope.INTERNAL);
        try {
            LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(
                searcher, mapperService, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, fromSeqNo, toSeqNo, requiredFullRange);
            searcher = null;
            return snapshot;
        } catch (Exception e) {
            try {
                maybeFailEngine("acquire changes snapshot", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            IOUtils.close(searcher);
        }
    }

    @Override
    public boolean hasCompleteOperationHistory(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            return getMinRetainedSeqNo() <= startingSeqNo;
        }

        final long currentLocalCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
        // avoid scanning translog if not necessary
        if (startingSeqNo > currentLocalCheckpoint) {
            return true;
        }
        final LocalCheckpointTracker tracker = new LocalCheckpointTracker(startingSeqNo, startingSeqNo - 1);
        try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(startingSeqNo)) {
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                if (operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    tracker.markSeqNoAsProcessed(operation.seqNo());
                }
            }
        }
        return tracker.getProcessedCheckpoint() >= currentLocalCheckpoint;
    }

    /**
     * Returns the minimum seqno that is retained in the Lucene index.
     * Operations whose seq# are at least this value should exist in the Lucene index.
     */
    public final long getMinRetainedSeqNo() {
        assert softDeleteEnabled : Thread.currentThread().getName();
        return softDeletesPolicy.getMinRetainedSeqNo();
    }

    @Override
    public Closeable acquireRetentionLock() {
        if (softDeleteEnabled) {
            return softDeletesPolicy.acquireRetentionLock();
        } else {
            return translog.acquireRetentionLock();
        }
    }

    @Override
    public boolean isRecovering() {
        return pendingTranslogRecovery.get();
    }

    /**
     * Gets the commit data from {@link IndexWriter} as a map.
     */
    private static Map<String, String> commitDataAsMap(final IndexWriter indexWriter) {
        final Map<String, String> commitData = new HashMap<>(8);
        for (Map.Entry<String, String> entry : indexWriter.getLiveCommitData()) {
            commitData.put(entry.getKey(), entry.getValue());
        }
        return commitData;
    }

    private final class AssertingIndexWriter extends IndexWriter {
        AssertingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }
        @Override
        public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
            assert softDeleteEnabled == false : "Call #updateDocument but soft-deletes is enabled";
            return super.updateDocument(term, doc);
        }
        @Override
        public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
            assert softDeleteEnabled == false : "Call #updateDocuments but soft-deletes is enabled";
            return super.updateDocuments(delTerm, docs);
        }
        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            assert softDeleteEnabled == false : "Call #deleteDocuments but soft-deletes is enabled";
            return super.deleteDocuments(terms);
        }
        @Override
        public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
            assert softDeleteEnabled : "Call #softUpdateDocument but soft-deletes is disabled";
            return super.softUpdateDocument(term, doc, softDeletes);
        }
        @Override
        public long softUpdateDocuments(Term term, Iterable<? extends Iterable<? extends IndexableField>> docs,
                                            Field... softDeletes) throws IOException {
            assert softDeleteEnabled : "Call #softUpdateDocuments but soft-deletes is disabled";
            return super.softUpdateDocuments(term, docs, softDeletes);
        }
        @Override
        public long tryDeleteDocument(IndexReader readerIn, int docID) {
            assert false : "#tryDeleteDocument is not supported. See Lucene#DirectoryReaderWithAllLiveDocs";
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returned the last local checkpoint value has been refreshed internally.
     */
    final long lastRefreshedCheckpoint() {
        return lastRefreshedCheckpointListener.refreshedCheckpoint.get();
    }


    private final Object refreshIfNeededMutex = new Object();

    /**
     * Refresh this engine **internally** iff the requesting seq_no is greater than the last refreshed checkpoint.
     */
    protected final void refreshIfNeeded(String source, long requestingSeqNo) {
        if (lastRefreshedCheckpoint() < requestingSeqNo) {
            synchronized (refreshIfNeededMutex) {
                if (lastRefreshedCheckpoint() < requestingSeqNo) {
                    refresh(source, SearcherScope.INTERNAL, true);
                }
            }
        }
    }

    private final class LastRefreshedCheckpointListener implements ReferenceManager.RefreshListener {
        final AtomicLong refreshedCheckpoint;
        private long pendingCheckpoint;

        LastRefreshedCheckpointListener(long initialLocalCheckpoint) {
            this.refreshedCheckpoint = new AtomicLong(initialLocalCheckpoint);
        }

        @Override
        public void beforeRefresh() {
            // all changes until this point should be visible after refresh
            pendingCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh) {
                updateRefreshedCheckpoint(pendingCheckpoint);
            }
        }

        void updateRefreshedCheckpoint(long checkpoint) {
            refreshedCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));
            assert refreshedCheckpoint.get() >= checkpoint : refreshedCheckpoint.get() + " < " + checkpoint;
        }
    }

    @Override
    public final long getMaxSeenAutoIdTimestamp() {
        return maxSeenAutoIdTimestamp.get();
    }

    @Override
    public final void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        updateAutoIdTimestamp(newTimestamp, true);
    }

    private void updateAutoIdTimestamp(long newTimestamp, boolean unsafe) {
        assert newTimestamp >= -1 : "invalid timestamp [" + newTimestamp + "]";
        maxSeenAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        if (unsafe) {
            maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        }
        assert maxUnsafeAutoIdTimestamp.get() <= maxSeenAutoIdTimestamp.get();
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes.get();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        if (maxSeqNoOfUpdatesOnPrimary == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            assert false : "max_seq_no_of_updates on primary is unassigned";
            throw new IllegalArgumentException("max_seq_no_of_updates on primary is unassigned");
        }
        this.maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, maxSeqNoOfUpdatesOnPrimary));
    }

    private boolean assertMaxSeqNoOfUpdatesIsAdvanced(Term id, long seqNo, boolean allowDeleted, boolean relaxIfGapInSeqNo) {
        final long maxSeqNoOfUpdates = getMaxSeqNoOfUpdatesOrDeletes();
        // We treat a delete on the tombstones on replicas as a regular document, then use updateDocument (not addDocument).
        if (allowDeleted) {
            final VersionValue versionValue = versionMap.getVersionForAssert(id.bytes());
            if (versionValue != null && versionValue.isDelete()) {
                return true;
            }
        }
        // Operations can be processed on a replica in a different order than on the primary. If the order on the primary is index-1,
        // delete-2, index-3, and the order on a replica is index-1, index-3, delete-2, then the msu of index-3 on the replica is 2
        // even though it is an update (overwrites index-1). We should relax this assertion if there is a pending gap in the seq_no.
        if (relaxIfGapInSeqNo && localCheckpointTracker.getProcessedCheckpoint() < maxSeqNoOfUpdates) {
            return true;
        }
        assert seqNo <= maxSeqNoOfUpdates : "id=" + id + " seq_no=" + seqNo + " msu=" + maxSeqNoOfUpdates;
        return true;
    }

    private static void trimUnsafeCommits(EngineConfig engineConfig) throws IOException {
        final Store store = engineConfig.getStore();
        final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        final Path translogPath = engineConfig.getTranslogConfig().getTranslogPath();
        final long globalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
        final long minRetainedTranslogGen = Translog.readMinTranslogGeneration(translogPath, translogUUID);
        store.trimUnsafeCommits(globalCheckpoint, minRetainedTranslogGen, engineConfig.getIndexSettings().getIndexVersionCreated());
    }

    /**
     * Restores the live version map and local checkpoint of this engine using documents (including soft-deleted)
     * after the local checkpoint in the safe commit. This step ensures the live version map and checkpoint tracker
     * are in sync with the Lucene commit.
     */
    private void restoreVersionMapAndCheckpointTracker(DirectoryReader directoryReader) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setQueryCache(null);
        final Query query = LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, getPersistedLocalCheckpoint() + 1, Long.MAX_VALUE);
        final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext leaf : directoryReader.leaves()) {
            final Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            final CombinedDocValues dv = new CombinedDocValues(leaf.reader());
            final IdOnlyFieldVisitor idFieldVisitor = new IdOnlyFieldVisitor();
            final DocIdSetIterator iterator = scorer.iterator();
            int docId;
            while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                final long primaryTerm = dv.docPrimaryTerm(docId);
                if (primaryTerm == -1L) {
                    continue; // skip children docs which do not have primary term
                }
                final long seqNo = dv.docSeqNo(docId);
                localCheckpointTracker.markSeqNoAsProcessed(seqNo);
                localCheckpointTracker.markSeqNoAsPersisted(seqNo);
                idFieldVisitor.reset();
                leaf.reader().document(docId, idFieldVisitor);
                if (idFieldVisitor.getId() == null) {
                    assert dv.isTombstone(docId);
                    continue;
                }
                final BytesRef uid = new Term(IdFieldMapper.NAME, Uid.encodeId(idFieldVisitor.getId())).bytes();
                try (Releasable ignored = versionMap.acquireLock(uid)) {
                    final VersionValue curr = versionMap.getUnderLock(uid);
                    if (curr == null ||
                        compareOpToVersionMapOnSeqNo(idFieldVisitor.getId(), seqNo, primaryTerm, curr) == OpVsLuceneDocStatus.OP_NEWER) {
                        if (dv.isTombstone(docId)) {
                            // use 0L for the start time so we can prune this delete tombstone quickly
                            // when the local checkpoint advances (i.e., after a recovery completed).
                            final long startTime = 0L;
                            versionMap.putDeleteUnderLock(uid, new DeleteVersionValue(dv.docVersion(docId), seqNo, primaryTerm, startTime));
                        } else {
                            versionMap.putIndexUnderLock(uid, new IndexVersionValue(null, dv.docVersion(docId), seqNo, primaryTerm));
                        }
                    }
                }
            }
        }
        // remove live entries in the version map
        refresh("restore_version_map_and_checkpoint_tracker", SearcherScope.INTERNAL, true);
    }

}

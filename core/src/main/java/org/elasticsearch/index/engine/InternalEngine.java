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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;

public class InternalEngine extends Engine {

    /**
     * When we last pruned expired tombstones from versionMap.deletes:
     */
    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;
    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final SearcherFactory searcherFactory;
    private final SearcherManager searcherManager;

    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();

    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final LiveVersionMap versionMap;

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private final AtomicBoolean versionMapRefreshPending = new AtomicBoolean();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final SequenceNumbersService seqNoService;

    // How many callers are currently requesting index throttling.  Currently there are only two situations where we do this: when merges
    // are falling behind and when writing indexing buffer to disk is too slow.  When this is 0, there is no throttling, else we throttling
    // incoming indexing ops to a single thread:
    private final AtomicInteger throttleRequestCount = new AtomicInteger();
    private final EngineConfig.OpenMode openMode;
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final CounterMetric numVersionLookups = new CounterMetric();
    private final CounterMetric numIndexVersionsLookups = new CounterMetric();

    public InternalEngine(EngineConfig engineConfig) throws EngineException {
        super(engineConfig);
        openMode = engineConfig.getOpenMode();
        if (engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_5_0_0_beta1)) {
            // no optimization for pre 5.0.0.alpha6 since translog might not have all information needed
            maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        } else {
            maxUnsafeAutoIdTimestamp.set(engineConfig.getMaxUnsafeAutoIdTimestamp());
        }
        this.versionMap = new LiveVersionMap();
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        SearcherManager manager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();

            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            this.searcherFactory = new SearchFactory(logger, isClosed, engineConfig);
            try {
                final SeqNoStats seqNoStats;
                switch (openMode) {
                    case OPEN_INDEX_AND_TRANSLOG:
                        writer = createWriter(false);
                        final long globalCheckpoint = Translog.readGlobalCheckpoint(engineConfig.getTranslogConfig().getTranslogPath());
                        seqNoStats = store.loadSeqNoStats(globalCheckpoint);
                        break;
                    case OPEN_INDEX_CREATE_TRANSLOG:
                        writer = createWriter(false);
                        seqNoStats = store.loadSeqNoStats(SequenceNumbersService.UNASSIGNED_SEQ_NO);
                        break;
                    case CREATE_INDEX_AND_TRANSLOG:
                        writer = createWriter(true);
                        seqNoStats = new SeqNoStats(
                            SequenceNumbersService.NO_OPS_PERFORMED,
                            SequenceNumbersService.NO_OPS_PERFORMED,
                            SequenceNumbersService.UNASSIGNED_SEQ_NO);
                        break;
                    default:
                        throw new IllegalArgumentException(openMode.toString());
                }
                logger.trace("recovered [{}]", seqNoStats);
                seqNoService = sequenceNumberService(shardId, engineConfig.getIndexSettings(), seqNoStats);
                // norelease
                /*
                 * We have no guarantees that all operations above the local checkpoint are in the Lucene commit or the translog. This means
                 * that we there might be operations greater than the local checkpoint that will not be replayed. Here we force the local
                 * checkpoint to the maximum sequence number in the commit (at the potential expense of correctness).
                 */
                while (seqNoService().getLocalCheckpoint() < seqNoService().getMaxSeqNo()) {
                    seqNoService().markSeqNoAsCompleted(seqNoService().getLocalCheckpoint() + 1);
                }
                indexWriter = writer;
                translog = openTranslog(engineConfig, writer, () -> seqNoService().getGlobalCheckpoint());
                assert translog.getGeneration() != null;
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

            this.translog = translog;
            manager = createSearcherManager();
            this.searcherManager = manager;
            this.versionMap.setManager(searcherManager);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            // don't allow commits until we are done with recovering
            pendingTranslogRecovery.set(openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
            if (engineConfig.getRefreshListeners() != null) {
                searcherManager.addListener(engineConfig.getRefreshListeners());
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, manager, scheduler);
                versionMap.clear();
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    private static SequenceNumbersService sequenceNumberService(
        final ShardId shardId,
        final IndexSettings indexSettings,
        final SeqNoStats seqNoStats) {
        return new SequenceNumbersService(
            shardId,
            indexSettings,
            seqNoStats.getMaxSeqNo(),
            seqNoStats.getLocalCheckpoint(),
            seqNoStats.getGlobalCheckpoint());
    }

    @Override
    public InternalEngine recoverFromTranslog() throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
                throw new IllegalStateException("Can't recover from translog with open mode: " + openMode);
            }
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslog(engineConfig.getTranslogRecoveryPerformer());
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

    private void recoverFromTranslog(TranslogRecoveryPerformer handler) throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        try {
            Translog.Snapshot snapshot = translog.newSnapshot();
            opsRecovered = handler.recoveryFromSnapshot(this, snapshot);
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
        // flush if we recovered something or if we have references to older translogs
        // note: if opsRecovered == 0 and we have older translogs it means they are corrupted or 0 length.
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog. ops recovered [{}]. committed translog id [{}]. current id [{}]",
                opsRecovered, translogGeneration == null ? null : translogGeneration.translogFileGeneration, translog.currentFileGeneration());
            flush(true, true);
        } else if (translog.isCurrent(translogGeneration) == false) {
            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    private Translog openTranslog(EngineConfig engineConfig, IndexWriter writer, LongSupplier globalCheckpointSupplier) throws IOException {
        assert openMode != null;
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        Translog.TranslogGeneration generation = null;
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            generation = loadTranslogIdFromCommit(writer);
            // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
            if (generation == null) {
                throw new IllegalStateException("no translog generation present in commit data but translog is expected to exist");
            }
            if (generation.translogUUID == null) {
                throw new IndexFormatTooOldException("trasnlog", "translog has no generation nor a UUID - this might be an index from a previous version consider upgrading to N-1 first");
            }
        }
        final Translog translog = new Translog(translogConfig, generation, globalCheckpointSupplier);
        if (generation == null || generation.translogUUID == null) {
            assert openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG : "OpenMode must not be "
                + EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
            if (generation == null) {
                logger.debug("no translog ID present in the current generation - creating one");
            } else if (generation.translogUUID == null) {
                logger.debug("upgraded translog to pre 2.0 format, associating translog with index - writing translog UUID");
            }
            boolean success = false;
            try {
                commitIndexWriter(writer, translog, openMode == EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG
                    ? commitDataAsMap(writer).get(SYNC_COMMIT_ID) : null);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(translog);
                }
            }
        }
        return translog;
    }

    @Override
    public Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    /**
     * Reads the current stored translog ID from the IW commit data. If the id is not found, recommits the current
     * translog id into lucene and returns null.
     */
    @Nullable
    private Translog.TranslogGeneration loadTranslogIdFromCommit(IndexWriter writer) throws IOException {
        // commit on a just opened writer will commit even if there are no changes done to it
        // we rely on that for the commit data translog id key
        final Map<String, String> commitUserData = commitDataAsMap(writer);
        if (commitUserData.containsKey("translog_id")) {
            assert commitUserData.containsKey(Translog.TRANSLOG_UUID_KEY) == false : "legacy commit contains translog UUID";
            return new Translog.TranslogGeneration(null, Long.parseLong(commitUserData.get("translog_id")));
        } else if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY)) {
            if (commitUserData.containsKey(Translog.TRANSLOG_UUID_KEY) == false) {
                throw new IllegalStateException("commit doesn't contain translog UUID");
            }
            final String translogUUID = commitUserData.get(Translog.TRANSLOG_UUID_KEY);
            final long translogGen = Long.parseLong(commitUserData.get(Translog.TRANSLOG_GENERATION_KEY));
            return new Translog.TranslogGeneration(translogUUID, translogGen);
        }
        return null;
    }

    private SearcherManager createSearcherManager() throws EngineException {
        boolean success = false;
        SearcherManager searcherManager = null;
        try {
            try {
                final DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                searcherManager = new SearcherManager(directoryReader, searcherFactory);
                lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
                success = true;
                return searcherManager;
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
                IOUtils.closeWhileHandlingException(searcherManager, indexWriter);
            }
        }
    }

    @Override
    public GetResult get(Get get, Function<String, Searcher> searcherFactory) throws EngineException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            if (get.realtime()) {
                VersionValue versionValue = versionMap.getUnderLock(get.uid());
                if (versionValue != null) {
                    if (versionValue.delete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version(), get.version())) {
                        Uid uid = Uid.createUid(get.uid().text());
                        throw new VersionConflictEngineException(shardId, uid.type(), uid.id(),
                            get.versionType().explainConflictForReads(versionValue.version(), get.version()));
                    }
                    refresh("realtime_get");
                }
            }

            // no version, get the version from the index, we know that we refresh on flush
            return getFromSearcher(get, searcherFactory);
        }
    }

    /**
     * Checks for version conflicts. If a non-critical version conflict exists <code>true</code> is returned. In the case of a critical
     * version conflict (if operation origin is primary) a {@link VersionConflictEngineException} is thrown.
     *
     * @param op              the operation
     * @param currentVersion  the current version
     * @param expectedVersion the expected version
     * @param deleted         {@code true} if the current version is not found or represents a delete
     * @return <code>true</code> iff a non-critical version conflict (origin recovery or replica) is found otherwise <code>false</code>
     * @throws VersionConflictEngineException if a critical version conflict was found where the operation origin is primary
     * @throws IllegalArgumentException if an unsupported version type is used.
     */
    private boolean checkVersionConflict(final Operation op, final long currentVersion, final long expectedVersion, final boolean deleted) {
        if (op.versionType() == VersionType.FORCE) {
            if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
                // If index was created in 5.0 or later, 'force' is not allowed at all
                throw new IllegalArgumentException("version type [FORCE] may not be used for indices created after 6.0");
            } else if (op.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                // For earlier indices, 'force' is only allowed for translog recovery
                throw new IllegalArgumentException("version type [FORCE] may not be used for non-translog operations");
            }
        }

        if (op.versionType().isVersionConflictForWrites(currentVersion, expectedVersion, deleted)) {
            if (op.origin() == Operation.Origin.PRIMARY) {
                // fatal version conflict
                throw new VersionConflictEngineException(
                        shardId,
                        op.type(),
                        op.id(),
                        op.versionType().explainConflictForWrites(currentVersion, expectedVersion, deleted));

            } else {
                /* Version conflicts during recovery and on replicas are normal due to asynchronous execution; as such, we should return a
                 * successful result.*/
                return true;
            }
        } else {
            return false;
        }
    }

    private long checkDeletedAndGCed(VersionValue versionValue) {
        long currentVersion;
        if (engineConfig.isEnableGcDeletes() && versionValue.delete() && (engineConfig.getThreadPool().relativeTimeInMillis() - versionValue.time()) > getGcDeletesInMillis()) {
            currentVersion = Versions.NOT_FOUND; // deleted, and GC
        } else {
            currentVersion = versionValue.version();
        }
        return currentVersion;
    }

    private boolean canOptimizeAddDocument(Index index) {
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert index.getAutoGeneratedIdTimestamp() >= 0 : "autoGeneratedIdTimestamp must be positive but was: "
                + index.getAutoGeneratedIdTimestamp();
            switch (index.origin()) {
                case PRIMARY:
                    assert (index.version() == Versions.MATCH_ANY && index.versionType() == VersionType.INTERNAL)
                        : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                case PEER_RECOVERY:
                case REPLICA:
                    assert index.version() == 1 && index.versionType() == VersionType.EXTERNAL
                        : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                case LOCAL_TRANSLOG_RECOVERY:
                    assert index.isRetry();
                    return false; // even if retry is set we never optimize local recovery
                default:
                    throw new IllegalArgumentException("unknown origin " + index.origin());
            }
        }
        return false;
    }

    private boolean assertVersionType(final Engine.Operation operation) {
        if (operation.origin() == Operation.Origin.REPLICA ||
                operation.origin() == Operation.Origin.PEER_RECOVERY ||
                operation.origin() == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            // ensure that replica operation has expected version type for replication
            // ensure that versionTypeForReplicationAndRecovery is idempotent
            assert operation.versionType() == operation.versionType().versionTypeForReplicationAndRecovery()
                    : "unexpected version type in request from [" + operation.origin().name() + "] " +
                    "found [" + operation.versionType().name() + "] " +
                    "expected [" + operation.versionType().versionTypeForReplicationAndRecovery().name() + "]";
        }
        return true;
    }

    private boolean assertSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1_UNRELEASED) && origin == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            // legacy support
            assert seqNo == SequenceNumbersService.UNASSIGNED_SEQ_NO : "old op recovering but it already has a seq no.;" +
                " index version: " + engineConfig.getIndexSettings().getIndexVersionCreated() + ", seqNo: " + seqNo;
        } else if (origin == Operation.Origin.PRIMARY) {
            // sequence number should not be set when operation origin is primary
            assert seqNo == SequenceNumbersService.UNASSIGNED_SEQ_NO : "primary ops should never have an assigned seq no.; seqNo: " + seqNo;
        } else if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
            // sequence number should be set when operation origin is not primary
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertSequenceNumber(index.origin(), index.seqNo());
            assert assertVersionType(index);
            final Translog.Location location;
            long seqNo = index.seqNo();
            try (Releasable ignored = acquireLock(index.uid());
                Releasable indexThrottle = doThrottle ? () -> {} : throttle.acquireThrottle()) {
                lastWriteNanos = index.startTime();
                /* if we have an autoGeneratedID that comes into the engine we can potentially optimize
                 * and just use addDocument instead of updateDocument and skip the entire version and index lookup across the board.
                 * Yet, we have to deal with multiple document delivery, for this we use a property of the document that is added
                 * to detect if it has potentially been added before. We use the documents timestamp for this since it's something
                 * that:
                 *  - doesn't change per document
                 *  - is preserved in the transaction log
                 *  - and is assigned before we start to index / replicate
                 * NOTE: it's not important for this timestamp to be consistent across nodes etc. it's just a number that is in the common
                 * case increasing and can be used in the failure case when we retry and resent documents to establish a happens before relationship.
                 * for instance:
                 *  - doc A has autoGeneratedIdTimestamp = 10, isRetry = false
                 *  - doc B has autoGeneratedIdTimestamp = 9, isRetry = false
                 *
                 *  while both docs are in in flight, we disconnect on one node, reconnect and send doc A again
                 *  - now doc A' has autoGeneratedIdTimestamp = 10, isRetry = true
                 *
                 *  if A' arrives on the shard first we update maxUnsafeAutoIdTimestamp to 10 and use update document. All subsequent
                 *  documents that arrive (A and B) will also use updateDocument since their timestamps are less than maxUnsafeAutoIdTimestamp.
                 *  While this is not strictly needed for doc B it is just much simpler to implement since it will just de-optimize some doc in the worst case.
                 *
                 *  if A arrives on the shard first we use addDocument since maxUnsafeAutoIdTimestamp is < 10. A` will then just be skipped or calls
                 *  updateDocument.
                 */
                long currentVersion;
                final boolean deleted;
                // if anything is fishy here ie. there is a retry we go and force updateDocument below so we are updating the document in the
                // lucene index without checking the version map but we still do the version check
                final boolean forceUpdateDocument;
                final boolean canOptimizeAddDocument = canOptimizeAddDocument(index);
                if (canOptimizeAddDocument) {
                    forceUpdateDocument = isForceUpdateDocument(index);
                    currentVersion = Versions.NOT_FOUND;
                    deleted = true;
                } else {
                    // update the document
                    forceUpdateDocument = false; // we don't force it - it depends on the version
                    final VersionValue versionValue = versionMap.getUnderLock(index.uid());
                    assert incrementVersionLookup();
                    if (versionValue == null) {
                        currentVersion = loadCurrentVersionFromIndex(index.uid());
                        deleted = currentVersion == Versions.NOT_FOUND;
                    } else {
                        currentVersion = checkDeletedAndGCed(versionValue);
                        deleted = versionValue.delete();
                    }
                }
                final long expectedVersion = index.version();
                Optional<IndexResult> resultOnVersionConflict;
                try {
                    final boolean isVersionConflict = checkVersionConflict(index, currentVersion, expectedVersion, deleted);
                    resultOnVersionConflict = isVersionConflict ? Optional.of(new IndexResult(currentVersion, index.seqNo(), false))
                        : Optional.empty();
                } catch (IllegalArgumentException | VersionConflictEngineException ex) {
                    resultOnVersionConflict = Optional.of(new IndexResult(ex, currentVersion, index.seqNo()));
                }

                final IndexResult indexResult;
                if (resultOnVersionConflict.isPresent()) {
                    indexResult = resultOnVersionConflict.get();
                } else {
                    // no version conflict
                    if (index.origin() == Operation.Origin.PRIMARY) {
                        seqNo = seqNoService().generateSeqNo();
                    }
                    indexResult = indexIntoLucene(index, seqNo, currentVersion, deleted, forceUpdateDocument, canOptimizeAddDocument, expectedVersion);
                }
                if (indexResult.hasFailure() == false) {
                    location = index.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY
                        ? translog.add(new Translog.Index(index, indexResult))
                        : null;
                    indexResult.setTranslogLocation(location);
                }
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
            } finally {
                if (seqNo != SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                    seqNoService().markSeqNoAsCompleted(seqNo);
                }
            }
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    private IndexResult indexIntoLucene(Index index, long seqNo, long currentVersion, boolean deleted, boolean forceUpdateDocument, boolean canOptimizeAddDocument, long expectedVersion) throws IOException {
        /* Update the document's sequence number and primary term; the sequence number here is derived here from either the sequence
         * number service if this is on the primary, or the existing document's sequence number if this is on the replica. The
         * primary term here has already been set, see IndexShard#prepareIndex where the Engine$Index operation is created.
         */
        index.parsedDoc().updateSeqID(seqNo, index.primaryTerm());
        final long updatedVersion = index.versionType().updateVersion(currentVersion, expectedVersion);
        index.parsedDoc().version().setLongValue(updatedVersion);
        try {
            if (currentVersion == Versions.NOT_FOUND && forceUpdateDocument == false) {
                // document does not exists, we can optimize for create, but double check if assertions are running
                assert assertDocDoesNotExist(index, canOptimizeAddDocument == false);
                index(index.docs(), indexWriter);
            } else {
                update(index.uid(), index.docs(), indexWriter);
            }
            versionMap.putUnderLock(index.uid().bytes(), new VersionValue(updatedVersion));
            return new IndexResult(updatedVersion, seqNo, deleted);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                /* There is no tragic event recorded so this must be a document failure.
                 *
                 * The handling inside IW doesn't guarantee that an tragic / aborting exception
                 * will be used as THE tragicEventException since if there are multiple exceptions causing an abort in IW
                 * only one wins. Yet, only the one that wins will also close the IW and in turn fail the engine such that
                 * we can potentially handle the exception before the engine is failed.
                 * Bottom line is that we can only rely on the fact that if it's a document failure then
                 * `indexWriter.getTragicException()` will be null otherwise we have to rethrow and treat it as fatal or rather
                 * non-document failure
                 */
                return new IndexResult(ex, currentVersion, index.seqNo());
            } else {
                throw ex;
            }
        }
    }

    private boolean isForceUpdateDocument(Index index) {
        boolean forceUpdateDocument;
        long deOptimizeTimestamp = maxUnsafeAutoIdTimestamp.get();
        if (index.isRetry()) {
            forceUpdateDocument = true;
            do {
                deOptimizeTimestamp = maxUnsafeAutoIdTimestamp.get();
                if (deOptimizeTimestamp >= index.getAutoGeneratedIdTimestamp()) {
                    break;
                }
            } while (maxUnsafeAutoIdTimestamp.compareAndSet(deOptimizeTimestamp,
                index.getAutoGeneratedIdTimestamp()) == false);
            assert maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            // in this case we force
            forceUpdateDocument = deOptimizeTimestamp >= index.getAutoGeneratedIdTimestamp();
        }
        return forceUpdateDocument;
    }

    private static void index(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    /**
     * Asserts that the doc in the index operation really doesn't exist
     */
    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        final VersionValue versionValue = versionMap.getUnderLock(index.uid());
        if (versionValue != null) {
            if (versionValue.delete() == false || allowDeleted == false) {
                throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists in version map (version " + versionValue + ")");
            }
        } else {
            try (Searcher searcher = acquireSearcher("assert doc doesn't exist")) {
                final long docsWithId = searcher.searcher().count(new TermQuery(index.uid()));
                if (docsWithId > 0) {
                    throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists [" + docsWithId + "] times in index");
                }
            }
        }
        return true;
    }

    private static void update(final Term uid, final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.updateDocuments(uid, docs);
        } else {
            indexWriter.updateDocument(uid, docs.get(0));
        }
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        DeleteResult result;
        try (ReleasableLock ignored = readLock.acquire()) {
            assert assertVersionType(delete);
            ensureOpen();
            // NOTE: we don't throttle this when merges fall behind because delete-by-id does not create new segments:
            result = innerDelete(delete);
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        maybePruneDeletedTombstones();
        return result;
    }

    private void maybePruneDeletedTombstones() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        if (engineConfig.isEnableGcDeletes() && engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    private DeleteResult innerDelete(Delete delete) throws IOException {
        assert assertSequenceNumber(delete.origin(), delete.seqNo());
        final Translog.Location location;
        final long updatedVersion;
        final boolean found;
        long seqNo = delete.seqNo();
        try (Releasable ignored = acquireLock(delete.uid())) {
            lastWriteNanos = delete.startTime();
            final long currentVersion;
            final boolean deleted;
            final VersionValue versionValue = versionMap.getUnderLock(delete.uid());
            assert incrementVersionLookup();
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(delete.uid());
                deleted = currentVersion == Versions.NOT_FOUND;
            } else {
                currentVersion = checkDeletedAndGCed(versionValue);
                deleted = versionValue.delete();
            }

            final long expectedVersion = delete.version();
            Optional<DeleteResult> resultOnVersionConflict;
            try {
                final boolean isVersionConflict = checkVersionConflict(delete, currentVersion, expectedVersion, deleted);
                resultOnVersionConflict = isVersionConflict ? Optional.of(new DeleteResult(expectedVersion, delete.seqNo(), true))
                    : Optional.empty();
            } catch (IllegalArgumentException | VersionConflictEngineException ex) {
                resultOnVersionConflict = Optional.of(new DeleteResult(ex, expectedVersion, delete.seqNo()));
            }
            final DeleteResult deleteResult;
            if (resultOnVersionConflict.isPresent()) {
                deleteResult = resultOnVersionConflict.get();
            } else {
                if (delete.origin() == Operation.Origin.PRIMARY) {
                    seqNo = seqNoService().generateSeqNo();
                }
                updatedVersion = delete.versionType().updateVersion(currentVersion, expectedVersion);
                found = deleteIfFound(delete.uid(), currentVersion, deleted, versionValue);
                deleteResult = new DeleteResult(updatedVersion, seqNo, found);

                versionMap.putUnderLock(delete.uid().bytes(),
                    new DeleteVersionValue(updatedVersion, engineConfig.getThreadPool().relativeTimeInMillis()));
            }
            if (!deleteResult.hasFailure()) {
                location = delete.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY
                    ? translog.add(new Translog.Delete(delete, deleteResult))
                    : null;
                deleteResult.setTranslogLocation(location);
            }
            deleteResult.setTook(System.nanoTime() - delete.startTime());
            deleteResult.freeze();
            return deleteResult;
        } finally {
            if (seqNo != SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                seqNoService().markSeqNoAsCompleted(seqNo);
            }
        }
    }

    private boolean deleteIfFound(Term uid, long currentVersion, boolean deleted, VersionValue versionValue) throws IOException {
        assert uid != null : "uid must not be null";
        final boolean found;
        if (currentVersion == Versions.NOT_FOUND) {
            // doc does not exist and no prior deletes
            found = false;
        } else if (versionValue != null && deleted) {
            // a "delete on delete", in this case, we still increment the version, log it, and return that version
            found = false;
        } else {
            // we deleted a currently existing document
            // any exception that comes from this is a either an ACE or a fatal exception there can't be any document failures coming
            // from this.
            indexWriter.deleteDocuments(uid);
            found = true;
        }
        return found;
    }

    @Override
    public NoOpResult noOp(final NoOp noOp) {
        NoOpResult noOpResult;
        try (ReleasableLock ignored = readLock.acquire()) {
            noOpResult = innerNoOp(noOp);
        } catch (final Exception e) {
            noOpResult = new NoOpResult(noOp.seqNo(), e);
        }
        return noOpResult;
    }

    private NoOpResult innerNoOp(final NoOp noOp) throws IOException {
        assert noOp.seqNo() > SequenceNumbersService.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();
        try {
            final NoOpResult noOpResult = new NoOpResult(noOp.seqNo());
            final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
            noOpResult.setTranslogLocation(location);
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        } finally {
            if (seqNo != SequenceNumbersService.UNASSIGNED_SEQ_NO) {
                seqNoService().markSeqNoAsCompleted(seqNo);
            }
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            searcherManager.maybeRefreshBlocking();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }

        // TODO: maybe we should just put a scheduled job in threadPool?
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        maybePruneDeletedTombstones();
        versionMapRefreshPending.set(false);
        mergeScheduler.refreshConfig();
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

        // we obtain a read lock here, since we don't want a flush to happen while we are writing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();

            // TODO: it's not great that we secretly tie searcher visibility to "freeing up heap" here... really we should keep two
            // searcher managers, one for searching which is only refreshed by the schedule the user requested (refresh_interval, or invoking
            // refresh API), and another for version map interactions.  See #15768.
            final long versionMapBytes = versionMap.ramBytesUsedForRefresh();
            final long indexingBufferBytes = indexWriter.ramBytesUsed();

            final boolean useRefresh = versionMapRefreshPending.get() || (indexingBufferBytes / 4 < versionMapBytes);
            if (useRefresh) {
                // The version map is using > 25% of the indexing buffer, so we do a refresh so the version map also clears
                logger.debug("use refresh to write indexing buffer (heap size=[{}]), to also clear version map (heap size=[{}])",
                        new ByteSizeValue(indexingBufferBytes), new ByteSizeValue(versionMapBytes));
                refresh("write indexing buffer");
            } else {
                // Most of our heap is used by the indexing buffer, so we do a cheaper (just writes segments, doesn't open a new searcher) IW.flush:
                logger.debug("use IndexWriter.flush to write indexing buffer (heap size=[{}]) since version map is small (heap size=[{}])",
                        new ByteSizeValue(indexingBufferBytes), new ByteSizeValue(versionMapBytes));
                indexWriter.flush();
            }
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("writeIndexingBuffer failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
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
            if (syncId != null && translog.totalOperations() == 0 && indexWriter.hasUncommittedChanges()) {
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
        if (renewed) { // refresh outside of the write lock
            refresh("renew sync commit");
        }

        return renewed;
    }

    @Override
    public CommitId flush() throws EngineException {
        return flush(false, false);
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
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
                if (indexWriter.hasUncommittedChanges() || force) {
                    ensureCanFlush();
                    try {
                        translog.prepareCommit();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");
                        // we need to refresh in order to clear older version values
                        refresh("version_table_flush");
                        // after refresh documents can be retrieved from the index so we can now commit the translog
                        translog.commit();
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
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

    private void pruneDeletedTombstones() {
        long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();

        // TODO: not good that we reach into LiveVersionMap here; can we move this inside VersionMap instead?  problem is the dirtyLock...

        // we only need to prune the deletes map; the current/old version maps are cleared on refresh:
        for (Map.Entry<BytesRef, VersionValue> entry : versionMap.getAllTombstones()) {
            BytesRef uid = entry.getKey();
            try (Releasable ignored = acquireLock(uid)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?

                // Must re-get it here, vs using entry.getValue(), in case the uid was indexed/deleted since we pulled the iterator:
                VersionValue versionValue = versionMap.getTombstoneUnderLock(uid);
                if (versionValue != null) {
                    if (timeMSec - versionValue.time() > getGcDeletesInMillis()) {
                        versionMap.removeTombstoneUnderLock(uid);
                    }
                }
            }
        }

        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           final boolean upgrade, final boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
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
        assert indexWriter.getConfig().getMergePolicy() instanceof ElasticsearchMergePolicy : "MergePolicy is " + indexWriter.getConfig().getMergePolicy().getClass().getName();
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
            ensureOpen();
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
                mp.setUpgradeInProgress(false, false); // reset it just to make sure we reset it in a case of an error
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    @Override
    public IndexCommit acquireIndexCommit(final boolean flushFirst) throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            logger.trace("pulling snapshot");
            return deletionPolicy.snapshot();
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        }
    }

    @SuppressWarnings("finally")
    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        // if we are already closed due to some tragic exception
        // we need to fail the engine. it might have already been failed before
        // but we are double-checking it's failed and closed
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            if (indexWriter.getTragicException() instanceof Error) {
                try {
                    logger.error("tragic event in index writer", ex);
                } finally {
                    throw (Error) indexWriter.getTragicException();
                }
            } else {
                failEngine("already closed by tragic event on the index writer", (Exception) indexWriter.getTragicException());
                engineFailed = true;
            }
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
    protected final void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                try {
                    IOUtils.close(searcherManager);
                } catch (Exception e) {
                    logger.warn("Failed to close SearcherManager", e);
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
                store.decRef();
                logger.debug("engine closed [{}]", reason);
            }
        }
    }

    @Override
    protected SearcherManager getSearcherManager() {
        return searcherManager;
    }

    private Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    private Releasable acquireLock(Term uid) {
        return acquireLock(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        assert incrementIndexVersionLookup();
        try (Searcher searcher = acquireSearcher("load_version")) {
            return Versions.loadVersion(searcher.reader(), uid);
        }
    }

    private IndexWriter createWriter(boolean create) throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig(create);
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    // pkg-private for testing
    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        return new IndexWriter(directory, iwc);
    }

    private IndexWriterConfig getIndexWriterConfig(boolean create) {
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCommitOnClose(false); // we by default don't commit on close
        iwc.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
        iwc.setIndexDeletionPolicy(deletionPolicy);
        // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {
        }
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        iwc.setMergeScheduler(mergeScheduler);
        MergePolicy mergePolicy = config().getMergePolicy();
        // Give us the opportunity to upgrade old segments while performing
        // background merges
        mergePolicy = new ElasticsearchMergePolicy(mergePolicy);
        iwc.setMergePolicy(mergePolicy);
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setCodec(engineConfig.getCodec());
        iwc.setUseCompoundFile(true); // always use compound on flush - reduces # of file-handles on refresh
        return iwc;
    }

    /** Extended SearcherFactory that warms the segments if needed when acquiring a new searcher */
    static final class SearchFactory extends EngineSearcherFactory {
        private final Engine.Warmer warmer;
        private final Logger logger;
        private final AtomicBoolean isEngineClosed;

        SearchFactory(Logger logger, AtomicBoolean isEngineClosed, EngineConfig engineConfig) {
            super(engineConfig);
            warmer = engineConfig.getWarmer();
            this.logger = logger;
            this.isEngineClosed = isEngineClosed;
        }

        @Override
        public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
            IndexSearcher searcher = super.newSearcher(reader, previousReader);
            if (reader instanceof LeafReader && isMergedSegment((LeafReader) reader)) {
                // we call newSearcher from the IndexReaderWarmer which warms segments during merging
                // in that case the reader is a LeafReader and all we need to do is to build a new Searcher
                // and return it since it does it's own warming for that particular reader.
                return searcher;
            }
            if (warmer != null) {
                try {
                    assert searcher.getIndexReader() instanceof ElasticsearchDirectoryReader : "this class needs an ElasticsearchDirectoryReader but got: " + searcher.getIndexReader().getClass();
                    warmer.warm(new Searcher("top_reader_warming", searcher));
                } catch (Exception e) {
                    if (isEngineClosed.get() == false) {
                        logger.warn("failed to prepare/warm", e);
                    }
                }
            }
            return searcher;
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
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    deactivateThrottling();
                }
            }
            if (indexWriter.hasPendingMerges() == false && System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
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
                    protected void doRun() throws Exception {
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

            }
        }

        @Override
        protected void handleMergeException(final Directory dir, final Throwable exc) {
            logger.error("failed to merge", exc);
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    MergePolicy.MergeException e = new MergePolicy.MergeException(exc, dir);
                    failEngine("merge failed", e);
                }
            });
        }
    }

    private void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
        ensureCanFlush();
        try {
            Translog.TranslogGeneration translogGeneration = translog.getGeneration();

            final String translogFileGen = Long.toString(translogGeneration.translogFileGeneration);
            final String translogUUID = translogGeneration.translogUUID;
            final String localCheckpoint = Long.toString(seqNoService().getLocalCheckpoint());

            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different - we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index.  Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the max_seq_no to the time of invocation
                 * of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
                final Map<String, String> commitData = new HashMap<>(6);
                commitData.put(Translog.TRANSLOG_GENERATION_KEY, translogFileGen);
                commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpoint);
                if (syncId != null) {
                    commitData.put(Engine.SYNC_COMMIT_ID, syncId);
                }
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(seqNoService().getMaxSeqNo()));
                if (logger.isTraceEnabled()) {
                    logger.trace("committing writer with commit data [{}]", commitData);
                }
                return commitData.entrySet().iterator();
            });

            writer.commit();
        } catch (Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (AssertionError e) {
            // IndexWriter throws AssertionError on commit, if asserts are enabled, if any files don't exist, but tests that
            // randomly throw FNFE/NSFE can also hit this:
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (Exception inner) {
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
        maybePruneDeletedTombstones();
        if (engineConfig.getMaxUnsafeAutoIdTimestamp() == Long.MAX_VALUE) {
            // this is an anti-viral settings you can only opt out for the entire index
            // only if a shard starts up again due to relocation or if the index is closed
            // the setting will be re-interpreted if it's set to true
            this.maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    @Override
    public SequenceNumbersService seqNoService() {
        return seqNoService;
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

    /**
     * Returns <code>true</code> iff the index writer has any deletions either buffered in memory or
     * in the index.
     */
    boolean indexWriterHasDeletions() {
        return indexWriter.hasDeletions();
    }

    @Override
    public boolean isRecovering() {
        return pendingTranslogRecovery.get();
    }

    /**
     * Gets the commit data from {@link IndexWriter} as a map.
     */
    private static Map<String, String> commitDataAsMap(final IndexWriter indexWriter) {
        Map<String, String> commitData = new HashMap<>(6);
        for (Map.Entry<String, String> entry : indexWriter.getLiveCommitData()) {
            commitData.put(entry.getKey(), entry.getValue());
        }
        return commitData;
    }
}

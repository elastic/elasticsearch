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
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

public class InternalEngine extends Engine {

    /**
     * When we last pruned expired tombstones from versionMap.deletes:
     */
    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;
    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final ExternalSearcherManager externalSearcherManager;
    private final SearcherManager internalSearcherManager;

    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();

    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final LiveVersionMap versionMap = new LiveVersionMap();

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final SequenceNumbersService seqNoService;

    private final String uidField;

    private final CombinedDeletionPolicy deletionPolicy;

    // How many callers are currently requesting index throttling.  Currently there are only two situations where we do this: when merges
    // are falling behind and when writing indexing buffer to disk is too slow.  When this is 0, there is no throttling, else we throttling
    // incoming indexing ops to a single thread:
    private final AtomicInteger throttleRequestCount = new AtomicInteger();
    private final EngineConfig.OpenMode openMode;
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    public static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID = "max_unsafe_auto_id_timestamp";
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);
    private final CounterMetric numVersionLookups = new CounterMetric();
    private final CounterMetric numIndexVersionsLookups = new CounterMetric();
    /**
     * How many bytes we are currently moving to disk, via either IndexWriter.flush or refresh.  IndexingMemoryController polls this
     * across all shards to decide if throttling is necessary because moving bytes to disk is falling behind vs incoming documents
     * being indexed/deleted.
     */
    private final AtomicLong writingBytes = new AtomicLong();

    @Nullable
    private final String historyUUID;

    public InternalEngine(EngineConfig engineConfig) {
        this(engineConfig, InternalEngine::sequenceNumberService);
    }

    InternalEngine(
            final EngineConfig engineConfig,
            final BiFunction<EngineConfig, SeqNoStats, SequenceNumbersService> seqNoServiceSupplier) {
        super(engineConfig);
        openMode = engineConfig.getOpenMode();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
        this.uidField = engineConfig.getIndexSettings().isSingleType() ? IdFieldMapper.NAME : UidFieldMapper.NAME;
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis()
        );
        this.deletionPolicy = new CombinedDeletionPolicy(
                new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy()), translogDeletionPolicy, openMode);
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        ExternalSearcherManager externalSearcherManager = null;
        SearcherManager internalSearcherManager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();

            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
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
                        seqNoStats = store.loadSeqNoStats(SequenceNumbers.UNASSIGNED_SEQ_NO);
                        break;
                    case CREATE_INDEX_AND_TRANSLOG:
                        writer = createWriter(true);
                        seqNoStats = new SeqNoStats(
                                SequenceNumbers.NO_OPS_PERFORMED,
                                SequenceNumbers.NO_OPS_PERFORMED,
                                SequenceNumbers.UNASSIGNED_SEQ_NO);
                        break;
                    default:
                        throw new IllegalArgumentException(openMode.toString());
                }
                logger.trace("recovered [{}]", seqNoStats);
                seqNoService = seqNoServiceSupplier.apply(engineConfig, seqNoStats);
                updateMaxUnsafeAutoIdTimestampFromWriter(writer);
                historyUUID = loadOrGenerateHistoryUUID(writer, engineConfig.getForceNewHistoryUUID());
                Objects.requireNonNull(historyUUID, "history uuid should not be null");
                indexWriter = writer;
                translog = openTranslog(engineConfig, writer, translogDeletionPolicy, () -> seqNoService.getGlobalCheckpoint());
                assert translog.getGeneration() != null;
                // we can only do this after we generated and committed a translog uuid. other wise the combined
                // retention policy, which listens to commits, gets all confused.
                persistHistoryUUIDIfNeeded();
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
            externalSearcherManager = createSearcherManager(new SearchFactory(logger, isClosed, engineConfig));
            internalSearcherManager = externalSearcherManager.internalSearcherManager;
            this.internalSearcherManager = internalSearcherManager;
            this.externalSearcherManager = externalSearcherManager;
            internalSearcherManager.addListener(versionMap);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            // don't allow commits until we are done with recovering
            pendingTranslogRecovery.set(openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
            for (ReferenceManager.RefreshListener listener: engineConfig.getExternalRefreshListener()) {
                this.externalSearcherManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener: engineConfig.getInternalRefreshListener()) {
                this.internalSearcherManager.addListener(listener);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, internalSearcherManager, externalSearcherManager, scheduler);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    /**
     * This reference manager delegates all it's refresh calls to another (internal) SearcherManager
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
    private static final class ExternalSearcherManager extends ReferenceManager<IndexSearcher> {
        private final SearcherFactory searcherFactory;
        private final SearcherManager internalSearcherManager;

        ExternalSearcherManager(SearcherManager internalSearcherManager, SearcherFactory searcherFactory) throws IOException {
            IndexSearcher acquire = internalSearcherManager.acquire();
            try {
                IndexReader indexReader = acquire.getIndexReader();
                assert indexReader instanceof ElasticsearchDirectoryReader:
                    "searcher's IndexReader should be an ElasticsearchDirectoryReader, but got " + indexReader;
                indexReader.incRef(); // steal the reader - getSearcher will decrement if it fails
                current = SearcherManager.getSearcher(searcherFactory, indexReader, null);
            } finally {
                internalSearcherManager.release(acquire);
            }
            this.searcherFactory = searcherFactory;
            this.internalSearcherManager = internalSearcherManager;
        }

        @Override
        protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
            // we simply run a blocking refresh on the internal reference manager and then steal it's reader
            // it's a save operation since we acquire the reader which incs it's reference but then down the road
            // steal it by calling incRef on the "stolen" reader
            internalSearcherManager.maybeRefreshBlocking();
            IndexSearcher acquire = internalSearcherManager.acquire();
            final IndexReader previousReader = referenceToRefresh.getIndexReader();
            assert previousReader instanceof ElasticsearchDirectoryReader:
                "searcher's IndexReader should be an ElasticsearchDirectoryReader, but got " + previousReader;
            try {
                final IndexReader newReader = acquire.getIndexReader();
                if (newReader == previousReader) {
                    // nothing has changed - both ref managers share the same instance so we can use reference equality
                    return null;
                } else {
                    newReader.incRef(); // steal the reader - getSearcher will decrement if it fails
                    return SearcherManager.getSearcher(searcherFactory, newReader, previousReader);
                }
            } finally {
                internalSearcherManager.release(acquire);
            }
        }

        @Override
        protected boolean tryIncRef(IndexSearcher reference) {
            return reference.getIndexReader().tryIncRef();
        }

        @Override
        protected int getRefCount(IndexSearcher reference) {
            return reference.getIndexReader().getRefCount();
        }

        @Override
        protected void decRef(IndexSearcher reference) throws IOException { reference.getIndexReader().decRef(); }
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = seqNoService.getLocalCheckpoint();
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(localCheckpoint + 1)) {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.seqNo() > localCheckpoint) {
                        seqNoService.markSeqNoAsCompleted(operation.seqNo());
                    }
                }
            }
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = seqNoService.getLocalCheckpoint();
            final long maxSeqNo = seqNoService.getMaxSeqNo();
            int numNoOpsAdded = 0;
            for (
                    long seqNo = localCheckpoint + 1;
                    seqNo <= maxSeqNo;
                    seqNo = seqNoService.getLocalCheckpoint() + 1 /* the local checkpoint might have advanced so we leap-frog */) {
                innerNoOp(new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
                numNoOpsAdded++;
                assert seqNo <= seqNoService.getLocalCheckpoint()
                        : "local checkpoint did not advance; was [" + seqNo + "], now [" + seqNoService.getLocalCheckpoint() + "]";

            }
            return numNoOpsAdded;
        }
    }

    private void updateMaxUnsafeAutoIdTimestampFromWriter(IndexWriter writer) {
        long commitMaxUnsafeAutoIdTimestamp = Long.MIN_VALUE;
        for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
            if (entry.getKey().equals(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)) {
                commitMaxUnsafeAutoIdTimestamp = Long.parseLong(entry.getValue());
                break;
            }
        }
        maxUnsafeAutoIdTimestamp.set(Math.max(maxUnsafeAutoIdTimestamp.get(), commitMaxUnsafeAutoIdTimestamp));
    }

    static SequenceNumbersService sequenceNumberService(
        final EngineConfig engineConfig,
        final SeqNoStats seqNoStats) {
        return new SequenceNumbersService(
            engineConfig.getShardId(),
            engineConfig.getAllocationId(),
            engineConfig.getIndexSettings(),
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
                recoverFromTranslogInternal();
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

    private void recoverFromTranslogInternal() throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        final long translogGen = Long.parseLong(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(translogGen)) {
            opsRecovered = config().getTranslogRecoveryRunner().run(this, snapshot);
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
            refresh("translog_recovery");
        } else if (translog.isCurrent(translogGeneration) == false) {
            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
            refreshLastCommittedSegmentInfos();
        } else if (lastCommittedSegmentInfos.getUserData().containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) == false)  {
            assert engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) :
                "index was created on version " + engineConfig.getIndexSettings().getIndexVersionCreated() + "but has "
                    + "no sequence numbers info in commit";

            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
            refreshLastCommittedSegmentInfos();
        }
        // clean up what's not needed
        translog.trimUnreferencedReaders();
    }

    private Translog openTranslog(EngineConfig engineConfig, IndexWriter writer, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier) throws IOException {
        assert openMode != null;
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        String translogUUID = null;
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            translogUUID = loadTranslogUUIDFromCommit(writer);
            // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
            if (translogUUID == null) {
                throw new IndexFormatTooOldException("translog", "translog has no generation nor a UUID - this might be an index from a previous version consider upgrading to N-1 first");
            }
        }
        final Translog translog = new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier);
        if (translogUUID == null) {
            assert openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG : "OpenMode must not be "
                + EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
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

    /** If needed, updates the metadata in the index writer to match the potentially new translog and history uuid */
    private void persistHistoryUUIDIfNeeded() throws IOException {
        Objects.requireNonNull(historyUUID);
        final Map<String, String> commitUserData = commitDataAsMap(indexWriter);
        if (historyUUID.equals(commitUserData.get(HISTORY_UUID_KEY)) == false) {
            assert openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG :
                "if the translog was created, history should have been committed";
            assert commitUserData.containsKey(HISTORY_UUID_KEY) == false || config().getForceNewHistoryUUID();
            Map<String, String> newData = new HashMap<>(commitUserData);
            newData.put(HISTORY_UUID_KEY, historyUUID);
            commitIndexWriter(indexWriter, newData.entrySet());
        } else {
            assert openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG || config().getForceNewHistoryUUID() == false :
                "history uuid is already committed, but the translog uuid isn't committed and a new history id was generated";
        }
    }


    @Override
    public Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /** Returns how many bytes we are currently moving from indexing buffer to segments on disk */
    @Override
    public long getWritingBytes() {
        return writingBytes.get();
    }

    /**
     * Reads the current stored translog ID from the IW commit data. If the id is not found, recommits the current
     * translog id into lucene and returns null.
     */
    @Nullable
    private String loadTranslogUUIDFromCommit(IndexWriter writer) throws IOException {
        // commit on a just opened writer will commit even if there are no changes done to it
        // we rely on that for the commit data translog id key
        final Map<String, String> commitUserData = commitDataAsMap(writer);
        if (commitUserData.containsKey(Translog.TRANSLOG_UUID_KEY)) {
            if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
                throw new IllegalStateException("commit doesn't contain translog generation id");
            }
            return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
        } else {
            return null;
        }
    }

    /**
     * Reads the current stored history ID from the IW commit data. Generates a new UUID if not found or if generation is forced.
     */
    private String loadOrGenerateHistoryUUID(final IndexWriter writer, boolean forceNew) throws IOException {
        String uuid = commitDataAsMap(writer).get(HISTORY_UUID_KEY);
        if (uuid == null || forceNew) {
            assert
                forceNew || // recovery from a local store creates an index that doesn't have yet a history_uuid
                openMode == EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG ||
                config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_rc1) :
                "existing index was created after 6_0_0_rc1 but has no history uuid";
            uuid = UUIDs.randomBase64UUID();
        }
        return uuid;
    }

    private ExternalSearcherManager createSearcherManager(SearchFactory externalSearcherFactory) throws EngineException {
        boolean success = false;
        SearcherManager internalSearcherManager = null;
        try {
            try {
                final DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                internalSearcherManager = new SearcherManager(directoryReader, new SearcherFactory());
                lastCommittedSegmentInfos = readLastCommittedSegmentInfos(internalSearcherManager, store);
                ExternalSearcherManager externalSearcherManager = new ExternalSearcherManager(internalSearcherManager,
                    externalSearcherFactory);
                success = true;
                return externalSearcherManager;
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
                IOUtils.closeWhileHandlingException(internalSearcherManager, indexWriter);
            }
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        assert Objects.equals(get.uid().field(), uidField) : get.uid().field();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            SearcherScope scope;
            if (get.realtime()) {
                VersionValue versionValue = versionMap.getUnderLock(get.uid().bytes());
                if (versionValue != null) {
                    if (versionValue.isDelete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(shardId, get.type(), get.id(),
                            get.versionType().explainConflictForReads(versionValue.version, get.version()));
                    }
                    refresh("realtime_get", SearcherScope.INTERNAL);
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

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnSeqNo(final Operation op) throws IOException {
        assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "resolving ops based on seq# but no seqNo is found";
        final OpVsLuceneDocStatus status;
        final VersionValue versionValue = versionMap.getUnderLock(op.uid().bytes());
        assert incrementVersionLookup();
        if (versionValue != null) {
            if  (op.seqNo() > versionValue.seqNo ||
                (op.seqNo() == versionValue.seqNo && op.primaryTerm() > versionValue.term))
                status = OpVsLuceneDocStatus.OP_NEWER;
            else {
                status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
            }
        } else {
            // load from index
            assert incrementIndexVersionLookup();
            try (Searcher searcher = acquireSearcher("load_seq_no", SearcherScope.INTERNAL)) {
                DocIdAndSeqNo docAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.reader(), op.uid());
                if (docAndSeqNo == null) {
                    status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
                } else if (op.seqNo() > docAndSeqNo.seqNo) {
                    status = OpVsLuceneDocStatus.OP_NEWER;
                } else if (op.seqNo() == docAndSeqNo.seqNo) {
                    // load term to tie break
                    final long existingTerm = VersionsAndSeqNoResolver.loadPrimaryTerm(docAndSeqNo, op.uid().field());
                    if (op.primaryTerm() > existingTerm) {
                        status = OpVsLuceneDocStatus.OP_NEWER;
                    } else {
                        status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                    }
                } else {
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                }
            }
        }
        return status;
    }

    /** resolves the current version of the document, returning null if not found */
    private VersionValue resolveDocVersion(final Operation op) throws IOException {
        assert incrementVersionLookup(); // used for asserting in tests
        VersionValue versionValue = versionMap.getUnderLock(op.uid().bytes());
        if (versionValue == null) {
            assert incrementIndexVersionLookup(); // used for asserting in tests
            final long currentVersion = loadCurrentVersionFromIndex(op.uid());
            if (currentVersion != Versions.NOT_FOUND) {
                versionValue = new VersionValue(currentVersion, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
            }
        } else if (engineConfig.isEnableGcDeletes() && versionValue.isDelete() &&
            (engineConfig.getThreadPool().relativeTimeInMillis() - ((DeleteVersionValue)versionValue).time) > getGcDeletesInMillis()) {
            versionValue = null;
        }
        return versionValue;
    }

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnVersions(final Operation op)
        throws IOException {
        assert op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO : "op is resolved based on versions but have a seq#";
        assert op.version() >= 0 : "versions should be non-negative. got " + op.version();
        final VersionValue versionValue = resolveDocVersion(op);
        if (versionValue == null) {
            return OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
        } else {
            return op.versionType().isVersionConflictForWrites(versionValue.version, op.version(), versionValue.isDelete()) ?
                OpVsLuceneDocStatus.OP_STALE_OR_EQUAL : OpVsLuceneDocStatus.OP_NEWER;
        }
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
                    return true; // allow to optimize in order to update the max safe time stamp
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

    private boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) && origin == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            // legacy support
            assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : "old op recovering but it already has a seq no.;" +
                " index version: " + engineConfig.getIndexSettings().getIndexVersionCreated() + ", seqNo: " + seqNo;
        } else if (origin == Operation.Origin.PRIMARY) {
            assert assertOriginPrimarySequenceNumber(seqNo);
        } else if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
            // sequence number should be set when operation origin is not primary
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    protected boolean assertOriginPrimarySequenceNumber(final long seqNo) {
        // sequence number should not be set when operation origin is primary
        assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    private boolean assertSequenceNumberBeforeIndexing(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1) ||
            origin == Operation.Origin.PRIMARY) {
            // sequence number should be set when operation origin is primary or when all shards are on new nodes
            assert seqNo >= 0 : "ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    private long generateSeqNoForOperation(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        return doGenerateSeqNoForOperation(operation);
    }

    /**
     * Generate the sequence number for the specified operation.
     *
     * @param operation the operation
     * @return the sequence number
     */
    protected long doGenerateSeqNoForOperation(final Operation operation) {
        return seqNoService.generateSeqNo();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        assert Objects.equals(index.uid().field(), uidField) : index.uid().field();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            assert assertVersionType(index);
            try (Releasable ignored = acquireLock(index.uid());
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
                final IndexingStrategy plan;

                if (index.origin() == Operation.Origin.PRIMARY) {
                    plan = planIndexingAsPrimary(index);
                } else {
                    // non-primary mode (i.e., replica or recovery)
                    plan = planIndexingAsNonPrimary(index);
                }

                final IndexResult indexResult;
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    indexResult = plan.earlyResultOnPreFlightError.get();
                    assert indexResult.hasFailure();
                } else if (plan.indexIntoLucene) {
                    indexResult = indexIntoLucene(index, plan);
                } else {
                    indexResult = new IndexResult(
                            plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
                }
                if (index.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                    final Translog.Location location;
                    if (indexResult.hasFailure() == false) {
                        location = translog.add(new Translog.Index(index, indexResult));
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        // if we have document failure, record it as a no-op in the translog with the generated seq_no
                        location = translog.add(new Translog.NoOp(indexResult.getSeqNo(), index.primaryTerm(), indexResult.getFailure().getMessage()));
                    } else {
                        location = null;
                    }
                    indexResult.setTranslogLocation(location);
                }
                if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    seqNoService.markSeqNoAsCompleted(indexResult.getSeqNo());
                }
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
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

    private IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
        final IndexingStrategy plan;
        if (canOptimizeAddDocument(index) && mayHaveBeenIndexedBefore(index) == false) {
            // no need to deal with out of order delivery - we never saw this one
            assert index.version() == 1L : "can optimize on replicas but incoming version is [" + index.version() + "]";
            plan = IndexingStrategy.optimizedAppendOnly(index.seqNo());
        } else {
            // drop out of order operations
            assert index.versionType().versionTypeForReplicationAndRecovery() == index.versionType() :
                "resolving out of order delivery based on versioning but version type isn't fit for it. got [" + index.versionType() + "]";
            // unlike the primary, replicas don't really care to about creation status of documents
            // this allows to ignore the case where a document was found in the live version maps in
            // a delete state and return false for the created flag in favor of code simplicity
            final OpVsLuceneDocStatus opVsLucene;
            if (index.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                // This can happen if the primary is still on an old node and send traffic without seq# or we recover from translog
                // created by an old version.
                assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) :
                    "index is newly created but op has no sequence numbers. op: " + index;
                opVsLucene = compareOpToLuceneDocBasedOnVersions(index);
            } else if (index.seqNo() <= seqNoService.getLocalCheckpoint()){
                // the operation seq# is lower then the current local checkpoint and thus was already put into lucene
                // this can happen during recovery where older operations are sent from the translog that are already
                // part of the lucene commit (either from a peer recovery or a local translog)
                // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
                // question may have been deleted in an out of order op that is not replayed.
                // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
                // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
                opVsLucene = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
            } else {
                opVsLucene = compareOpToLuceneDocBasedOnSeqNo(index);
            }
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            } else {
                plan = IndexingStrategy.processNormally(
                    opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, index.seqNo(), index.version()
                );
            }
        }
        return plan;
    }

    private IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
        assert index.origin() == Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        final IndexingStrategy plan;
        // resolve an external operation into an internal one which is safe to replay
        if (canOptimizeAddDocument(index)) {
            if (mayHaveBeenIndexedBefore(index)) {
                plan = IndexingStrategy.overrideExistingAsIfNotThere(generateSeqNoForOperation(index), 1L);
            } else {
                plan = IndexingStrategy.optimizedAppendOnly(generateSeqNoForOperation(index));
            }
        } else {
            // resolves incoming version
            final VersionValue versionValue = resolveDocVersion(index);
            final long currentVersion;
            final boolean currentNotFoundOrDeleted;
            if (versionValue == null) {
                currentVersion = Versions.NOT_FOUND;
                currentNotFoundOrDeleted = true;
            } else {
                currentVersion = versionValue.version;
                currentNotFoundOrDeleted = versionValue.isDelete();
            }
            if (index.versionType().isVersionConflictForWrites(
                currentVersion, index.version(), currentNotFoundOrDeleted)) {
                final VersionConflictEngineException e =
                        new VersionConflictEngineException(shardId, index, currentVersion, currentNotFoundOrDeleted);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else {
                plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted,
                    generateSeqNoForOperation(index),
                    index.versionType().updateVersion(currentVersion, index.version())
                );
            }
        }
        return plan;
    }

    private IndexResult indexIntoLucene(Index index, IndexingStrategy plan)
        throws IOException {
        assert assertSequenceNumberBeforeIndexing(index.origin(), plan.seqNoForIndexing);
        assert plan.versionForIndexing >= 0 : "version must be set. got " + plan.versionForIndexing;
        assert plan.indexIntoLucene;
        /* Update the document's sequence number and primary term; the sequence number here is derived here from either the sequence
         * number service if this is on the primary, or the existing document's sequence number if this is on the replica. The
         * primary term here has already been set, see IndexShard#prepareIndex where the Engine$Index operation is created.
         */
        index.parsedDoc().updateSeqID(plan.seqNoForIndexing, index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        try {
            if (plan.useLuceneUpdateDocument) {
                update(index.uid(), index.docs(), indexWriter);
            } else {
                // document does not exists, we can optimize for create, but double check if assertions are running
                assert assertDocDoesNotExist(index, canOptimizeAddDocument(index) == false);
                index(index.docs(), indexWriter);
            }
            versionMap.putUnderLock(index.uid().bytes(),
                new VersionValue(plan.versionForIndexing, plan.seqNoForIndexing, index.primaryTerm()));
            return new IndexResult(plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
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
                 *
                 * we return a `MATCH_ANY` version to indicate no document was index. The value is
                 * not used anyway
                 */
                return new IndexResult(ex, Versions.MATCH_ANY, plan.seqNoForIndexing);
            } else {
                throw ex;
            }
        }
    }

    /**
     * returns true if the indexing operation may have already be processed by this engine.
     * Note that it is OK to rarely return true even if this is not the case. However a `false`
     * return value must always be correct.
     *
     */
    private boolean mayHaveBeenIndexedBefore(Index index) {
        assert canOptimizeAddDocument(index);
        boolean mayHaveBeenIndexBefore;
        long deOptimizeTimestamp = maxUnsafeAutoIdTimestamp.get();
        if (index.isRetry()) {
            mayHaveBeenIndexBefore = true;
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
            mayHaveBeenIndexBefore = deOptimizeTimestamp >= index.getAutoGeneratedIdTimestamp();
        }
        return mayHaveBeenIndexBefore;
    }

    private static void index(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    private static final class IndexingStrategy {
        final boolean currentNotFoundOrDeleted;
        final boolean useLuceneUpdateDocument;
        final long seqNoForIndexing;
        final long versionForIndexing;
        final boolean indexIntoLucene;
        final Optional<IndexResult> earlyResultOnPreFlightError;

        private IndexingStrategy(boolean currentNotFoundOrDeleted, boolean useLuceneUpdateDocument,
                                 boolean indexIntoLucene, long seqNoForIndexing,
                                 long versionForIndexing, IndexResult earlyResultOnPreFlightError) {
            assert useLuceneUpdateDocument == false || indexIntoLucene :
                "use lucene update is set to true, but we're not indexing into lucene";
            assert (indexIntoLucene && earlyResultOnPreFlightError != null) == false :
                "can only index into lucene or have a preflight result but not both." +
                    "indexIntoLucene: " + indexIntoLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
            this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
            this.useLuceneUpdateDocument = useLuceneUpdateDocument;
            this.seqNoForIndexing = seqNoForIndexing;
            this.versionForIndexing = versionForIndexing;
            this.indexIntoLucene = indexIntoLucene;
            this.earlyResultOnPreFlightError =
                earlyResultOnPreFlightError == null ? Optional.empty() :
                    Optional.of(earlyResultOnPreFlightError);
        }

        static IndexingStrategy optimizedAppendOnly(long seqNoForIndexing) {
            return new IndexingStrategy(true, false, true, seqNoForIndexing, 1, null);
        }

        static IndexingStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, boolean currentNotFoundOrDeleted, long currentVersion) {
            final IndexResult result = new IndexResult(e, currentVersion);
            return new IndexingStrategy(
                    currentNotFoundOrDeleted, false, false, SequenceNumbers.UNASSIGNED_SEQ_NO, Versions.NOT_FOUND, result);
        }

        static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted,
                                                long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, currentNotFoundOrDeleted == false,
                true, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy overrideExistingAsIfNotThere(
            long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(true, true, true, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy processButSkipLucene(boolean currentNotFoundOrDeleted,
                                                     long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, false,
                false, seqNoForIndexing, versionForIndexing, null);
        }
    }

    /**
     * Asserts that the doc in the index operation really doesn't exist
     */
    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        final VersionValue versionValue = versionMap.getUnderLock(index.uid().bytes());
        if (versionValue != null) {
            if (versionValue.isDelete() == false || allowDeleted == false) {
                throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists in version map (version " + versionValue + ")");
            }
        } else {
            try (Searcher searcher = acquireSearcher("assert doc doesn't exist", SearcherScope.INTERNAL)) {
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
        assert Objects.equals(delete.uid().field(), uidField) : delete.uid().field();
        assert assertVersionType(delete);
        assert assertIncomingSequenceNumber(delete.origin(), delete.seqNo());
        final DeleteResult deleteResult;
        // NOTE: we don't throttle this when merges fall behind because delete-by-id does not create new segments:
        try (ReleasableLock ignored = readLock.acquire(); Releasable ignored2 = acquireLock(delete.uid())) {
            ensureOpen();
            lastWriteNanos = delete.startTime();
            final DeletionStrategy plan;
            if (delete.origin() == Operation.Origin.PRIMARY) {
                plan = planDeletionAsPrimary(delete);
            } else {
                plan = planDeletionAsNonPrimary(delete);
            }

            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else if (plan.deleteFromLucene) {
                deleteResult = deleteInLucene(delete, plan);
            } else {
                deleteResult = new DeleteResult(
                        plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            }
            if (delete.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                final Translog.Location location;
                if (deleteResult.hasFailure() == false) {
                    location = translog.add(new Translog.Delete(delete, deleteResult));
                } else if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    location = translog.add(new Translog.NoOp(deleteResult.getSeqNo(),
                            delete.primaryTerm(), deleteResult.getFailure().getMessage()));
                } else {
                    location = null;
                }
                deleteResult.setTranslogLocation(location);
            }
            if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seqNoService.markSeqNoAsCompleted(deleteResult.getSeqNo());
            }
            deleteResult.setTook(System.nanoTime() - delete.startTime());
            deleteResult.freeze();
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        maybePruneDeletedTombstones();
        return deleteResult;
    }

    private DeletionStrategy planDeletionAsNonPrimary(Delete delete) throws IOException {
        assert delete.origin() != Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        // drop out of order operations
        assert delete.versionType().versionTypeForReplicationAndRecovery() == delete.versionType() :
            "resolving out of order delivery based on versioning but version type isn't fit for it. got ["
                + delete.versionType() + "]";
        // unlike the primary, replicas don't really care to about found status of documents
        // this allows to ignore the case where a document was found in the live version maps in
        // a delete state and return true for the found flag in favor of code simplicity
        final OpVsLuceneDocStatus opVsLucene;
        if (delete.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) :
                "index is newly created but op has no sequence numbers. op: " + delete;
            opVsLucene = compareOpToLuceneDocBasedOnVersions(delete);
        } else if (delete.seqNo() <= seqNoService.getLocalCheckpoint()) {
            // the operation seq# is lower then the current local checkpoint and thus was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            opVsLucene = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        } else {
            opVsLucene = compareOpToLuceneDocBasedOnSeqNo(delete);
        }

        final DeletionStrategy plan;
        if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
            plan = DeletionStrategy.processButSkipLucene(false, delete.seqNo(), delete.version());
        } else {
            plan = DeletionStrategy.processNormally(
                opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND,
                delete.seqNo(), delete.version());
        }
        return plan;
    }

    private DeletionStrategy planDeletionAsPrimary(Delete delete) throws IOException {
        assert delete.origin() == Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        // resolve operation from external to internal
        final VersionValue versionValue = resolveDocVersion(delete);
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
        if (delete.versionType().isVersionConflictForWrites(currentVersion, delete.version(), currentlyDeleted)) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete, currentVersion, currentlyDeleted);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
        } else {
            plan = DeletionStrategy.processNormally(
                    currentlyDeleted,
                    generateSeqNoForOperation(delete),
                    delete.versionType().updateVersion(currentVersion, delete.version()));
        }
        return plan;
    }

    private DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan)
        throws IOException {
        try {
            if (plan.currentlyDeleted == false) {
                // any exception that comes from this is a either an ACE or a fatal exception there
                // can't be any document failures  coming from this
                indexWriter.deleteDocuments(delete.uid());
            }
            versionMap.putUnderLock(delete.uid().bytes(),
                new DeleteVersionValue(plan.versionOfDeletion, plan.seqNoOfDeletion, delete.primaryTerm(),
                    engineConfig.getThreadPool().relativeTimeInMillis()));
            return new DeleteResult(
                plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                // there is no tragic event and such it must be a document level failure
                return new DeleteResult(
                        ex, plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            } else {
                throw ex;
            }
        }
    }

    private static final class DeletionStrategy {
        // of a rare double delete
        final boolean deleteFromLucene;
        final boolean currentlyDeleted;
        final long seqNoOfDeletion;
        final long versionOfDeletion;
        final Optional<DeleteResult> earlyResultOnPreflightError;

        private DeletionStrategy(boolean deleteFromLucene, boolean currentlyDeleted,
                                 long seqNoOfDeletion, long versionOfDeletion,
                                 DeleteResult earlyResultOnPreflightError) {
            assert (deleteFromLucene && earlyResultOnPreflightError != null) == false :
                "can only delete from lucene or have a preflight result but not both." +
                    "deleteFromLucene: " + deleteFromLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
            this.deleteFromLucene = deleteFromLucene;
            this.currentlyDeleted = currentlyDeleted;
            this.seqNoOfDeletion = seqNoOfDeletion;
            this.versionOfDeletion = versionOfDeletion;
            this.earlyResultOnPreflightError = earlyResultOnPreflightError == null ?
                Optional.empty() : Optional.of(earlyResultOnPreflightError);
        }

        static DeletionStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, long currentVersion, boolean currentlyDeleted) {
            final long unassignedSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            final DeleteResult deleteResult = new DeleteResult(e, currentVersion, unassignedSeqNo, currentlyDeleted == false);
            return new DeletionStrategy(false, currentlyDeleted, unassignedSeqNo, Versions.NOT_FOUND, deleteResult);
        }

        static DeletionStrategy processNormally(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(true, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);

        }

        public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(false, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }
    }

    private void maybePruneDeletedTombstones() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        if (engineConfig.isEnableGcDeletes() && engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
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
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        assert noOp.seqNo() > SequenceNumbers.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();
        try {
            final NoOpResult noOpResult = new NoOpResult(noOp.seqNo());
            final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
            noOpResult.setTranslogLocation(location);
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        } finally {
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seqNoService.markSeqNoAsCompleted(seqNo);
            }
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL);
    }

    final void refresh(String source, SearcherScope scope) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        // both refresh types will result in an internal refresh but only the external will also
        // pass the new reader reference to the external reader manager.

        // this will also cause version map ram to be freed hence we always account for it.
        final long bytes = indexWriter.ramBytesUsed() + versionMap.ramBytesUsedForRefresh();
        writingBytes.addAndGet(bytes);
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            switch (scope) {
                case EXTERNAL:
                    // even though we maintain 2 managers we really do the heavy-lifting only once.
                    // the second refresh will only do the extra work we have to do for warming caches etc.
                    externalSearcherManager.maybeRefreshBlocking();
                    // the break here is intentional we never refresh both internal / external together
                    break;
                case INTERNAL:
                    internalSearcherManager.maybeRefreshBlocking();
                    break;
                default:
                    throw new IllegalArgumentException("unknown scope: " + scope);
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
        }  finally {
            writingBytes.addAndGet(-bytes);
        }

        // TODO: maybe we should just put a scheduled job in threadPool?
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        maybePruneDeletedTombstones();
        mergeScheduler.refreshConfig();
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are writing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        refresh("write indexing buffer", SearcherScope.INTERNAL);
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
            refresh("sync_flush", SearcherScope.INTERNAL);
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
            if (syncId != null && translog.uncommittedOperations() == 0 && indexWriter.hasUncommittedChanges()) {
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
            // we have to refresh internal searcher here to ensure we release unreferenced segments.
            refresh("renew sync commit", SearcherScope.INTERNAL);
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
                        translog.rollGeneration();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");
                        // we need to refresh in order to clear older version values
                        refresh("version_table_flush", SearcherScope.INTERNAL);
                        translog.trimUnreferencedReaders();
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
    public void trimTranslog() throws EngineException {
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

    private void pruneDeletedTombstones() {
        long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();

        // TODO: not good that we reach into LiveVersionMap here; can we move this inside VersionMap instead?  problem is the dirtyLock...

        // we only need to prune the deletes map; the current/old version maps are cleared on refresh:
        for (Map.Entry<BytesRef, DeleteVersionValue> entry : versionMap.getAllTombstones()) {
            BytesRef uid = entry.getKey();
            try (Releasable ignored = acquireLock(uid)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?

                // Must re-get it here, vs using entry.getValue(), in case the uid was indexed/deleted since we pulled the iterator:
                DeleteVersionValue versionValue = versionMap.getTombstoneUnderLock(uid);
                if (versionValue != null) {
                    if (timeMSec - versionValue.time > getGcDeletesInMillis()) {
                        versionMap.removeTombstoneUnderLock(uid);
                    }
                }
            }
        }

        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    // testing
    void clearDeletedTombstones() {
        versionMap.clearTombstones();
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
    public IndexCommitRef acquireIndexCommit(final boolean flushFirst) throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        try (ReleasableLock lock = readLock.acquire()) {
            logger.trace("pulling snapshot");
            return new IndexCommitRef(deletionPolicy.getIndexDeletionPolicy());
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        }
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        // if we are already closed due to some tragic exception
        // we need to fail the engine. it might have already been failed before
        // but we are double-checking it's failed and closed
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            maybeDie("tragic event in index writer", indexWriter.getTragicException());
            failEngine("already closed by tragic event on the index writer", (Exception) indexWriter.getTragicException());
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
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                if (internalSearcherManager != null) {
                    internalSearcherManager.removeListener(versionMap);
                }
                try {
                    IOUtils.close(externalSearcherManager, internalSearcherManager);
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
    protected ReferenceManager<IndexSearcher> getSearcherManager(String source, SearcherScope scope) {
        switch (scope) {
            case INTERNAL:
                return internalSearcherManager;
            case EXTERNAL:
                return externalSearcherManager;
            default:
                throw new IllegalStateException("unknown scope: " + scope);
        }
    }

    private Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    private Releasable acquireLock(Term uid) {
        return acquireLock(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        assert incrementIndexVersionLookup();
        try (Searcher searcher = acquireSearcher("load_version", SearcherScope.INTERNAL)) {
            return VersionsAndSeqNoResolver.loadVersion(searcher.reader(), uid);
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
        if (config().getIndexSort() != null) {
            iwc.setIndexSort(config().getIndexSort());
        }
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
                    maybeDie("fatal error while merging", exc);
                    logger.error("failed to merge", exc);
                    failEngine("merge failed", new MergePolicy.MergeException(exc, dir));
                }
            });
        }
    }

    /**
     * If the specified throwable is a fatal error, this throwable will be thrown. Callers should ensure that there are no catch statements
     * that would catch an error in the stack as the fatal error here should go uncaught and be handled by the uncaught exception handler
     * that we install during bootstrap. If the specified throwable is indeed a fatal error, the specified message will attempt to be logged
     * before throwing the fatal error. If the specified throwable is not a fatal error, this method is a no-op.
     *
     * @param maybeMessage the message to maybe log
     * @param maybeFatal the throwable that is maybe fatal
     */
    @SuppressWarnings("finally")
    private void maybeDie(final String maybeMessage, final Throwable maybeFatal) {
        if (maybeFatal instanceof Error) {
            try {
                logger.error(maybeMessage, maybeFatal);
            } finally {
                throw (Error) maybeFatal;
            }
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
        final long localCheckpoint = seqNoService.getLocalCheckpoint();
        final Translog.TranslogGeneration translogGeneration = translog.getMinGenerationForSeqNo(localCheckpoint + 1);
        final String translogFileGeneration = Long.toString(translogGeneration.translogFileGeneration);
        final String translogUUID = translogGeneration.translogUUID;
        final String localCheckpointValue = Long.toString(localCheckpoint);

        final Iterable<Map.Entry<String, String>> commitIterable = () -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different, we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index. Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the maximum sequence number to the time
                 * of invocation of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
            final Map<String, String> commitData = new HashMap<>(6);
            commitData.put(Translog.TRANSLOG_GENERATION_KEY, translogFileGeneration);
            commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
            commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpointValue);
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(seqNoService.getMaxSeqNo()));
            commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
            commitData.put(HISTORY_UUID_KEY, historyUUID);
            logger.trace("committing writer with commit data [{}]", commitData);
            return commitData.entrySet().iterator();
        };
        commitIndexWriter(writer, commitIterable);
    }

    private void commitIndexWriter(IndexWriter writer, Iterable<Map.Entry<String, String>> userData) throws IOException {
        try {
            ensureCanFlush();
            writer.setLiveCommitData(userData);
            writer.commit();
            // assert we don't loose key entries
            assert commitDataAsMap(writer).containsKey(Translog.TRANSLOG_UUID_KEY) : "commit misses translog uuid";
            assert commitDataAsMap(writer).containsKey(Translog.TRANSLOG_GENERATION_KEY) : "commit misses translog generation";
            assert commitDataAsMap(writer).containsKey(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) ||
                config().getIndexSettings().getIndexVersionCreated().before(Version.V_5_5_0): "commit misses max unsafe timestamp";
            assert commitDataAsMap(writer).containsKey(HISTORY_UUID_KEY) : "commit misses a history uuid";
            assert commitDataAsMap(writer).containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) ||
                config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1): "commit misses local checkpoint";
            assert commitDataAsMap(writer).containsKey(SequenceNumbers.MAX_SEQ_NO) ||
                config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) : "commit misses max seq no";

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
        maybePruneDeletedTombstones();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            // this is an anti-viral settings you can only opt out for the entire index
            // only if a shard starts up again due to relocation or if the index is closed
            // the setting will be re-interpreted if it's set to true
            this.maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translog.getDeletionPolicy();
        final IndexSettings indexSettings = engineConfig.getIndexSettings();
        translogDeletionPolicy.setRetentionAgeInMillis(indexSettings.getTranslogRetentionAge().getMillis());
        translogDeletionPolicy.setRetentionSizeInBytes(indexSettings.getTranslogRetentionSize().getBytes());
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    public final SequenceNumbersService seqNoService() {
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

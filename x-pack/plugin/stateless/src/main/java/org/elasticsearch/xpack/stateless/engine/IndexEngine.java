/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.CommitBCCResolver;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.ShardLocalReadersTracker;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.ElasticsearchMergeScheduler;
import org.elasticsearch.index.engine.ElasticsearchReaderManager;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.LiveVersionMapArchive;
import org.elasticsearch.index.engine.MergeMemoryEstimateProvider;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.engine.ThreadPoolMergeExecutorService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;

/**
 * {@link Engine} implementation for index shards
 */
public class IndexEngine extends InternalEngine {

    public static final String TRANSLOG_RECOVERY_START_FILE = "translog_recovery_start_file";
    public static final String TRANSLOG_RELEASE_END_FILE = "translog_release_end_file";
    public static final Setting<Boolean> MERGE_PREWARM = Setting.boolSetting("stateless.merge.prewarm", true, Setting.Property.NodeScope);
    // If the size of a merge is greater than or equal to this, force a refresh to allow its space to be reclaimed immediately.
    public static final Setting<ByteSizeValue> MERGE_FORCE_REFRESH_SIZE = Setting.byteSizeSetting(
        "stateless.merge.force_refresh_size",
        ByteSizeValue.ofMb(64),
        Setting.Property.NodeScope
    );
    // A flag for whether the flush call is originated from a refresh
    private static final ThreadLocal<Boolean> IS_FLUSH_BY_REFRESH = ThreadLocal.withInitial(() -> false);

    private final TranslogReplicator translogReplicator;
    private final StatelessCommitService statelessCommitService;
    private final HollowShardsService hollowShardsService;
    private final Function<String, BlobContainer> translogBlobContainer;
    private final RefreshThrottler refreshThrottler;
    private final long mergeForceRefreshSize;
    private final CommitBCCResolver commitBCCResolver;
    private final DocumentSizeAccumulator documentSizeAccumulator;
    private final DocumentSizeReporter documentParsingReporter;
    private final TranslogRecoveryMetrics translogRecoveryMetrics;
    private final SharedBlobCacheWarmingService cacheWarmingService;
    private final Predicate<ShardId> shouldSkipMerges;
    private volatile long hollowMaxSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    // This is written and then accessed on the same thread under the flush lock. So not need for volatile
    private long translogStartFileForNextCommit = 0;
    private final ShardLocalReadersTracker localReadersTracker;

    private final AtomicBoolean ongoingFlushMustUpload = new AtomicBoolean(false);
    private final AtomicInteger forceMergesInProgress = new AtomicInteger(0);
    private final AtomicInteger queuedOrRunningMergesCount = new AtomicInteger();

    @SuppressWarnings("this-escape")
    public IndexEngine(
        EngineConfig engineConfig,
        TranslogReplicator translogReplicator,
        Function<String, BlobContainer> translogBlobContainer,
        StatelessCommitService statelessCommitService,
        HollowShardsService hollowShardsService,
        SharedBlobCacheWarmingService cacheWarmingService,
        RefreshThrottler.Factory refreshThrottlerFactory,
        CommitBCCResolver commitBCCResolver,
        DocumentParsingProvider documentParsingProvider,
        EngineMetrics metrics,
        ShardLocalReadersTracker shardLocalReadersTracker
    ) {
        this(
            engineConfig,
            translogReplicator,
            translogBlobContainer,
            statelessCommitService,
            hollowShardsService,
            cacheWarmingService,
            refreshThrottlerFactory,
            commitBCCResolver,
            documentParsingProvider,
            metrics,
            (shardId) -> false,
            shardLocalReadersTracker
        );
    }

    @SuppressWarnings("this-escape")
    public IndexEngine(
        EngineConfig engineConfig,
        TranslogReplicator translogReplicator,
        Function<String, BlobContainer> translogBlobContainer,
        StatelessCommitService statelessCommitService,
        HollowShardsService hollowShardsService,
        SharedBlobCacheWarmingService cacheWarmingService,
        RefreshThrottler.Factory refreshThrottlerFactory,
        CommitBCCResolver commitBCCResolver,
        DocumentParsingProvider documentParsingProvider,
        EngineMetrics metrics,
        Predicate<ShardId> shouldSkipMerges,
        ShardLocalReadersTracker shardLocalReadersTracker
    ) {
        super(engineConfig);
        assert engineConfig.isPromotableToPrimary();
        this.translogReplicator = translogReplicator;
        this.translogBlobContainer = translogBlobContainer;
        this.statelessCommitService = statelessCommitService;
        this.hollowShardsService = hollowShardsService;
        this.cacheWarmingService = cacheWarmingService;
        this.refreshThrottler = refreshThrottlerFactory.create(this::doExternalRefresh);
        this.mergeForceRefreshSize = MERGE_FORCE_REFRESH_SIZE.get(config().getIndexSettings().getSettings()).getBytes();
        this.commitBCCResolver = commitBCCResolver;
        this.documentSizeAccumulator = documentParsingProvider.createDocumentSizeAccumulator();
        this.documentParsingReporter = documentParsingProvider.newDocumentSizeReporter(
            shardId.getIndex(),
            engineConfig.getMapperService(),
            documentSizeAccumulator
        );
        this.shouldSkipMerges = shouldSkipMerges;
        this.localReadersTracker = shardLocalReadersTracker;
        // We have to track the initial BCC references held by local readers at this point instead of doing it in
        // #createInternalReaderManager because that method is called from the super constructor and at that point,
        // commitBCCResolver field is not set yet.
        var referenceManager = getReferenceManager(SearcherScope.INTERNAL);
        try {
            ElasticsearchDirectoryReader directoryReader = referenceManager.acquire();
            try {
                trackOpenLocalReader(directoryReader);
            } finally {
                referenceManager.release(directoryReader);
            }
        } catch (IOException e) {
            throw new EngineCreationFailureException(engineConfig.getShardId(), "Failed to create an index engine", e);
        }
        this.translogRecoveryMetrics = metrics.translogRecoveryMetrics();
    }

    @Override
    protected LongConsumer translogPersistedSeqNoConsumer() {
        return seqNo -> {};
    }

    public LongConsumer objectStorePersistedSeqNoConsumer() {
        return seqNo -> {
            final LocalCheckpointTracker tracker = getLocalCheckpointTracker();
            if (tracker != null) {
                tracker.markSeqNoAsPersisted(seqNo);
            }
        };
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return getPersistedLocalCheckpoint();
    }

    @Override
    protected ElasticsearchReaderManager createInternalReaderManager(ElasticsearchDirectoryReader directoryReader) {
        return new ElasticsearchReaderManager(directoryReader) {
            @Override
            protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
                ElasticsearchDirectoryReader next = super.refreshIfNeeded(referenceToRefresh);
                if (next == null) {
                    return null;
                }
                boolean success = false;
                try {
                    trackOpenLocalReader(next);
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(next);
                    }
                }
                return next;
            }
        };
    }

    private void trackOpenLocalReader(ElasticsearchDirectoryReader directoryReader) {
        long generation = getLatestCommittedGeneration(directoryReader);

        var referencedBCCsForCommit = commitBCCResolver.resolveReferencedBCCsForCommit(generation);
        // If the set of referenced BCC commits is empty it means that the shard has been relocated or closed,
        // in that case we're not interested tracking this newly open reader for file deletions.
        if (referencedBCCsForCommit.isEmpty()) {
            return;
        }

        ElasticsearchDirectoryReader.addReaderCloseListener(
            directoryReader,
            ignored -> localReadersTracker.onLocalReaderClosed(directoryReader)
        );
        localReadersTracker.trackOpenReader(directoryReader, referencedBCCsForCommit);
    }

    static long getLatestCommittedGeneration(DirectoryReader directoryReader) {
        if (FilterDirectoryReader.unwrap(directoryReader) instanceof StandardDirectoryReader standardDirectoryReader) {
            // If there's a concurrent flush while the refresh is executed, the generation from
            // getIndexCommit().getGeneration() will point to the yet to be committed Lucene commit generation
            // therefore, we need to fetch the last generation that refers to the latest successful Lucene commit
            return standardDirectoryReader.getSegmentInfos().getLastGeneration();
        }

        try {
            assert false;
            return directoryReader.getIndexCommit().getGeneration();
        } catch (IOException e) {
            assert false;
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean refreshNeeded() {
        var lastCommittedSegmentInfos = getLastCommittedSegmentInfos();
        if (isLastCommitHollow(lastCommittedSegmentInfos)) {
            return false;
        }
        boolean committedLocalCheckpointNeedsUpdate = getProcessedLocalCheckpoint() > Long.parseLong(
            lastCommittedSegmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)
        );
        // It is possible that the index writer has uncommitted changes. We could check here, but we will check before actually
        // triggering the flush anyway.
        return hasUncommittedChanges() || committedLocalCheckpointNeedsUpdate || super.refreshNeeded();
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        // A regular flush, i.e. not converted from refresh, must trigger to a commit generation to be uploaded
        // (by increase maxGenerationToUploadDueToFlush).
        // We set ongoingFlushMustUpload to true so that if this thread does not flush on its own (because the
        // flush lock is held by another thread and this thread does not wait for it), some other concurrent
        // flushing thread will promise to do it.
        // This protocol does not care exactly which thread ends up doing the job. It could be any of the concurrent
        // flush threads including this one. The setMaxGenerationToUploadDueToFlush method will be called
        // exactly once to a generation processed by one of the threads.
        // If the flush thread errors before ongoingFlushMustUpload can be cleared, the next flush will handle it.
        // Note the behaviour is still same if we just check `IS_FLUSH_BY_REFRESH.get() == false`.
        // However this may in some cases trigger more than one uploads, e.g. another thread may see the flag and
        // trigger upload immediately while this thread creates a new commit and should also upload.
        if (IS_FLUSH_BY_REFRESH.get()) {
            logger.trace("flush-by-refresh for {}", shardId);
        } else {
            logger.trace("flush for {}", shardId);
            if (force == false && waitIfOngoing == false) {
                ongoingFlushMustUpload.set(true);
            }
        }
        super.flushHoldingLock(force, waitIfOngoing, listener);
    }

    @Override
    protected void commitIndexWriter(IndexWriter writer, Translog translog) throws IOException {
        // We must fetch the max uploaded translog file BEFORE performing the commit. Since all of those operations were written to
        // Lucene at this point, it is safe to start with the next file. The flush thread synchronously kicks of the commit upload
        // process, so for now we just store the start file as a thread local.
        translogStartFileForNextCommit = translogReplicator.getMaxUploadedFile() + 1;
        super.commitIndexWriter(writer, translog);
    }

    @Override
    protected void afterFlush(long generation) {
        assert isFlushLockIsHeldByCurrentThread() == false;
        if (ongoingFlushMustUpload.compareAndSet(true, false) || IS_FLUSH_BY_REFRESH.get() == false) {
            logger.trace("flush sets max generation of {} to generation [{}]", shardId, generation);
            statelessCommitService.ensureMaxGenerationToUploadForFlush(shardId, generation);
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        checkNoNewOperationsWhileHollow();
        ParsedDocument parsedDocument = index.parsedDoc();

        documentParsingReporter.onParsingCompleted(parsedDocument);
        IndexResult result = super.index(index);

        if (result.getResultType() == Result.Type.SUCCESS) {
            documentParsingReporter.onIndexingCompleted(parsedDocument);
        }
        return result;
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        checkNoNewOperationsWhileHollow();
        return super.delete(delete);
    }

    /**
     * Marks the engine as hollow by making sure a last commit is flushed that has a {@link #TRANSLOG_RECOVERY_START_FILE} user data equal
     * to {@link co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit#HOLLOW_TRANSLOG_RECOVERY_START_FILE}.
     * This function should be called only when there is no concurrent ingestion, e.g., when holding/blocking all shard's primary permits.
     *
     * Note that even if concurrent flush(es) are ongoing, this function will force a hollow commit.
     * The function is ineffective if the last commit is already hollow.
     *
     * Since no translog will be associated with the hollow commit, any new operations attempted to be ingested by the engine will throw
     * an exception. This is done by pinpointing the max sequence number when hollowing (while holding the primary permits), and any
     * future attempt to generate a next sequence number will throw. To ingest new data, the shard will need to be unhollowed (i.e.,
     * producing a non-hollow blob) with a new engine.
     */
    protected void flushHollow(ActionListener<FlushResult> listener) {
        if (isLastCommitHollow()) {
            listener.onResponse(FlushResult.FLUSH_REQUEST_PROCESSED_AND_NOT_PERFORMED);
        } else {
            long maxSeqNo = getMaxSeqNo();
            assert hollowMaxSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || hollowMaxSeqNo == maxSeqNo
                : "engine hollowed with max seq no [" + hollowMaxSeqNo + "] cannot be hollowed again with max seq no [" + maxSeqNo + "]";
            this.hollowMaxSeqNo = maxSeqNo;
            flush(true, true, listener);
        }
    }

    protected void checkNoNewOperationsWhileHollow() {
        // If flushHollow() has been called (which assumes holding the primary permits), we expect no new operations to be ingested.
        // Any new operation attempted to be ingested after hollowing will throw an exception (that signifies a bug since we expect
        // the shard to be unhollowed with a new engine to process ingestion).
        if (hollowMaxSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalStateException("cannot ingest new operation when engine is hollow");
        }
        if (Assertions.ENABLED && isLastCommitHollow()) {
            hollowShardsService.ensureHollowShard(shardId, true);
        }
    }

    public boolean isLastCommitHollow() {
        return isLastCommitHollow(getLastCommittedSegmentInfos());
    }

    public static boolean isLastCommitHollow(SegmentInfos segmentInfos) {
        String translogRecoveryStartFile = segmentInfos.getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE);
        return translogRecoveryStartFile != null ? Long.parseLong(translogRecoveryStartFile) == HOLLOW_TRANSLOG_RECOVERY_START_FILE : false;
    }

    @Override
    protected Map<String, String> getCommitExtraUserData(final long localCheckpoint) {
        var accumulatorUserData = documentSizeAccumulator.getAsCommitUserData(getLastCommittedSegmentInfos());
        final Map<String, String> commitExtraUserData;

        long translogRecoveryStartFile = translogStartFileForNextCommit;
        // We check the local checkpoint to ensure that only a commit that has committed all operations is marked as hollow.
        if (hollowMaxSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO && localCheckpoint == hollowMaxSeqNo) {
            assert hollowMaxSeqNo == getMaxSeqNo()
                : "engine has ingested after being hollowed with max seq no ["
                    + hollowMaxSeqNo
                    + "] and current max seq no ["
                    + getMaxSeqNo()
                    + "]";
            logger.debug(
                () -> "flushing hollow commit with max seq no " + hollowMaxSeqNo + " and generation " + (getCurrentGeneration() + 1)
            );
            commitExtraUserData = Maps.newMapWithExpectedSize(2 + accumulatorUserData.size());
            commitExtraUserData.put(TRANSLOG_RELEASE_END_FILE, Long.toString(translogRecoveryStartFile));
            translogRecoveryStartFile = HOLLOW_TRANSLOG_RECOVERY_START_FILE;
        } else {
            commitExtraUserData = Maps.newMapWithExpectedSize(1 + accumulatorUserData.size());
        }

        commitExtraUserData.putAll(accumulatorUserData);
        commitExtraUserData.put(TRANSLOG_RECOVERY_START_FILE, Long.toString(translogRecoveryStartFile));
        return Collections.unmodifiableMap(commitExtraUserData);
    }

    @Override
    public void prepareForEngineReset() throws IOException {
        // We do not need to care about primary term and generation listeners of the engine as these are used only in the search tier.
        logger.debug(() -> "preparing to reset index engine for shard " + shardId);
        // The shard is not yet marked as hollow in the HollowShardsService. It will be marked as hollow after the engine is reset.
        hollowShardsService.ensureHollowShard(shardId, false, "hollowing the index engine requires the shard to be unhollow");
        // The primary relocation will wait for the hollowed commit to upload. Even if the flush fails, the engine will be reset to a hollow
        // engine and either closed (upon a successful relocation) or continue to live and be unhollowed by any lingering or new ingestion.
        flushHollow(ActionListener.noop());
    }

    @Override
    protected RefreshResult refreshInternalSearcher(String source, boolean block) throws EngineException {
        if (source.equals(REAL_TIME_GET_REFRESH_SOURCE) || source.equals(UNSAFE_VERSION_MAP_REFRESH_SOURCE)) {
            try {
                IS_FLUSH_BY_REFRESH.set(true);
                // TODO: Eventually the Refresh API will also need to transition (maybe) to an async API here.
                flush(true, true);
            } finally {
                IS_FLUSH_BY_REFRESH.set(false);
            }
        }
        // TODO: could we avoid this refresh if we have flushed above?
        return super.refreshInternalSearcher(source, block);
    }

    // visible for testing
    public long getCurrentGeneration() {
        return getLastCommittedSegmentInfos().getGeneration();
    }

    // visible for testing
    Map<DirectoryReader, Set<PrimaryTermAndGeneration>> getOpenReaders() {
        return localReadersTracker.getOpenReaders();
    }

    @Override
    public boolean allowSearchIdleOptimization() {
        return false;
    }

    @Override
    public void externalRefresh(String source, ActionListener<RefreshResult> listener) {
        // TODO: should we first check if a flush/refresh is needed or not? If not we could simply not go
        // through the throttler.
        refreshThrottler.maybeThrottle(new RefreshThrottler.Request(source, listener));
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        try {
            IS_FLUSH_BY_REFRESH.set(true);
            Thread originalThread = Thread.currentThread();
            // Maybe refresh is called on scheduled periodic refreshes and needs to flush so that the search shards received the data.
            flush(false, false, listener.delegateFailure((l, flushResult) -> {
                ActionRunnable<RefreshResult> refreshRunnable = new ActionRunnable<>(listener) {

                    @Override
                    protected void doRun() {
                        IndexEngine.super.maybeRefresh(source, listener);
                    }
                };

                dispatchRefreshRunnable(originalThread, refreshRunnable);
            }));
        } finally {
            IS_FLUSH_BY_REFRESH.set(false);
        }
    }

    private void doExternalRefresh(RefreshThrottler.Request request) {
        try {
            IS_FLUSH_BY_REFRESH.set(true);
            Thread originalThread = Thread.currentThread();
            flush(true, true, request.listener().delegateFailure((l, flushResult) -> {
                ActionRunnable<RefreshResult> refreshRunnable = new ActionRunnable<>(l) {

                    @Override
                    protected void doRun() {
                        IndexEngine.super.externalRefresh(request.source(), listener);
                    }
                };
                dispatchRefreshRunnable(originalThread, refreshRunnable);
            }));
        } catch (AlreadyClosedException ace) {
            request.listener().onFailure(ace);
        } finally {
            IS_FLUSH_BY_REFRESH.set(false);
        }
    }

    private void dispatchRefreshRunnable(Thread originalThread, ActionRunnable<RefreshResult> refreshRunnable) {
        // Sometimes a flush will have been performed meaning we are likely on the object store thread pool now. Dispatch back if the thread
        // has changed
        ThreadPool threadPool = engineConfig.getThreadPool();
        if (Thread.currentThread() == originalThread) {
            refreshRunnable.run();
        } else {
            threadPool.executor(ThreadPool.Names.REFRESH).execute(refreshRunnable);
        }
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        super.asyncEnsureTranslogSynced(location, e -> {
            if (e != null) {
                listener.accept(e);
            } else {
                translogReplicator.sync(shardId, location, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        listener.accept(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.accept(e);
                    }
                });
            }
        });
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return super.isTranslogSyncNeeded() || translogReplicator.isSyncNeeded(shardId);
    }

    @Override
    public void syncTranslog() throws IOException {
        assert Thread.currentThread().getName().contains("[" + ThreadPool.Names.WRITE + "]") == false
            : "Expected current thread [" + Thread.currentThread() + "] to not be on a write thread. Reason: [syncTranslog]";
        super.syncTranslog();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        try {
            future.actionGet();
        } catch (Exception e) {
            throw new IOException("Exception while syncing translog remotely", e);
        }
    }

    public void syncTranslogReplicator(ActionListener<Void> listener) {
        translogReplicator.syncAll(shardId, listener);
    }

    @Override
    public void flushAndClose() throws IOException {
        // Don't flush on closing to avoid doing blobstore IO for reading back the latest commit from the repository
        // if it's not cached or doing an actual flush if there's outstanding translog operations.
        close();
    }

    // package-protected for testing
    void awaitClose() {
        super.awaitPendingClose();
    }

    @Override
    public LiveVersionMapArchive createLiveVersionMapArchive() {
        return new StatelessLiveVersionMapArchive(this::getPreCommitSegmentGeneration);
    }

    public void commitSuccess(long generation) {
        ((StatelessLiveVersionMapArchive) getLiveVersionMapArchive()).afterUnpromotablesRefreshed(generation);
    }

    @Override
    protected Translog.Snapshot newTranslogSnapshot(long fromSeqNo, long toSeqNo) throws IOException {
        IndexDirectory indexDirectory = IndexDirectory.unwrapDirectory(this.store.directory());
        Optional<String> nodeEphemeralId = indexDirectory.getRecoveryCommitMetadataNodeEphemeralId();
        long translogRecoveryStartFile = indexDirectory.getTranslogRecoveryStartFile();

        if (nodeEphemeralId.isPresent()) {
            logger.debug("new translog snapshot seqnos [{}]-[{}] and node ephemeral id [{}]", fromSeqNo, toSeqNo, nodeEphemeralId.get());
            BlobContainer translogBlobContainer = this.translogBlobContainer.apply(nodeEphemeralId.get());
            TranslogReplicatorReader reader = new TranslogReplicatorReader(
                translogBlobContainer,
                shardId,
                fromSeqNo,
                toSeqNo,
                translogRecoveryStartFile,
                this::isClosing,
                translogRecoveryMetrics
            );
            return new Translog.Snapshot() {
                @Override
                public int totalOperations() {
                    // The reader returns an estimated number of operations which will inform the stats.
                    return reader.totalOperations();
                }

                @Override
                public Translog.Operation next() throws IOException {
                    Translog.Operation next = reader.next();
                    if (next != null) {
                        advanceMaxSeqNoOfUpdatesOrDeletes(next.seqNo());
                    }
                    return next;
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };
        } else {
            return Translog.Snapshot.EMPTY;
        }
    }

    @Override
    protected void waitForCommitDurability(long generation, ActionListener<Void> listener) {
        try {
            ensureOpen();
        } catch (AlreadyClosedException e) {
            listener.onFailure(e);
            return;
        }
        if (getLastCommittedSegmentInfos().getGeneration() < generation) {
            listener.onFailure(new IllegalStateException("Cannot wait on generation which has not been committed"));
        } else {
            // Wait for upload to complete only for true flushes, i.e. _not_ converted from refreshes, which guarantee
            // a commit to be uploaded.
            if (IS_FLUSH_BY_REFRESH.get() == false) {
                statelessCommitService.addListenerForUploadedGeneration(shardId, generation, listener);
            } else {
                logger.trace(() -> Strings.format("no need to wait for non-uploaded generation [%s]", generation));
                listener.onResponse(null);
            }
        }
    }

    @Override
    protected void reclaimVersionMapMemory() {
        // For Stateless LVM, we need to refresh AND flush as a refresh by itself doesn't decrease the memory usage of the version map.
        refresh("write indexing buffer", SearcherScope.INTERNAL, false);
        flush(false, false, ActionListener.noop());
    }

    // For cleanup after resharding
    public void deleteUnownedDocuments(ShardSplittingQuery query) throws Exception {
        super.deleteByQuery(query);
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) throws EngineException,
        IOException {
        forceMergesInProgress.incrementAndGet();
        try (Releasable ignored = forceMergesInProgress::decrementAndGet) {
            int before = getLastCommittedSegmentInfos().size();
            super.forceMerge(flush, maxNumSegments, onlyExpungeDeletes, forceMergeUUID);
            if (flush) {
                var info = getLastCommittedSegmentInfos();
                int after = info.size();
                boolean merged = Objects.equals(info.getUserData().get(FORCE_MERGE_UUID_KEY), forceMergeUUID);
                logger.info("Completed force merge for shard {}. forceMergeSet={}, segment count {} -> {}", shardId, merged, before, after);
            } else {
                logger.info("Completed force merge for shard {} without flushing", shardId);
            }
        } catch (EngineException | IOException e) {
            logger.warn(() -> Strings.format("Force merge failed for shard %s", shardId), e);
            throw e;
        }
    }

    // package private for testing

    RefreshThrottler getRefreshThrottler() {
        return refreshThrottler;
    }

    public StatelessCommitService getStatelessCommitService() {
        return statelessCommitService;
    }

    private void onAfterMerge(OnGoingMerge merge) {
        // A merge can occupy a lot of disk space that can't be reused until it has been pushed into the object store, so it
        // can be worth refreshing immediately to allow that space to be reclaimed faster.
        if (merge.getTotalBytesSize() >= mergeForceRefreshSize) {
            try {
                maybeRefresh("large merge", ActionListener.noop());
            } catch (AlreadyClosedException e) {
                // There can be a race with a merge when the IW is closing and trying to flush. This is fine to ignore.
            }
        }
    }

    @Override
    protected ElasticsearchMergeScheduler createMergeScheduler(
        ShardId shardId,
        IndexSettings indexSettings,
        @Nullable ThreadPoolMergeExecutorService threadPoolMergeExecutorService,
        MergeMetrics mergeMetrics
    ) {
        if (threadPoolMergeExecutorService != null) {
            return new StatelessThreadPoolMergeScheduler(
                shardId,
                indexSettings,
                threadPoolMergeExecutorService,
                this::estimateMergeBytes,
                mergeMetrics
            );
        } else {
            return super.createMergeScheduler(shardId, indexSettings, threadPoolMergeExecutorService, mergeMetrics);
        }
    }

    private void onMergeEnqueued(OnGoingMerge merge) {
        var queuedOrRunningMerges = queuedOrRunningMergesCount.incrementAndGet();
        assert queuedOrRunningMerges > 0;
    }

    private void onMergeExecutedOrAborted(OnGoingMerge merge) {
        var remainingMerges = queuedOrRunningMergesCount.decrementAndGet();
        assert remainingMerges >= 0;
    }

    public boolean hasQueuedOrRunningMerges() {
        return queuedOrRunningMergesCount.get() > 0;
    }

    private final class StatelessThreadPoolMergeScheduler extends org.elasticsearch.index.engine.ThreadPoolMergeScheduler {
        private final boolean prewarm;

        StatelessThreadPoolMergeScheduler(
            ShardId shardId,
            IndexSettings indexSettings,
            ThreadPoolMergeExecutorService threadPoolMergeExecutorService,
            MergeMemoryEstimateProvider mergeMemoryEstimateProvider,
            MergeMetrics mergeMetrics
        ) {
            super(shardId, indexSettings, threadPoolMergeExecutorService, mergeMemoryEstimateProvider, mergeMetrics);
            prewarm = MERGE_PREWARM.get(indexSettings.getSettings());
        }

        @Override
        protected void beforeMerge(OnGoingMerge merge) {
            if (prewarm) {
                cacheWarmingService.warmCacheForMerge(merge.getId(), shardId, store, merge.getMerge(), fileName -> {
                    BatchedCompoundCommit latestUploadedBcc = statelessCommitService.getLatestUploadedBcc(shardId);
                    BlobLocation blobLocation = statelessCommitService.getBlobLocation(shardId, fileName);
                    if (blobLocation != null && latestUploadedBcc != null) {
                        // Only return the location if the file is uploaded as we don't want to try warming an un-uploaded file
                        if (blobLocation.getBatchedCompoundCommitTermAndGeneration()
                            .compareTo(latestUploadedBcc.primaryTermAndGeneration()) <= 0) {
                            return blobLocation;
                        }
                    }
                    return null;
                });
            }
        }

        @Override
        protected void afterMerge(OnGoingMerge merge) {
            onAfterMerge(merge);
        }

        @Override
        protected void mergeQueued(OnGoingMerge merge) {
            onMergeEnqueued(merge);
        }

        @Override
        protected void mergeExecutedOrAborted(OnGoingMerge merge) {
            onMergeExecutedOrAborted(merge);
        }

        @Override
        protected boolean shouldSkipMerge() {
            return forceMergesInProgress.get() == 0 && shouldSkipMerges.test(shardId);
        }

        @Override
        protected boolean isAutoThrottle() {
            return false;
        }

        @Override
        protected int getMaxMergeCount() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected int getMaxThreadCount() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void refreshConfig() {
            // no-op
        }

        @Override
        protected void handleMergeException(Throwable t) {
            mergeException(t);
        }
    }

    public record EngineMetrics(
        TranslogRecoveryMetrics translogRecoveryMetrics,
        MergeMetrics mergeMetrics,
        HollowShardsMetrics hollowShardsMetrics
    ) {}
}

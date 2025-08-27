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
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.commits.HollowIndexEngineDeletionPolicy;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.ElasticsearchIndexDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.LazySoftDeletesDirectoryReaderWrapper;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An {@link org.elasticsearch.index.engine.Engine} implementation for hollow index shards, i.e. shards that can't process ingestion
 * until they are unhollowed and the engine is swapped with an {@link co.elastic.elasticsearch.stateless.engine.IndexEngine}.
 *
 * The main objective of the hollow index engine is to decrease the memory footprint of inactive (ingestion-less) indexing shards.
 */
public class HollowIndexEngine extends Engine {

    private final StatelessCommitService statelessCommitService;
    private final HollowShardsService hollowShardsService;
    private final ShardFieldStats shardFieldStats;
    private final DocsStats docsStats;
    private final List<ReferenceManager.RefreshListener> externalRefreshListeners;
    private final List<ReferenceManager.RefreshListener> internalRefreshListeners;
    private final SegmentInfos segmentInfos;
    private final IndexCommit indexCommit;
    private final SafeCommitInfo safeCommitInfo;
    private final SeqNoStats seqNoStats;

    /**
     * Only used for managing acquired/released index commits
     */
    private final ElasticsearchIndexDeletionPolicy indexDeletionPolicy;

    @SuppressWarnings("this-escape")
    public HollowIndexEngine(
        EngineConfig config,
        StatelessCommitService statelessCommitService,
        HollowShardsService hollowShardsService,
        MapperService mapperService
    ) {
        super(config);
        this.statelessCommitService = statelessCommitService;
        this.hollowShardsService = hollowShardsService;
        this.externalRefreshListeners = config.getExternalRefreshListener();
        this.internalRefreshListeners = config.getInternalRefreshListener();

        try {
            store.incRef();
            Directory directory = store.directory();
            final var shardId = engineConfig.getShardId();
            boolean success = false;
            try {
                assert Transports.assertNotTransportThread("opening directory reader of a read-only hollow engine");
                this.segmentInfos = Lucene.readSegmentInfos(directory);
                this.seqNoStats = buildSeqNoStats(config, segmentInfos);
                this.safeCommitInfo = new SafeCommitInfo(seqNoStats.getLocalCheckpoint(), segmentInfos.totalMaxDoc());

                try {
                    var policy = config.getIndexDeletionPolicyWrapper().apply(null);
                    if (policy instanceof HollowIndexEngineDeletionPolicy hollowIndexEngineDeletionPolicy) {
                        hollowIndexEngineDeletionPolicy.onInit(Lucene.getIndexCommit(segmentInfos, directory), safeCommitInfo);
                        // Acquires the latest commit for the life of the HollowIndexEngine, it will be released in #closeNoLock
                        var acquiredCommit = hollowIndexEngineDeletionPolicy.acquireIndexCommit(true);
                        boolean release = true;
                        try (
                            var reader = ElasticsearchDirectoryReader.wrap(
                                new LazySoftDeletesDirectoryReaderWrapper(
                                    DirectoryReader.open(
                                        acquiredCommit,
                                        IndexVersions.MINIMUM_READONLY_COMPATIBLE.luceneVersion().major,
                                        null
                                    ),
                                    Lucene.SOFT_DELETES_FIELD
                                ),
                                shardId,
                                null
                            )
                        ) {
                            boolean isStateless = DiscoveryNode.isStateless(getEngineConfig().getIndexSettings().getNodeSettings());
                            this.shardFieldStats = shardFieldStats(reader.getContext().leaves(), isStateless);
                            this.docsStats = docsStats(reader);
                            this.indexDeletionPolicy = hollowIndexEngineDeletionPolicy;
                            this.indexCommit = acquiredCommit;
                            release = false;
                        } finally {
                            if (release) {
                                hollowIndexEngineDeletionPolicy.releaseIndexCommit(acquiredCommit);
                            }
                        }
                    } else {
                        throw new IllegalStateException(
                            "Expected ["
                                + HollowIndexEngineDeletionPolicy.class.getName()
                                + "] but got ["
                                + policy.getClass().getName()
                                + ']'
                        );
                    }
                } catch (Exception e) {
                    throw new EngineCreationFailureException(
                        config.getShardId(),
                        "Failed to initialize index deletion policy for hollow index engine",
                        e
                    );
                }
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(store::decRef);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // this is stupid
        }
    }

    public StatelessCommitService getStatelessCommitService() {
        return statelessCommitService;
    }

    @Override
    public ShardFieldStats shardFieldStats() {
        return shardFieldStats;
    }

    @Override
    public FieldInfos shardFieldInfos() {
        // Field caps API is serviced by the search tier. So it is fine to return empty field infos for hollow indexing shards.
        return FieldInfos.EMPTY;
    }

    @Override
    public SegmentInfos getLastCommittedSegmentInfos() {
        return segmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return segmentInfos.userData.get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStats();
    }

    @Override
    public DocsStats docStats() {
        return docsStats;
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        return new SegmentsStats();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments() {
        return segments(true);
    }

    @Override
    public List<Segment> segments(boolean includeVectorFormatsInfo) {
        throw new UnsupportedOperationException("hollow shard does not support reading segments");
    }

    @Override
    public DenseVectorStats denseVectorStats(MappingLookup mappingLookup) {
        return new DenseVectorStats();
    }

    @Override
    public SparseVectorStats sparseVectorStats(MappingLookup mappingLookup) {
        return new SparseVectorStats();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {}

    @Override
    public IndexResult index(Index index) {
        assert false : "index should not be called on a hollow engine";
        throw new UnsupportedOperationException("indexing is not supported on a hollow engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        assert false : "delete should not be called on a hollow engine";
        throw new UnsupportedOperationException("deletes are not supported on a hollow engine");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        assert false : "noOp should not be called on a hollow engine";
        throw new UnsupportedOperationException("no-ops are not supported on a hollow engine");
    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        assert false : "get should not be called on a hollow engine";
        throw new UnsupportedOperationException("gets are not supported on a hollow engine");
    }

    @Override
    public void prepareForEngineReset() {
        hollowShardsService.ensureHollowShard(shardId, true, "hollow index engine requires the shard to be hollow");
        logger.debug(() -> "preparing to reset hollow index engine for shard " + shardId);
    }

    @Override
    public RefreshResult refresh(String source) {
        // Acquire the engine reset write lock to avoid shard engine resets to run concurrently while calling the refresh listeners.
        // We could use the engine reset read lock instead, but since we need refresh listeners to be called by a single thread at a time
        // using the write lock avoids maintaining a second exclusive lock just for this.
        //
        // In some cases, the engine reset write lock is already held when this refresh method is called:
        // - when the HollowIndexEngine is created upon a reset during the relocation action
        // - when the post_recovery refresh is executed during shard recovery
        //
        // In other cases, during normal run of the shard, sometimes the will be a no-op and the refresh listeners won't be called if the
        // engine reset read lock is held by something else (e.g., concurrent getEngine usages). We think it is OK.
        final var engineWriteLock = engineConfig.getEngineResetLock().writeLock();
        if (engineWriteLock.tryLock()) {
            try {
                // The reader is opened at hollowing time once and is never refreshed internally.
                // We should still call the refresh listeners as some downstream logic depends on refresh listeners being invoked
                // to populate internal data structures.
                try {
                    executeListeners(externalRefreshListeners, ReferenceManager.RefreshListener::beforeRefresh);
                    executeListeners(internalRefreshListeners, ReferenceManager.RefreshListener::beforeRefresh);
                } finally {
                    executeListeners(externalRefreshListeners, listener -> listener.afterRefresh(false));
                    executeListeners(internalRefreshListeners, listener -> listener.afterRefresh(false));
                }
            } finally {
                engineWriteLock.unlock();
            }
        }
        return new RefreshResult(false, config().getPrimaryTermSupplier().getAsLong(), getLastCommittedSegmentInfos().getGeneration());
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        ActionListener.completeWith(listener, () -> refresh(source));
    }

    @Override
    public void writeIndexingBuffer() throws IOException {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        // This returns a flush result which is not skipped due to collision, but does not actually flush anything. Mostly to appease
        // flushOnIdle so it does not retry endlessly unnecessarily.
        listener.onResponse(new FlushResult(false, segmentInfos.getGeneration()));
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {}

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {}

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) throws EngineException,
        IOException {
        if (maxNumSegments == ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS) {
            // noop
        } else if (maxNumSegments < segmentInfos.size()) {
            throw new UnsupportedOperationException(
                "force merge is not supported on a hollow engine, "
                    + "target max number of segments["
                    + maxNumSegments
                    + "], "
                    + "current number of segments["
                    + segmentInfos.size()
                    + "]."
            );
        } else {
            logger.debug(
                "current number of segments[{}] is not greater than target max number of segments[{}].",
                segmentInfos.size(),
                maxNumSegments
            );
        }
    }

    private IndexCommitRef acquireIndexCommitRef(final boolean acquiringSafeCommit) {
        store.incRef();
        boolean success = false;
        try {
            final var indexCommit = indexDeletionPolicy.acquireIndexCommit(acquiringSafeCommit);
            final var commitRef = new IndexCommitRef(
                indexCommit,
                () -> IOUtils.close(() -> indexDeletionPolicy.releaseIndexCommit(indexCommit), store::decRef)
            );
            success = true;
            return commitRef;
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return acquireIndexCommitRef(false);
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return acquireIndexCommitRef(true);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                IOUtils.close(() -> indexDeletionPolicy.releaseIndexCommit(indexCommit), store::decRef);
            } catch (Exception ex) {
                logger.warn("failed to close hollow engine", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    // package-protected for testing
    void awaitClose() {
        super.awaitPendingClose();
    }

    @Override
    public void activateThrottling() {
        assert false : "hollow index engine does not ingest and thus should not be throttled";
    }

    @Override
    public void deactivateThrottling() {}

    @Override
    public void suspendThrottling() {}

    @Override
    public void resumeThrottling() {}

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @Override
    public void skipTranslogRecovery() {}

    @Override
    public void maybePruneDeletes() {}

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {}

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return seqNoStats.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        assert maxSeqNoOfUpdatesOnPrimary <= getMaxSeqNoOfUpdatesOrDeletes()
            : maxSeqNoOfUpdatesOnPrimary + ">" + getMaxSeqNoOfUpdatesOrDeletes();
    }

    @Override
    public ShardLongFieldRange getRawFieldRange(String field) throws IOException {
        // Unknown because more docs may be added in the future, e.g., by unhollowing the shard and ingesting data.
        return ShardLongFieldRange.UNKNOWN;
    }

    @Override
    public GetResult getFromTranslog(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return null;
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        assert false : "getting reference manager / searcher should not be called on a hollow engine";
        throw new UnsupportedOperationException("getting reference manager / searcher is not supported on a hollow engine");
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        listener.accept(null);
    }

    @Override
    public void asyncEnsureGlobalCheckpointSynced(long globalCheckpoint, Consumer<Exception> listener) {
        listener.accept(null);
    }

    @Override
    public void syncTranslog() throws IOException {}

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    @Override
    public int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException {
        return 0;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        long maxChunkSize
    ) throws IOException {
        return Translog.Snapshot.EMPTY;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return startingSeqNo > seqNoStats.getMaxSeqNo();
    }

    @Override
    public long getMinRetainedSeqNo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return new TranslogStats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0, 0, 0);
    }

    @Override
    public long getMaxSeqNo() {
        return seqNoStats.getMaxSeqNo();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        // Hollow shards do not ingest, and flush before being hollow, so we expect the GCP to be the same as the one stored.
        assert seqNoStats.getGlobalCheckpoint() == globalCheckpoint
            : "expected global checkpoint [" + seqNoStats.getGlobalCheckpoint() + "] but got [" + globalCheckpoint + "]";
        return seqNoStats;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return seqNoStats.getGlobalCheckpoint();
    }

    @Override
    public long getLastUnsafeSegmentGenerationForGets() {
        return getLastCommittedSegmentInfos().getGeneration();
    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    private static <T> void executeListeners(List<T> listeners, CheckedConsumer<T, IOException> consumer) {
        if (listeners != null) {
            for (T listener : listeners) {
                try {
                    consumer.accept(listener);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}

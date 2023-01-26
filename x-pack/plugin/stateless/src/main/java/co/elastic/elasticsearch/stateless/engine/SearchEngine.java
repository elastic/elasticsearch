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

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.CompletionStatsCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.RefreshResult;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * {@link Engine} implementation for search shards
 *
 * This class implements the minimal behavior to allow a stateless search shard to recover as a replica of an index/primary shard. Most of
 * indexing behavior is faked and will be removed once operations are not replicated anymore (ES-4861).
 *
 * // TODO Remove methods related to indexing operations and local/global checkpoints
 * - {@link #index(Index)}
 * - {@link #delete(Delete)}
 * - {@link #noOp(NoOp)}
 * - {@link #getPersistedLocalCheckpoint()}
 */
public class SearchEngine extends Engine {

    private final AtomicLong lastSeqNo = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
    private final AtomicLong lastTranslogLocation = new AtomicLong(0);

    private final StatelessReaderManager statelessReaderManager;

    public SearchEngine(EngineConfig config, ObjectStoreService objectStoreService) {
        super(config);
        assert config.isPromotableToPrimary() == false;
        store.incRef();
        try {
            StatelessReaderManager readerManager = null;
            boolean success = false;
            try {
                readerManager = new StatelessReaderManager(
                    objectStoreService,
                    config.getShardId(),
                    store,
                    config.getThreadPool(),
                    config.getGlobalCheckpointSupplier()
                );
                readerManager.reloadReaderManager();
                var localCheckpoint = readerManager.getSeqNoStats().getLocalCheckpoint();
                this.lastSeqNo.set(Math.max(localCheckpoint, config.getGlobalCheckpointSupplier().getAsLong()));
                this.statelessReaderManager = readerManager;
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readerManager, store::decRef);
                }
            }
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "Failed to create a search engine", e);
        }
    }

    public void onCommitNotification(
        final long primaryTerm,
        final long generation,
        final Map<String, StoreFileMetadata> commitFiles,
        ActionListener<Void> listener
    ) throws IOException {
        statelessReaderManager.onNewCommit(primaryTerm, generation, commitFiles, listener);
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                IOUtils.close(statelessReaderManager, store::decRef);
            } catch (Exception ex) {
                logger.warn("failed to close reader", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        IndexResult result = new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), true, index.id()) {
            @Override
            public Translog.Location getTranslogLocation() {
                return new Translog.Location(0, lastTranslogLocation.incrementAndGet(), 1);
            }
        };
        this.lastSeqNo.accumulateAndGet(index.seqNo(), Math::max);
        return result;
    }

    @Override
    public DeleteResult delete(Delete delete) {
        DeleteResult result = new DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true, delete.id()) {
            @Override
            public Translog.Location getTranslogLocation() {
                return new Translog.Location(0, lastTranslogLocation.incrementAndGet(), 1);
            }
        };
        this.lastSeqNo.accumulateAndGet(delete.seqNo(), Math::max);
        return result;
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        // TODO when removing this NoOpResult constructor won't require to be public anymore
        return new NoOpResult(noOp.primaryTerm(), noOp.seqNo()) {
            @Override
            public Translog.Location getTranslogLocation() {
                return new Translog.Location(0, lastTranslogLocation.incrementAndGet(), 1);
            }
        };
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return statelessReaderManager.getSegmentInfos();
    }

    @Override
    public String getHistoryUUID() {
        return statelessReaderManager.getSegmentInfos().getUserData().get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0, lastTranslogLocation.get(), 0);
    }

    @Override
    public long getMaxSeqNo() {
        // TODO: Integrate properly
        return lastSeqNo.get();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        // TODO: Integrate properly
        return lastSeqNo.get();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        // TODO: Integrate properly
        return lastSeqNo.get();
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        // TODO: Integrate properly
        return lastSeqNo.get();
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStatsCache(() -> acquireSearcher("completion_stats")).get(fieldNamePatterns);
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
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {

    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return getFromSearcher(get, acquireSearcher("get", SearcherScope.EXTERNAL, searcherWrapper), false);
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return statelessReaderManager.getReaderManager();
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        return false;
    }

    @Override
    public void syncTranslog() throws IOException {

    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return null;
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
        boolean accessStats
    ) throws IOException {
        return null;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public long getMinRetainedSeqNo() {
        return 0;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return null;
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        assert false;
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {}

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return statelessReaderManager.getSeqNoStats();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments() {
        return null;
    }

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        return RefreshResult.NO_REFRESH;
    }

    @Override
    public RefreshResult maybeRefresh(String source) throws EngineException {
        return RefreshResult.NO_REFRESH;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public boolean flush(boolean force, boolean waitIfOngoing) throws EngineException {
        return false;
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {

    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {

    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) throws EngineException,
        IOException {

    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return null;
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return null;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return statelessReaderManager.getSafeCommitInfo();
    }

    @Override
    public void activateThrottling() {

    }

    @Override
    public void deactivateThrottling() {

    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        return null;
    }

    @Override
    public void skipTranslogRecovery() {

    }

    @Override
    public void maybePruneDeletes() {

    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    @Override
    public ShardLongFieldRange getRawFieldRange(String field) throws IOException {
        return ShardLongFieldRange.UNKNOWN;
    }

    @Override
    public void addSegmentGenerationListener(long minGeneration, ActionListener<Long> listener) {
        statelessReaderManager.addSegmentGenerationListener(minGeneration, listener);
    }
}

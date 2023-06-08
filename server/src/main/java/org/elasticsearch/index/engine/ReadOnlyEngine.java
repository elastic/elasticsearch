/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.ESCacheHelper;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A basic read-only engine that allows switching a shard to be true read-only temporarily or permanently.
 * Note: this engine can be opened side-by-side with a read-write engine but will not reflect any changes made to the read-write
 * engine.
 *
 * @see #ReadOnlyEngine(EngineConfig, SeqNoStats, TranslogStats, boolean, Function, boolean, boolean)
 */
public class ReadOnlyEngine extends Engine {

    public static final String FIELD_RANGE_SEARCH_SOURCE = "field_range";

    /**
     * Reader attributes used for read only engines. These attributes prevent loading term dictionaries on-heap even if the field is an
     * ID field.
     */
    private final SegmentInfos lastCommittedSegmentInfos;
    private final SeqNoStats seqNoStats;
    private final ElasticsearchReaderManager readerManager;
    private final IndexCommit indexCommit;
    private final Lock indexWriterLock;
    private final SafeCommitInfo safeCommitInfo;
    private final CompletionStatsCache completionStatsCache;
    private final boolean requireCompleteHistory;
    final boolean lazilyLoadSoftDeletes;

    protected volatile TranslogStats translogStats;
    private final String commitId;

    /**
     * Creates a new ReadOnlyEngine. This ctor can also be used to open a read-only engine on top of an already opened
     * read-write engine. It allows to optionally obtain the writer locks for the shard which would time-out if another
     * engine is still open.
     *
     * @param config the engine configuration
     * @param seqNoStats sequence number statistics for this engine or null if not provided
     * @param translogStats translog stats for this engine or null if not provided
     * @param obtainLock if <code>true</code> this engine will try to obtain the {@link IndexWriter#WRITE_LOCK_NAME} lock. Otherwise
     *                   the lock won't be obtained
     * @param readerWrapperFunction allows to wrap the index-reader for this engine.
     * @param requireCompleteHistory indicates whether this engine permits an incomplete history (i.e. LCP &lt; MSN)
     * @param lazilyLoadSoftDeletes indicates whether this engine should load the soft-delete based liveDocs eagerly, or on first access
     */
    public ReadOnlyEngine(
        EngineConfig config,
        SeqNoStats seqNoStats,
        TranslogStats translogStats,
        boolean obtainLock,
        Function<DirectoryReader, DirectoryReader> readerWrapperFunction,
        boolean requireCompleteHistory,
        boolean lazilyLoadSoftDeletes
    ) {
        super(config);
        this.requireCompleteHistory = requireCompleteHistory;
        try {
            Store store = config.getStore();
            store.incRef();
            ElasticsearchDirectoryReader reader = null;
            Directory directory = store.directory();
            Lock indexWriterLock = null;
            boolean success = false;
            try {
                // we obtain the IW lock even though we never modify the index.
                // yet this makes sure nobody else does. including some testing tools that try to be messy
                indexWriterLock = obtainLock ? directory.obtainLock(IndexWriter.WRITE_LOCK_NAME) : null;
                this.lastCommittedSegmentInfos = Lucene.readSegmentInfos(directory);
                this.commitId = generateSearcherId(lastCommittedSegmentInfos);
                if (seqNoStats == null) {
                    seqNoStats = buildSeqNoStats(config, lastCommittedSegmentInfos);
                    ensureMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats);
                }
                this.seqNoStats = seqNoStats;
                this.indexCommit = Lucene.getIndexCommit(lastCommittedSegmentInfos, directory);
                this.lazilyLoadSoftDeletes = lazilyLoadSoftDeletes;
                reader = wrapReader(open(indexCommit), readerWrapperFunction, null);
                readerManager = new ElasticsearchReaderManager(reader);
                assert translogStats != null || obtainLock : "mutiple translogs instances should not be opened at the same time";
                this.translogStats = translogStats != null ? translogStats : translogStats(config, lastCommittedSegmentInfos);
                this.indexWriterLock = indexWriterLock;
                this.safeCommitInfo = new SafeCommitInfo(seqNoStats.getLocalCheckpoint(), lastCommittedSegmentInfos.totalMaxDoc());

                completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
                // no need to register a refresh listener to invalidate completionStatsCache since this engine is readonly

                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(reader, indexWriterLock, store::decRef);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // this is stupid
        }
    }

    /**
     * Generate a searcher id using the ids of the underlying segments of an index commit. Here we can't use the commit id directly
     * as the search id because the commit id changes whenever IndexWriter#commit is called although the segment files stay unchanged.
     * Any recovery except the local recovery performs IndexWriter#commit to generate a new translog uuid or history_uuid.
     */
    static String generateSearcherId(SegmentInfos sis) {
        final MessageDigest md = MessageDigests.sha256();
        for (SegmentCommitInfo si : sis) {
            final byte[] segmentId = si.getId();
            if (segmentId != null) {
                md.update(segmentId);
            } else {
                // old segments do not have segment ids
                return null;
            }
        }
        return MessageDigests.toHexString(md.digest());
    }

    protected void ensureMaxSeqNoEqualsToGlobalCheckpoint(final SeqNoStats seqNoStats) {
        if (requireCompleteHistory == false) {
            return;
        }
        // Before 8.0 the global checkpoint is not known and up to date when the engine is created after
        // peer recovery, so we only check the max seq no / global checkpoint coherency when the global
        // checkpoint is different from the unassigned sequence number value.
        // In addition to that we only execute the check if the index the engine belongs to has been
        // created after the refactoring of the Close Index API and its TransportVerifyShardBeforeCloseAction
        // that guarantee that all operations have been flushed to Lucene.
        final Version indexVersionCreated = engineConfig.getIndexSettings().getIndexVersionCreated();
        if (indexVersionCreated.onOrAfter(Version.V_7_2_0) || (seqNoStats.getGlobalCheckpoint() != SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assert assertMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats.getMaxSeqNo(), seqNoStats.getGlobalCheckpoint());
            if (seqNoStats.getMaxSeqNo() != seqNoStats.getGlobalCheckpoint()) {
                throw new IllegalStateException(
                    "Maximum sequence number ["
                        + seqNoStats.getMaxSeqNo()
                        + "] from last commit does not match global checkpoint ["
                        + seqNoStats.getGlobalCheckpoint()
                        + "]"
                );
            }
        }
    }

    protected boolean assertMaxSeqNoEqualsToGlobalCheckpoint(final long maxSeqNo, final long globalCheckpoint) {
        assert maxSeqNo == globalCheckpoint : "max seq. no. [" + maxSeqNo + "] does not match [" + globalCheckpoint + "]";
        return true;
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        // the value of the global checkpoint is verified when the read-only engine is opened,
        // and it is not expected to change during the lifecycle of the engine. We could also
        // check this value before closing the read-only engine but if something went wrong
        // and the global checkpoint is not in-sync with the max. sequence number anymore,
        // checking the value here again would prevent the read-only engine to be closed and
        // reopened as an internal engine, which would be the path to fix the issue.
    }

    protected final ElasticsearchDirectoryReader wrapReader(
        DirectoryReader reader,
        Function<DirectoryReader, DirectoryReader> readerWrapperFunction,
        @Nullable ESCacheHelper esCacheHelper
    ) throws IOException {
        reader = readerWrapperFunction.apply(reader);
        return ElasticsearchDirectoryReader.wrap(reader, engineConfig.getShardId(), esCacheHelper);
    }

    protected DirectoryReader open(IndexCommit commit) throws IOException {
        assert Transports.assertNotTransportThread("opening index commit of a read-only engine");
        DirectoryReader directoryReader = DirectoryReader.open(
            commit,
            org.apache.lucene.util.Version.MIN_SUPPORTED_MAJOR,
            engineConfig.getLeafSorter()
        );
        if (lazilyLoadSoftDeletes) {
            return new LazySoftDeletesDirectoryReaderWrapper(directoryReader, Lucene.SOFT_DELETES_FIELD);
        } else {
            return new SoftDeletesDirectoryReaderWrapper(directoryReader, Lucene.SOFT_DELETES_FIELD);
        }
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                IOUtils.close(readerManager, indexWriterLock, store::decRef);
            } catch (Exception ex) {
                logger.warn("failed to close reader", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    private static SeqNoStats buildSeqNoStats(EngineConfig config, SegmentInfos infos) {
        final SequenceNumbers.CommitInfo seqNoStats = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(infos.userData.entrySet());
        long maxSeqNo = seqNoStats.maxSeqNo;
        long localCheckpoint = seqNoStats.localCheckpoint;
        return new SeqNoStats(maxSeqNo, localCheckpoint, config.getGlobalCheckpointSupplier().getAsLong());
    }

    private static TranslogStats translogStats(final EngineConfig config, final SegmentInfos infos) throws IOException {
        final String translogUuid = infos.getUserData().get(Translog.TRANSLOG_UUID_KEY);
        if (translogUuid == null) {
            throw new IllegalStateException("commit doesn't contain translog unique id");
        }
        final TranslogConfig translogConfig = config.getTranslogConfig();
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy();
        final long localCheckpoint = Long.parseLong(infos.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        translogDeletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
        try (
            Translog translog = new Translog(
                translogConfig,
                translogUuid,
                translogDeletionPolicy,
                config.getGlobalCheckpointSupplier(),
                config.getPrimaryTermSupplier(),
                seqNo -> {}
            )
        ) {
            return translog.stats();
        }
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
        return readerManager;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return lastCommittedSegmentInfos.userData.get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public long getWritingBytes() {
        return 0;
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
    public IndexResult index(Index index) {
        assert false : "this should not be called";
        throw new UnsupportedOperationException("indexing is not supported on a read-only engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        assert false : "this should not be called";
        throw new UnsupportedOperationException("deletes are not supported on a read-only engine");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        assert false : "this should not be called";
        throw new UnsupportedOperationException("no-ops are not supported on a read-only engine");
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
    public void syncTranslog() {}

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    @Override
    public int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException {
        try (Translog.Snapshot snapshot = newChangesSnapshot(source, fromSeqNo, toSeqNo, false, true, true)) {
            return snapshot.totalOperations();
        }
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats
    ) {
        return Translog.Snapshot.EMPTY;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        // we can do operation-based recovery if we don't have to replay any operation.
        return startingSeqNo > seqNoStats.getMaxSeqNo();
    }

    @Override
    public long getMinRetainedSeqNo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return translogStats;
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
        return new SeqNoStats(seqNoStats.getMaxSeqNo(), seqNoStats.getLocalCheckpoint(), globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return seqNoStats.getGlobalCheckpoint();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments() {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos));
    }

    @Override
    public RefreshResult refresh(String source) {
        // we could allow refreshes if we want down the road the reader manager will then reflect changes to a rw-engine
        // opened side-by-side
        return RefreshResult.NO_REFRESH;
    }

    @Override
    public RefreshResult maybeRefresh(String source) throws EngineException {
        return RefreshResult.NO_REFRESH;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        listener.onResponse(new FlushResult(true, lastCommittedSegmentInfos.getGeneration()));
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) {
        if (maxNumSegments == ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS) {
            // noop
        } else if (maxNumSegments < lastCommittedSegmentInfos.size()) {
            throw new UnsupportedOperationException(
                "force merge is not supported on a read-only engine, "
                    + "target max number of segments["
                    + maxNumSegments
                    + "], "
                    + "current number of segments["
                    + lastCommittedSegmentInfos.size()
                    + "]."
            );
        } else {
            logger.debug(
                "current number of segments[{}] is not greater than target max number of segments[{}].",
                lastCommittedSegmentInfos.size(),
                maxNumSegments
            );
        }
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
        store.incRef();
        return new IndexCommitRef(indexCommit, store::decRef);
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() {
        return acquireLastIndexCommit(false);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
    }

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public void trimUnreferencedTranslogFiles() {}

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() {}

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog(final TranslogRecoveryRunner translogRecoveryRunner, final long recoverUpToSeqNo) {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            try {
                translogRecoveryRunner.run(this, Translog.Snapshot.EMPTY);
            } catch (final Exception e) {
                throw new EngineException(shardId, "failed to recover from empty translog snapshot", e);
            }
        }
        return this;
    }

    @Override
    public void skipTranslogRecovery() {}

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) {}

    @Override
    public void maybePruneDeletes() {}

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return seqNoStats.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        assert maxSeqNoOfUpdatesOnPrimary <= getMaxSeqNoOfUpdatesOrDeletes()
            : maxSeqNoOfUpdatesOnPrimary + ">" + getMaxSeqNoOfUpdatesOrDeletes();
    }

    protected DirectoryReader openDirectory(Directory directory) throws IOException {
        assert Transports.assertNotTransportThread("opening directory reader of a read-only engine");
        final DirectoryReader reader = DirectoryReader.open(directory);
        if (lazilyLoadSoftDeletes) {
            return new LazySoftDeletesDirectoryReaderWrapper(reader, Lucene.SOFT_DELETES_FIELD);
        } else {
            return new SoftDeletesDirectoryReaderWrapper(reader, Lucene.SOFT_DELETES_FIELD);
        }
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return completionStatsCache.get(fieldNamePatterns);
    }

    /**
     * @return a {@link ShardLongFieldRange} containing the min and max raw values of the given field for this shard, or {@link
     * ShardLongFieldRange#EMPTY} if this field is not found or empty.
     */
    @Override
    public ShardLongFieldRange getRawFieldRange(String field) throws IOException {
        try (Searcher searcher = acquireSearcher(FIELD_RANGE_SEARCH_SOURCE)) {
            final DirectoryReader directoryReader = searcher.getDirectoryReader();

            final byte[] minPackedValue = PointValues.getMinPackedValue(directoryReader, field);
            final byte[] maxPackedValue = PointValues.getMaxPackedValue(directoryReader, field);

            if (minPackedValue == null || maxPackedValue == null) {
                assert minPackedValue == null && maxPackedValue == null
                    : Arrays.toString(minPackedValue) + "-" + Arrays.toString(maxPackedValue);
                return ShardLongFieldRange.EMPTY;
            }

            return ShardLongFieldRange.of(LongPoint.decodeDimension(minPackedValue, 0), LongPoint.decodeDimension(maxPackedValue, 0));
        }
    }

    @Override
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        final SearcherSupplier delegate = super.acquireSearcherSupplier(wrapper, scope);
        return new SearcherSupplier(wrapper) {
            @Override
            protected void doClose() {
                delegate.close();
            }

            @Override
            protected Searcher acquireSearcherInternal(String source) {
                return delegate.acquireSearcherInternal(source);
            }

            @Override
            public String getSearcherId() {
                return commitId;
            }
        };
    }

    public final String getCommitId() {
        return commitId;
    }
}

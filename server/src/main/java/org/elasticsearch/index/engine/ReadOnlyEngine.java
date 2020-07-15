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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.transport.Transports;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A basic read-only engine that allows switching a shard to be true read-only temporarily or permanently.
 * Note: this engine can be opened side-by-side with a read-write engine but will not reflect any changes made to the read-write
 * engine.
 *
 * @see #ReadOnlyEngine(EngineConfig, SeqNoStats, TranslogStats, boolean, Function, boolean)
 */
public class ReadOnlyEngine extends Engine {

    /**
     * Reader attributes used for read only engines. These attributes prevent loading term dictionaries on-heap even if the field is an
     * ID field.
     */
    private final SegmentInfos lastCommittedSegmentInfos;
    private final SeqNoStats seqNoStats;
    private final ElasticsearchReaderManager readerManager;
    private final IndexCommit indexCommit;
    private final Lock indexWriterLock;
    private final RamAccountingRefreshListener refreshListener;
    private final SafeCommitInfo safeCommitInfo;
    private final CompletionStatsCache completionStatsCache;
    private final boolean requireCompleteHistory;

    protected volatile TranslogStats translogStats;

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
     */
    public ReadOnlyEngine(EngineConfig config, SeqNoStats seqNoStats, TranslogStats translogStats, boolean obtainLock,
                          Function<DirectoryReader, DirectoryReader> readerWrapperFunction, boolean requireCompleteHistory) {
        super(config);
        this.refreshListener = new RamAccountingRefreshListener(engineConfig.getCircuitBreakerService());
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
                if (seqNoStats == null) {
                    seqNoStats = buildSeqNoStats(config, lastCommittedSegmentInfos);
                    ensureMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats);
                }
                this.seqNoStats = seqNoStats;
                this.indexCommit = Lucene.getIndexCommit(lastCommittedSegmentInfos, directory);
                reader = wrapReader(open(indexCommit), readerWrapperFunction);
                readerManager = new ElasticsearchReaderManager(reader, refreshListener);
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
        if (indexVersionCreated.onOrAfter(Version.V_7_2_0) ||
            (seqNoStats.getGlobalCheckpoint() != SequenceNumbers.UNASSIGNED_SEQ_NO)) {
            assert assertMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats.getMaxSeqNo(), seqNoStats.getGlobalCheckpoint());
            if (seqNoStats.getMaxSeqNo() != seqNoStats.getGlobalCheckpoint()) {
                throw new IllegalStateException("Maximum sequence number [" + seqNoStats.getMaxSeqNo()
                    + "] from last commit does not match global checkpoint [" + seqNoStats.getGlobalCheckpoint() + "]");
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

    protected final ElasticsearchDirectoryReader wrapReader(DirectoryReader reader,
                                                    Function<DirectoryReader, DirectoryReader> readerWrapperFunction) throws IOException {
        reader = readerWrapperFunction.apply(reader);
        return ElasticsearchDirectoryReader.wrap(reader, engineConfig.getShardId());
    }

    protected DirectoryReader open(IndexCommit commit) throws IOException {
        assert Transports.assertNotTransportThread("opening index commit of a read-only engine");
        return new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(commit), Lucene.SOFT_DELETES_FIELD);
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
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(infos.userData.entrySet());
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
        try (Translog translog = new Translog(translogConfig, translogUuid, translogDeletionPolicy, config.getGlobalCheckpointSupplier(),
                config.getPrimaryTermSupplier(), seqNo -> {})
        ) {
            return translog.stats();
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Engine.Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, searcherFactory, SearcherScope.EXTERNAL);
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
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        return false;
    }

    @Override
    public void syncTranslog() {
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, MapperService mapperService, long fromSeqNo, long toSeqNo,
                                                boolean requiredFullRange)  {
        return newEmptySnapshot();
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
        return new Translog.Location(0,0,0);
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
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos, verbose));
    }

    @Override
    public void refresh(String source) {
        // we could allow refreshes if we want down the road the reader manager will then reflect changes to a rw-engine
        // opened side-by-side
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        // noop
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           boolean upgrade, boolean upgradeOnlyAncientSegments, String forceMergeUUID) {
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
    public void activateThrottling() {
    }

    @Override
    public void deactivateThrottling() {
    }

    @Override
    public void trimUnreferencedTranslogFiles() {
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() {
    }

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
            try (Translog.Snapshot snapshot = newEmptySnapshot()) {
                translogRecoveryRunner.run(this, snapshot);
            } catch (final Exception e) {
                throw new EngineException(shardId, "failed to recover from empty translog snapshot", e);
            }
        }
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) {
    }

    @Override
    public void maybePruneDeletes() {
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    protected void processReader(ElasticsearchDirectoryReader reader) {
        refreshListener.accept(reader, null);
    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    private Translog.Snapshot newEmptySnapshot() {
        return new Translog.Snapshot() {
            @Override
            public void close() {
            }

            @Override
            public int totalOperations() {
                return 0;
            }

            @Override
            public Translog.Operation next() {
                return null;
            }
        };
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return seqNoStats.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        assert maxSeqNoOfUpdatesOnPrimary <= getMaxSeqNoOfUpdatesOrDeletes() :
            maxSeqNoOfUpdatesOnPrimary + ">" + getMaxSeqNoOfUpdatesOrDeletes();
    }

    protected static DirectoryReader openDirectory(Directory directory) throws IOException {
        assert Transports.assertNotTransportThread("opening directory reader of a read-only engine");
        final DirectoryReader reader = DirectoryReader.open(directory);
        return new SoftDeletesDirectoryReaderWrapper(reader, Lucene.SOFT_DELETES_FIELD);
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return completionStatsCache.get(fieldNamePatterns);
    }
}

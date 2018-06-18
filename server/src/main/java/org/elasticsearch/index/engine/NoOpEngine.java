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
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * NoOpEngine is an engine implementation that does nothing but the bare minimum
 * required in order to have an engine. All attempts to do something (search,
 * index, get), throw {@link UnsupportedOperationException}. This does maintain
 * a translog with a deletion policy so that when flushing, no translog is
 * retained on disk (setting a retention size and age of 0).
 *
 * It's also important to notice that this does list the commits of the Store's
 * Directory so that the last commit's user data can be read for the historyUUID
 * and last committed segment info.
 */
final class NoOpEngine extends Engine {

    private static final Translog.Snapshot EMPTY_TRANSLOG_SNAPSHOT = new Translog.Snapshot() {
        @Override
        public int totalOperations() {
            return 0;
        }

        @Override
        public Translog.Operation next() {
            return null;
        }

        @Override
        public void close() {
        }
    };

    private static final TranslogStats EMPTY_TRANSLOG_STATS = new TranslogStats(0, 0, 0, 0, 0);
    private static final Translog.Location EMPTY_TRANSLOG_LOCATION = new Translog.Location(0, 0, 0);

    private final IndexCommit lastCommit;
    private final long localCheckpoint;
    private final long maxSeqNo;
    private final String historyUUID;
    private final SegmentInfos lastCommittedSegmentInfos;

    NoOpEngine(EngineConfig engineConfig) {
        super(engineConfig);

        store.incRef();
        boolean success = false;

        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            List<IndexCommit> indexCommits = DirectoryReader.listCommits(store.directory());
            lastCommit = indexCommits.get(indexCommits.size() - 1);
            historyUUID = lastCommit.getUserData().get(HISTORY_UUID_KEY);
            localCheckpoint = Long.parseLong(lastCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            maxSeqNo = Long.parseLong(lastCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO));

            // The deletion policy for the translog should not keep any translogs around, so the min age/size is set to -1
            final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(-1, -1);

            // The translog is opened and closed to validate that the translog UUID from lucene is the same as the one in the translog
            try (Translog translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier())) {
                if (translog.totalOperations() != 0) {
                    throw new IllegalArgumentException("expected 0 translog operations but there were " + translog.totalOperations());
                }
            }

            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new NoOpEngine");
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy,
                                  LongSupplier globalCheckpointSupplier) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier,
                engineConfig.getPrimaryTermSupplier());
    }

    /**
     * Reads the current stored translog ID from the last commit data.
     */
    @Nullable
    private String loadTranslogUUIDFromLastCommit() {
        final Map<String, String> commitUserData = lastCommittedSegmentInfos.getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
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
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
    }

    @Override
    public IndexResult index(Index index) {
        throw new UnsupportedOperationException("indexing is not supported on a noOp engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException("deletion is not supported on a noOp engine");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException("noOp is not supported on a noOp engine");
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException("synced flush is not supported on a noOp engine");
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        throw new UnsupportedOperationException("gets are not supported on a noOp engine");
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        throw new UnsupportedOperationException("searching is not supported on a noOp engine");
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        throw new UnsupportedOperationException("translog synchronization should never be needed");
    }

    @Override
    public void syncTranslog() {
    }

    @Override
    public Closeable acquireTranslogRetentionLock() {
        return () -> { };
    }

    @Override
    public Translog.Snapshot newTranslogSnapshotFromMinSeqNo(long minSeqNo) {
        return EMPTY_TRANSLOG_SNAPSHOT;
    }

    @Override
    public int estimateTranslogOperationsFromMinSeq(long minSeqNo) {
        return 0;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return EMPTY_TRANSLOG_STATS;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return EMPTY_TRANSLOG_LOCATION;
    }

    @Override
    public long getLocalCheckpoint() {
        return this.localCheckpoint;
    }

    @Override
    public void waitForOpsToComplete(long seqNo) {
    }

    @Override
    public void resetLocalCheckpoint(long localCheckpoint) {
        assert localCheckpoint == getLocalCheckpoint() : "expected reset to existing local checkpoint of " +
            getLocalCheckpoint() + " got: " + localCheckpoint;
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
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
    public void refresh(String source) throws EngineException {
    }

    // Override the refreshNeeded method so that we don't attempt to acquire a searcher checking if we need to refresh
    @Override
    public boolean refreshNeeded() {
        // We never need to refresh a noOp engine so always return false
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
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public CommitId flush() throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
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
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade,
                           boolean upgradeOnlyAncientSegments) throws EngineException {
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        return new Engine.IndexCommitRef(lastCommit, () -> {});
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return acquireLastIndexCommit(false);
    }

    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() :
                "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                store.decRef();
                logger.debug("engine closed [{}]", reason);
            } finally {
                closedLatch.countDown();
            }

        }
    }

    @Override
    public void activateThrottling() {
        throw new UnsupportedOperationException("closed engine can't throttle");
    }

    @Override
    public void deactivateThrottling() {
        throw new UnsupportedOperationException("closed engine can't throttle");
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() {
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog() {
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
    }

    @Override
    public void maybePruneDeletes() {
    }
}

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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * NoopEngine is an engine implementation that does nothing but the bare minimum
 * required in order to have an engine. All attempts to do something (search,
 * index, get), throw {@link UnsupportedOperationException}. This does maintain
 * a translog with a deletion policy so that when flushing, no translog is
 * retained on disk (setting a retention size and age of 0).
 *
 * It's also important to notice that this does list the commits of the Store's
 * Directory so that the last commit's user data can be read for the historyUUID
 * and last committed segment info.
 */
final class NoopEngine extends Engine {

    private final Translog translog;
    private final IndexCommit lastCommit;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final String historyUUID;
    private final SegmentInfos lastCommittedSegmentInfos;

    public NoopEngine(EngineConfig engineConfig) {
        super(engineConfig);

        store.incRef();
        boolean success = false;
        Translog translog = null;

        try {
            // The deletion policy for the translog should not keep any translogs around, so the min age/size is set to -1
            final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(-1, -1);

            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier());
            assert translog.getGeneration() != null;
            this.translog = translog;
            List<IndexCommit> indexCommits = DirectoryReader.listCommits(store.directory());
            lastCommit = indexCommits.get(indexCommits.size()-1);
            historyUUID = lastCommit.getUserData().get(HISTORY_UUID_KEY);
            // We don't want any translogs hanging around for recovery, so we need to set these accordingly
            final long lastGen = Long.parseLong(lastCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
            translogDeletionPolicy.setTranslogGenerationOfLastCommit(lastGen);
            translogDeletionPolicy.setMinTranslogGenerationForRecovery(lastGen);

            localCheckpointTracker = createLocalCheckpointTracker();
            success = true;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(translog);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new NoopEngine");
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

    private LocalCheckpointTracker createLocalCheckpointTracker() {
        final long maxSeqNo;
        final long localCheckpoint;
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommittedSegmentInfos.userData.entrySet());
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;
        logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        return new LocalCheckpointTracker(maxSeqNo, localCheckpoint);
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
    public IndexResult index(Index index) {
        throw new UnsupportedOperationException("indexing is not supported on a noop engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException("deletion is not supported on a noop engine");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException("noop is not supported on a noop engine");
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException("synced flush is not supported on a noop engine");
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        throw new UnsupportedOperationException("gets are not supported on a noop engine");
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        throw new UnsupportedOperationException("searching is not supported on a noop engine");
    }

    @Override
    public Translog getTranslog() {
        return translog;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        return false;
    }

    @Override
    public void syncTranslog() {
    }

    @Override
    public LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
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
        // We never need to refresh a noop engine so always return false
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
    public void trimTranslog() throws EngineException {
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
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() :
                "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                IOUtils.close(translog);
            } catch (Exception e) {
                logger.warn("Failed to close translog", e);
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

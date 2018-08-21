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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A minimal engine that does not accept writes, and always points stats, searcher to the last commit.
 */
public final class SearchOnlyEngine extends Engine {
    private final SegmentInfos lastCommittedSegmentInfos;
    private final SeqNoStats seqNoStats;
    private final TranslogStats translogStats;
    private final SearcherManager searcherManager;

    public SearchOnlyEngine(EngineConfig config, SeqNoStats seqNoStats) {
        super(config);
        this.seqNoStats = seqNoStats;
        try {
            store.incRef();
            ElasticsearchDirectoryReader reader = null;
            boolean success = false;
            try {
                this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                this.translogStats = new TranslogStats(0, 0, 0, 0, 0);
                reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), config.getShardId());
                this.searcherManager = new SearcherManager(reader, new RamAccountingSearcherFactory(config.getCircuitBreakerService()));
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(reader, store::decRef);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                IOUtils.close(searcherManager, store::decRef);
            } catch (Exception ex) {
                logger.warn("failed to close engine", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, this::acquireSearcher, SearcherScope.INTERNAL);
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        store.incRef();
        Releasable releasable = store::decRef;
        try (ReleasableLock ignored = readLock.acquire()) {
            final EngineSearcher searcher = new EngineSearcher(source, searcherManager, store, logger);
            releasable = null; // hand over the reference to the engine searcher
            return searcher;
        } catch (AlreadyClosedException ex) {
            throw ex;
        } catch (Exception ex) {
            ensureOpen(ex); // throw AlreadyClosedException if it's closed
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            Releasables.close(releasable);
        }
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
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public Closeable acquireTranslogRetentionLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Translog.Snapshot newTranslogSnapshotFromMinSeqNo(long minSeqNo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int estimateTranslogOperationsFromMinSeq(long minSeqNo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return translogStats;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public void waitForOpsToComplete(long seqNo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetLocalCheckpoint(long newCheckpoint) {
        throw new UnsupportedOperationException();
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
    public void refresh(String source) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommitId flush() throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           boolean upgrade, boolean upgradeOnlyAncientSegments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void activateThrottling() {
    }

    @Override
    public void deactivateThrottling() {
    }

    @Override
    public void trimUnreferencedTranslogFiles() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Engine recoverFromTranslog(long recoverUpToSeqNo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipTranslogRecovery() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void maybePruneDeletes() {
    }
}

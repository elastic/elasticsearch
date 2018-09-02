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
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
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

    public SearchOnlyEngine(EngineConfig config, SeqNoStats seqNoStats, TranslogStats translogStats) {
        super(config);
        this.seqNoStats = seqNoStats;
        this.translogStats = translogStats;
        try {
            store.incRef();
            DirectoryReader reader = null;
            boolean success = false;
            try {
                this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                reader = DirectoryReader.open(store.directory());
                if (config.getIndexSettings().isSoftDeleteEnabled()) {
                    reader = new SoftDeletesDirectoryReaderWrapper(reader, Lucene.SOFT_DELETES_FIELD);
                }
                this.searcherManager = new SearcherManager(reader, new SearcherFactory());
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
    public void flushAndClose() throws IOException {
        // make a flush as a noop so that callers can close (and flush) this engine without worrying about the engine type.
        close();
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, this::acquireSearcher, SearcherScope.INTERNAL);
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        ensureOpen();
        Releasable releasable = null;
        try (ReleasableLock ignored = readLock.acquire()) {
            store.incRef();
            releasable = store::decRef;
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
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public DeleteResult delete(Delete delete) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        return ensureUnsupportedMethodNeverCalled();
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
        // noop
    }

    @Override
    public Closeable acquireRetentionLockForPeerRecovery() {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, MapperService mapperService,
                                                long fromSeqNo, long toSeqNo, boolean requiredFullRange) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public Translog.Snapshot readHistoryOperations(String source, MapperService mapperService, long startingSeqNo) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public int estimateNumberOfHistoryOperations(String source, MapperService mapperService, long startingSeqNo) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public boolean hasCompleteOperationHistory(String source, MapperService mapperService, long startingSeqNo) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return translogStats;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        // noop - returns null as the caller treats null as noop.
        return null;
    }

    @Override
    public long getLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public void waitForOpsToComplete(long seqNo) {
        ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public void resetLocalCheckpoint(long newCheckpoint) {
        ensureUnsupportedMethodNeverCalled();
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
        // noop
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
        return SyncedFlushResult.PENDING_OPERATIONS;
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public CommitId flush() throws EngineException {
        return flush(false, false);
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           boolean upgrade, boolean upgradeOnlyAncientSegments) {
        ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public void activateThrottling() {
    }

    @Override
    public void deactivateThrottling() {
    }

    @Override
    public void trimUnreferencedTranslogFiles() {
        ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        // noop
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() {
        ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public Engine recoverFromTranslog(long recoverUpToSeqNo) {
        return ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public void skipTranslogRecovery() {
        ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        ensureUnsupportedMethodNeverCalled();
    }

    @Override
    public void maybePruneDeletes() {

    }

    private <T> T ensureUnsupportedMethodNeverCalled() {
        assert false : "invoking an unsupported method in a search-only engine";
        throw new UnsupportedOperationException();
    }
}

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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ShadowEngine is a specialized engine that only allows read-only operations
 * on the underlying Lucene index. An {@code IndexReader} is opened instead of
 * an {@code IndexWriter}. All methods that would usually perform write
 * operations are no-ops, this means:
 *
 * - No operations are written to or read from the translog
 * - Create, Index, and Delete do nothing
 * - Flush does not fsync any files, or make any on-disk changes
 *
 * In order for new segments to become visible, the ShadowEngine may perform
 * stage1 of the traditional recovery process (copying segment files) from a
 * regular primary (which uses {@link org.elasticsearch.index.engine.InternalEngine})
 *
 * Notice that since this Engine does not deal with the translog, any
 * {@link #get(Get get)} request goes directly to the searcher, meaning it is
 * non-realtime.
 */
public class ShadowEngine extends Engine {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());
    private final Lock failReleasableLock = new ReentrantLock();
    private final RecoveryCounter onGoingRecoveries;

    private volatile boolean closedOrFailed = false;
    private volatile SearcherManager searcherManager;

    private SegmentInfos lastCommittedSegmentInfos;

    public ShadowEngine(EngineConfig engineConfig)  {
        super(engineConfig);
        SearcherFactory searcherFactory = new EngineSearcherFactory(engineConfig);
        this.onGoingRecoveries = new RecoveryCounter(store);
        try {
            DirectoryReader reader = null;
            store.incRef();
            boolean success = false;
            try {
                reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), shardId);
                this.searcherManager = new SearcherManager(reader, searcherFactory);
                this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                success = true;
            } catch (Throwable e) {
                logger.warn("failed to create new reader", e);
                throw e;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(reader);
                    store.decRef();
                }
            }
        } catch (IOException ex) {
            throw new EngineCreationFailureException(shardId, "failed to open index reader", ex);
        }
    }


    @Override
    public void create(Create create) throws EngineException {
        throw new UnsupportedOperationException("create operation not allowed on shadow engine");
    }

    @Override
    public void index(Index index) throws EngineException {
        throw new UnsupportedOperationException("index operation not allowed on shadow engine");
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        throw new UnsupportedOperationException("delete operation not allowed on shadow engine");
    }

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        throw new UnsupportedOperationException("delete-by-query operation not allowed on shadow engine");
    }

    @Override
    public void flush() throws EngineException {
        flush(false, false);
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        logger.trace("skipping FLUSH on shadow engine");
        // reread the last committed segment infos
        refresh("flush");
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Throwable e) {
            if (isClosed.get() == false) {
                logger.warn("failed to read latest segment infos on flush", e);
                if (Lucene.isCorruptionException(e)) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        }
    }

    @Override
    public void forceMerge(boolean flush) {
        forceMerge(flush, 1, false, false);
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade) throws EngineException {
        // no-op
        logger.trace("skipping FORCE-MERGE on shadow engine");
    }

    @Override
    public GetResult get(Get get) throws EngineException {
        // There is no translog, so we can get it directly from the searcher
        return getFromSearcher(get);
    }

    @Override
    public SegmentsStats segmentsStats() {
        ensureOpen();
        try (final Searcher searcher = acquireSearcher("segments_stats")) {
            SegmentsStats stats = new SegmentsStats();
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                final SegmentReader segmentReader = segmentReader(reader.reader());
                stats.add(1, segmentReader.ramBytesUsed());
                stats.addTermsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPostingsReader()));
                stats.addStoredFieldsMemoryInBytes(guardedRamBytesUsed(segmentReader.getFieldsReader()));
                stats.addTermVectorsMemoryInBytes(guardedRamBytesUsed(segmentReader.getTermVectorsReader()));
                stats.addNormsMemoryInBytes(guardedRamBytesUsed(segmentReader.getNormsReader()));
                stats.addDocValuesMemoryInBytes(guardedRamBytesUsed(segmentReader.getDocValuesReader()));
            }
            // No version map for shadow engine
            stats.addVersionMapMemoryInBytes(0);
            // Since there is no IndexWriter, these are 0
            stats.addIndexWriterMemoryInBytes(0);
            stats.addIndexWriterMaxMemoryInBytes(0);
            return stats;
        }
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock _ = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);
            for (int i = 0; i < segmentsArr.length; i++) {
                // hard code all segments as committed, because they are in
                // order for the shadow replica to see them
                segmentsArr[i].committed = true;
            }
            return Arrays.asList(segmentsArr);
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        try (ReleasableLock _ = readLock.acquire()) {
            ensureOpen();
            searcherManager.maybeRefreshBlocking();
        } catch (AlreadyClosedException e) {
            ensureOpen();
        } catch (EngineClosedException e) {
            throw e;
        } catch (Throwable t) {
            failEngine("refresh failed", t);
            throw new RefreshFailedEngineException(shardId, t);
        }
    }

    @Override
    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        flush(false, true);
        try (ReleasableLock _ = readLock.acquire()) {
            ensureOpen();
            return deletionPolicy.snapshot();
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        }
    }

    @Override
    public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        // take a write lock here so it won't happen while a flush is in progress
        // this means that next commits will not be allowed once the lock is released
        try (ReleasableLock _ = writeLock.acquire()) {
            if (closedOrFailed) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            onGoingRecoveries.startRecovery();
        }

        SnapshotIndexCommit phase1Snapshot;
        try {
            phase1Snapshot = deletionPolicy.snapshot();
        } catch (Throwable e) {
            maybeFailEngine("recovery", e);
            Releasables.closeWhileHandlingException(onGoingRecoveries);
            throw new RecoveryEngineException(shardId, 1, "Snapshot failed", e);
        }

        boolean success = false;
        try {
            recoveryHandler.phase1(phase1Snapshot);
            success = true;
        } catch (Throwable e) {
            maybeFailEngine("recovery phase 1", e);
            Releasables.closeWhileHandlingException(onGoingRecoveries, phase1Snapshot);
            throw new RecoveryEngineException(shardId, 1, "Execution failed", wrapIfClosed(e));
        } finally {
            Releasables.close(success, onGoingRecoveries, writeLock, phase1Snapshot);
        }

        // Since operations cannot be replayed from a translog on a shadow
        // engine, there is no phase2 and phase3 of recovery
    }

    @Override
    public void failEngine(String reason, Throwable failure) {
        // Note, there is no IndexWriter, so nothing to rollback here
        assert failure != null;
        if (failReleasableLock.tryLock()) {
            try {
                try {
                    // we first mark the store as corrupted before we notify any listeners
                    // this must happen first otherwise we might try to reallocate so quickly
                    // on the same node that we don't see the corrupted marker file when
                    // the shard is initializing
                    if (Lucene.isCorruptionException(failure)) {
                        try {
                            store.markStoreCorrupted(ExceptionsHelper.unwrapCorruption(failure));
                        } catch (IOException e) {
                            logger.warn("Couldn't marks store corrupted", e);
                        }
                    }
                } finally {
                    if (failedEngine != null) {
                        logger.debug("tried to fail engine but engine is already failed. ignoring. [{}]", reason, failure);
                        return;
                    }
                    logger.warn("failed engine [{}]", failure, reason);
                    // we must set a failure exception, generate one if not supplied
                    failedEngine = failure;
                    failedEngineListener.onFailedEngine(shardId, reason, failure);
                }
            } catch (Throwable t) {
                // don't bubble up these exceptions up
                logger.warn("failEngine threw exception", t);
            } finally {
                closedOrFailed = true;
            }
        } else {
            logger.debug("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason, failure);
        }
    }

    @Override
    protected SearcherManager getSearcherManager() {
        return searcherManager;
    }

    @Override
    public void close() throws IOException {
        logger.debug("shadow replica close now acquiring writeLock");
        try (ReleasableLock _ = writeLock.acquire()) {
            logger.debug("shadow replica close acquired writeLock");
            if (isClosed.compareAndSet(false, true)) {
                try {
                    logger.debug("shadow replica close searcher manager refCount: {}", store.refCount());
                    IOUtils.close(searcherManager);
                } catch (Throwable t) {
                    logger.warn("shadow replica failed to close searcher manager", t);
                } finally {
                    store.decRef();
                }
            }
        }
    }
}

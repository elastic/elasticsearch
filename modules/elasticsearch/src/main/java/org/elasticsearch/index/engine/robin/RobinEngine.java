/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.engine.robin;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.IndexWriters;
import org.elasticsearch.common.lucene.ReaderSearcherHolder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.resource.AcquirableResource;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.lucene.Lucene.*;
import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.common.util.concurrent.resource.AcquirableResourceFactory.*;

/**
 * @author kimchy (shay.banon)
 */
public class RobinEngine extends AbstractIndexShardComponent implements Engine, ScheduledRefreshableEngine {

    private volatile ByteSizeValue indexingBufferSize;

    private final TimeValue refreshInterval;

    private final int termIndexInterval;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private final AtomicBoolean refreshMutex = new AtomicBoolean();

    private final AtomicBoolean optimizeMutex = new AtomicBoolean();

    private final Store store;

    private final SnapshotDeletionPolicy deletionPolicy;

    private final Translog translog;

    private final MergePolicyProvider mergePolicyProvider;

    private final MergeSchedulerProvider mergeScheduler;

    private final AnalysisService analysisService;

    private final SimilarityService similarityService;

    // no need for volatile, its always used under a lock
    private IndexWriter indexWriter;

    private volatile AcquirableResource<ReaderSearcherHolder> nrtResource;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile int disableFlushCounter = 0;

    @Inject public RobinEngine(ShardId shardId, @IndexSettings Settings indexSettings, Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                               MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                               AnalysisService analysisService, SimilarityService similarityService) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.indexingBufferSize = componentSettings.getAsBytesSize("indexing_buffer_size", new ByteSizeValue(64, ByteSizeUnit.MB));
        this.refreshInterval = componentSettings.getAsTime("refresh_interval", timeValueSeconds(1));
        this.termIndexInterval = componentSettings.getAsInt("term_index_interval", IndexWriter.DEFAULT_TERM_INDEX_INTERVAL);

        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
    }

    @Override public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
        rwl.readLock().lock();
        try {
            // LUCENE MONITOR - If this restriction is removed from Lucene, remove it from here
            if (indexingBufferSize.mbFrac() > 2048.0) {
                this.indexingBufferSize = new ByteSizeValue(2048, ByteSizeUnit.MB);
            } else {
                this.indexingBufferSize = indexingBufferSize;
            }
            IndexWriter indexWriter = this.indexWriter;
            if (indexWriter != null) {
                indexWriter.setRAMBufferSizeMB(this.indexingBufferSize.mbFrac());
            }
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void start() throws EngineException {
        rwl.writeLock().lock();
        try {
            if (indexWriter != null) {
                throw new EngineAlreadyStartedException(shardId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Starting engine with ram_buffer_size[" + indexingBufferSize + "], refresh_interval[" + refreshInterval + "]");
            }
            try {
                this.indexWriter = createWriter();
            } catch (IOException e) {
                throw new EngineCreationFailureException(shardId, "Failed to create engine", e);
            }

            try {
                translog.newTranslog(newTransactionLogId());
                this.nrtResource = buildNrtResource(indexWriter);
            } catch (IOException e) {
                try {
                    indexWriter.rollback();
                } catch (IOException e1) {
                    // ignore
                } finally {
                    try {
                        indexWriter.close();
                    } catch (IOException e1) {
                        // ignore
                    }
                }
                throw new EngineCreationFailureException(shardId, "Failed to open reader on writer", e);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public TimeValue refreshInterval() {
        return refreshInterval;
    }

    @Override public EngineException[] bulk(Bulk bulk) throws EngineException {
        EngineException[] failures = null;
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }
            for (int i = 0; i < bulk.ops().length; i++) {
                Operation op = bulk.ops()[i];
                if (op == null) {
                    continue;
                }
                try {
                    switch (op.opType()) {
                        case CREATE:
                            Create create = (Create) op;
                            writer.addDocument(create.doc(), create.analyzer());
                            translog.add(new Translog.Create(create));
                            break;
                        case INDEX:
                            Index index = (Index) op;
                            writer.updateDocument(index.uid(), index.doc(), index.analyzer());
                            translog.add(new Translog.Index(index));
                            break;
                        case DELETE:
                            Delete delete = (Delete) op;
                            writer.deleteDocuments(delete.uid());
                            translog.add(new Translog.Delete(delete));
                            break;
                    }
                } catch (Exception e) {
                    if (failures == null) {
                        failures = new EngineException[bulk.ops().length];
                    }
                    switch (op.opType()) {
                        case CREATE:
                            failures[i] = new CreateFailedEngineException(shardId, (Create) op, e);
                            break;
                        case INDEX:
                            failures[i] = new IndexFailedEngineException(shardId, (Index) op, e);
                            break;
                        case DELETE:
                            failures[i] = new DeleteFailedEngineException(shardId, (Delete) op, e);
                            break;
                    }
                }
            }
            dirty = true;
        } finally {
            rwl.readLock().unlock();
        }
        return failures;
    }

    @Override public void create(Create create) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }
            writer.addDocument(create.doc(), create.analyzer());
            translog.add(new Translog.Create(create));
            dirty = true;
        } catch (IOException e) {
            throw new CreateFailedEngineException(shardId, create, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void index(Index index) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }
            writer.updateDocument(index.uid(), index.doc(), index.analyzer());
            translog.add(new Translog.Index(index));
            dirty = true;
        } catch (IOException e) {
            throw new IndexFailedEngineException(shardId, index, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void delete(Delete delete) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }
            writer.deleteDocuments(delete.uid());
            translog.add(new Translog.Delete(delete));
            dirty = true;
        } catch (IOException e) {
            throw new DeleteFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void delete(DeleteByQuery delete) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }
            writer.deleteDocuments(delete.query());
            translog.add(new Translog.DeleteByQuery(delete));
            dirty = true;
        } catch (IOException e) {
            throw new DeleteByQueryFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public Searcher searcher() throws EngineException {
        AcquirableResource<ReaderSearcherHolder> holder;
        for (; ;) {
            holder = this.nrtResource;
            if (holder.acquire()) {
                break;
            }
            Thread.yield();
        }
        return new RobinSearchResult(holder);
    }

    @Override public ByteSizeValue estimateFlushableMemorySize() {
        rwl.readLock().lock();
        try {
            long bytes = IndexWriters.estimateRamSize(indexWriter);
            bytes += translog.estimateMemorySize().bytes();
            return new ByteSizeValue(bytes);
        } catch (Exception e) {
            return null;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void refresh(Refresh refresh) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        rwl.readLock().lock();
        if (indexWriter == null) {
            throw new EngineClosedException(shardId);
        }
        try {
            // this engine always acts as if waitForOperations=true
            if (refreshMutex.compareAndSet(false, true)) {
                IndexWriter currentWriter = indexWriter;
                if (currentWriter == null) {
                    throw new EngineClosedException(shardId);
                }
                try {
                    if (dirty) {
                        dirty = false;
                        AcquirableResource<ReaderSearcherHolder> current = nrtResource;
                        IndexReader newReader = current.resource().reader().reopen(true);
                        if (newReader != current.resource().reader()) {
                            IndexSearcher indexSearcher = new IndexSearcher(newReader);
                            indexSearcher.setSimilarity(similarityService.defaultSearchSimilarity());
                            nrtResource = newAcquirableResource(new ReaderSearcherHolder(indexSearcher));
                            current.markForClose();
                        }
                    }
                } catch (AlreadyClosedException e) {
                    // an index writer got replaced on us, ignore
                } catch (Exception e) {
                    if (indexWriter == null) {
                        throw new EngineClosedException(shardId);
                    } else if (currentWriter != indexWriter) {
                        // an index writer got replaced on us, ignore
                    } else {
                        throw new RefreshFailedEngineException(shardId, e);
                    }
                } finally {
                    refreshMutex.set(false);
                }
            }
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void flush(Flush flush) throws EngineException {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId);
        }
        // check outside the lock as well so we can check without blocking on the write lock
        if (disableFlushCounter > 0) {
            throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
        }
        rwl.writeLock().lock();
        try {
            if (indexWriter == null) {
                throw new EngineClosedException(shardId);
            }
            if (disableFlushCounter > 0) {
                throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
            }
            if (flush.full()) {
                // disable refreshing, not dirty
                dirty = false;
                refreshMutex.set(true);
                try {
                    // that's ok if the index writer failed and is in inconsistent state
                    // we will get an exception on a dirty operation, and will cause the shard
                    // to be allocated to a different node
                    indexWriter.close();
                    indexWriter = createWriter();
                    AcquirableResource<ReaderSearcherHolder> current = nrtResource;
                    nrtResource = buildNrtResource(indexWriter);
                    current.markForClose();
                    translog.newTranslog(newTransactionLogId());
                } catch (IOException e) {
                    throw new FlushFailedEngineException(shardId, e);
                } finally {
                    refreshMutex.set(false);
                }
            } else {
                try {
                    indexWriter.commit();
                    translog.newTranslog(newTransactionLogId());
                } catch (IOException e) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        } finally {
            rwl.writeLock().unlock();
        }
        if (flush.refresh()) {
            refresh(new Refresh(false));
        }
    }

    @Override public void optimize(Optimize optimize) throws EngineException {
        if (optimizeMutex.compareAndSet(false, true)) {
            rwl.readLock().lock();
            try {
                if (indexWriter == null) {
                    throw new EngineClosedException(shardId);
                }
                int maxNumberOfSegments = optimize.maxNumSegments();
                if (maxNumberOfSegments == -1) {
                    // not set, optimize down to half the configured number of segments
                    if (indexWriter.getMergePolicy() instanceof LogMergePolicy) {
                        maxNumberOfSegments = ((LogMergePolicy) indexWriter.getMergePolicy()).getMergeFactor() / 2;
                        if (maxNumberOfSegments < 0) {
                            maxNumberOfSegments = 1;
                        }
                    }
                }
                if (optimize.onlyExpungeDeletes()) {
                    indexWriter.expungeDeletes(optimize.waitForMerge());
                } else {
                    indexWriter.optimize(maxNumberOfSegments, optimize.waitForMerge());
                }
                // once we did the optimization, we are "dirty" since we removed deletes potentially which
                // affects TermEnum
                dirty = true;
            } catch (Exception e) {
                throw new OptimizeFailedEngineException(shardId, e);
            } finally {
                rwl.readLock().unlock();
                optimizeMutex.set(false);
            }
        }
        if (optimize.flush()) {
            flush(new Flush());
        }
        if (optimize.refresh()) {
            refresh(new Refresh(false));
        }
    }

    @Override public <T> T snapshot(SnapshotHandler<T> snapshotHandler) throws EngineException {
        SnapshotIndexCommit snapshotIndexCommit = null;
        Translog.Snapshot traslogSnapshot = null;
        rwl.readLock().lock();
        try {
            snapshotIndexCommit = deletionPolicy.snapshot();
            traslogSnapshot = translog.snapshot();
        } catch (Exception e) {
            if (snapshotIndexCommit != null) snapshotIndexCommit.release();
            throw new SnapshotFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }

        try {
            return snapshotHandler.snapshot(snapshotIndexCommit, traslogSnapshot);
        } finally {
            snapshotIndexCommit.release();
            traslogSnapshot.release();
        }
    }

    @Override public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        // take a write lock here so it won't happen while a flush is in progress
        // this means that next commits will not be allowed once the lock is released
        rwl.writeLock().lock();
        try {
            disableFlushCounter++;
        } finally {
            rwl.writeLock().unlock();
        }

        SnapshotIndexCommit phase1Snapshot;
        try {
            phase1Snapshot = deletionPolicy.snapshot();
        } catch (IOException e) {
            --disableFlushCounter;
            throw new RecoveryEngineException(shardId, 1, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase1(phase1Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            throw new RecoveryEngineException(shardId, 1, "Execution failed", e);
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            phase2Snapshot.release();
            throw new RecoveryEngineException(shardId, 2, "Execution failed", e);
        }

        rwl.writeLock().lock();
        Translog.Snapshot phase3Snapshot;
        try {
            phase3Snapshot = translog.snapshot(phase2Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            throw new RecoveryEngineException(shardId, 3, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase3(phase3Snapshot);
        } catch (Exception e) {
            throw new RecoveryEngineException(shardId, 3, "Execution failed", e);
        } finally {
            --disableFlushCounter;
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            phase3Snapshot.release();
        }
    }

    @Override public void close() throws ElasticSearchException {
        if (closed) {
            return;
        }
        closed = true;
        rwl.writeLock().lock();
        try {
            if (nrtResource != null) {
                this.nrtResource.forceClose();
            }
            // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
            if (indexWriter != null) {
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException e) {
                    // ignore
                }
            }
        } catch (IOException e) {
            logger.debug("failed to rollback writer on close", e);
        } finally {
            indexWriter = null;
            rwl.writeLock().unlock();
        }
    }

    private IndexWriter createWriter() throws IOException {
        IndexWriter indexWriter = null;
        try {
            // release locks when started
            if (IndexWriter.isLocked(store.directory())) {
                logger.warn("shard is locked, releasing lock");
                IndexWriter.unlock(store.directory());
            }
            boolean create = !IndexReader.indexExists(store.directory());
            indexWriter = new IndexWriter(store.directory(),
                    analysisService.defaultIndexAnalyzer(), create, deletionPolicy, IndexWriter.MaxFieldLength.UNLIMITED);
            indexWriter.setMergeScheduler(mergeScheduler.newMergeScheduler());
            indexWriter.setMergePolicy(mergePolicyProvider.newMergePolicy(indexWriter));
            indexWriter.setSimilarity(similarityService.defaultIndexSimilarity());
            indexWriter.setRAMBufferSizeMB(indexingBufferSize.mbFrac());
            indexWriter.setTermIndexInterval(termIndexInterval);
        } catch (IOException e) {
            safeClose(indexWriter);
            throw e;
        }
        return indexWriter;
    }

    private AcquirableResource<ReaderSearcherHolder> buildNrtResource(IndexWriter indexWriter) throws IOException {
        IndexReader indexReader = indexWriter.getReader();
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(similarityService.defaultSearchSimilarity());
        return newAcquirableResource(new ReaderSearcherHolder(indexSearcher));
    }

    private long newTransactionLogId() throws IOException {
        try {
            return IndexWriters.rollbackSegmentInfos(indexWriter).getVersion();
        } catch (Exception e) {
            return IndexReader.getCurrentVersion(store.directory());
        }
    }

    private static class RobinSearchResult implements Searcher {

        private final AcquirableResource<ReaderSearcherHolder> nrtHolder;

        private RobinSearchResult(AcquirableResource<ReaderSearcherHolder> nrtHolder) {
            this.nrtHolder = nrtHolder;
        }

        @Override public IndexReader reader() {
            return nrtHolder.resource().reader();
        }

        @Override public IndexSearcher searcher() {
            return nrtHolder.resource().searcher();
        }

        @Override public boolean release() throws ElasticSearchException {
            nrtHolder.release();
            return true;
        }
    }
}

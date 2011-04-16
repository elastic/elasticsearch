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

import org.apache.lucene.index.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.IndexWriters;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.ReaderSearcherHolder;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.resource.AcquirableResource;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.policy.EnableMergePolicy;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.lucene.Lucene.*;
import static org.elasticsearch.common.util.concurrent.resource.AcquirableResourceFactory.*;

/**
 * @author kimchy (shay.banon)
 */
public class RobinEngine extends AbstractIndexShardComponent implements Engine {

    private volatile ByteSizeValue indexingBufferSize;

    private volatile int termIndexInterval;

    private volatile int termIndexDivisor;

    private volatile int indexConcurrency;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private final AtomicBoolean optimizeMutex = new AtomicBoolean();

    private final long gcDeletesInMillis;

    private final ThreadPool threadPool;

    private final IndexSettingsService indexSettingsService;

    private final Store store;

    private final SnapshotDeletionPolicy deletionPolicy;

    private final Translog translog;

    private final MergePolicyProvider mergePolicyProvider;

    private final MergeSchedulerProvider mergeScheduler;

    private final AnalysisService analysisService;

    private final SimilarityService similarityService;

    private final BloomCache bloomCache;

    private final boolean asyncLoadBloomFilter;

    // no need for volatile, its always used under a lock
    private IndexWriter indexWriter;

    private volatile AcquirableResource<ReaderSearcherHolder> nrtResource;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile boolean possibleMergeNeeded = false;

    private volatile int disableFlushCounter = 0;

    // indexing searcher is initialized
    private final AtomicReference<Searcher> indexingSearcher = new AtomicReference<Searcher>();

    private final AtomicBoolean flushing = new AtomicBoolean();

    private final ConcurrentMap<String, VersionValue> versionMap;

    private final Object[] dirtyLocks;

    private final Object refreshMutex = new Object();

    private final ApplySettings applySettings = new ApplySettings();

    private Throwable failedEngine = null;
    private final Object failedEngineMutex = new Object();
    private final CopyOnWriteArrayList<FailedEngineListener> failedEngineListeners = new CopyOnWriteArrayList<FailedEngineListener>();

    @Inject public RobinEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                               IndexSettingsService indexSettingsService,
                               Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                               MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                               AnalysisService analysisService, SimilarityService similarityService,
                               BloomCache bloomCache) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.gcDeletesInMillis = indexSettings.getAsTime("index.gc_deletes", TimeValue.timeValueSeconds(60)).millis();
        this.indexingBufferSize = componentSettings.getAsBytesSize("index_buffer_size", new ByteSizeValue(64, ByteSizeUnit.MB)); // not really important, as it is set by the IndexingMemory manager
        this.termIndexInterval = indexSettings.getAsInt("index.term_index_interval", IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL);
        this.termIndexDivisor = indexSettings.getAsInt("index.term_index_divisor", 1); // IndexReader#DEFAULT_TERMS_INDEX_DIVISOR
        this.asyncLoadBloomFilter = componentSettings.getAsBoolean("async_load_bloom", true); // Here for testing, should always be true

        this.threadPool = threadPool;
        this.indexSettingsService = indexSettingsService;
        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.bloomCache = bloomCache;

        this.indexConcurrency = indexSettings.getAsInt("index.index_concurrency", IndexWriterConfig.DEFAULT_MAX_THREAD_STATES);
        this.versionMap = new ConcurrentHashMap<String, VersionValue>();
        this.dirtyLocks = new Object[indexConcurrency * 10]; // we multiply it by 10 to have enough...
        for (int i = 0; i < dirtyLocks.length; i++) {
            dirtyLocks[i] = new Object();
        }

        this.indexSettingsService.addListener(applySettings);
    }

    @Override public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
        ByteSizeValue preValue = this.indexingBufferSize;
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
                indexWriter.getConfig().setRAMBufferSizeMB(this.indexingBufferSize.mbFrac());
            }
        } finally {
            rwl.readLock().unlock();
        }
        // its inactive, make sure we do a full flush in this case, since the memory
        // changes only after a "data" change has happened to the writer
        if (indexingBufferSize == Engine.INACTIVE_SHARD_INDEXING_BUFFER && preValue != Engine.INACTIVE_SHARD_INDEXING_BUFFER) {
            try {
                flush(new Flush().full(true));
            } catch (Exception e) {
                logger.warn("failed to flush after setting shard to inactive", e);
            }
        }
    }

    @Override public void addFailedEngineListener(FailedEngineListener listener) {
        failedEngineListeners.add(listener);
    }

    @Override public void start() throws EngineException {
        rwl.writeLock().lock();
        try {
            if (indexWriter != null) {
                throw new EngineAlreadyStartedException(shardId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Starting engine");
            }
            try {
                this.indexWriter = createWriter();
            } catch (IOException e) {
                throw new EngineCreationFailureException(shardId, "Failed to create engine", e);
            }

            try {
                translog.newTranslog(newTransactionLogId());
                this.nrtResource = buildNrtResource(indexWriter);
                if (indexingSearcher.get() != null) {
                    indexingSearcher.get().release();
                    indexingSearcher.set(null);
                }
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

    @Override public TimeValue defaultRefreshInterval() {
        return new TimeValue(1, TimeUnit.SECONDS);
    }

    @Override public void create(Create create) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerCreate(create, writer);
            dirty = true;
            possibleMergeNeeded = true;
        } catch (IOException e) {
            throw new CreateFailedEngineException(shardId, create, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new CreateFailedEngineException(shardId, create, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    private void innerCreate(Create create, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(create.uid())) {
            UidField uidField = create.uidField();
            if (create.origin() == Operation.Origin.RECOVERY) {
                // on recovery, we get the actual version we want to use
                if (create.version() != 0) {
                    versionMap.put(create.uid().text(), new VersionValue(create.version(), false, threadPool.estimatedTimeInMillis()));
                }
                uidField.version(create.version());
                writer.addDocument(create.doc(), create.analyzer());
                translog.add(new Translog.Create(create));
            } else {
                long currentVersion;
                VersionValue versionValue = versionMap.get(create.uid().text());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(create.uid());
                } else {
                    if (versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                        currentVersion = -1; // deleted, and GC
                    } else {
                        currentVersion = versionValue.version();
                    }
                }

                // same logic as index
                long updatedVersion;
                if (create.origin() == Operation.Origin.PRIMARY) {
                    if (create.versionType() == VersionType.INTERNAL) { // internal version type
                        long expectedVersion = create.version();
                        if (expectedVersion != 0 && currentVersion != -2) { // -2 means we don't have a version, so ignore...
                            // an explicit version is provided, see if there is a conflict
                            // if the current version is -1, means we did not find anything, and
                            // a version is provided, so we do expect to find a doc under that version
                            // this is important, since we don't allow to preset a version in order to handle deletes
                            if (currentVersion == -1) {
                                throw new VersionConflictEngineException(shardId, create.type(), create.id(), -1, expectedVersion);
                            } else if (expectedVersion != currentVersion) {
                                throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
                            }
                        }
                        updatedVersion = currentVersion < 0 ? 1 : currentVersion + 1;
                    } else { // external version type
                        // an external version is provided, just check, if a local version exists, that its higher than it
                        // the actual version checking is one in an external system, and we just want to not index older versions
                        if (currentVersion >= 0) { // we can check!, its there
                            if (currentVersion >= create.version()) {
                                throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, create.version());
                            }
                        }
                        updatedVersion = create.version();
                    }
                } else { // if (index.origin() == Operation.Origin.REPLICA) {
                    long expectedVersion = create.version();
                    if (currentVersion != -2) { // -2 means we don't have a version, so ignore...
                        // if it does not exists, and its considered the first index operation (replicas are 1 of)
                        // then nothing to do
                        if (!(currentVersion == -1 && create.version() == 1)) {
                            // with replicas, we only check for previous version, we allow to set a future version
                            if (expectedVersion <= currentVersion) {
                                throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
                            }
                        }
                    }
                    // replicas already hold the "future" version
                    updatedVersion = create.version();
                }

                // if the doc does not exists or it exists but not delete
                if (versionValue != null) {
                    if (!versionValue.delete()) {
                        throw new DocumentAlreadyExistsEngineException(shardId, create.type(), create.id());
                    }
                } else if (currentVersion != -1) {
                    // its not deleted, its already there
                    throw new DocumentAlreadyExistsEngineException(shardId, create.type(), create.id());
                }

                versionMap.put(create.uid().text(), new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis()));
                uidField.version(updatedVersion);
                create.version(updatedVersion);

                writer.addDocument(create.doc(), create.analyzer());
                translog.add(new Translog.Create(create));
            }
        }
    }

    @Override public void index(Index index) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }

            innerIndex(index, writer);
            dirty = true;
            possibleMergeNeeded = true;
        } catch (IOException e) {
            throw new IndexFailedEngineException(shardId, index, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new IndexFailedEngineException(shardId, index, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    private void innerIndex(Index index, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(index.uid())) {
            UidField uidField = index.uidField();
            if (index.origin() == Operation.Origin.RECOVERY) {
                // on recovery, we get the actual version we want to use
                if (index.version() != 0) {
                    versionMap.put(index.uid().text(), new VersionValue(index.version(), false, threadPool.estimatedTimeInMillis()));
                }
                uidField.version(index.version());
                writer.updateDocument(index.uid(), index.doc(), index.analyzer());
                translog.add(new Translog.Index(index));
            } else {
                long currentVersion;
                VersionValue versionValue = versionMap.get(index.uid().text());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(index.uid());
                } else {
                    if (versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                        currentVersion = -1; // deleted, and GC
                    } else {
                        currentVersion = versionValue.version();
                    }
                }

                long updatedVersion;
                if (index.origin() == Operation.Origin.PRIMARY) {
                    if (index.versionType() == VersionType.INTERNAL) { // internal version type
                        long expectedVersion = index.version();
                        if (expectedVersion != 0 && currentVersion != -2) { // -2 means we don't have a version, so ignore...
                            // an explicit version is provided, see if there is a conflict
                            // if the current version is -1, means we did not find anything, and
                            // a version is provided, so we do expect to find a doc under that version
                            // this is important, since we don't allow to preset a version in order to handle deletes
                            if (currentVersion == -1) {
                                throw new VersionConflictEngineException(shardId, index.type(), index.id(), -1, expectedVersion);
                            } else if (expectedVersion != currentVersion) {
                                throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
                            }
                        }
                        updatedVersion = currentVersion < 0 ? 1 : currentVersion + 1;
                    } else { // external version type
                        // an external version is provided, just check, if a local version exists, that its higher than it
                        // the actual version checking is one in an external system, and we just want to not index older versions
                        if (currentVersion >= 0) { // we can check!, its there
                            if (currentVersion >= index.version()) {
                                throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, index.version());
                            }
                        }
                        updatedVersion = index.version();
                    }
                } else { // if (index.origin() == Operation.Origin.REPLICA) {
                    long expectedVersion = index.version();
                    if (currentVersion != -2) { // -2 means we don't have a version, so ignore...
                        // if it does not exists, and its considered the first index operation (replicas are 1 of)
                        // then nothing to do
                        if (!(currentVersion == -1 && index.version() == 1)) {
                            // with replicas, we only check for previous version, we allow to set a future version
                            if (expectedVersion <= currentVersion) {
                                throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
                            }
                        }
                    }
                    // replicas already hold the "future" version
                    updatedVersion = index.version();
                }

                versionMap.put(index.uid().text(), new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis()));
                uidField.version(updatedVersion);
                index.version(updatedVersion);

                if (currentVersion == -1) {
                    // document does not exists, we can optimize for create
                    writer.addDocument(index.doc(), index.analyzer());
                } else {
                    writer.updateDocument(index.uid(), index.doc(), index.analyzer());
                }
                translog.add(new Translog.Index(index));
            }
        }
    }

    @Override public void delete(Delete delete) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerDelete(delete, writer);
            dirty = true;
            possibleMergeNeeded = true;
        } catch (IOException e) {
            throw new DeleteFailedEngineException(shardId, delete, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new DeleteFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    private void innerDelete(Delete delete, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(delete.uid())) {
            if (delete.origin() == Operation.Origin.RECOVERY) {
                // update the version with the exact version from recovery, assuming we have it
                if (delete.version() != 0) {
                    versionMap.put(delete.uid().text(), new VersionValue(delete.version(), true, threadPool.estimatedTimeInMillis()));
                }

                writer.deleteDocuments(delete.uid());
                translog.add(new Translog.Delete(delete));
            } else {
                long currentVersion;
                VersionValue versionValue = versionMap.get(delete.uid().text());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(delete.uid());
                } else {
                    if (versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                        currentVersion = -1; // deleted, and GC
                    } else {
                        currentVersion = versionValue.version();
                    }
                }

                long updatedVersion;
                if (delete.origin() == Operation.Origin.PRIMARY) {
                    if (delete.versionType() == VersionType.INTERNAL) { // internal version type
                        if (delete.version() != 0 && currentVersion != -2) { // -2 means we don't have a version, so ignore...
                            // an explicit version is provided, see if there is a conflict
                            // if the current version is -1, means we did not find anything, and
                            // a version is provided, so we do expect to find a doc under that version
                            if (currentVersion == -1) {
                                throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), -1, delete.version());
                            } else if (delete.version() != currentVersion) {
                                throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion, delete.version());
                            }
                        }
                        updatedVersion = currentVersion < 0 ? 1 : currentVersion + 1;
                    } else { // External
                        if (currentVersion == -1) {
                            throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), -1, delete.version());
                        } else if (currentVersion >= delete.version()) {
                            throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion, delete.version());
                        }
                        updatedVersion = delete.version();
                    }
                } else { // if (delete.origin() == Operation.Origin.REPLICA) {
                    // on replica, the version is the future value expected (returned from the operation on the primary)
                    if (currentVersion != -2) { // -2 means we don't have a version in the index, ignore
                        // only check if we have a version for it, otherwise, ignore (see later)
                        if (currentVersion != -1) {
                            // with replicas, we only check for previous version, we allow to set a future version
                            if (delete.version() <= currentVersion) {
                                throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion - 1, delete.version());
                            }
                        }
                    }
                    // replicas already hold the "future" version
                    updatedVersion = delete.version();
                }

                if (currentVersion == -1) {
                    // if the doc does not exists, just update with doc 0
                    delete.version(0).notFound(true);
                } else if (versionValue != null && versionValue.delete()) {
                    // if its a delete on delete and we have the current delete version, return it
                    delete.version(versionValue.version()).notFound(true);
                } else {
                    versionMap.put(delete.uid().text(), new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis()));
                    delete.version(updatedVersion);
                    writer.deleteDocuments(delete.uid());
                    translog.add(new Translog.Delete(delete));
                }
            }
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
            possibleMergeNeeded = true;
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

    @Override public boolean refreshNeeded() {
        return dirty;
    }

    @Override public boolean possibleMergeNeeded() {
        return this.possibleMergeNeeded;
    }

    @Override public void refresh(Refresh refresh) throws EngineException {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId);
        }
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        rwl.readLock().lock();
        try {
            // this engine always acts as if waitForOperations=true
            IndexWriter currentWriter = indexWriter;
            if (currentWriter == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            try {
                // we need to obtain a mutex here, to make sure we don't leave dangling readers
                // we could have used an AtomicBoolean#compareAndSet, but, then we might miss refresh requests
                // compared to on going ones
                synchronized (refreshMutex) {
                    if (dirty || refresh.force()) {
                        dirty = false;
                        AcquirableResource<ReaderSearcherHolder> current = nrtResource;
                        IndexReader newReader = current.resource().reader().reopen(true);
                        if (newReader != current.resource().reader()) {
                            ExtendedIndexSearcher indexSearcher = new ExtendedIndexSearcher(newReader);
                            indexSearcher.setSimilarity(similarityService.defaultSearchSimilarity());
                            nrtResource = newAcquirableResource(new ReaderSearcherHolder(indexSearcher));
                            current.markForClose();
                        }
                    }
                }
            } catch (AlreadyClosedException e) {
                // an index writer got replaced on us, ignore
            } catch (Exception e) {
                if (indexWriter == null) {
                    throw new EngineClosedException(shardId, failedEngine);
                } else if (currentWriter != indexWriter) {
                    // an index writer got replaced on us, ignore
                } else {
                    throw new RefreshFailedEngineException(shardId, e);
                }
            } catch (OutOfMemoryError e) {
                failEngine(e);
                throw new RefreshFailedEngineException(shardId, e);
            }
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void flush(Flush flush) throws EngineException {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId, failedEngine);
        }
        // check outside the lock as well so we can check without blocking on the write lock
        if (disableFlushCounter > 0) {
            throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
        }
        // don't allow for concurrent flush operations...
        if (!flushing.compareAndSet(false, true)) {
            throw new FlushNotAllowedEngineException(shardId, "Already flushing...");
        }

        // We can't do prepareCommit here, since we rely on the the segment version for the translog version

        rwl.writeLock().lock();
        try {
            if (indexWriter == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            if (disableFlushCounter > 0) {
                throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
            }
            if (indexingSearcher.get() != null) {
                indexingSearcher.get().release();
                indexingSearcher.set(null);
            }
            if (flush.full()) {
                // disable refreshing, not dirty
                dirty = false;
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
                } catch (Exception e) {
                    throw new FlushFailedEngineException(shardId, e);
                } catch (OutOfMemoryError e) {
                    failEngine(e);
                    throw new FlushFailedEngineException(shardId, e);
                }
            } else {
                try {
                    indexWriter.commit();
                    translog.newTranslog(newTransactionLogId());
                } catch (Exception e) {
                    throw new FlushFailedEngineException(shardId, e);
                } catch (OutOfMemoryError e) {
                    failEngine(e);
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
            // remove all version except for deletes, which we expire based on GC value
            long time = threadPool.estimatedTimeInMillis();
            for (Map.Entry<String, VersionValue> entry : versionMap.entrySet()) {
                if (entry.getValue().delete()) {
                    if ((time - entry.getValue().time()) > gcDeletesInMillis) {
                        versionMap.remove(entry.getKey());
                    }
                } else {
                    versionMap.remove(entry.getKey());
                }
            }
            dirty = true; // force a refresh
            // we need to do a refresh here so we sync versioning support
            refresh(new Refresh(true).force(true));
        } finally {
            rwl.writeLock().unlock();
            flushing.set(false);
        }
        // we refresh anyhow before...
//        if (flush.refresh()) {
//            refresh(new Refresh(false));
//        }
    }

    @Override public void maybeMerge() throws EngineException {
        if (!possibleMergeNeeded) {
            return;
        }
        possibleMergeNeeded = false;
        rwl.readLock().lock();
        try {
            if (indexWriter == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            if (indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).enableMerge();
            }
            indexWriter.maybeMerge();
        } catch (Exception e) {
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new OptimizeFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
            if (indexWriter != null && indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).disableMerge();
            }
        }
    }

    @Override public void optimize(Optimize optimize) throws EngineException {
        if (optimizeMutex.compareAndSet(false, true)) {
            rwl.readLock().lock();
            try {
                if (indexWriter == null) {
                    throw new EngineClosedException(shardId, failedEngine);
                }
                if (indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                    ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).enableMerge();
                }
                if (optimize.onlyExpungeDeletes()) {
                    indexWriter.expungeDeletes(false);
                } else if (optimize.maxNumSegments() <= 0) {
                    indexWriter.maybeMerge();
                    possibleMergeNeeded = false;
                } else {
                    indexWriter.optimize(optimize.maxNumSegments(), false);
                }
            } catch (Exception e) {
                throw new OptimizeFailedEngineException(shardId, e);
            } catch (OutOfMemoryError e) {
                failEngine(e);
                throw new OptimizeFailedEngineException(shardId, e);
            } finally {
                if (indexWriter != null && indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                    ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).disableMerge();
                }
                rwl.readLock().unlock();
                optimizeMutex.set(false);
            }
        }
        // wait for the merges outside of the read lock
        if (optimize.waitForMerge()) {
            indexWriter.waitForMerges();
        }
        if (optimize.flush()) {
            flush(new Flush());
        }
        if (optimize.refresh()) {
            refresh(new Refresh(false).force(true));
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
        } catch (Exception e) {
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
        rwl.writeLock().lock();
        try {
            innerClose();
        } finally {
            rwl.writeLock().unlock();
        }
    }

    private void failEngine(Throwable failure) {
        synchronized (failedEngineMutex) {
            if (failedEngine != null) {
                return;
            }
            logger.warn("failed engine", failure);
            failedEngine = failure;
            for (FailedEngineListener listener : failedEngineListeners) {
                listener.onFailedEngine(shardId, failure);
            }
            innerClose();
        }
    }

    private void innerClose() {
        if (closed) {
            return;
        }
        indexSettingsService.removeListener(applySettings);
        closed = true;
        this.versionMap.clear();
        this.failedEngineListeners.clear();
        try {
            if (indexingSearcher.get() != null) {
                indexingSearcher.get().release();
                indexingSearcher.set(null);
            }
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
        } catch (Exception e) {
            logger.debug("failed to rollback writer on close", e);
        } finally {
            indexWriter = null;
        }
    }

    private Object dirtyLock(Term uid) {
        return dirtyLocks[Math.abs(uid.hashCode()) % dirtyLocks.length];
    }

    private long loadCurrentVersionFromIndex(Term uid) {
        UnicodeUtil.UTF8Result utf8 = Unicode.fromStringAsUtf8(uid.text());
        // no version, get the version from the index
        Searcher searcher = indexingSearcher.get();
        if (searcher == null) {
            Searcher tmpSearcher = searcher();
            if (!indexingSearcher.compareAndSet(null, tmpSearcher)) {
                // someone beat us to it, release the one we got
                tmpSearcher.release();
            }
            // it must have a value, since someone set it already, and this code gets called
            // under a readLock, while the indexSearcher gets nullified on a writeLock
            searcher = indexingSearcher.get();
        }
        for (IndexReader reader : searcher.searcher().subReaders()) {
            BloomFilter filter = bloomCache.filter(reader, UidFieldMapper.NAME, asyncLoadBloomFilter);
            // we know that its not there...
            if (!filter.isPresent(utf8.result, 0, utf8.length)) {
                continue;
            }
            long version = UidField.loadVersion(reader, uid);
            // either -2 (its there, but no version associated), or an actual version
            if (version != -1) {
                return version;
            }
        }
        return -1;
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
            IndexWriterConfig config = new IndexWriterConfig(Lucene.VERSION, analysisService.defaultIndexAnalyzer());
            config.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            config.setIndexDeletionPolicy(deletionPolicy);
            config.setMergeScheduler(mergeScheduler.newMergeScheduler());
            config.setMergePolicy(mergePolicyProvider.newMergePolicy());
            config.setSimilarity(similarityService.defaultIndexSimilarity());
            config.setRAMBufferSizeMB(indexingBufferSize.mbFrac());
            config.setTermIndexInterval(termIndexInterval);
            config.setReaderTermsIndexDivisor(termIndexDivisor);
            config.setMaxThreadStates(indexConcurrency);

            indexWriter = new IndexWriter(store.directory(), config);

            // we commit here on a fresh index since we want to have a commit point to support snapshotting
            if (create) {
                indexWriter.commit();
            }

        } catch (IOException e) {
            safeClose(indexWriter);
            throw e;
        }
        return indexWriter;
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override public void onRefreshSettings(Settings settings) {
            int termIndexInterval = settings.getAsInt("index.term_index_interval", RobinEngine.this.termIndexInterval);
            int termIndexDivisor = settings.getAsInt("index.term_index_divisor", RobinEngine.this.termIndexDivisor); // IndexReader#DEFAULT_TERMS_INDEX_DIVISOR
            int indexConcurrency = settings.getAsInt("index.index_concurrency", RobinEngine.this.indexConcurrency);
            boolean requiresFlushing = false;
            if (termIndexInterval != RobinEngine.this.termIndexInterval || termIndexDivisor != RobinEngine.this.termIndexDivisor) {
                rwl.readLock().lock();
                try {
                    if (termIndexInterval != RobinEngine.this.termIndexInterval) {
                        logger.info("updating index.term_index_interval from [{}] to [{}]", RobinEngine.this.termIndexInterval, termIndexInterval);
                        RobinEngine.this.termIndexInterval = termIndexInterval;
                        indexWriter.getConfig().setTermIndexInterval(termIndexInterval);
                    }
                    if (termIndexDivisor != RobinEngine.this.termIndexDivisor) {
                        logger.info("updating index.term_index_divisor from [{}] to [{}]", RobinEngine.this.termIndexDivisor, termIndexDivisor);
                        RobinEngine.this.termIndexDivisor = termIndexDivisor;
                        indexWriter.getConfig().setReaderTermsIndexDivisor(termIndexDivisor);
                        // we want to apply this right now for readers, even "current" ones
                        requiresFlushing = true;
                    }
                    if (indexConcurrency != RobinEngine.this.indexConcurrency) {
                        logger.info("updating index.index_concurrency from [{}] to [{}]", RobinEngine.this.indexConcurrency, indexConcurrency);
                        RobinEngine.this.indexConcurrency = indexConcurrency;
                        // we have to flush in this case, since it only applies on a new index writer
                        requiresFlushing = true;
                    }
                } finally {
                    rwl.readLock().unlock();
                }
                if (requiresFlushing) {
                    flush(new Flush().full(true));
                }
            }
        }
    }

    private AcquirableResource<ReaderSearcherHolder> buildNrtResource(IndexWriter indexWriter) throws IOException {
        IndexReader indexReader = IndexReader.open(indexWriter, true);
        ExtendedIndexSearcher indexSearcher = new ExtendedIndexSearcher(indexReader);
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

        @Override public ExtendedIndexSearcher searcher() {
            return nrtHolder.resource().searcher();
        }

        @Override public boolean release() throws ElasticSearchException {
            nrtHolder.release();
            return true;
        }
    }

    static class VersionValue {
        private final long version;
        private final boolean delete;
        private final long time;

        VersionValue(long version, boolean delete, long time) {
            this.version = version;
            this.delete = delete;
            this.time = time;
        }

        public long time() {
            return this.time;
        }

        public long version() {
            return version;
        }

        public boolean delete() {
            return delete;
        }
    }
}

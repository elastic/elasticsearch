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
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.IndexWriters;
import org.elasticsearch.common.lucene.ReaderSearcherHolder;
import org.elasticsearch.common.lucene.search.ExtendedIndexSearcher;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.resource.AcquirableResource;
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
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    private final boolean compoundFormat;

    private final int termIndexInterval;

    private final int termIndexDivisor;

    private final TimeValue refreshInterval;

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

    private final BloomCache bloomCache;

    private final boolean asyncLoadBloomFilter;

    // no need for volatile, its always used under a lock
    private IndexWriter indexWriter;

    private volatile AcquirableResource<ReaderSearcherHolder> nrtResource;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile int disableFlushCounter = 0;

    private volatile Searcher postFlushSearcher;

    private final AtomicBoolean flushing = new AtomicBoolean();

    private final ConcurrentMap<String, VersionValue> versionMap;

    private final Object[] dirtyLocks;

    @Inject public RobinEngine(ShardId shardId, @IndexSettings Settings indexSettings, Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                               MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                               AnalysisService analysisService, SimilarityService similarityService,
                               BloomCache bloomCache) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.indexingBufferSize = componentSettings.getAsBytesSize("index_buffer_size", new ByteSizeValue(64, ByteSizeUnit.MB)); // not really important, as it is set by the IndexingMemory manager
        this.termIndexInterval = indexSettings.getAsInt("index.term_index_interval", IndexWriter.DEFAULT_TERM_INDEX_INTERVAL);
        this.termIndexDivisor = indexSettings.getAsInt("index.term_index_divisor", 1); // IndexReader#DEFAULT_TERMS_INDEX_DIVISOR
        this.compoundFormat = indexSettings.getAsBoolean("index.compound_format", indexSettings.getAsBoolean("index.merge.policy.use_compound_file", store == null ? false : store.suggestUseCompoundFile()));
        this.refreshInterval = componentSettings.getAsTime("refresh_interval", indexSettings.getAsTime("index.refresh_interval", timeValueSeconds(1)));
        this.asyncLoadBloomFilter = componentSettings.getAsBoolean("async_load_bloom", true); // Here for testing, should always be true

        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.bloomCache = bloomCache;

        this.versionMap = new ConcurrentHashMap<String, VersionValue>(1000);
        this.dirtyLocks = new Object[componentSettings.getAsInt("concurrency", 10000)];
        for (int i = 0; i < dirtyLocks.length; i++) {
            dirtyLocks[i] = new Object();
        }
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
                if (postFlushSearcher != null) {
                    postFlushSearcher.release();
                }
                postFlushSearcher = searcher();
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
                            innerCreate(create, writer);
                            break;
                        case INDEX:
                            Index index = (Index) op;
                            innerIndex(index, writer);
                            break;
                        case DELETE:
                            Delete delete = (Delete) op;
                            innerDelete(delete, writer);
                            break;
                    }
                } catch (Exception e) {
                    if (failures == null) {
                        failures = new EngineException[bulk.ops().length];
                    }
                    switch (op.opType()) {
                        case CREATE:
                            if (e instanceof EngineException) {
                                failures[i] = (EngineException) e;
                            } else {
                                failures[i] = new CreateFailedEngineException(shardId, (Create) op, e);
                            }
                            break;
                        case INDEX:
                            if (e instanceof EngineException) {
                                failures[i] = (EngineException) e;
                            } else {
                                failures[i] = new IndexFailedEngineException(shardId, (Index) op, e);
                            }
                            break;
                        case DELETE:
                            if (e instanceof EngineException) {
                                failures[i] = (EngineException) e;
                            } else {
                                failures[i] = new DeleteFailedEngineException(shardId, (Delete) op, e);
                            }
                            break;
                    }
                }
            }
            dirty = true;
            if (bulk.refresh()) {
                try {
                    refresh(new Refresh(false));
                } catch (Exception e) {
                    //ignore
                }
            }
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
            innerCreate(create, writer);
            dirty = true;
            if (create.refresh()) {
                refresh(new Refresh(false));
            }
        } catch (IOException e) {
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
                    versionMap.put(create.uid().text(), new VersionValue(create.version(), false));
                }
                uidField.version(create.version());
                writer.addDocument(create.doc(), create.analyzer());
                translog.add(new Translog.Create(create));
            } else {
                long expectedVersion = create.version();
                long currentVersion;
                VersionValue versionValue = versionMap.get(create.uid().text());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(create.uid());
                } else {
                    currentVersion = versionValue.version();
                }

                // same logic as index
                long updatedVersion;
                if (create.origin() == Operation.Origin.PRIMARY) {
                    if (expectedVersion != 0 && currentVersion != -2) { // -2 means we don't have a version, so ignore...
                        // an explicit version is provided, see if there is a conflict
                        // if the current version is -1, means we did not find anything, and
                        // a version is provided, so we do expect to find a doc under that version
                        if (currentVersion == -1) {
                            throw new VersionConflictEngineException(shardId, create.type(), create.id(), -1, expectedVersion);
                        } else if (expectedVersion != currentVersion) {
                            throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
                        }
                    }
                    updatedVersion = currentVersion < 0 ? 1 : currentVersion + 1;
                } else { // if (index.origin() == Operation.Origin.REPLICA) {
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

                versionMap.put(create.uid().text(), new VersionValue(updatedVersion, false));
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
                throw new EngineClosedException(shardId);
            }

            innerIndex(index, writer);
            dirty = true;
            if (index.refresh()) {
                refresh(new Refresh(false));
            }
        } catch (IOException e) {
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
                    versionMap.put(index.uid().text(), new VersionValue(index.version(), false));
                }
                uidField.version(index.version());
                writer.updateDocument(index.uid(), index.doc(), index.analyzer());
                translog.add(new Translog.Index(index));
            } else {
                long expectedVersion = index.version();
                long currentVersion;
                VersionValue versionValue = versionMap.get(index.uid().text());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(index.uid());
                } else {
                    currentVersion = versionValue.version();
                }

                long updatedVersion;
                if (index.origin() == Operation.Origin.PRIMARY) {
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
                } else { // if (index.origin() == Operation.Origin.REPLICA) {
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

                versionMap.put(index.uid().text(), new VersionValue(updatedVersion, false));
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
                throw new EngineClosedException(shardId);
            }
            innerDelete(delete, writer);
            dirty = true;
            if (delete.refresh()) {
                refresh(new Refresh(false));
            }
        } catch (IOException e) {
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
                    versionMap.put(delete.uid().text(), new VersionValue(delete.version(), true));
                }

                writer.deleteDocuments(delete.uid());
                translog.add(new Translog.Delete(delete));
            } else {
                long currentVersion;
                VersionValue versionValue = versionMap.get(delete.uid().text());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(delete.uid());
                } else {
                    currentVersion = versionValue.version();
                }

                long updatedVersion;
                if (delete.origin() == Operation.Origin.PRIMARY) {
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
                    versionMap.put(delete.uid().text(), new VersionValue(updatedVersion, true));
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
                            ExtendedIndexSearcher indexSearcher = new ExtendedIndexSearcher(newReader);
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
        // don't allow for concurrent flush operations...
        if (!flushing.compareAndSet(false, true)) {
            throw new FlushNotAllowedEngineException(shardId, "Already flushing...");
        }

        // We can't do prepareCommit here, since we rely on the the segment version for the translog version

        // call maybeMerge outside of the write lock since it gets called anyhow within commit/refresh
        // and we want not to suffer this cost within the write lock
        // only do it if we don't have an async merging going on, otherwise, we know that we won't do any
        // merge operation
        try {
            if (indexWriter.getMergePolicy() instanceof EnableMergePolicy) {
                ((EnableMergePolicy) indexWriter.getMergePolicy()).enableMerge();
            }
            indexWriter.maybeMerge();
        } catch (Exception e) {
            flushing.set(false);
            throw new FlushFailedEngineException(shardId, e);
        } finally {
            // don't allow merge when committing under write lock
            if (indexWriter.getMergePolicy() instanceof EnableMergePolicy) {
                ((EnableMergePolicy) indexWriter.getMergePolicy()).disableMerge();
            }
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
            versionMap.clear();
            dirty = true; // force a refresh
            // we need to do a refresh here so we sync versioning support
            refresh(new Refresh(true));
            if (postFlushSearcher != null) {
                postFlushSearcher.release();
            }
            // only need to load for this flush version searcher, since we keep a map for all
            // the changes since the previous flush in memory
            postFlushSearcher = searcher();
        } finally {
            rwl.writeLock().unlock();
            flushing.set(false);
        }
        // we refresh anyhow before...
//        if (flush.refresh()) {
//            refresh(new Refresh(false));
//        }
    }

    @Override public void optimize(Optimize optimize) throws EngineException {
        if (optimizeMutex.compareAndSet(false, true)) {
            rwl.readLock().lock();
            try {
                if (indexWriter == null) {
                    throw new EngineClosedException(shardId);
                }
                if (indexWriter.getMergePolicy() instanceof EnableMergePolicy) {
                    ((EnableMergePolicy) indexWriter.getMergePolicy()).enableMerge();
                }
                if (optimize.onlyExpungeDeletes()) {
                    indexWriter.expungeDeletes(false);
                } else if (optimize.maxNumSegments() <= 0) {
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.optimize(optimize.maxNumSegments(), false);
                }
            } catch (Exception e) {
                throw new OptimizeFailedEngineException(shardId, e);
            } finally {
                if (indexWriter != null && indexWriter.getMergePolicy() instanceof EnableMergePolicy) {
                    ((EnableMergePolicy) indexWriter.getMergePolicy()).disableMerge();
                }
                rwl.readLock().unlock();
                optimizeMutex.set(false);
            }
        }
        // wait for the merges outside of the read lock
        if (optimize.waitForMerge()) {
            indexWriter.waitForMerges();
        }
        // once we did the optimization, we are "dirty" since we removed deletes potentially which
        // affects TermEnum
        dirty = true;
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
        this.versionMap.clear();
        try {
            if (postFlushSearcher != null) {
                postFlushSearcher.release();
                postFlushSearcher = null;
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
        } catch (IOException e) {
            logger.debug("failed to rollback writer on close", e);
        } finally {
            indexWriter = null;
            rwl.writeLock().unlock();
        }
    }

    private Object dirtyLock(Term uid) {
        return dirtyLocks[Math.abs(uid.hashCode()) % dirtyLocks.length];
    }

    private long loadCurrentVersionFromIndex(Term uid) {
        UnicodeUtil.UTF8Result utf8 = Unicode.fromStringAsUtf8(uid.text());
        // no version, get the version from the index
        Searcher searcher = postFlushSearcher;
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
            indexWriter = new IndexWriter(store.directory(),
                    analysisService.defaultIndexAnalyzer(), create, deletionPolicy, IndexWriter.MaxFieldLength.UNLIMITED);
            indexWriter.setMergeScheduler(mergeScheduler.newMergeScheduler());
            indexWriter.setMergePolicy(mergePolicyProvider.newMergePolicy(indexWriter));
            indexWriter.setSimilarity(similarityService.defaultIndexSimilarity());
            indexWriter.setRAMBufferSizeMB(indexingBufferSize.mbFrac());
            indexWriter.setTermIndexInterval(termIndexInterval);
            indexWriter.setReaderTermsIndexDivisor(termIndexDivisor);
            indexWriter.setUseCompoundFile(compoundFormat);
        } catch (IOException e) {
            safeClose(indexWriter);
            throw e;
        }
        return indexWriter;
    }

    private AcquirableResource<ReaderSearcherHolder> buildNrtResource(IndexWriter indexWriter) throws IOException {
        IndexReader indexReader = indexWriter.getReader();
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
        private long version;
        private final boolean delete;

        VersionValue(long version, boolean delete) {
            this.version = version;
            this.delete = delete;
        }

        public long version() {
            return version;
        }

        public boolean delete() {
            return delete;
        }
    }
}

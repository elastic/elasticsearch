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

import com.google.common.collect.Lists;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.merge.policy.ElasticsearchMergePolicy;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.search.nested.IncludeNestedDocsQuery;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class InternalEngine extends Engine {

    private final FailEngineOnMergeFailure mergeSchedulerFailureListener;
    private final MergeSchedulerListener mergeSchedulerListener;

    /** When we last pruned expired tombstones from versionMap.deletes: */
    private volatile long lastDeleteVersionPruneTimeMSec;

    private final ShardIndexingService indexingService;
    @Nullable
    private final IndicesWarmer warmer;
    private final Translog translog;
    private final MergePolicyProvider mergePolicyProvider;
    private final MergeSchedulerProvider mergeScheduler;

    private final IndexWriter indexWriter;

    private final SearcherFactory searcherFactory;
    private final SearcherManager searcherManager;

    // we use flushNeeded here, since if there are no changes, then the commit won't write
    // will not really happen, and then the commitUserData and the new translog will not be reflected
    private volatile boolean flushNeeded = false;
    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();

    protected final FlushingRecoveryCounter onGoingRecoveries;
    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final LiveVersionMap versionMap;

    private final Object[] dirtyLocks;

    private final AtomicLong translogIdGenerator = new AtomicLong();
    private final AtomicBoolean versionMapRefreshPending = new AtomicBoolean();

    private SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;
    private volatile boolean possibleMergeNeeded;

    public InternalEngine(EngineConfig engineConfig) throws EngineException {
        super(engineConfig);
        this.versionMap = new LiveVersionMap();
        store.incRef();
        IndexWriter writer = null;
        SearcherManager manager = null;
        boolean success = false;
        try {
            try {
                upgrade3xSegments(store);
            } catch (IOException ex) {
                throw new EngineCreationFailureException(shardId, "failed to upgrade 3x segments", ex);
            }
            this.onGoingRecoveries = new FlushingRecoveryCounter(this, store, logger);
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().estimatedTimeInMillis();
            this.indexingService = engineConfig.getIndexingService();
            this.warmer = engineConfig.getWarmer();
            this.translog = engineConfig.getTranslog();
            this.mergePolicyProvider = engineConfig.getMergePolicyProvider();
            this.mergeScheduler = engineConfig.getMergeScheduler();
            this.dirtyLocks = new Object[engineConfig.getIndexConcurrency() * 50]; // we multiply it to have enough...
            for (int i = 0; i < dirtyLocks.length; i++) {
                dirtyLocks[i] = new Object();
            }

            throttle = new IndexThrottle();
            this.searcherFactory = new SearchFactory(engineConfig);
            try {
                writer = createWriter();
            } catch (IOException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            }
            indexWriter = writer;
            manager = createSearcherManager();
            this.searcherManager = manager;
            this.mergeSchedulerFailureListener = new FailEngineOnMergeFailure();
            this.mergeSchedulerListener = new MergeSchedulerListener();
            this.mergeScheduler.addListener(mergeSchedulerListener);
            this.mergeScheduler.addFailureListener(mergeSchedulerFailureListener);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, manager);
                versionMap.clear();
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
    }

    private SearcherManager createSearcherManager() throws EngineException {
        boolean success = false;
        SearcherManager searcherManager = null;
        try {
            try {
                // commit on a just opened writer will commit even if there are no changes done to it
                // we rely on that for the commit data translog id key
                final long translogId;
                if (Lucene.indexExists(store.directory())) {
                    Map<String, String> commitUserData = Lucene.readSegmentInfos(store.directory()).getUserData();
                    if (commitUserData.containsKey(Translog.TRANSLOG_ID_KEY)) {
                        translogId = Long.parseLong(commitUserData.get(Translog.TRANSLOG_ID_KEY));
                    } else {
                        translogId = System.currentTimeMillis();
                        indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                        indexWriter.commit();
                    }
                } else {
                    translogId = System.currentTimeMillis();
                    indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                    commitIndexWriter(indexWriter);
                }
                searcherManager = new SearcherManager(indexWriter, true, searcherFactory);
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                translog.newTranslog(translogId);
                versionMap.setManager(searcherManager);
                translogIdGenerator.set(translogId);
                success = true;
                return searcherManager;
            } catch (IOException e) {
                maybeFailEngine("start", e);
                try {
                    indexWriter.rollback();
                } catch (IOException e1) { // iw is closed below
                    e.addSuppressed(e1);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            if (success == false) { // release everything we created on a failure
                IOUtils.closeWhileHandlingException(searcherManager, indexWriter);
            }
        }
    }

    private void updateIndexWriterSettings() {
        try {
            final LiveIndexWriterConfig iwc = indexWriter.getConfig();
            iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().mbFrac());
            iwc.setUseCompoundFile(engineConfig.isCompoundOnFlush());
            iwc.setCheckIntegrityAtMerge(engineConfig.isChecksumOnMerge());
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    @Override
    public GetResult get(Get get) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (get.realtime()) {
                VersionValue versionValue = versionMap.getUnderLock(get.uid().bytes());
                if (versionValue != null) {
                    if (versionValue.delete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version(), get.version())) {
                        Uid uid = Uid.createUid(get.uid().text());
                        throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), versionValue.version(), get.version());
                    }
                    if (!get.loadSource()) {
                        return new GetResult(true, versionValue.version(), null);
                    }
                    Translog.Operation op = translog.read(versionValue.translogLocation());
                    if (op != null) {
                        return new GetResult(true, versionValue.version(), op.getSource());
                    }
                }
            }

            // no version, get the version from the index, we know that we refresh on flush
            return getFromSearcher(get);
        }
    }

    @Override
    public void create(Create create) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (create.origin() == Operation.Origin.RECOVERY) {
                // Don't throttle recovery operations
                innerCreate(create);
            } else {
                try (Releasable r = throttle.acquireThrottle()) {
                    innerCreate(create);
                }
            }
            flushNeeded = true;
        } catch (OutOfMemoryError | IllegalStateException | IOException t) {
            maybeFailEngine("create", t);
            throw new CreateFailedEngineException(shardId, create, t);
        }
        checkVersionMapRefresh();
    }

    private void innerCreate(Create create) throws IOException {
        if (engineConfig.isOptimizeAutoGenerateId() && create.autoGeneratedId() && !create.canHaveDuplicates()) {
            // We don't need to lock because this ID cannot be concurrently updated:
            innerCreateNoLock(create, Versions.NOT_FOUND, null);
        } else {
            synchronized (dirtyLock(create.uid())) {
                final long currentVersion;
                final VersionValue versionValue;
                versionValue = versionMap.getUnderLock(create.uid().bytes());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(create.uid());
                } else {
                    if (engineConfig.isEnableGcDeletes() && versionValue.delete() && (engineConfig.getThreadPool().estimatedTimeInMillis() - versionValue.time()) > engineConfig.getGcDeletesInMillis()) {
                        currentVersion = Versions.NOT_FOUND; // deleted, and GC
                    } else {
                        currentVersion = versionValue.version();
                    }
                }
                innerCreateNoLock(create, currentVersion, versionValue);
            }
        }
    }

    private void innerCreateNoLock(Create create, long currentVersion, VersionValue versionValue) throws IOException {

        // same logic as index
        long updatedVersion;
        long expectedVersion = create.version();
        if (create.versionType().isVersionConflictForWrites(currentVersion, expectedVersion)) {
            if (create.origin() == Operation.Origin.RECOVERY) {
                return;
            } else {
                throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
            }
        }
        updatedVersion = create.versionType().updateVersion(currentVersion, expectedVersion);

        // if the doc exists
        boolean doUpdate = false;
        if ((versionValue != null && versionValue.delete() == false) || (versionValue == null && currentVersion != Versions.NOT_FOUND)) {
            if (create.origin() == Operation.Origin.RECOVERY) {
                return;
            } else if (create.origin() == Operation.Origin.REPLICA) {
                // #7142: the primary already determined it's OK to index this document, and we confirmed above that the version doesn't
                // conflict, so we must also update here on the replica to remain consistent:
                doUpdate = true;
            } else if (create.origin() == Operation.Origin.PRIMARY && create.autoGeneratedId() && create.canHaveDuplicates() && currentVersion == 1 && create.version() == Versions.MATCH_ANY) {
                /**
                 * If bulk index request fails due to a disconnect, unavailable shard etc. then the request is
                 * retried before it actually fails. However, the documents might already be indexed.
                 * For autogenerated ids this means that a version conflict will be reported in the bulk request
                 * although the document was indexed properly.
                 * To avoid this we have to make sure that the index request is treated as an update and set updatedVersion to 1.
                 * See also discussion on https://github.com/elasticsearch/elasticsearch/pull/9125
                 */
                doUpdate = true;
                updatedVersion = 1;
            } else {
                // On primary, we throw DAEE if the _uid is already in the index with an older version:
                assert create.origin() == Operation.Origin.PRIMARY;
                throw new DocumentAlreadyExistsException(shardId, create.type(), create.id());
            }
        }

        create.updateVersion(updatedVersion);

        if (doUpdate) {
            if (create.docs().size() > 1) {
                indexWriter.updateDocuments(create.uid(), create.docs(), create.analyzer());
            } else {
                indexWriter.updateDocument(create.uid(), create.docs().get(0), create.analyzer());
            }
        } else {
            if (create.docs().size() > 1) {
                indexWriter.addDocuments(create.docs(), create.analyzer());
            } else {
                indexWriter.addDocument(create.docs().get(0), create.analyzer());
            }
        }
        Translog.Location translogLocation = translog.add(new Translog.Create(create));

        versionMap.putUnderLock(create.uid().bytes(), new VersionValue(updatedVersion, translogLocation));

        indexingService.postCreateUnderLock(create);
    }

    @Override
    public void index(Index index) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (index.origin() == Operation.Origin.RECOVERY) {
                // Don't throttle recovery operations
                innerIndex(index);
            } else {
                try (Releasable r = throttle.acquireThrottle()) {
                    innerIndex(index);
                }
            }
            flushNeeded = true;
        } catch (OutOfMemoryError | IllegalStateException | IOException t) {
            maybeFailEngine("index", t);
            throw new IndexFailedEngineException(shardId, index, t);
        }
        checkVersionMapRefresh();
    }

    /**
     * Forces a refresh if the versionMap is using too much RAM
     */
    private void checkVersionMapRefresh() {
        if (versionMap.ramBytesUsedForRefresh() > config().getVersionMapSize().bytes() && versionMapRefreshPending.getAndSet(true) == false) {
            try {
                if (isClosed.get()) {
                    // no point...
                    return;
                }
                // Now refresh to clear versionMap:
                engineConfig.getThreadPool().executor(ThreadPool.Names.REFRESH).execute(new Runnable() {
                    public void run() {
                        try {
                            refresh("version_table_full");
                        } catch (EngineClosedException ex) {
                            // ignore
                        }
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                // that is fine too.. we might be shutting down
            }
        }
    }

    private void innerIndex(Index index) throws IOException {
        synchronized (dirtyLock(index.uid())) {
            final long currentVersion;
            VersionValue versionValue = versionMap.getUnderLock(index.uid().bytes());
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(index.uid());
            } else {
                if (engineConfig.isEnableGcDeletes() && versionValue.delete() && (engineConfig.getThreadPool().estimatedTimeInMillis() - versionValue.time()) > engineConfig.getGcDeletesInMillis()) {
                    currentVersion = Versions.NOT_FOUND; // deleted, and GC
                } else {
                    currentVersion = versionValue.version();
                }
            }

            long updatedVersion;
            long expectedVersion = index.version();
            if (index.versionType().isVersionConflictForWrites(currentVersion, expectedVersion)) {
                if (index.origin() == Operation.Origin.RECOVERY) {
                    return;
                } else {
                    throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
                }
            }
            updatedVersion = index.versionType().updateVersion(currentVersion, expectedVersion);

            index.updateVersion(updatedVersion);
            if (currentVersion == Versions.NOT_FOUND) {
                // document does not exists, we can optimize for create
                index.created(true);
                if (index.docs().size() > 1) {
                    indexWriter.addDocuments(index.docs(), index.analyzer());
                } else {
                    indexWriter.addDocument(index.docs().get(0), index.analyzer());
                }
            } else {
                if (versionValue != null) {
                    index.created(versionValue.delete()); // we have a delete which is not GC'ed...
                }
                if (index.docs().size() > 1) {
                    indexWriter.updateDocuments(index.uid(), index.docs(), index.analyzer());
                } else {
                    indexWriter.updateDocument(index.uid(), index.docs().get(0), index.analyzer());
                }
            }
            Translog.Location translogLocation = translog.add(new Translog.Index(index));

            versionMap.putUnderLock(index.uid().bytes(), new VersionValue(updatedVersion, translogLocation));

            indexingService.postIndexUnderLock(index);
        }
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            // NOTE: we don't throttle this when merges fall behind because delete-by-id does not create new segments:
            innerDelete(delete);
            flushNeeded = true;
        } catch (OutOfMemoryError | IllegalStateException | IOException t) {
            maybeFailEngine("delete", t);
            throw new DeleteFailedEngineException(shardId, delete, t);
        }

        maybePruneDeletedTombstones();
        checkVersionMapRefresh();
    }

    private void maybePruneDeletedTombstones() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        if (engineConfig.isEnableGcDeletes() && engineConfig.getThreadPool().estimatedTimeInMillis() - lastDeleteVersionPruneTimeMSec > engineConfig.getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    private void innerDelete(Delete delete) throws IOException {
        synchronized (dirtyLock(delete.uid())) {
            final long currentVersion;
            VersionValue versionValue = versionMap.getUnderLock(delete.uid().bytes());
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(delete.uid());
            } else {
                if (engineConfig.isEnableGcDeletes() && versionValue.delete() && (engineConfig.getThreadPool().estimatedTimeInMillis() - versionValue.time()) > engineConfig.getGcDeletesInMillis()) {
                    currentVersion = Versions.NOT_FOUND; // deleted, and GC
                } else {
                    currentVersion = versionValue.version();
                }
            }

            long updatedVersion;
            long expectedVersion = delete.version();
            if (delete.versionType().isVersionConflictForWrites(currentVersion, expectedVersion)) {
                if (delete.origin() == Operation.Origin.RECOVERY) {
                    return;
                } else {
                    throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion, expectedVersion);
                }
            }
            updatedVersion = delete.versionType().updateVersion(currentVersion, expectedVersion);
            final boolean found;
            if (currentVersion == Versions.NOT_FOUND) {
                // doc does not exist and no prior deletes
                found = false;
            } else if (versionValue != null && versionValue.delete()) {
                // a "delete on delete", in this case, we still increment the version, log it, and return that version
                found = false;
            } else {
                // we deleted a currently existing document
                indexWriter.deleteDocuments(delete.uid());
                found = true;
            }

            delete.updateVersion(updatedVersion, found);
            Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
            versionMap.putUnderLock(delete.uid().bytes(), new DeleteVersionValue(updatedVersion, engineConfig.getThreadPool().estimatedTimeInMillis(), translogLocation));

            indexingService.postDeleteUnderLock(delete);
        }
    }

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (delete.origin() == Operation.Origin.RECOVERY) {
                // Don't throttle recovery operations
                innerDelete(delete);
            } else {
                try (Releasable r = throttle.acquireThrottle()) {
                    innerDelete(delete);
                }
            }
        }
    }

    private void innerDelete(DeleteByQuery delete) throws EngineException {
        try {
            Query query;
            if (delete.nested() && delete.aliasFilter() != null) {
                query = new IncludeNestedDocsQuery(new XFilteredQuery(delete.query(), delete.aliasFilter()), delete.parentFilter());
            } else if (delete.nested()) {
                query = new IncludeNestedDocsQuery(delete.query(), delete.parentFilter());
            } else if (delete.aliasFilter() != null) {
                query = new XFilteredQuery(delete.query(), delete.aliasFilter());
            } else {
                query = delete.query();
            }

            indexWriter.deleteDocuments(query);
            translog.add(new Translog.DeleteByQuery(delete));
            flushNeeded = true;
        } catch (Throwable t) {
            maybeFailEngine("delete_by_query", t);
            throw new DeleteByQueryFailedEngineException(shardId, delete, t);
        }

        // TODO: This is heavy, since we refresh, but we must do this because we don't know which documents were in fact deleted (i.e., our
        // versionMap isn't updated), so we must force a cutover to a new reader to "see" the deletions:
        refresh("delete_by_query");
    }

    @Override
    public void refresh(String source) throws EngineException {
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            updateIndexWriterSettings();
            searcherManager.maybeRefreshBlocking();
        } catch (AlreadyClosedException e) {
            ensureOpen();
            maybeFailEngine("refresh", e);
        } catch (EngineClosedException e) {
            throw e;
        } catch (Throwable t) {
            failEngine("refresh failed", t);
            throw new RefreshFailedEngineException(shardId, t);
        }

        // TODO: maybe we should just put a scheduled job in threadPool?
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        maybePruneDeletedTombstones();
        versionMapRefreshPending.set(false);
    }

    @Override
    public void flush() throws EngineException {
        flush(true, false, false);
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        flush(true, force, waitIfOngoing);
    }

    private void flush(boolean commitTranslog, boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        if (commitTranslog) {
            // check outside the lock as well so we can check without blocking on the write lock
            if (onGoingRecoveries.get() > 0) {
                throw new FlushNotAllowedEngineException(shardId, "recovery is in progress, flush with committing translog is not allowed");
            }
        }
        /*
         * Unfortunately the lock order is important here. We have to acquire the readlock fist otherwise
         * if we are flushing at the end of the recovery while holding the write lock we can deadlock if:
         *  Thread 1: flushes via API and gets the flush lock but blocks on the readlock since Thread 2 has the writeLock
         *  Thread 2: flushes at the end of the recovery holding the writeLock and blocks on the flushLock owned by Thread 1
         */
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            updateIndexWriterSettings();
            if (flushLock.tryLock() == false) {
                // if we can't get the lock right away we block if needed otherwise barf
                if (waitIfOngoing) {
                    logger.trace("waiting fore in-flight flush to finish");
                    flushLock.lock();
                    logger.trace("acquired flush lock after blocking");
                } else {
                    throw new FlushNotAllowedEngineException(shardId, "already flushing...");
                }
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                if (commitTranslog) {
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }

                    if (flushNeeded || force) {
                        flushNeeded = false;
                        try {
                            long translogId = translogIdGenerator.incrementAndGet();
                            translog.newTransientTranslog(translogId);
                            indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                            commitIndexWriter(indexWriter);
                            // we need to refresh in order to clear older version values
                            refresh("version_table_flush");
                            // we need to move transient to current only after we refresh
                            // so items added to current will still be around for realtime get
                            // when tans overrides it
                            translog.makeTransientCurrent();

                        } catch (Throwable e) {
                            translog.revertTransient();
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                } else {
                    // note, its ok to just commit without cleaning the translog, its perfectly fine to replay a
                    // translog on an index that was opened on a committed point in time that is "in the future"
                    // of that translog
                    // we allow to *just* commit if there is an ongoing recovery happening...
                    // its ok to use this, only a flush will cause a new translogId, and we are locked here from
                    // other flushes use flushLock
                    try {
                        long translogId = translog.currentId();
                        indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                        commitIndexWriter(indexWriter);
                    } catch (Throwable e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }

                }
                /*
                 * we have to inc-ref the store here since if the engine is closed by a tragic event
                 * we don't acquire the write lock and wait until we have exclusive access. This might also
                 * dec the store reference which can essentially close the store and unless we can inc the reference
                 * we can't use it.
                 */
                store.incRef();
                try {
                    // reread the last committed segment infos
                    lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                } catch (Throwable e) {
                    if (isClosed.get() == false) {
                        logger.warn("failed to read latest segment infos on flush", e);
                        if (Lucene.isCorruptionException(e)) {
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                } finally {
                    store.decRef();
                }
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } finally {
                flushLock.unlock();
            }
        }
        // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
        // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
        if (engineConfig.isEnableGcDeletes()) {
            pruneDeletedTombstones();
        }
    }

    private void pruneDeletedTombstones() {
        long timeMSec = engineConfig.getThreadPool().estimatedTimeInMillis();

        // TODO: not good that we reach into LiveVersionMap here; can we move this inside VersionMap instead?  problem is the dirtyLock...

        // we only need to prune the deletes map; the current/old version maps are cleared on refresh:
        for (Map.Entry<BytesRef, VersionValue> entry : versionMap.getAllTombstones()) {
            BytesRef uid = entry.getKey();
            synchronized (dirtyLock(uid)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?

                // Must re-get it here, vs using entry.getValue(), in case the uid was indexed/deleted since we pulled the iterator:
                VersionValue versionValue = versionMap.getTombstoneUnderLock(uid);
                if (versionValue != null) {
                    if (timeMSec - versionValue.time() > engineConfig.getGcDeletesInMillis()) {
                        versionMap.removeTombstoneUnderLock(uid);
                    }
                }
            }
        }

        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    @Override
    public boolean possibleMergeNeeded() {
        IndexWriter writer = this.indexWriter;
        if (writer == null) {
            return false;
        }
        // a merge scheduler might bail without going through all its pending merges
        // so make sure we also check if there are pending merges
        return this.possibleMergeNeeded || writer.hasPendingMerges();
    }

    @Override
    public void maybeMerge() throws EngineException {
        if (!possibleMergeNeeded()) {
            return;
        }
        possibleMergeNeeded = false;
        try (ReleasableLock _ = readLock.acquire()) {
            indexWriter.maybeMerge();
        } catch (Throwable t) {
            maybeFailEngine("maybe_merge", t);
            throw new OptimizeFailedEngineException(shardId, t);
        }
    }

    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, final boolean upgrade) throws EngineException {
        /*
         * We do NOT acquire the readlock here since we are waiting on the merges to finish
         * that's fine since the IW.rollback should stop all the threads and trigger an IOException
         * causing us to fail the forceMerge
         *
         * The way we implement upgrades is a bit hackish in the sense that we set an instance
         * variable and that this setting will thus apply to the next forced merge that will be run.
         * This is ok because (1) this is the only place we call forceMerge, (2) we have a single
         * thread for optimize, and the 'optimizeLock' guarding this code, and (3) ConcurrentMergeScheduler
         * syncs calls to findForcedMerges.
         */
        assert indexWriter.getConfig().getMergePolicy() instanceof ElasticsearchMergePolicy : "MergePolicy is " + indexWriter.getConfig().getMergePolicy().getClass().getName();
        ElasticsearchMergePolicy mp = (ElasticsearchMergePolicy) indexWriter.getConfig().getMergePolicy();
        optimizeLock.lock();
        try {
            ensureOpen();
            if (upgrade) {
                logger.info("starting segment upgrade");
                mp.setUpgradeInProgress(true);
            }
            store.incRef(); // increment the ref just to ensure nobody closes the store while we optimize
            try {
                if (onlyExpungeDeletes) {
                    assert upgrade == false;
                    indexWriter.forceMergeDeletes(true /* blocks and waits for merges*/);
                } else if (maxNumSegments <= 0) {
                    assert upgrade == false;
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.forceMerge(maxNumSegments, true /* blocks and waits for merges*/);
                }
                if (flush) {
                    flush(true, true, true);
                }
                if (upgrade) {
                    logger.info("finished segment upgrade");
                }
            } finally {
                store.decRef();
            }
        } catch (Throwable t) {
            OptimizeFailedEngineException ex = new OptimizeFailedEngineException(shardId, t);
            maybeFailEngine("force merge", ex);
            throw ex;
        } finally {
            try {
                mp.setUpgradeInProgress(false); // reset it just to make sure we reset it in a case of an error
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    @Override
    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        flush(false, false, true);
        try (ReleasableLock lock = readLock.acquire()) {
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
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
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

        try {
            recoveryHandler.phase1(phase1Snapshot);
        } catch (Throwable e) {
            maybeFailEngine("recovery phase 1", e);
            // close the snapshot first to release the reference to the translog file, so a flush post recovery can delete it
            Releasables.closeWhileHandlingException(phase1Snapshot, onGoingRecoveries);
            throw new RecoveryEngineException(shardId, 1, "Execution failed", wrapIfClosed(e));
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Throwable e) {
            maybeFailEngine("snapshot recovery", e);
            Releasables.closeWhileHandlingException(phase1Snapshot, onGoingRecoveries);
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", wrapIfClosed(e));
        }
        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Throwable e) {
            maybeFailEngine("recovery phase 2", e);
            // close the snapshots first to release the reference to the translog file, so a flush post recovery can delete it
            Releasables.closeWhileHandlingException(phase1Snapshot, phase2Snapshot, onGoingRecoveries);
            throw new RecoveryEngineException(shardId, 2, "Execution failed", wrapIfClosed(e));
        }

        writeLock.acquire();
        Translog.Snapshot phase3Snapshot = null;
        boolean success = false;
        try {
            ensureOpen();
            phase3Snapshot = translog.snapshot(phase2Snapshot);
            recoveryHandler.phase3(phase3Snapshot);
            success = true;
        } catch (Throwable e) {
            maybeFailEngine("recovery phase 3", e);
            throw new RecoveryEngineException(shardId, 3, "Execution failed", wrapIfClosed(e));
        } finally {
            // close the snapshots first to release the reference to the translog file, so a flush post recovery can delete it
            Releasables.close(success, phase1Snapshot, phase2Snapshot, phase3Snapshot,
                    onGoingRecoveries, writeLock); // hmm why can't we use try-with here?
        }
    }

    @Override
    protected boolean maybeFailEngine(String source, Throwable t) {
        boolean shouldFail = super.maybeFailEngine(source, t);
        if (shouldFail) {
            return true;
        }

        // Check for AlreadyClosedException
        if (t instanceof AlreadyClosedException) {
            // if we are already closed due to some tragic exception
            // we need to fail the engine. it might have already been failed before
            // but we are double-checking it's failed and closed
            if (isOpen(indexWriter) == false && getTragicException(indexWriter) != null) {
                failEngine("already closed by tragic event", getTragicException(indexWriter));
            }
            return true;
        } else if (t != null && isOpen(indexWriter) == false && getTragicException(indexWriter) == t) {
            // this spot on - we are handling the tragic event exception here so we have to fail the engine
            // right away
            failEngine(source, t);
            return true;
        }
        return false;
    }

    // lucene 5.0 has a getter for this on the IW.. this is just a workaround
    private boolean isOpen(IndexWriter writer) {
        try {
            writer.ramBytesUsed(); // with throw ACE if closed
            return true;
        } catch (AlreadyClosedException ex) {
            return false;
        }
    }

    // lucene 5.0 has a getter for this on the IW.. this is just a workaround
    private Throwable getTragicException(IndexWriter writer) {
        try {
            writer.ramBytesUsed(); // with throw ACE if closed
            return null;
        } catch (AlreadyClosedException ex) {
            return ex.getCause();
        }
    }

    @Override
    protected final void writerSegmentStats(SegmentsStats stats) {
        stats.addVersionMapMemoryInBytes(versionMap.ramBytesUsed());
        stats.addIndexWriterMemoryInBytes(indexWriter.ramBytesUsed());
        stats.addIndexWriterMaxMemoryInBytes((long) (indexWriter.getConfig().getRAMBufferSizeMB() * 1024 * 1024));
    }

    @Override
    public List<Segment> segments() {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos);

            // fill in the merges flag
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }
            return Arrays.asList(segmentsArr);
        }
    }


    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    protected final void closeNoLock(String reason) throws ElasticsearchException {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                logger.trace("close searcherManager");
                try {
                    IOUtils.close(searcherManager);
                } catch (Throwable t) {
                    logger.warn("Failed to close SearcherManager", t);
                }
                // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
                logger.trace("rollback indexWriter");
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException e) {
                    // ignore
                }
                logger.trace("rollback indexWriter done");
            } catch (Throwable e) {
                logger.warn("failed to rollback writer on close", e);
            } finally {
                store.decRef();
                this.mergeScheduler.removeListener(mergeSchedulerListener);
                this.mergeScheduler.removeFailureListener(mergeSchedulerFailureListener);
                logger.debug("engine closed [{}]", reason);
            }
        }
    }

    @Override
    protected SearcherManager getSearcherManager() {
        return searcherManager;
    }

    private Object dirtyLock(BytesRef uid) {
        int hash = DjbHashFunction.DJB_HASH(uid.bytes, uid.offset, uid.length);
        return dirtyLocks[MathUtils.mod(hash, dirtyLocks.length)];
    }

    private Object dirtyLock(Term uid) {
        return dirtyLock(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        try (final Searcher searcher = acquireSearcher("load_version")) {
            return Versions.loadVersion(searcher.reader(), uid);
        }
    }

    private IndexWriter createWriter() throws IOException {
        try {
            boolean create = !Lucene.indexExists(store.directory());
            final IndexWriterConfig iwc = new IndexWriterConfig(Lucene.VERSION, engineConfig.getAnalyzer());
            iwc.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            iwc.setIndexDeletionPolicy(deletionPolicy);
            iwc.setInfoStream(new LoggerInfoStream(logger));
            iwc.setMergeScheduler(mergeScheduler.newMergeScheduler());
            MergePolicy mergePolicy = mergePolicyProvider.getMergePolicy();
            // Give us the opportunity to upgrade old segments while performing
            // background merges
            mergePolicy = new ElasticsearchMergePolicy(mergePolicy);
            iwc.setMergePolicy(mergePolicy);
            iwc.setSimilarity(engineConfig.getSimilarity());
            iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().mbFrac());
            iwc.setMaxThreadStates(engineConfig.getIndexConcurrency());
            iwc.setCodec(engineConfig.getCodec());
            /* We set this timeout to a highish value to work around
             * the default poll interval in the Lucene lock that is
             * 1000ms by default. We might need to poll multiple times
             * here but with 1s poll this is only executed twice at most
             * in combination with the default writelock timeout*/
            iwc.setWriteLockTimeout(5000);
            iwc.setUseCompoundFile(this.engineConfig.isCompoundOnFlush());
            iwc.setCheckIntegrityAtMerge(engineConfig.isChecksumOnMerge());
            // Warm-up hook for newly-merged segments. Warming up segments here is better since it will be performed at the end
            // of the merge operation and won't slow down _refresh
            iwc.setMergedSegmentWarmer(new IndexReaderWarmer() {
                @Override
                public void warm(AtomicReader reader) throws IOException {
                    try {
                        assert isMergedSegment(reader);
                        if (warmer != null) {
                            final Engine.Searcher searcher = new Searcher("warmer", new IndexSearcher(reader));
                            final IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId, searcher);
                            warmer.warmNewReaders(context);
                        }
                    } catch (Throwable t) {
                        // Don't fail a merge if the warm-up failed
                        if (isClosed.get() == false) {
                            logger.warn("Warm-up failed", t);
                        }
                        if (t instanceof Error) {
                            // assertion/out-of-memory error, don't ignore those
                            throw (Error) t;
                        }
                    }
                }
            });
            return new IndexWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            boolean isLocked = IndexWriter.isLocked(store.directory());
            logger.warn("Could not lock IndexWriter isLocked [{}]", ex, isLocked);
            throw ex;
        }
    }

    protected void upgrade3xSegments(Store store) throws IOException {
        store.incRef();
        try {
            if (Lucene.upgradeLucene3xSegmentsMetadata(store.directory())) {
                logger.debug("upgraded pending 3.x segments on startup");
            } else {
                logger.debug("no 3.x segments needed upgrading");
            }
        } finally {
            store.decRef();
        }
    }

    /** Extended SearcherFactory that warms the segments if needed when acquiring a new searcher */
    class SearchFactory extends EngineSearcherFactory {

        SearchFactory(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        public IndexSearcher newSearcher(IndexReader reader) throws IOException {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(engineConfig.getSimilarity());
            if (warmer != null) {
                // we need to pass a custom searcher that does not release anything on Engine.Search Release,
                // we will release explicitly
                IndexSearcher newSearcher = null;
                boolean closeNewSearcher = false;
                try {
                    if (searcherManager == null) {
                        // we are starting up - no writer active so we can't acquire a searcher.
                        newSearcher = searcher;
                    } else {
                        try (final Searcher currentSearcher = acquireSearcher("search_factory")) {
                            // figure out the newSearcher, with only the new readers that are relevant for us
                            List<IndexReader> readers = Lists.newArrayList();
                            for (AtomicReaderContext newReaderContext : searcher.getIndexReader().leaves()) {
                                if (isMergedSegment(newReaderContext.reader())) {
                                    // merged segments are already handled by IndexWriterConfig.setMergedSegmentWarmer
                                    continue;
                                }
                                boolean found = false;
                                for (AtomicReaderContext currentReaderContext : currentSearcher.reader().leaves()) {
                                    if (currentReaderContext.reader().getCoreCacheKey().equals(newReaderContext.reader().getCoreCacheKey())) {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    readers.add(newReaderContext.reader());
                                }
                            }
                            if (!readers.isEmpty()) {
                                // we don't want to close the inner readers, just increase ref on them
                                newSearcher = new IndexSearcher(new MultiReader(readers.toArray(new IndexReader[readers.size()]), false));
                                closeNewSearcher = true;
                            }
                        }
                    }

                    if (newSearcher != null) {
                        IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId, new Searcher("warmer", newSearcher));
                        warmer.warmNewReaders(context);
                    }
                    warmer.warmTopReader(new IndicesWarmer.WarmerContext(shardId, new Searcher("warmer", searcher)));
                } catch (Throwable e) {
                    if (isClosed.get() == false) {
                        logger.warn("failed to prepare/warm", e);
                    }
                } finally {
                    // no need to release the fullSearcher, nothing really is done...
                    if (newSearcher != null && closeNewSearcher) {
                        IOUtils.closeWhileHandlingException(newSearcher.getIndexReader()); // ignore
                    }
                }
            }
            return searcher;
        }
    }

    public void activateThrottling() {
        throttle.activate();
    }

    public void deactivateThrottling() {
        throttle.deactivate();
    }

    long getGcDeletesInMillis() {
        return engineConfig.getGcDeletesInMillis();
    }

    LiveIndexWriterConfig getCurrentIndexWriterConfig() {
        return indexWriter.getConfig();
    }


    class FailEngineOnMergeFailure implements MergeSchedulerProvider.FailureListener {
        @Override
        public void onFailedMerge(MergePolicy.MergeException e) {
            if (Lucene.isCorruptionException(e)) {
                if (engineConfig.isFailEngineOnCorruption()) {
                    failEngine("corrupt file detected source: [merge]", e);
                } else {
                    logger.warn("corrupt file detected source: [merge] but [{}] is set to [{}]", e, EngineConfig.INDEX_FAIL_ON_CORRUPTION_SETTING, engineConfig.isFailEngineOnCorruption());
                }
            } else if (engineConfig.isFailOnMergeFailure()) {
                failEngine("merge exception", e);
            }
        }
    }

    class MergeSchedulerListener implements MergeSchedulerProvider.Listener {
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        private final AtomicBoolean isThrottling = new AtomicBoolean();

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMerges();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    indexingService.throttlingActivated();
                    activateThrottling();
                }
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMerges();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    indexingService.throttlingDeactivated();
                    deactivateThrottling();
                }
            }
        }
    }


    private void commitIndexWriter(IndexWriter writer) throws IOException {
        try {
            writer.commit();
        } catch (Throwable ex) {
            failEngine("lucene commit failed", ex);
            throw ex;
        }
    }
}

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

package org.elasticsearch.index.engine.internal;

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
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.merge.policy.ElasticsearchMergePolicy;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.search.nested.IncludeNestedDocsQuery;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.indices.warmer.InternalIndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class InternalEngine extends AbstractIndexShardComponent implements Engine {

    private volatile boolean failEngineOnCorruption;
    private volatile ByteSizeValue indexingBufferSize;
    private volatile int indexConcurrency;
    private volatile boolean compoundOnFlush = true;
    private volatile boolean checksumOnMerge = true;

    private long gcDeletesInMillis;

    /** When we last pruned expired tombstones from versionMap.deletes: */
    private volatile long lastDeleteVersionPruneTimeMSec;

    private volatile boolean enableGcDeletes = true;
    private volatile String codecName;
    private final boolean optimizeAutoGenerateId;

    private final ThreadPool threadPool;

    private final ShardIndexingService indexingService;
    private final IndexSettingsService indexSettingsService;
    @Nullable
    private final InternalIndicesWarmer warmer;
    private final Store store;
    private final SnapshotDeletionPolicy deletionPolicy;
    private final Translog translog;
    private final MergePolicyProvider mergePolicyProvider;
    private final MergeSchedulerProvider mergeScheduler;
    private final AnalysisService analysisService;
    private final SimilarityService similarityService;
    private final CodecService codecService;


    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final InternalLock readLock = new InternalLock(rwl.readLock());
    private final InternalLock writeLock = new InternalLock(rwl.writeLock());

    private volatile IndexWriter indexWriter;

    private final SearcherFactory searcherFactory = new SearchFactory();
    private volatile SearcherManager searcherManager;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile boolean possibleMergeNeeded = false;

    private final AtomicBoolean optimizeMutex = new AtomicBoolean();
    // we use flushNeeded here, since if there are no changes, then the commit won't write
    // will not really happen, and then the commitUserData and the new translog will not be reflected
    private volatile boolean flushNeeded = false;
    private final AtomicInteger flushing = new AtomicInteger();
    private final Lock flushLock = new ReentrantLock();

    private final RecoveryCounter onGoingRecoveries = new RecoveryCounter();


    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    private final LiveVersionMap versionMap;

    private final Object[] dirtyLocks;

    private final Object refreshMutex = new Object();

    private final ApplySettings applySettings = new ApplySettings();

    private volatile boolean failOnMergeFailure;
    private Throwable failedEngine = null;
    private final Lock failEngineLock = new ReentrantLock();
    private final CopyOnWriteArrayList<FailedEngineListener> failedEngineListeners = new CopyOnWriteArrayList<>();

    private final AtomicLong translogIdGenerator = new AtomicLong();
    private final AtomicBoolean versionMapRefreshPending = new AtomicBoolean();

    private SegmentInfos lastCommittedSegmentInfos;

    private IndexThrottle throttle;

    @Inject
    public InternalEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                          IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer,
                          Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                          MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                          AnalysisService analysisService, SimilarityService similarityService, CodecService codecService) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.gcDeletesInMillis = indexSettings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueSeconds(60)).millis();
        this.indexingBufferSize = componentSettings.getAsBytesSize("index_buffer_size", new ByteSizeValue(64, ByteSizeUnit.MB)); // not really important, as it is set by the IndexingMemory manager
        this.codecName = indexSettings.get(INDEX_CODEC, "default");

        this.threadPool = threadPool;
        this.lastDeleteVersionPruneTimeMSec = threadPool.estimatedTimeInMillis();
        this.indexSettingsService = indexSettingsService;
        this.indexingService = indexingService;
        this.warmer = (InternalIndicesWarmer) warmer;
        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.codecService = codecService;
        this.compoundOnFlush = indexSettings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, this.compoundOnFlush);
        this.checksumOnMerge = indexSettings.getAsBoolean(INDEX_CHECKSUM_ON_MERGE, this.checksumOnMerge);
        this.indexConcurrency = indexSettings.getAsInt(INDEX_INDEX_CONCURRENCY, Math.max(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, (int) (EsExecutors.boundedNumberOfProcessors(indexSettings) * 0.65)));
        this.versionMap = new LiveVersionMap();
        this.dirtyLocks = new Object[indexConcurrency * 50]; // we multiply it to have enough...
        for (int i = 0; i < dirtyLocks.length; i++) {
            dirtyLocks[i] = new Object();
        }
        this.optimizeAutoGenerateId = indexSettings.getAsBoolean("index.optimize_auto_generated_id", true);

        this.indexSettingsService.addListener(applySettings);
        this.failEngineOnCorruption = indexSettings.getAsBoolean(INDEX_FAIL_ON_CORRUPTION, true);
        this.failOnMergeFailure = indexSettings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, true);
        if (failOnMergeFailure) {
            this.mergeScheduler.addFailureListener(new FailEngineOnMergeFailure());
        }
        store.incRef();
    }

    @Override
    public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
        ByteSizeValue preValue = this.indexingBufferSize;
        try (InternalLock _ = readLock.acquire()) {
            this.indexingBufferSize = indexingBufferSize;
            IndexWriter indexWriter = this.indexWriter;
            if (indexWriter != null) {
                indexWriter.getConfig().setRAMBufferSizeMB(this.indexingBufferSize.mbFrac());
            }
        }
        if (preValue.bytes() != indexingBufferSize.bytes()) {
            // its inactive, make sure we do a full flush in this case, since the memory
            // changes only after a "data" change has happened to the writer
            if (indexingBufferSize == Engine.INACTIVE_SHARD_INDEXING_BUFFER && preValue != Engine.INACTIVE_SHARD_INDEXING_BUFFER) {
                logger.debug("updating index_buffer_size from [{}] to (inactive) [{}]", preValue, indexingBufferSize);
                try {
                    flush(new Flush().type(Flush.Type.COMMIT));
                } catch (EngineClosedException e) {
                    // ignore
                } catch (FlushNotAllowedEngineException e) {
                    // ignore
                } catch (Throwable e) {
                    logger.warn("failed to flush after setting shard to inactive", e);
                }
            } else {
                logger.debug("updating index_buffer_size from [{}] to [{}]", preValue, indexingBufferSize);
            }
        }
    }

    @Override
    public void addFailedEngineListener(FailedEngineListener listener) {
        failedEngineListeners.add(listener);
    }

    @Override
    public void start() throws EngineException {
        store.incRef();
        try (InternalLock _ = writeLock.acquire()) {

            if (indexWriter != null) {
                throw new EngineAlreadyStartedException(shardId);
            }
            if (closed) {
                throw new EngineClosedException(shardId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("starting engine");
            }
            try {
                this.indexWriter = createWriter();
                mergeScheduler.removeListener(this.throttle);
                this.throttle = new IndexThrottle(mergeScheduler, logger, indexingService);
                mergeScheduler.addListener(throttle);
            } catch (IOException e) {
                maybeFailEngine(e, "start");
                if (this.indexWriter != null) {
                    try {
                        IndexWriter pending = indexWriter;
                        indexWriter = null;
                        pending.rollback();
                    } catch (IOException e1) {
                       e.addSuppressed(e1);
                    }
                }
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            }

            try {
                // commit on a just opened writer will commit even if there are no changes done to it
                // we rely on that for the commit data translog id key
                if (Lucene.indexExists(store.directory())) {
                    Map<String, String> commitUserData = Lucene.readSegmentInfos(store.directory()).getUserData();
                    if (commitUserData.containsKey(Translog.TRANSLOG_ID_KEY)) {
                        translogIdGenerator.set(Long.parseLong(commitUserData.get(Translog.TRANSLOG_ID_KEY)));
                    } else {
                        translogIdGenerator.set(System.currentTimeMillis());
                        indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())));
                        indexWriter.commit();
                    }
                } else {
                    translogIdGenerator.set(System.currentTimeMillis());
                    indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())));
                    indexWriter.commit();
                }
                translog.newTranslog(translogIdGenerator.get());
                this.searcherManager = buildSearchManager(indexWriter);
                versionMap.setManager(searcherManager);
                readLastCommittedSegmentsInfo();
            } catch (IOException e) {
                maybeFailEngine(e, "start");
                try {
                    indexWriter.rollback();
                } catch (IOException e1) {
                    // ignore
                } finally {
                    IOUtils.closeWhileHandlingException(indexWriter);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            store.decRef();
        }
    }

    private void readLastCommittedSegmentsInfo() throws IOException {
        lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
    }

    @Override
    public TimeValue defaultRefreshInterval() {
        return new TimeValue(1, TimeUnit.SECONDS);
    }

    /** return the current indexing buffer size setting * */
    public ByteSizeValue indexingBufferSize() {
        return indexingBufferSize;
    }

    @Override
    public void enableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    public GetResult get(Get get) throws EngineException {
        try (InternalLock _ = readLock.acquire()) {
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
            final Searcher searcher = acquireSearcher("get");
            final Versions.DocIdAndVersion docIdAndVersion;
            try {
                docIdAndVersion = Versions.loadDocIdAndVersion(searcher.reader(), get.uid());
            } catch (Throwable e) {
                Releasables.closeWhileHandlingException(searcher);
                //TODO: A better exception goes here
                throw new EngineException(shardId(), "Couldn't resolve version", e);
            }

            if (docIdAndVersion != null) {
                if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) {
                    Releasables.close(searcher);
                    Uid uid = Uid.createUid(get.uid().text());
                    throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), docIdAndVersion.version, get.version());
                }
            }

            if (docIdAndVersion != null) {
                // don't release the searcher on this path, it is the responsability of the caller to call GetResult.release
                return new GetResult(searcher, docIdAndVersion);
            } else {
                Releasables.close(searcher);
                return GetResult.NOT_EXISTS;
            }
        }
    }

    @Override
    public void create(Create create) throws EngineException {
        final IndexWriter writer;
        try (InternalLock _ = readLock.acquire()) {
            writer = currentIndexWriter();
            try (Releasable r = throttle.acquireThrottle()) {
                innerCreate(create, writer);
            }
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (OutOfMemoryError | IllegalStateException | IOException t) {
            maybeFailEngine(t, "create");
            throw new CreateFailedEngineException(shardId, create, t);
        }
        checkVersionMapRefresh();
    }


    private void innerCreate(Create create, IndexWriter writer) throws IOException {
        if (optimizeAutoGenerateId && create.autoGeneratedId() && !create.canHaveDuplicates()) {
            // We don't need to lock because this ID cannot be concurrently updated:
            innerCreateNoLock(create, writer, Versions.NOT_FOUND, null);
        } else {
            synchronized (dirtyLock(create.uid())) {
                final long currentVersion;
                final VersionValue versionValue;
                versionValue = versionMap.getUnderLock(create.uid().bytes());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(create.uid());
                } else {
                    if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
                        currentVersion = Versions.NOT_FOUND; // deleted, and GC
                    } else {
                        currentVersion = versionValue.version();
                    }
                }
                innerCreateNoLock(create, writer, currentVersion, versionValue);
            }
        }
    }

    private void innerCreateNoLock(Create create, IndexWriter writer, long currentVersion, VersionValue versionValue) throws IOException {

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
            } else {
                // On primary, we throw DAEE if the _uid is already in the index with an older version:
                assert create.origin() == Operation.Origin.PRIMARY;
                throw new DocumentAlreadyExistsException(shardId, create.type(), create.id());
            }
        }

        create.updateVersion(updatedVersion);

        if (doUpdate) {
            if (create.docs().size() > 1) {
                writer.updateDocuments(create.uid(), create.docs(), create.analyzer());
            } else {
                writer.updateDocument(create.uid(), create.docs().get(0), create.analyzer());
            }
        } else {
            if (create.docs().size() > 1) {
                writer.addDocuments(create.docs(), create.analyzer());
            } else {
                writer.addDocument(create.docs().get(0), create.analyzer());
            }
        }
        Translog.Location translogLocation = translog.add(new Translog.Create(create));

        versionMap.putUnderLock(create.uid().bytes(), new VersionValue(updatedVersion, translogLocation));

        indexingService.postCreateUnderLock(create);
    }

    @Override
    public void index(Index index) throws EngineException {
        final IndexWriter writer;
        try (InternalLock _ = readLock.acquire()) {
            writer = currentIndexWriter();
            try (Releasable r = throttle.acquireThrottle()) {
                innerIndex(index, writer);
            }
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (OutOfMemoryError | IllegalStateException | IOException t) {
            maybeFailEngine(t, "index");
            throw new IndexFailedEngineException(shardId, index, t);
        }
        checkVersionMapRefresh();
    }

    /**
     * Forces a refresh if the versionMap is using too much RAM (currently > 25% of IndexWriter's RAM buffer).
     */
    private void checkVersionMapRefresh() {
        // TODO: we force refresh when versionMap is using > 25% of IW's RAM buffer; should we make this separately configurable?
        if (versionMap.ramBytesUsedForRefresh() > 0.25 * indexingBufferSize.bytes() && versionMapRefreshPending.getAndSet(true) == false) {
            try {
                if (closed) {
                    // no point...
                    return;
                }
                // Now refresh to clear versionMap:
                threadPool.executor(ThreadPool.Names.REFRESH).execute(new Runnable() {
                    public void run() {
                        try {
                            refresh(new Refresh("version_table_full"));
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

    private void innerIndex(Index index, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(index.uid())) {
            final long currentVersion;
            VersionValue versionValue = versionMap.getUnderLock(index.uid().bytes());
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(index.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
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
                    writer.addDocuments(index.docs(), index.analyzer());
                } else {
                    writer.addDocument(index.docs().get(0), index.analyzer());
                }
            } else {
                if (versionValue != null) {
                    index.created(versionValue.delete()); // we have a delete which is not GC'ed...
                }
                if (index.docs().size() > 1) {
                    writer.updateDocuments(index.uid(), index.docs(), index.analyzer());
                } else {
                    writer.updateDocument(index.uid(), index.docs().get(0), index.analyzer());
                }
            }
            Translog.Location translogLocation = translog.add(new Translog.Index(index));

            versionMap.putUnderLock(index.uid().bytes(), new VersionValue(updatedVersion, translogLocation));

            indexingService.postIndexUnderLock(index);
        }
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        try (InternalLock _ = readLock.acquire()) {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerDelete(delete, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (OutOfMemoryError | IllegalStateException | IOException t) {
            maybeFailEngine(t, "delete");
            throw new DeleteFailedEngineException(shardId, delete, t);
        }

        maybePruneDeletedTombstones();
    }

    private void maybePruneDeletedTombstones() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        if (enableGcDeletes && threadPool.estimatedTimeInMillis() - lastDeleteVersionPruneTimeMSec > gcDeletesInMillis * 0.25) {
            pruneDeletedTombstones();
        }
    }

    private void innerDelete(Delete delete, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(delete.uid())) {
            final long currentVersion;
            VersionValue versionValue = versionMap.getUnderLock(delete.uid().bytes());
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(delete.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
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
                writer.deleteDocuments(delete.uid());
                found = true;
            }

            delete.updateVersion(updatedVersion, found);
            Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
            versionMap.putUnderLock(delete.uid().bytes(), new DeleteVersionValue(updatedVersion, threadPool.estimatedTimeInMillis(), translogLocation));

            indexingService.postDeleteUnderLock(delete);
        }
    }

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        try (InternalLock _ = readLock.acquire()) {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId);
            }

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

            writer.deleteDocuments(query);
            translog.add(new Translog.DeleteByQuery(delete));
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (Throwable t) {
            maybeFailEngine(t, "delete_by_query");
            throw new DeleteByQueryFailedEngineException(shardId, delete, t);
        }

        // TODO: This is heavy, since we refresh, but we must do this because we don't know which documents were in fact deleted (i.e., our
        // versionMap isn't updated), so we must force a cutover to a new reader to "see" the deletions:
        refresh(new Refresh("delete_by_query").force(true));
    }

    @Override
    public final Searcher acquireSearcher(String source) throws EngineException {
        boolean success = false;
         /* Acquire order here is store -> manager since we need
          * to make sure that the store is not closed before
          * the searcher is acquired. */
        store.incRef();
        try {
            SearcherManager manager = this.searcherManager;
            if (manager == null) {
                ensureOpen();
                try (InternalLock _ = this.readLock.acquire()) {
                    // we might start up right now and the searcherManager is not initialized
                    // we take the read lock and retry again since write lock is taken
                    // while start() is called and otherwise the ensureOpen() call will
                    // barf.
                    manager = this.searcherManager;
                    assert manager != null : "SearcherManager is null but shouldn't";
                }
            }
            /* This might throw NPE but that's fine we will run ensureOpen()
            *  in the catch block and throw the right exception */
            final IndexSearcher searcher = manager.acquire();
            try {
                final Searcher retVal = newSearcher(source, searcher, manager);
                success = true;
                return retVal;
            } finally {
                if (!success) {
                    manager.release(searcher);
                }
            }
        } catch (EngineClosedException ex) {
            throw ex;
        } catch (Throwable ex) {
            ensureOpen(); // throw EngineCloseException here if we are already closed
            logger.error("failed to acquire searcher, source {}", ex, source);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            if (!success) {  // release the ref in the case of an error...
                store.decRef();
            }
        }
    }

    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) {
        return new EngineSearcher(source, searcher, manager);
    }

    @Override
    public boolean refreshNeeded() {
        if (store.tryIncRef()) {
            /*
              we need to inc the store here since searcherManager.isSearcherCurrent()
              acquires a searcher internally and that might keep a file open on the
              store. this violates the assumption that all files are closed when
              the store is closed so we need to make sure we increment it here
             */
            try {
                // we are either dirty due to a document added or due to a
                // finished merge - either way we should refresh
                return dirty || !searcherManager.isSearcherCurrent();
            } catch (IOException e) {
                logger.error("failed to access searcher manager", e);
                failEngine("failed to access searcher manager", e);
                throw new EngineException(shardId, "failed to access searcher manager", e);
            } finally {
                store.decRef();
            }
        }
        return false;
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
    public void refresh(Refresh refresh) throws EngineException {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId);
        }
        // we obtain a read lock here, since we don't want a flush to happen while we are refreshing
        // since it flushes the index as well (though, in terms of concurrency, we are allowed to do it)
        try (InternalLock _ = readLock.acquire()) {
            ensureOpen();
            // maybeRefresh will only allow one refresh to execute, and the rest will "pass through",
            // but, we want to make sure not to loose ant refresh calls, if one is taking time
            synchronized (refreshMutex) {
                if (refreshNeeded() || refresh.force()) {
                    // we set dirty to false, even though the refresh hasn't happened yet
                    // as the refresh only holds for data indexed before it. Any data indexed during
                    // the refresh will not be part of it and will set the dirty flag back to true
                    dirty = false;
                    boolean refreshed = searcherManager.maybeRefresh();
                    assert refreshed : "failed to refresh even though refreshMutex was acquired";
                }
            }
        } catch (AlreadyClosedException e) {
            // an index writer got replaced on us, ignore
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
    public void flush(Flush flush) throws EngineException {
        ensureOpen();
        if (flush.type() == Flush.Type.NEW_WRITER || flush.type() == Flush.Type.COMMIT_TRANSLOG) {
            // check outside the lock as well so we can check without blocking on the write lock
            if (onGoingRecoveries.get() > 0) {
                throw new FlushNotAllowedEngineException(shardId, "recovery is in progress, flush [" + flush.type() + "] is not allowed");
            }
        }
        int currentFlushing = flushing.incrementAndGet();
        if (currentFlushing > 1 && !flush.waitIfOngoing()) {
            flushing.decrementAndGet();
            throw new FlushNotAllowedEngineException(shardId, "already flushing...");
        }

        flushLock.lock();
        try {
            if (flush.type() == Flush.Type.NEW_WRITER) {
                try (InternalLock _ = writeLock.acquire()) {
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }
                    // disable refreshing, not dirty
                    dirty = false;
                    try {
                        { // commit and close the current writer - we write the current tanslog ID just in case
                            final long translogId = translog.currentId();
                            indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                            indexWriter.commit();
                            indexWriter.rollback();
                        }
                        indexWriter = createWriter();
                        mergeScheduler.removeListener(this.throttle);

                        this.throttle = new IndexThrottle(mergeScheduler, this.logger, indexingService);
                        mergeScheduler.addListener(throttle);
                        // commit on a just opened writer will commit even if there are no changes done to it
                        // we rely on that for the commit data translog id key
                        if (flushNeeded || flush.force()) {
                            flushNeeded = false;
                            long translogId = translogIdGenerator.incrementAndGet();
                            indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                            indexWriter.commit();
                            translog.newTranslog(translogId);
                        }

                        SearcherManager current = this.searcherManager;
                        this.searcherManager = buildSearchManager(indexWriter);
                        versionMap.setManager(searcherManager);

                        try {
                            IOUtils.close(current);
                        } catch (Throwable t) {
                            logger.warn("Failed to close current SearcherManager", t);
                        }

                        maybePruneDeletedTombstones();

                    } catch (Throwable t) {
                        throw new FlushFailedEngineException(shardId, t);
                    }
                }
            } else if (flush.type() == Flush.Type.COMMIT_TRANSLOG) {
                try (InternalLock _ = readLock.acquire()) {
                    final IndexWriter indexWriter = currentIndexWriter();
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }

                    if (flushNeeded || flush.force()) {
                        flushNeeded = false;
                        try {
                            long translogId = translogIdGenerator.incrementAndGet();
                            translog.newTransientTranslog(translogId);
                            indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                            indexWriter.commit();
                            // we need to refresh in order to clear older version values
                            refresh(new Refresh("version_table_flush").force(true));
                            // we need to move transient to current only after we refresh
                            // so items added to current will still be around for realtime get
                            // when tans overrides it
                            translog.makeTransientCurrent();

                        } catch (Throwable e) {
                            translog.revertTransient();
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                }

                // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
                // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
                if (enableGcDeletes) {
                    pruneDeletedTombstones();
                }

            } else if (flush.type() == Flush.Type.COMMIT) {
                // note, its ok to just commit without cleaning the translog, its perfectly fine to replay a
                // translog on an index that was opened on a committed point in time that is "in the future"
                // of that translog
                try (InternalLock _ = readLock.acquire()) {
                    final IndexWriter indexWriter = currentIndexWriter();
                    // we allow to *just* commit if there is an ongoing recovery happening...
                    // its ok to use this, only a flush will cause a new translogId, and we are locked here from
                    // other flushes use flushLock
                    try {
                        long translogId = translog.currentId();
                        indexWriter.setCommitData(Collections.singletonMap(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)));
                        indexWriter.commit();
                    } catch (Throwable e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                }

                // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
                // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
                if (enableGcDeletes) {
                    pruneDeletedTombstones();
                }

            } else {
                throw new ElasticsearchIllegalStateException("flush type [" + flush.type() + "] not supported");
            }

            // reread the last committed segment infos
            try (InternalLock _ = readLock.acquire()) {
                ensureOpen();
                readLastCommittedSegmentsInfo();
            } catch (Throwable e) {
                if (!closed) {
                    logger.warn("failed to read latest segment infos on flush", e);
                    if (Lucene.isCorruptionException(e)) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                }
            }
        } catch (FlushFailedEngineException ex) {
            maybeFailEngine(ex, "flush");
            throw ex;
        } finally {
            flushLock.unlock();
            flushing.decrementAndGet();
        }
    }

    private void ensureOpen() {
        if (indexWriter == null) {
            throw new EngineClosedException(shardId, failedEngine);
        }
    }

    /**
     * Returns the current index writer. This method will never return <code>null</code>
     *
     * @throws EngineClosedException if the engine is already closed
     */
    private IndexWriter currentIndexWriter() {
        final IndexWriter writer = indexWriter;
        if (writer == null) {
            throw new EngineClosedException(shardId, failedEngine);
        }
        return writer;
    }

    private void pruneDeletedTombstones() {
        long timeMSec = threadPool.estimatedTimeInMillis();

        // TODO: not good that we reach into LiveVersionMap here; can we move this inside VersionMap instead?  problem is the dirtyLock...

        // we only need to prune the deletes map; the current/old version maps are cleared on refresh:
        for (Map.Entry<BytesRef, VersionValue> entry : versionMap.getAllTombstones()) {
            BytesRef uid = entry.getKey();
            synchronized (dirtyLock(uid)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?

                // Must re-get it here, vs using entry.getValue(), in case the uid was indexed/deleted since we pulled the iterator:                
                VersionValue versionValue = versionMap.getTombstoneUnderLock(uid);
                if (versionValue != null) {
                    if (timeMSec - versionValue.time() > gcDeletesInMillis) {
                        versionMap.removeTombstoneUnderLock(uid);
                    }
                }
            }
        }

        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    @Override
    public void maybeMerge() throws EngineException {
        if (!possibleMergeNeeded()) {
            return;
        }
        possibleMergeNeeded = false;
        try (InternalLock _ = readLock.acquire()) {
            currentIndexWriter().maybeMerge();
        } catch (Throwable t) {
            maybeFailEngine(t, "maybe_merge");
            throw new OptimizeFailedEngineException(shardId, t);
        }
    }
    
    private void waitForMerges(boolean flushAfter) {
        try {
            currentIndexWriter().waitForMerges();
        } catch (IOException e) {
            throw new OptimizeFailedEngineException(shardId, e);
        }
        if (flushAfter) {
            flush(new Flush().force(true).waitIfOngoing(true));
        }
    }

    @Override
    public void optimize(Optimize optimize) throws EngineException {
        if (optimizeMutex.compareAndSet(false, true)) {
            try (InternalLock _ = readLock.acquire()) {
                final IndexWriter writer = currentIndexWriter();

                /*
                 * The way we implement upgrades is a bit hackish in the sense that we set an instance
                 * variable and that this setting will thus apply to the next forced merge that will be run.
                 * This is ok because (1) this is the only place we call forceMerge, (2) we have a single
                 * thread for optimize, and the 'optimizeMutex' guarding this code, and (3) ConcurrentMergeScheduler
                 * syncs calls to findForcedMerges.
                 */
                MergePolicy mp = writer.getConfig().getMergePolicy();
                assert mp instanceof ElasticsearchMergePolicy : "MergePolicy is " + mp.getClass().getName();
                if (optimize.upgrade()) {
                    ((ElasticsearchMergePolicy)mp).setUpgradeInProgress(true);
                }
                
                if (optimize.onlyExpungeDeletes()) {
                    writer.forceMergeDeletes(false);
                } else if (optimize.maxNumSegments() <= 0) {
                    writer.maybeMerge();
                    possibleMergeNeeded = false;
                } else {
                    writer.forceMerge(optimize.maxNumSegments(), false);
                }
            } catch (Throwable t) {
                maybeFailEngine(t, "optimize");
                throw new OptimizeFailedEngineException(shardId, t);
            } finally {
                optimizeMutex.set(false);
            }
        }
        
        // wait for the merges outside of the read lock
        if (optimize.waitForMerge()) {
            waitForMerges(optimize.flush());
        } else if (optimize.flush()) {
            // we only need to monitor merges for async calls if we are going to flush
            threadPool.executor(ThreadPool.Names.OPTIMIZE).execute(new AbstractRunnable() {
                @Override
                public void run() {
                    try {
                        waitForMerges(true);
                    } catch (Exception e) {
                        logger.error("Exception while waiting for merges asynchronously after optimize", e);
                    }
                }
            });
        }
    }


    @Override
    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        flush(new Flush().type(Flush.Type.COMMIT).waitIfOngoing(true));
        try (InternalLock _ = readLock.acquire()) {
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
        try (InternalLock _ = writeLock.acquire()) {
            if (closed) {
                throw new EngineClosedException(shardId);
            }
            onGoingRecoveries.startRecovery();
        }

        SnapshotIndexCommit phase1Snapshot;
        try {
            phase1Snapshot = deletionPolicy.snapshot();
        } catch (Throwable e) {
            maybeFailEngine(e, "recovery");
            Releasables.closeWhileHandlingException(onGoingRecoveries);
            throw new RecoveryEngineException(shardId, 1, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase1(phase1Snapshot);
        } catch (Throwable e) {
            maybeFailEngine(e, "recovery phase 1");
            Releasables.closeWhileHandlingException(onGoingRecoveries, phase1Snapshot);
            throw new RecoveryEngineException(shardId, 1, "Execution failed", wrapIfClosed(e));
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Throwable e) {
            maybeFailEngine(e, "snapshot recovery");
            Releasables.closeWhileHandlingException(onGoingRecoveries, phase1Snapshot);
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", wrapIfClosed(e));
        }
        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Throwable e) {
            maybeFailEngine(e, "recovery phase 2");
            Releasables.closeWhileHandlingException(onGoingRecoveries, phase1Snapshot, phase2Snapshot);
            throw new RecoveryEngineException(shardId, 2, "Execution failed", wrapIfClosed(e));
        }

        writeLock.acquire();
        Translog.Snapshot phase3Snapshot = null;
        boolean success = false;
        try {
            phase3Snapshot = translog.snapshot(phase2Snapshot);
            recoveryHandler.phase3(phase3Snapshot);
            success = true;
        } catch (Throwable e) {
            maybeFailEngine(e, "recovery phase 3");
            throw new RecoveryEngineException(shardId, 3, "Execution failed", wrapIfClosed(e));
        } finally {
            Releasables.close(success, onGoingRecoveries, writeLock, phase1Snapshot,
                    phase2Snapshot, phase3Snapshot); // hmm why can't we use try-with here?
        }
    }

    private boolean maybeFailEngine(Throwable t, String source) {
        if (Lucene.isCorruptionException(t)) {
            if (this.failEngineOnCorruption) {
                failEngine("corrupt file detected source: [" + source + "]", t);
                return true;
            } else {
                logger.warn("corrupt file detected source: [{}] but [{}] is set to [{}]", t, source, INDEX_FAIL_ON_CORRUPTION, this.failEngineOnCorruption);
            }
        }else if (ExceptionsHelper.isOOM(t)) {
            failEngine("out of memory", t);
            return true;
        }
        return false;
    }

    private Throwable wrapIfClosed(Throwable t) {
        if (closed) {
            return new EngineClosedException(shardId, t);
        }
        return t;
    }

    private static long getReaderRamBytesUsed(AtomicReaderContext reader) {
        final SegmentReader segmentReader = SegmentReaderUtils.segmentReader(reader.reader());
        return segmentReader.ramBytesUsed();
    }

    @Override
    public SegmentsStats segmentsStats() {
        try (InternalLock _ = readLock.acquire()) {
            ensureOpen();
            try (final Searcher searcher = acquireSearcher("segments_stats")) {
                SegmentsStats stats = new SegmentsStats();
                for (AtomicReaderContext reader : searcher.reader().leaves()) {
                    stats.add(1, getReaderRamBytesUsed(reader));
                }
                stats.addVersionMapMemoryInBytes(versionMap.ramBytesUsed());
                stats.addIndexWriterMemoryInBytes(indexWriter.ramBytesUsed());
                stats.addIndexWriterMaxMemoryInBytes((long) (indexWriter.getConfig().getRAMBufferSizeMB()*1024*1024));
                return stats;
            }
        }
    }

    @Override
    public List<Segment> segments() {
        try (InternalLock _ = readLock.acquire()) {
            ensureOpen();
            Map<String, Segment> segments = new HashMap<>();

            // first, go over and compute the search ones...
            Searcher searcher = acquireSearcher("segments");
            try {
                for (AtomicReaderContext reader : searcher.reader().leaves()) {
                    assert reader.reader() instanceof SegmentReader;
                    SegmentCommitInfo info = SegmentReaderUtils.segmentReader(reader.reader()).getSegmentInfo();
                    assert !segments.containsKey(info.info.name);
                    Segment segment = new Segment(info.info.name);
                    segment.search = true;
                    segment.docCount = reader.reader().numDocs();
                    segment.delDocCount = reader.reader().numDeletedDocs();
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace("failed to get size for [{}]", e, info.info.name);
                    }
                    segment.memoryInBytes = getReaderRamBytesUsed(reader);
                    segments.put(info.info.name, segment);
                }
            } finally {
                searcher.close();
            }

            // now, correlate or add the committed ones...
            if (lastCommittedSegmentInfos != null) {
                SegmentInfos infos = lastCommittedSegmentInfos;
                for (SegmentCommitInfo info : infos) {
                    Segment segment = segments.get(info.info.name);
                    if (segment == null) {
                        segment = new Segment(info.info.name);
                        segment.search = false;
                        segment.committed = true;
                        segment.docCount = info.info.getDocCount();
                        segment.delDocCount = info.getDelCount();
                        segment.version = info.info.getVersion();
                        segment.compound = info.info.getUseCompoundFile();
                        try {
                            segment.sizeInBytes = info.sizeInBytes();
                        } catch (IOException e) {
                            logger.trace("failed to get size for [{}]", e, info.info.name);
                        }
                        segments.put(info.info.name, segment);
                    } else {
                        segment.committed = true;
                    }
                }
            }

            Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
            Arrays.sort(segmentsArr, new Comparator<Segment>() {
                @Override
                public int compare(Segment o1, Segment o2) {
                    return (int) (o1.getGeneration() - o2.getGeneration());
                }
            });

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

    @Override
    public void close() throws ElasticsearchException {
        try (InternalLock _ = writeLock.acquire()) {
            if (!closed) {
                try {
                    closed = true;
                    indexSettingsService.removeListener(applySettings);
                    this.versionMap.clear();
                    this.failedEngineListeners.clear();
                    try {
                        IOUtils.close(searcherManager);
                    } catch (Throwable t) {
                        logger.warn("Failed to close SearcherManager", t);
                    }
                    // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
                    if (indexWriter != null) {
                        try {
                            indexWriter.rollback();
                        } catch (AlreadyClosedException e) {
                            // ignore
                        }
                    }
                } catch (Throwable e) {
                    logger.warn("failed to rollback writer on close", e);
                } finally {
                    indexWriter = null;
                    store.decRef();
                }
            }
        }
    }

    LiveIndexWriterConfig currentIndexWriterConfig() {
        ensureOpen();
        return this.indexWriter.getConfig();
    }

    class FailEngineOnMergeFailure implements MergeSchedulerProvider.FailureListener {
        @Override
        public void onFailedMerge(MergePolicy.MergeException e) {
            if (Lucene.isCorruptionException(e)) {
                if (failEngineOnCorruption) {
                    failEngine("corrupt file detected source: [merge]", e);
                } else {
                    logger.warn("corrupt file detected source: [merge] but [{}] is set to [{}]", e, INDEX_FAIL_ON_CORRUPTION, failEngineOnCorruption);
                }
            } else {
                failEngine("merge exception", e);
            }
        }
    }

    @Override
    public void failEngine(String reason, Throwable failure) {
        assert failure != null;
        if (failEngineLock.tryLock()) {
            try {
                // we first mark the store as corrupted before we notify any listeners
                // this must happen first otherwise we might try to reallocate so quickly
                // on the same node that we don't see the corrupted marker file when
                // the shard is initializing
                if (Lucene.isCorruptionException(failure)) {
                    try {
                        store.markStoreCorrupted(ExceptionsHelper.unwrap(failure, CorruptIndexException.class));
                    } catch (IOException e) {
                        logger.warn("Couldn't marks store corrupted", e);
                    }
                }
            } finally {
                assert !readLock.assertLockIsHeld() : "readLock is held by a thread that tries to fail the engine";
                if (failedEngine != null) {
                    logger.debug("tried to fail engine but engine is already failed. ignoring. [{}]", reason, failure);
                    return;
                }
                try {
                    logger.warn("failed engine [{}]", failure, reason);
                    // we must set a failure exception, generate one if not supplied
                    failedEngine = failure;
                    for (FailedEngineListener listener : failedEngineListeners) {
                        listener.onFailedEngine(shardId, reason, failure);
                    }
                } finally {
                    close();
                }
            }
        } else {
            logger.debug("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason, failure);
        }
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

    /**
     * Returns whether a leaf reader comes from a merge (versus flush or addIndexes).
     */
    private static boolean isMergedSegment(AtomicReader reader) {
        // We expect leaves to be segment readers
        final Map<String, String> diagnostics = SegmentReaderUtils.segmentReader(reader).getSegmentInfo().info.getDiagnostics();
        final String source = diagnostics.get(IndexWriter.SOURCE);
        assert Arrays.asList(IndexWriter.SOURCE_ADDINDEXES_READERS, IndexWriter.SOURCE_FLUSH, IndexWriter.SOURCE_MERGE).contains(source) : "Unknown source " + source;
        return IndexWriter.SOURCE_MERGE.equals(source);
    }

    private IndexWriter createWriter() throws IOException {
        try {
            // release locks when started
            if (IndexWriter.isLocked(store.directory())) {
                logger.warn("shard is locked, releasing lock");
                IndexWriter.unlock(store.directory());
            }
            boolean create = !Lucene.indexExists(store.directory());
            IndexWriterConfig config = new IndexWriterConfig(Lucene.VERSION, analysisService.defaultIndexAnalyzer());
            config.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            config.setIndexDeletionPolicy(deletionPolicy);
            config.setInfoStream(new LoggerInfoStream(indexSettings, shardId));
            config.setMergeScheduler(mergeScheduler.newMergeScheduler());
            MergePolicy mergePolicy = mergePolicyProvider.getMergePolicy();
            // Give us the opportunity to upgrade old segments while performing
            // background merges
            mergePolicy = new ElasticsearchMergePolicy(mergePolicy);
            config.setMergePolicy(mergePolicy);
            config.setSimilarity(similarityService.similarity());
            config.setRAMBufferSizeMB(indexingBufferSize.mbFrac());
            config.setMaxThreadStates(indexConcurrency);
            config.setCodec(codecService.codec(codecName));
            /* We set this timeout to a highish value to work around
             * the default poll interval in the Lucene lock that is
             * 1000ms by default. We might need to poll multiple times
             * here but with 1s poll this is only executed twice at most
             * in combination with the default writelock timeout*/
            config.setWriteLockTimeout(5000);
            config.setUseCompoundFile(this.compoundOnFlush);
            config.setCheckIntegrityAtMerge(checksumOnMerge);
            // Warm-up hook for newly-merged segments. Warming up segments here is better since it will be performed at the end
            // of the merge operation and won't slow down _refresh
            config.setMergedSegmentWarmer(new IndexReaderWarmer() {
                @Override
                public void warm(AtomicReader reader) throws IOException {
                    try {
                        assert isMergedSegment(reader);
                        if (warmer != null) {
                            final Engine.Searcher searcher = new SimpleSearcher("warmer", new IndexSearcher(reader));
                            final IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId, searcher);
                            warmer.warmNewReaders(context);
                        }
                    } catch (Throwable t) {
                        // Don't fail a merge if the warm-up failed
                        if (!closed) {
                            logger.warn("Warm-up failed", t);
                        }
                        if (t instanceof Error) {
                            // assertion/out-of-memory error, don't ignore those
                            throw (Error) t;
                        }
                    }
                }
            });
            return new IndexWriter(store.directory(), config);
        } catch (LockObtainFailedException ex) {
            boolean isLocked = IndexWriter.isLocked(store.directory());
            logger.warn("Could not lock IndexWriter isLocked [{}]", ex, isLocked);
            throw ex;
        }
    }

    public static final String INDEX_INDEX_CONCURRENCY = "index.index_concurrency";
    public static final String INDEX_COMPOUND_ON_FLUSH = "index.compound_on_flush";
    public static final String INDEX_CHECKSUM_ON_MERGE = "index.checksum_on_merge";
    public static final String INDEX_GC_DELETES = "index.gc_deletes";
    public static final String INDEX_FAIL_ON_MERGE_FAILURE = "index.fail_on_merge_failure";
    public static final String INDEX_FAIL_ON_CORRUPTION = "index.fail_on_corruption";


    class ApplySettings implements IndexSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            long gcDeletesInMillis = settings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueMillis(InternalEngine.this.gcDeletesInMillis)).millis();
            if (gcDeletesInMillis != InternalEngine.this.gcDeletesInMillis) {
                logger.info("updating index.gc_deletes from [{}] to [{}]", TimeValue.timeValueMillis(InternalEngine.this.gcDeletesInMillis), TimeValue.timeValueMillis(gcDeletesInMillis));
                InternalEngine.this.gcDeletesInMillis = gcDeletesInMillis;
            }

            final boolean compoundOnFlush = settings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, InternalEngine.this.compoundOnFlush);
            if (compoundOnFlush != InternalEngine.this.compoundOnFlush) {
                logger.info("updating {} from [{}] to [{}]", InternalEngine.INDEX_COMPOUND_ON_FLUSH, InternalEngine.this.compoundOnFlush, compoundOnFlush);
                InternalEngine.this.compoundOnFlush = compoundOnFlush;
                indexWriter.getConfig().setUseCompoundFile(compoundOnFlush);
            }
            
            final boolean checksumOnMerge = settings.getAsBoolean(INDEX_CHECKSUM_ON_MERGE, InternalEngine.this.checksumOnMerge);
            if (checksumOnMerge != InternalEngine.this.checksumOnMerge) {
                logger.info("updating {} from [{}] to [{}]", InternalEngine.INDEX_CHECKSUM_ON_MERGE, InternalEngine.this.checksumOnMerge, checksumOnMerge);
                InternalEngine.this.checksumOnMerge = checksumOnMerge;
                indexWriter.getConfig().setCheckIntegrityAtMerge(checksumOnMerge);
            }

            InternalEngine.this.failEngineOnCorruption = settings.getAsBoolean(INDEX_FAIL_ON_CORRUPTION, InternalEngine.this.failEngineOnCorruption);
            int indexConcurrency = settings.getAsInt(INDEX_INDEX_CONCURRENCY, InternalEngine.this.indexConcurrency);
            boolean failOnMergeFailure = settings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, InternalEngine.this.failOnMergeFailure);
            String codecName = settings.get(INDEX_CODEC, InternalEngine.this.codecName);
            final boolean codecBloomLoad = settings.getAsBoolean(CodecService.INDEX_CODEC_BLOOM_LOAD, codecService.isLoadBloomFilter());
            boolean requiresFlushing = false;
            if (indexConcurrency != InternalEngine.this.indexConcurrency ||
                    !codecName.equals(InternalEngine.this.codecName) ||
                    failOnMergeFailure != InternalEngine.this.failOnMergeFailure ||
                    codecBloomLoad != codecService.isLoadBloomFilter()) {
                try (InternalLock _ = readLock.acquire()) {
                    if (indexConcurrency != InternalEngine.this.indexConcurrency) {
                        logger.info("updating index.index_concurrency from [{}] to [{}]", InternalEngine.this.indexConcurrency, indexConcurrency);
                        InternalEngine.this.indexConcurrency = indexConcurrency;
                        // we have to flush in this case, since it only applies on a new index writer
                        requiresFlushing = true;
                    }
                    if (!codecName.equals(InternalEngine.this.codecName)) {
                        logger.info("updating index.codec from [{}] to [{}]", InternalEngine.this.codecName, codecName);
                        InternalEngine.this.codecName = codecName;
                        // we want to flush in this case, so the new codec will be reflected right away...
                        requiresFlushing = true;
                    }
                    if (failOnMergeFailure != InternalEngine.this.failOnMergeFailure) {
                        logger.info("updating {} from [{}] to [{}]", InternalEngine.INDEX_FAIL_ON_MERGE_FAILURE, InternalEngine.this.failOnMergeFailure, failOnMergeFailure);
                        InternalEngine.this.failOnMergeFailure = failOnMergeFailure;
                    }
                    if (codecBloomLoad != codecService.isLoadBloomFilter()) {
                        logger.info("updating {} from [{}] to [{}]", CodecService.INDEX_CODEC_BLOOM_LOAD, codecService.isLoadBloomFilter(), codecBloomLoad);
                        codecService.setLoadBloomFilter(codecBloomLoad);
                        // we need to flush in this case, to load/unload the bloom filters
                        requiresFlushing = true;
                    }
                }
                if (requiresFlushing) {
                    flush(new Flush().type(Flush.Type.NEW_WRITER));
                }
            }
        }
    }

    private SearcherManager buildSearchManager(IndexWriter indexWriter) throws IOException {
        return new SearcherManager(indexWriter, true, searcherFactory);
    }

    class EngineSearcher implements Searcher {
        private final String source;
        private final IndexSearcher searcher;
        private final SearcherManager manager;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private EngineSearcher(String source, IndexSearcher searcher, SearcherManager manager) {
            this.source = source;
            this.searcher = searcher;
            this.manager = manager;
        }

        @Override
        public String source() {
            return this.source;
        }

        @Override
        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        @Override
        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public void close() throws ElasticsearchException {
            if (!released.compareAndSet(false, true)) {
                /* In general, searchers should never be released twice or this would break reference counting. There is one rare case
                 * when it might happen though: when the request and the Reaper thread would both try to release it in a very short amount
                 * of time, this is why we only log a warning instead of throwing an exception.
                 */
                logger.warn("Searcher was released twice", new ElasticsearchIllegalStateException("Double release"));
                return;
            }
            try {
                manager.release(searcher);
            } catch (IOException e) {
                throw new ElasticsearchIllegalStateException("Cannot close", e);
            } catch (AlreadyClosedException e) {
                /* this one can happen if we already closed the
                 * underlying store / directory and we call into the
                 * IndexWriter to free up pending files. */
            } finally {
                store.decRef();
            }
        }
    }

    class SearchFactory extends SearcherFactory {

        @Override
        public IndexSearcher newSearcher(IndexReader reader) throws IOException {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(similarityService.similarity());
            if (warmer != null) {
                // we need to pass a custom searcher that does not release anything on Engine.Search Release,
                // we will release explicitly
                IndexSearcher newSearcher = null;
                boolean closeNewSearcher = false;
                try {
                    if (searcherManager == null) {
                        // fresh index writer, just do on all of it
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
                        IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId, new SimpleSearcher("warmer", newSearcher));
                        warmer.warmNewReaders(context);
                    }
                    warmer.warmTopReader(new IndicesWarmer.WarmerContext(shardId, new SimpleSearcher("warmer", searcher)));
                } catch (Throwable e) {
                    if (!closed) {
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

    private final class RecoveryCounter implements Releasable {
        private final AtomicInteger onGoingRecoveries = new AtomicInteger();

        public void startRecovery() {
            store.incRef();
            onGoingRecoveries.incrementAndGet();
        }

        public int get() {
            return onGoingRecoveries.get();
        }

        public void endRecovery() throws ElasticsearchException {
            store.decRef();
            onGoingRecoveries.decrementAndGet();
            assert onGoingRecoveries.get() >= 0 : "ongoingRecoveries must be >= 0 but was: " + onGoingRecoveries.get();
        }

        @Override
        public void close() throws ElasticsearchException {
            endRecovery();
        }
    }

    private static final class InternalLock implements Releasable {
        private final ThreadLocal<AtomicInteger> lockIsHeld;
        private final Lock lock;

        InternalLock(Lock lock) {
            ThreadLocal<AtomicInteger> tl = null;
            assert (tl = new ThreadLocal<>()) != null;
            lockIsHeld = tl;
            this.lock = lock;
        }

        @Override
        public void close() {
            lock.unlock();
            assert onAssertRelease();
        }

        InternalLock acquire() throws EngineException {
            lock.lock();
            assert onAssertLock();
            return this;
        }


        protected boolean onAssertRelease() {
            AtomicInteger count = lockIsHeld.get();
            if (count.decrementAndGet() == 0) {
                lockIsHeld.remove();
            }
            return true;
        }

        protected boolean onAssertLock() {
            AtomicInteger count = lockIsHeld.get();
            if (count == null) {
                count = new AtomicInteger(0);
                lockIsHeld.set(count);
            }
            count.incrementAndGet();
            return true;
        }

        boolean assertLockIsHeld() {
            AtomicInteger count = lockIsHeld.get();
            return count != null && count.get() > 0;
        }
    }



    static final class IndexThrottle implements MergeSchedulerProvider.Listener {

        private static final InternalLock NOOP_LOCK = new InternalLock(new NoOpLock());
        private final InternalLock lockReference = new InternalLock(new ReentrantLock());
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        private final AtomicBoolean isThrottling = new AtomicBoolean();
        private final MergeSchedulerProvider mergeScheduler;
        private final ESLogger logger;
        private final ShardIndexingService indexingService;

        private volatile InternalLock lock = NOOP_LOCK;

        public IndexThrottle(MergeSchedulerProvider mergeScheduler, ESLogger logger, ShardIndexingService indexingService) {
            this.mergeScheduler = mergeScheduler;
            this.logger = logger;
            this.indexingService = indexingService;
        }

        public Releasable acquireThrottle() {
            return lock.acquire();
        }

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMerges();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    indexingService.throttlingActivated();
                }
                lock = lockReference;
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMerges();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    indexingService.throttlingDeactivated();
                }
                lock = NOOP_LOCK;
            }
        }

    }

    private static final class NoOpLock implements Lock {

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }
}

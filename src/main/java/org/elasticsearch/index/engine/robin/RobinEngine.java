/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.merge.OnGoingMerge;
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
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.indices.warmer.InternalIndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class RobinEngine extends AbstractIndexShardComponent implements Engine {

    private volatile ByteSizeValue indexingBufferSize;
    private volatile int indexConcurrency;
    private volatile boolean compoundOnFlush = true;

    private long gcDeletesInMillis;
    private volatile boolean enableGcDeletes = true;
    private volatile String codecName;

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


    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private volatile IndexWriter indexWriter;

    private final SearcherFactory searcherFactory = new RobinSearchFactory();
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
    private final ConcurrentMap<HashedBytesRef, VersionValue> versionMap;

    private final Object[] dirtyLocks;

    private final Object refreshMutex = new Object();

    private final ApplySettings applySettings = new ApplySettings();

    private volatile boolean failOnMergeFailure;
    private Throwable failedEngine = null;
    private final Object failedEngineMutex = new Object();
    private final CopyOnWriteArrayList<FailedEngineListener> failedEngineListeners = new CopyOnWriteArrayList<FailedEngineListener>();

    private final AtomicLong translogIdGenerator = new AtomicLong();

    private SegmentInfos lastCommittedSegmentInfos;

    @Inject
    public RobinEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
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
        this.indexConcurrency = indexSettings.getAsInt(INDEX_INDEX_CONCURRENCY, Math.max(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, (int) (EsExecutors.boundedNumberOfProcessors(indexSettings) * 0.65)));
        this.versionMap = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        this.dirtyLocks = new Object[indexConcurrency * 50]; // we multiply it to have enough...
        for (int i = 0; i < dirtyLocks.length; i++) {
            dirtyLocks[i] = new Object();
        }

        this.indexSettingsService.addListener(applySettings);

        this.failOnMergeFailure = indexSettings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, true);
        if (failOnMergeFailure) {
            this.mergeScheduler.addFailureListener(new FailEngineOnMergeFailure());
        }
    }

    @Override
    public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
        ByteSizeValue preValue = this.indexingBufferSize;
        rwl.readLock().lock();
        try {
            this.indexingBufferSize = indexingBufferSize;
            IndexWriter indexWriter = this.indexWriter;
            if (indexWriter != null) {
                indexWriter.getConfig().setRAMBufferSizeMB(this.indexingBufferSize.mbFrac());
            }
        } finally {
            rwl.readLock().unlock();
        }
        if (preValue.bytes() != indexingBufferSize.bytes()) {
            // its inactive, make sure we do a full flush in this case, since the memory
            // changes only after a "data" change has happened to the writer
            if (indexingBufferSize == Engine.INACTIVE_SHARD_INDEXING_BUFFER && preValue != Engine.INACTIVE_SHARD_INDEXING_BUFFER) {
                logger.debug("updating index_buffer_size from [{}] to (inactive) [{}]", preValue, indexingBufferSize);
                try {
                    flush(new Flush().type(Flush.Type.NEW_WRITER));
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
        rwl.writeLock().lock();
        try {
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
            } catch (IOException e) {
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
                        indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())).map());
                        indexWriter.commit();
                    }
                } else {
                    translogIdGenerator.set(System.currentTimeMillis());
                    indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())).map());
                    indexWriter.commit();
                }
                translog.newTranslog(translogIdGenerator.get());
                this.searcherManager = buildSearchManager(indexWriter);
                readLastCommittedSegmentsInfo();
            } catch (IOException e) {
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
            rwl.writeLock().unlock();
        }
    }

    private void readLastCommittedSegmentsInfo() throws IOException {
        SegmentInfos infos = new SegmentInfos();
        infos.read(store.directory());
        lastCommittedSegmentInfos = infos;
    }

    @Override
    public TimeValue defaultRefreshInterval() {
        return new TimeValue(1, TimeUnit.SECONDS);
    }

    @Override
    public void enableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    public GetResult get(Get get) throws EngineException {
        rwl.readLock().lock();
        try {
            if (get.realtime()) {
                VersionValue versionValue = versionMap.get(versionKey(get.uid()));
                if (versionValue != null) {
                    if (versionValue.delete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (!get.loadSource()) {
                        return new GetResult(true, versionValue.version(), null);
                    }
                    byte[] data = translog.read(versionValue.translogLocation());
                    if (data != null) {
                        try {
                            Translog.Source source = TranslogStreams.readSource(data);
                            return new GetResult(true, versionValue.version(), source);
                        } catch (IOException e) {
                            // switched on us, read it from the reader
                        }
                    }
                }
            }

            // no version, get the version from the index, we know that we refresh on flush
            Searcher searcher = acquireSearcher("get");
            try {
                List<AtomicReaderContext> readers = searcher.reader().leaves();
                for (int i = 0; i < readers.size(); i++) {
                    AtomicReaderContext readerContext = readers.get(i);
                    UidField.DocIdAndVersion docIdAndVersion = UidField.loadDocIdAndVersion(readerContext, get.uid());
                    if (docIdAndVersion != null && docIdAndVersion.docId != Lucene.NO_DOC) {
                        // note, we don't release the searcher here, since it will be released as part of the external
                        // API usage, since it still needs it to load data...
                        return new GetResult(searcher, docIdAndVersion);
                    }
                }
            } catch (Throwable e) {
                searcher.release();
                //TODO: A better exception goes here
                throw new EngineException(shardId(), "failed to load document", e);
            }
            searcher.release();
            return GetResult.NOT_EXISTS;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void create(Create create) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerCreate(create, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new CreateFailedEngineException(shardId, create, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new CreateFailedEngineException(shardId, create, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new CreateFailedEngineException(shardId, create, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    private void innerCreate(Create create, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(create.uid())) {
            UidField uidField = create.uidField();
            HashedBytesRef versionKey = versionKey(create.uid());
            final long currentVersion;
            VersionValue versionValue = versionMap.get(versionKey);
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(create.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
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
            } else { // if (index.origin() == Operation.Origin.REPLICA || index.origin() == Operation.Origin.RECOVERY) {
                long expectedVersion = create.version();
                if (currentVersion != -2) { // -2 means we don't have a version, so ignore...
                    // if it does not exists, and its considered the first index operation (replicas/recovery are 1 of)
                    // then nothing to check
                    if (!(currentVersion == -1 && create.version() == 1)) {
                        // with replicas/recovery, we only check for previous version, we allow to set a future version
                        if (expectedVersion <= currentVersion) {
                            if (create.origin() == Operation.Origin.RECOVERY) {
                                return;
                            } else {
                                throw new VersionConflictEngineException(shardId, create.type(), create.id(), currentVersion, expectedVersion);
                            }
                        }
                    }
                }
                // replicas already hold the "future" version
                updatedVersion = create.version();
            }

            // if the doc does not exists or it exists but not delete
            if (versionValue != null) {
                if (!versionValue.delete()) {
                    if (create.origin() == Operation.Origin.RECOVERY) {
                        return;
                    } else {
                        throw new DocumentAlreadyExistsException(shardId, create.type(), create.id());
                    }
                }
            } else if (currentVersion != -1) {
                // its not deleted, its already there
                if (create.origin() == Operation.Origin.RECOVERY) {
                    return;
                } else {
                    throw new DocumentAlreadyExistsException(shardId, create.type(), create.id());
                }
            }

            uidField.version(updatedVersion);
            create.version(updatedVersion);

            if (create.docs().size() > 1) {
                writer.addDocuments(create.docs(), create.analyzer());
            } else {
                writer.addDocument(create.docs().get(0), create.analyzer());
            }
            Translog.Location translogLocation = translog.add(new Translog.Create(create));

            versionMap.put(versionKey, new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis(), translogLocation));

            indexingService.postCreateUnderLock(create);
        }
    }

    @Override
    public void index(Index index) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }

            innerIndex(index, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new IndexFailedEngineException(shardId, index, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new IndexFailedEngineException(shardId, index, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new IndexFailedEngineException(shardId, index, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    private void innerIndex(Index index, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(index.uid())) {
            UidField uidField = index.uidField();
            HashedBytesRef versionKey = versionKey(index.uid());
            final long currentVersion;
            VersionValue versionValue = versionMap.get(versionKey);
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(index.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
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
            } else { // if (index.origin() == Operation.Origin.REPLICA || index.origin() == Operation.Origin.RECOVERY) {
                long expectedVersion = index.version();
                if (currentVersion != -2) { // -2 means we don't have a version, so ignore...
                    // if it does not exists, and its considered the first index operation (replicas/recovery are 1 of)
                    // then nothing to check
                    if (!(currentVersion == -1 && index.version() == 1)) {
                        // with replicas/recovery, we only check for previous version, we allow to set a future version
                        if (expectedVersion <= currentVersion) {
                            if (index.origin() == Operation.Origin.RECOVERY) {
                                return;
                            } else {
                                throw new VersionConflictEngineException(shardId, index.type(), index.id(), currentVersion, expectedVersion);
                            }
                        }
                    }
                }
                // replicas already hold the "future" version
                updatedVersion = index.version();
            }

            uidField.version(updatedVersion);
            index.version(updatedVersion);

            if (currentVersion == -1) {
                // document does not exists, we can optimize for create
                if (index.docs().size() > 1) {
                    writer.addDocuments(index.docs(), index.analyzer());
                } else {
                    writer.addDocument(index.docs().get(0), index.analyzer());
                }
            } else {
                if (index.docs().size() > 1) {
                    writer.updateDocuments(index.uid(), index.docs(), index.analyzer());
                } else {
                    writer.updateDocument(index.uid(), index.docs().get(0), index.analyzer());
                }
            }
            Translog.Location translogLocation = translog.add(new Translog.Index(index));

            versionMap.put(versionKey, new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis(), translogLocation));

            indexingService.postIndexUnderLock(index);
        }
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        rwl.readLock().lock();
        try {
            IndexWriter writer = this.indexWriter;
            if (writer == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            innerDelete(delete, writer);
            dirty = true;
            possibleMergeNeeded = true;
            flushNeeded = true;
        } catch (IOException e) {
            throw new DeleteFailedEngineException(shardId, delete, e);
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new DeleteFailedEngineException(shardId, delete, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new DeleteFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    private void innerDelete(Delete delete, IndexWriter writer) throws IOException {
        synchronized (dirtyLock(delete.uid())) {
            final long currentVersion;
            HashedBytesRef versionKey = versionKey(delete.uid());
            VersionValue versionValue = versionMap.get(versionKey);
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(delete.uid());
            } else {
                if (enableGcDeletes && versionValue.delete() && (threadPool.estimatedTimeInMillis() - versionValue.time()) > gcDeletesInMillis) {
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
                        // its an external version, that's fine, we allow it to be set
                        //throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), -1, delete.version());
                    } else if (currentVersion >= delete.version()) {
                        throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion, delete.version());
                    }
                    updatedVersion = delete.version();
                }
            } else { // if (index.origin() == Operation.Origin.REPLICA || index.origin() == Operation.Origin.RECOVERY) {
                // on replica, the version is the future value expected (returned from the operation on the primary)
                if (currentVersion != -2) { // -2 means we don't have a version in the index, ignore
                    // only check if we have a version for it, otherwise, ignore (see later)
                    if (currentVersion != -1) {
                        // with replicas, we only check for previous version, we allow to set a future version
                        if (delete.version() <= currentVersion) {
                            if (delete.origin() == Operation.Origin.RECOVERY) {
                                return;
                            } else {
                                throw new VersionConflictEngineException(shardId, delete.type(), delete.id(), currentVersion - 1, delete.version());
                            }
                        }
                    }
                }
                // replicas already hold the "future" version
                updatedVersion = delete.version();
            }

            if (currentVersion == -1) {
                // doc does not exists and no prior deletes
                delete.version(updatedVersion).notFound(true);
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(versionKey, new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            } else if (versionValue != null && versionValue.delete()) {
                // a "delete on delete", in this case, we still increment the version, log it, and return that version
                delete.version(updatedVersion).notFound(true);
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(versionKey, new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            } else {
                delete.version(updatedVersion);
                writer.deleteDocuments(delete.uid());
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(versionKey, new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            }

            indexingService.postDeleteUnderLock(delete);
        }
    }

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        rwl.readLock().lock();
        try {
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
        } catch (IOException e) {
            throw new DeleteByQueryFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
        //TODO: This is heavy, since we refresh, but we really have to...
        refreshVersioningTable(System.currentTimeMillis());
    }

    @Override
    public final Searcher acquireSearcher(String source) throws EngineException {
        SearcherManager manager = this.searcherManager;
        if (manager == null) {
            throw new EngineClosedException(shardId);
        }
        try {
            IndexSearcher searcher = manager.acquire();
            return newSearcher(source, searcher, manager);
        } catch (Throwable ex) {
            logger.error("failed to acquire searcher, source {}", ex, source);
            throw new EngineException(shardId, "failed to acquire searcher", ex);
        }
    }

    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) {
        return new RobinSearcher(source, searcher, manager);
    }

    @Override
    public boolean refreshNeeded() {
        return dirty;
    }

    @Override
    public boolean possibleMergeNeeded() {
        return this.possibleMergeNeeded;
    }

    @Override
    public void refresh(Refresh refresh) throws EngineException {
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
                // maybeRefresh will only allow one refresh to execute, and the rest will "pass through",
                // but, we want to make sure not to loose ant refresh calls, if one is taking time
                synchronized (refreshMutex) {
                    if (dirty || refresh.force()) {
                        dirty = false;
                        searcherManager.maybeRefresh();
                    }
                }
            } catch (AlreadyClosedException e) {
                // an index writer got replaced on us, ignore
            } catch (OutOfMemoryError e) {
                failEngine(e);
                throw new RefreshFailedEngineException(shardId, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("OutOfMemoryError")) {
                    failEngine(e);
                }
                throw new RefreshFailedEngineException(shardId, e);
            } catch (Throwable e) {
                if (indexWriter == null) {
                    throw new EngineClosedException(shardId, failedEngine);
                } else if (currentWriter != indexWriter) {
                    // an index writer got replaced on us, ignore
                } else {
                    throw new RefreshFailedEngineException(shardId, e);
                }
            }
        } finally {
            rwl.readLock().unlock();
        }
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
                rwl.writeLock().lock();
                try {
                    ensureOpen();
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }
                    // disable refreshing, not dirty
                    dirty = false;
                    try {
                        // that's ok if the index writer failed and is in inconsistent state
                        // we will get an exception on a dirty operation, and will cause the shard
                        // to be allocated to a different node
                        indexWriter.close(false);
                        indexWriter = createWriter();

                        // commit on a just opened writer will commit even if there are no changes done to it
                        // we rely on that for the commit data translog id key
                        if (flushNeeded || flush.force()) {
                            flushNeeded = false;
                            long translogId = translogIdGenerator.incrementAndGet();
                            indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                            indexWriter.commit();
                            translog.newTranslog(translogId);
                        }

                        SearcherManager current = this.searcherManager;
                        this.searcherManager = buildSearchManager(indexWriter);
                        try {
                            IOUtils.close(current);
                        } catch (Throwable t) {
                            logger.warn("Failed to close current SearcherManager", t);
                        }
                        refreshVersioningTable(threadPool.estimatedTimeInMillis());
                    } catch (OutOfMemoryError e) {
                        failEngine(e);
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().contains("OutOfMemoryError")) {
                            failEngine(e);
                        }
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (Throwable e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                } finally {
                    rwl.writeLock().unlock();
                }
            } else if (flush.type() == Flush.Type.COMMIT_TRANSLOG) {
                rwl.readLock().lock();
                try {
                    ensureOpen();
                    if (onGoingRecoveries.get() > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }

                    if (flushNeeded || flush.force()) {
                        flushNeeded = false;
                        try {
                            long translogId = translogIdGenerator.incrementAndGet();
                            translog.newTransientTranslog(translogId);
                            indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                            indexWriter.commit();
                            refreshVersioningTable(threadPool.estimatedTimeInMillis());
                            // we need to move transient to current only after we refresh
                            // so items added to current will still be around for realtime get
                            // when tans overrides it
                            translog.makeTransientCurrent();
                        } catch (OutOfMemoryError e) {
                            translog.revertTransient();
                            failEngine(e);
                            throw new FlushFailedEngineException(shardId, e);
                        } catch (IllegalStateException e) {
                            if (e.getMessage().contains("OutOfMemoryError")) {
                                failEngine(e);
                            }
                            throw new FlushFailedEngineException(shardId, e);
                        } catch (Throwable e) {
                            translog.revertTransient();
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                } finally {
                    rwl.readLock().unlock();
                }
            } else if (flush.type() == Flush.Type.COMMIT) {
                // note, its ok to just commit without cleaning the translog, its perfectly fine to replay a
                // translog on an index that was opened on a committed point in time that is "in the future"
                // of that translog
                rwl.readLock().lock();
                try {
                    ensureOpen();
                    // we allow to *just* commit if there is an ongoing recovery happening...
                    // its ok to use this, only a flush will cause a new translogId, and we are locked here from
                    // other flushes use flushLock
                    try {
                        long translogId = translog.currentId();
                        indexWriter.setCommitData(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                        indexWriter.commit();
                    } catch (OutOfMemoryError e) {
                        translog.revertTransient();
                        failEngine(e);
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().contains("OutOfMemoryError")) {
                            failEngine(e);
                        }
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (Throwable e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                } finally {
                    rwl.readLock().unlock();
                }
            } else {
                throw new ElasticSearchIllegalStateException("flush type [" + flush.type() + "] not supported");
            }

            // reread the last committed segment infos
            rwl.readLock().lock();
            try {
                ensureOpen();
                readLastCommittedSegmentsInfo();
            } catch (Throwable e) {
                if (!closed) {
                    logger.warn("failed to read latest segment infos on flush", e);
                }
            } finally {
                rwl.readLock().unlock();
            }

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

    private void refreshVersioningTable(long time) {
        // we need to refresh in order to clear older version values
        refresh(new Refresh("version_table").force(true));
        for (Map.Entry<HashedBytesRef, VersionValue> entry : versionMap.entrySet()) {
            HashedBytesRef uid = entry.getKey();
            synchronized (dirtyLock(uid.bytes)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?
                VersionValue versionValue = versionMap.get(uid);
                if (versionValue == null) {
                    continue;
                }
                if (time - versionValue.time() <= 0) {
                    continue; // its a newer value, from after/during we refreshed, don't clear it
                }
                if (versionValue.delete()) {
                    if (enableGcDeletes && (time - versionValue.time()) > gcDeletesInMillis) {
                        versionMap.remove(uid);
                    }
                } else {
                    versionMap.remove(uid);
                }
            }
        }
    }

    @Override
    public void maybeMerge() throws EngineException {
        if (!possibleMergeNeeded) {
            return;
        }
        possibleMergeNeeded = false;
        rwl.readLock().lock();
        try {
            ensureOpen();
            indexWriter.maybeMerge();
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (Throwable e) {
            throw new OptimizeFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void optimize(Optimize optimize) throws EngineException {
        if (optimize.flush()) {
            flush(new Flush().force(true).waitIfOngoing(true));
        }
        if (optimizeMutex.compareAndSet(false, true)) {
            rwl.readLock().lock();
            try {
                ensureOpen();
                if (optimize.onlyExpungeDeletes()) {
                    indexWriter.forceMergeDeletes(false);
                } else if (optimize.maxNumSegments() <= 0) {
                    indexWriter.maybeMerge();
                    possibleMergeNeeded = false;
                } else {
                    indexWriter.forceMerge(optimize.maxNumSegments(), false);
                }
            } catch (OutOfMemoryError e) {
                failEngine(e);
                throw new OptimizeFailedEngineException(shardId, e);
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("OutOfMemoryError")) {
                    failEngine(e);
                }
                throw new OptimizeFailedEngineException(shardId, e);
            } catch (Throwable e) {
                throw new OptimizeFailedEngineException(shardId, e);
            } finally {
                rwl.readLock().unlock();
                optimizeMutex.set(false);
            }
        }
        // wait for the merges outside of the read lock
        if (optimize.waitForMerge()) {
            indexWriter.waitForMerges();
        }
        if (optimize.flush()) {
            flush(new Flush().force(true).waitIfOngoing(true));
        }
    }

    @Override
    public <T> T snapshot(SnapshotHandler<T> snapshotHandler) throws EngineException {
        SnapshotIndexCommit snapshotIndexCommit = null;
        Translog.Snapshot traslogSnapshot = null;
        rwl.readLock().lock();
        try {
            snapshotIndexCommit = deletionPolicy.snapshot();
            traslogSnapshot = translog.snapshot();
        } catch (Throwable e) {
            if (snapshotIndexCommit != null) {
                snapshotIndexCommit.release();
            }
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

    @Override
    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        rwl.readLock().lock();
        try {
            flush(new Flush().type(Flush.Type.COMMIT).waitIfOngoing(true));
            ensureOpen();
            return deletionPolicy.snapshot();
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        // take a write lock here so it won't happen while a flush is in progress
        // this means that next commits will not be allowed once the lock is released
        rwl.writeLock().lock();
        try {
            if (closed) {
                throw new EngineClosedException(shardId);
            }
            onGoingRecoveries.increment();
        } finally {
            rwl.writeLock().unlock();
        }

        SnapshotIndexCommit phase1Snapshot;
        try {
            phase1Snapshot = deletionPolicy.snapshot();
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            throw new RecoveryEngineException(shardId, 1, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase1(phase1Snapshot);
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            phase1Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 1, "Execution failed", e);
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            phase1Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Throwable e) {
            onGoingRecoveries.decrement();
            phase1Snapshot.release();
            phase2Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 2, "Execution failed", e);
        }

        rwl.writeLock().lock();
        Translog.Snapshot phase3Snapshot = null;
        try {
            phase3Snapshot = translog.snapshot(phase2Snapshot);
            recoveryHandler.phase3(phase3Snapshot);
        } catch (Throwable e) {
            throw new RecoveryEngineException(shardId, 3, "Execution failed", e);
        } finally {
            onGoingRecoveries.decrement();
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            if (phase3Snapshot != null) {
                phase3Snapshot.release();
            }
        }
    }

    @Override
    public SegmentsStats segmentsStats() {
        rwl.readLock().lock();
        try {
            ensureOpen();
            Searcher searcher = acquireSearcher("segments_stats");
            try {
                SegmentsStats stats = new SegmentsStats();
                stats.add(searcher.reader().leaves().size());
                return stats;
            } finally {
                searcher.release();
            }
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public List<Segment> segments() {
        rwl.readLock().lock();
        try {
            ensureOpen();
            Map<String, Segment> segments = new HashMap<String, Segment>();

            // first, go over and compute the search ones...
            Searcher searcher = acquireSearcher("segments");
            try {
                for (AtomicReaderContext reader : searcher.reader().leaves()) {
                    assert reader.reader() instanceof SegmentReader;
                    SegmentCommitInfo info = ((SegmentReader) reader.reader()).getSegmentInfo();
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
                    segments.put(info.info.name, segment);
                }
            } finally {
                searcher.release();
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
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void close() throws ElasticSearchException {
        rwl.writeLock().lock();
        try {
            innerClose();
        } finally {
            rwl.writeLock().unlock();
        }
        try {
            // wait for recoveries to join and close all resources / IO streams
            int ongoingRecoveries = onGoingRecoveries.awaitNoRecoveries(5000);
            if (ongoingRecoveries > 0) {
                logger.debug("Waiting for ongoing recoveries timed out on close currently ongoing disoveries: [{}]", ongoingRecoveries);
            }
        } catch (InterruptedException e) {
            // ignore & restore interrupt
            Thread.currentThread().interrupt();
        }

    }

    class FailEngineOnMergeFailure implements MergeSchedulerProvider.FailureListener {
        @Override
        public void onFailedMerge(MergePolicy.MergeException e) {
            failEngine(e);
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
            logger.debug("failed to rollback writer on close", e);
        } finally {
            indexWriter = null;
        }
    }

    private HashedBytesRef versionKey(Term uid) {
        return new HashedBytesRef(uid.bytes());
    }

    private Object dirtyLock(BytesRef uid) {
        int hash = DjbHashFunction.DJB_HASH(uid.bytes, uid.offset, uid.length);
        // abs returns Integer.MIN_VALUE, so we need to protect against it...
        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        }
        return dirtyLocks[Math.abs(hash) % dirtyLocks.length];
    }

    private Object dirtyLock(Term uid) {
        return dirtyLock(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) {
        final Searcher searcher = acquireSearcher("load_version");
        try {
            List<AtomicReaderContext> readers = searcher.reader().leaves();
            for (int i = 0; i < readers.size(); i++) {
                AtomicReaderContext readerContext = readers.get(i);
                long version = UidField.loadVersion(readerContext, uid);
                // either -2 (its there, but no version associated), or an actual version
                if (version != -1) {
                    return version;
                }
            }
            return -1;
        } finally {
            searcher.release();
        }
    }

    /**
     * Returns whether a leaf reader comes from a merge (versus flush or addIndexes).
     */
    private static boolean isMergedSegment(AtomicReader reader) {
        // We expect leaves to be segment readers
        final Map<String, String> diagnostics = ((SegmentReader) reader).getSegmentInfo().info.getDiagnostics();
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
            config.setMergeScheduler(mergeScheduler.newMergeScheduler());
            config.setMergePolicy(mergePolicyProvider.newMergePolicy());
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
            // Warm-up hook for newly-merged segments. Warming up segments here is better since it will be performed at the end
            // of the merge operation and won't slow down _refresh
            config.setMergedSegmentWarmer(new IndexReaderWarmer() {
                @Override
                public void warm(AtomicReader reader) throws IOException {
                    try {
                        assert isMergedSegment(reader);
                        final Engine.Searcher searcher = new SimpleSearcher("warmer", new IndexSearcher(reader));
                        final IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId, searcher);
                        if (warmer != null) warmer.warm(context);
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
    public static final String INDEX_GC_DELETES = "index.gc_deletes";
    public static final String INDEX_FAIL_ON_MERGE_FAILURE = "index.fail_on_merge_failure";

    class ApplySettings implements IndexSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            long gcDeletesInMillis = settings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueMillis(RobinEngine.this.gcDeletesInMillis)).millis();
            if (gcDeletesInMillis != RobinEngine.this.gcDeletesInMillis) {
                logger.info("updating index.gc_deletes from [{}] to [{}]", TimeValue.timeValueMillis(RobinEngine.this.gcDeletesInMillis), TimeValue.timeValueMillis(gcDeletesInMillis));
                RobinEngine.this.gcDeletesInMillis = gcDeletesInMillis;
            }

            final boolean compoundOnFlush = settings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, RobinEngine.this.compoundOnFlush);
            if (compoundOnFlush != RobinEngine.this.compoundOnFlush) {
                logger.info("updating {} from [{}] to [{}]", RobinEngine.INDEX_COMPOUND_ON_FLUSH, RobinEngine.this.compoundOnFlush, compoundOnFlush);
                RobinEngine.this.compoundOnFlush = compoundOnFlush;
                indexWriter.getConfig().setUseCompoundFile(compoundOnFlush);
            }

            int indexConcurrency = settings.getAsInt(INDEX_INDEX_CONCURRENCY, RobinEngine.this.indexConcurrency);
            boolean failOnMergeFailure = settings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, RobinEngine.this.failOnMergeFailure);
            String codecName = settings.get(INDEX_CODEC, RobinEngine.this.codecName);
            boolean requiresFlushing = false;
            if (indexConcurrency != RobinEngine.this.indexConcurrency || !codecName.equals(RobinEngine.this.codecName) || failOnMergeFailure != RobinEngine.this.failOnMergeFailure) {
                rwl.readLock().lock();
                try {
                    if (indexConcurrency != RobinEngine.this.indexConcurrency) {
                        logger.info("updating index.index_concurrency from [{}] to [{}]", RobinEngine.this.indexConcurrency, indexConcurrency);
                        RobinEngine.this.indexConcurrency = indexConcurrency;
                        // we have to flush in this case, since it only applies on a new index writer
                        requiresFlushing = true;
                    }
                    if (!codecName.equals(RobinEngine.this.codecName)) {
                        logger.info("updating index.codec from [{}] to [{}]", RobinEngine.this.codecName, codecName);
                        RobinEngine.this.codecName = codecName;
                        // we want to flush in this case, so the new codec will be reflected right away...
                        requiresFlushing = true;
                    }
                    if (failOnMergeFailure != RobinEngine.this.failOnMergeFailure) {
                        logger.info("updating {} from [{}] to [{}]", RobinEngine.INDEX_FAIL_ON_MERGE_FAILURE, RobinEngine.this.failOnMergeFailure, failOnMergeFailure);
                        RobinEngine.this.failOnMergeFailure = failOnMergeFailure;
                    }
                } finally {
                    rwl.readLock().unlock();
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

    static class RobinSearcher implements Searcher {

        private final String source;
        private final IndexSearcher searcher;
        private final SearcherManager manager;

        private RobinSearcher(String source, IndexSearcher searcher, SearcherManager manager) {
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
        public boolean release() throws ElasticSearchException {
            try {
                manager.release(searcher);
                return true;
            } catch (IOException e) {
                return false;
            } catch (AlreadyClosedException e) {
                /* this one can happen if we already closed the
                 * underlying store / directory and we call into the
                 * IndexWriter to free up pending files. */
                return false;
            }
        }
    }

    static class VersionValue {
        private final long version;
        private final boolean delete;
        private final long time;
        private final Translog.Location translogLocation;

        VersionValue(long version, boolean delete, long time, Translog.Location translogLocation) {
            this.version = version;
            this.delete = delete;
            this.time = time;
            this.translogLocation = translogLocation;
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

        public Translog.Location translogLocation() {
            return this.translogLocation;
        }
    }

    class RobinSearchFactory extends SearcherFactory {

        @Override
        public IndexSearcher newSearcher(IndexReader reader) throws IOException {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(similarityService.similarity());
            if (warmer != null) {
                // we need to pass a custom searcher that does not release anything on Engine.Search Release,
                // we will release explicitly
                Searcher currentSearcher = null;
                IndexSearcher newSearcher = null;
                boolean closeNewSearcher = false;
                try {
                    if (searcherManager == null) {
                        // fresh index writer, just do on all of it
                        newSearcher = searcher;
                    } else {
                        currentSearcher = acquireSearcher("search_factory");
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

                    if (newSearcher != null) {
                        IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId,
                                new SimpleSearcher("warmer", newSearcher));
                        warmer.warm(context);
                    }
                } catch (Throwable e) {
                    if (!closed) {
                        logger.warn("failed to prepare/warm", e);
                    }
                } finally {
                    // no need to release the fullSearcher, nothing really is done...
                    if (currentSearcher != null) {
                        currentSearcher.release();
                    }
                    if (newSearcher != null && closeNewSearcher) {
                        IOUtils.closeWhileHandlingException(newSearcher.getIndexReader()); // ignore
                    }
                }
            }
            return searcher;
        }
    }

    private static final class RecoveryCounter {
        private volatile int ongoingRecoveries = 0;

        synchronized void increment() {
            ongoingRecoveries++;
        }

        synchronized void decrement() {
            ongoingRecoveries--;
            if (ongoingRecoveries == 0) {
                notifyAll(); // notify waiting threads - we only wait on ongoingRecoveries == 0
            }
            assert ongoingRecoveries >= 0 : "ongoingRecoveries must be >= 0 but was: " + ongoingRecoveries;
        }

        int get() {
            // volatile read - no sync needed
            return ongoingRecoveries;
        }

        synchronized int awaitNoRecoveries(long timeout) throws InterruptedException {
            if (ongoingRecoveries > 0) { // no loop here - we either time out or we are done!
                wait(timeout);
            }
            return ongoingRecoveries;
        }
    }

}

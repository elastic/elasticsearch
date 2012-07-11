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
import org.apache.lucene.search.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.lucene.Lucene.safeClose;

/**
 *
 */
public class RobinEngine extends AbstractIndexShardComponent implements Engine {

    private volatile ByteSizeValue indexingBufferSize;

    private volatile int termIndexInterval;

    private volatile int termIndexDivisor;

    private volatile int indexConcurrency;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private final AtomicBoolean optimizeMutex = new AtomicBoolean();

    private long gcDeletesInMillis;

    private volatile boolean enableGcDeletes = true;

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

    private final BloomCache bloomCache;

    private final boolean asyncLoadBloomFilter;

    private volatile IndexWriter indexWriter;

    // LUCENE MONITOR: 3.6 Remove using the custom SearchManager and use the Lucene 3.6 one
    private final SearcherFactory searcherFactory = new RobinSearchFactory();
    private volatile SearcherManager searcherManager;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile boolean possibleMergeNeeded = false;

    // we use flushNeeded here, since if there are no changes, then the commit won't write
    // will not really happen, and then the commitUserData and the new translog will not be reflected
    private volatile boolean flushNeeded = false;

    private volatile int disableFlushCounter = 0;

    // indexing searcher is initialized
    private final AtomicBoolean flushing = new AtomicBoolean();

    private final ConcurrentMap<String, VersionValue> versionMap;

    private final Object[] dirtyLocks;

    private final Object refreshMutex = new Object();

    private final ApplySettings applySettings = new ApplySettings();

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
        this.indexingService = indexingService;
        this.warmer = (InternalIndicesWarmer) warmer;
        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.bloomCache = bloomCache;

        this.indexConcurrency = indexSettings.getAsInt("index.index_concurrency", IndexWriterConfig.DEFAULT_MAX_THREAD_STATES);
        this.versionMap = ConcurrentCollections.newConcurrentMap();
        this.dirtyLocks = new Object[indexConcurrency * 50]; // we multiply it to have enough...
        for (int i = 0; i < dirtyLocks.length; i++) {
            dirtyLocks[i] = new Object();
        }

        this.indexSettingsService.addListener(applySettings);
    }

    @Override
    public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
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
        if (preValue.bytes() != indexingBufferSize.bytes()) {
            // its inactive, make sure we do a full flush in this case, since the memory
            // changes only after a "data" change has happened to the writer
            if (indexingBufferSize == Engine.INACTIVE_SHARD_INDEXING_BUFFER && preValue != Engine.INACTIVE_SHARD_INDEXING_BUFFER) {
                logger.debug("updating index_buffer_size from [{}] to (inactive) [{}]", preValue, indexingBufferSize);
                try {
                    flush(new Flush().full(true));
                } catch (EngineClosedException e) {
                    // ignore
                } catch (FlushNotAllowedEngineException e) {
                    // ignore
                } catch (Exception e) {
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
                logger.debug("Starting engine");
            }
            try {
                this.indexWriter = createWriter();
            } catch (IOException e) {
                throw new EngineCreationFailureException(shardId, "Failed to create engine", e);
            }

            try {
                // commit on a just opened writer will commit even if there are no changes done to it
                // we rely on that for the commit data translog id key
                if (IndexReader.indexExists(store.directory())) {
                    Map<String, String> commitUserData = IndexReader.getCommitUserData(store.directory());
                    if (commitUserData.containsKey(Translog.TRANSLOG_ID_KEY)) {
                        translogIdGenerator.set(Long.parseLong(commitUserData.get(Translog.TRANSLOG_ID_KEY)));
                    } else {
                        translogIdGenerator.set(System.currentTimeMillis());
                        indexWriter.commit(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())).map());
                    }
                } else {
                    translogIdGenerator.set(System.currentTimeMillis());
                    indexWriter.commit(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogIdGenerator.get())).map());
                }
                translog.newTranslog(translogIdGenerator.get());
                this.searcherManager = buildSearchManager(indexWriter);
                SegmentInfos infos = new SegmentInfos();
                infos.read(store.directory());
                lastCommittedSegmentInfos = infos;
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
                VersionValue versionValue = versionMap.get(get.uid().text());
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
            Searcher searcher = searcher();
            try {
                UnicodeUtil.UTF8Result utf8 = Unicode.fromStringAsUtf8(get.uid().text());
                for (IndexReader reader : searcher.searcher().subReaders()) {
                    BloomFilter filter = bloomCache.filter(reader, UidFieldMapper.NAME, asyncLoadBloomFilter);
                    // we know that its not there...
                    if (!filter.isPresent(utf8.result, 0, utf8.length)) {
                        continue;
                    }
                    UidField.DocIdAndVersion docIdAndVersion = UidField.loadDocIdAndVersion(reader, get.uid());
                    if (docIdAndVersion != null && docIdAndVersion.docId != Lucene.NO_DOC) {
                        return new GetResult(searcher, docIdAndVersion);
                    }
                }
            } catch (Exception e) {
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
            final long currentVersion;
            VersionValue versionValue = versionMap.get(create.uid().text());
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

            versionMap.put(create.uid().text(), new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis(), translogLocation));

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
            final long currentVersion;
            VersionValue versionValue = versionMap.get(index.uid().text());
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

            versionMap.put(index.uid().text(), new VersionValue(updatedVersion, false, threadPool.estimatedTimeInMillis(), translogLocation));

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
            VersionValue versionValue = versionMap.get(delete.uid().text());
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
                versionMap.put(delete.uid().text(), new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            } else if (versionValue != null && versionValue.delete()) {
                // a "delete on delete", in this case, we still increment the version, log it, and return that version
                delete.version(updatedVersion).notFound(true);
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(delete.uid().text(), new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
            } else {
                delete.version(updatedVersion);
                writer.deleteDocuments(delete.uid());
                Translog.Location translogLocation = translog.add(new Translog.Delete(delete));
                versionMap.put(delete.uid().text(), new VersionValue(updatedVersion, true, threadPool.estimatedTimeInMillis(), translogLocation));
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
            if (delete.aliasFilter() == null) {
                query = delete.query();
            } else {
                query = new FilteredQuery(delete.query(), delete.aliasFilter());
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
    public Searcher searcher() throws EngineException {
        SearcherManager manager = this.searcherManager;
        IndexSearcher searcher = manager.acquire();
        return new RobinSearcher(searcher, manager);
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
            } catch (Exception e) {
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

        try {
            boolean makeTransientCurrent = false;
            if (flush.full()) {
                rwl.writeLock().lock();
                try {
                    if (indexWriter == null) {
                        throw new EngineClosedException(shardId, failedEngine);
                    }
                    if (disableFlushCounter > 0) {
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
                            indexWriter.commit(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                            translog.newTranslog(translogId);
                        }

                        SearcherManager current = this.searcherManager;
                        this.searcherManager = buildSearchManager(indexWriter);
                        current.close();

                        refreshVersioningTable(threadPool.estimatedTimeInMillis());
                    } catch (OutOfMemoryError e) {
                        failEngine(e);
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (IllegalStateException e) {
                        if (e.getMessage().contains("OutOfMemoryError")) {
                            failEngine(e);
                        }
                        throw new FlushFailedEngineException(shardId, e);
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                } finally {
                    rwl.writeLock().unlock();
                }
            } else {
                rwl.readLock().lock();
                try {
                    if (indexWriter == null) {
                        throw new EngineClosedException(shardId, failedEngine);
                    }
                    if (disableFlushCounter > 0) {
                        throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
                    }

                    if (flushNeeded || flush.force()) {
                        flushNeeded = false;
                        try {
                            long translogId = translogIdGenerator.incrementAndGet();
                            translog.newTransientTranslog(translogId);
                            indexWriter.commit(MapBuilder.<String, String>newMapBuilder().put(Translog.TRANSLOG_ID_KEY, Long.toString(translogId)).map());
                            if (flush.force()) {
                                // if we force, we might not have committed, we need to check that its the same id
                                Map<String, String> commitUserData = IndexReader.getCommitUserData(store.directory());
                                long committedTranslogId = Long.parseLong(commitUserData.get(Translog.TRANSLOG_ID_KEY));
                                if (committedTranslogId != translogId) {
                                    // we did not commit anything, revert to the old translog
                                    translog.revertTransient();
                                } else {
                                    makeTransientCurrent = true;
                                }
                            } else {
                                makeTransientCurrent = true;
                            }
                            if (makeTransientCurrent) {
                                refreshVersioningTable(threadPool.estimatedTimeInMillis());
                                // we need to move transient to current only after we refresh
                                // so items added to current will still be around for realtime get
                                // when tans overrides it
                                translog.makeTransientCurrent();
                            }
                        } catch (OutOfMemoryError e) {
                            translog.revertTransient();
                            failEngine(e);
                            throw new FlushFailedEngineException(shardId, e);
                        } catch (IllegalStateException e) {
                            if (e.getMessage().contains("OutOfMemoryError")) {
                                failEngine(e);
                            }
                            throw new FlushFailedEngineException(shardId, e);
                        } catch (Exception e) {
                            translog.revertTransient();
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                } finally {
                    rwl.readLock().unlock();
                }
            }
            try {
                SegmentInfos infos = new SegmentInfos();
                infos.read(store.directory());
                lastCommittedSegmentInfos = infos;
            } catch (Exception e) {
                if (!closed) {
                    logger.warn("failed to read latest segment infos on flush", e);
                }
            }
        } finally {
            flushing.set(false);
        }
    }

    private void refreshVersioningTable(long time) {
        // we need to refresh in order to clear older version values
        refresh(new Refresh(true).force(true));
        for (Map.Entry<String, VersionValue> entry : versionMap.entrySet()) {
            String id = entry.getKey();
            synchronized (dirtyLock(id)) { // can we do it without this lock on each value? maybe batch to a set and get the lock once per set?
                VersionValue versionValue = versionMap.get(id);
                if (versionValue == null) {
                    continue;
                }
                if (time - versionValue.time() <= 0) {
                    continue; // its a newer value, from after/during we refreshed, don't clear it
                }
                if (versionValue.delete()) {
                    if (enableGcDeletes && (time - versionValue.time()) > gcDeletesInMillis) {
                        versionMap.remove(id);
                    }
                } else {
                    versionMap.remove(id);
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
            if (indexWriter == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            if (indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).enableMerge();
            }
            indexWriter.maybeMerge();
        } catch (OutOfMemoryError e) {
            failEngine(e);
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("OutOfMemoryError")) {
                failEngine(e);
            }
            throw new OptimizeFailedEngineException(shardId, e);
        } catch (Exception e) {
            throw new OptimizeFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
            if (indexWriter != null && indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).disableMerge();
            }
        }
    }

    @Override
    public void optimize(Optimize optimize) throws EngineException {
        if (optimize.flush()) {
            flush(new Flush().force(true));
        }
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
            } catch (Exception e) {
                throw new OptimizeFailedEngineException(shardId, e);
            } finally {
                rwl.readLock().unlock();
                if (indexWriter != null && indexWriter.getConfig().getMergePolicy() instanceof EnableMergePolicy) {
                    ((EnableMergePolicy) indexWriter.getConfig().getMergePolicy()).disableMerge();
                }
                optimizeMutex.set(false);
            }
        }
        // wait for the merges outside of the read lock
        if (optimize.waitForMerge()) {
            indexWriter.waitForMerges();
        }
        if (optimize.flush()) {
            flush(new Flush().force(true));
        }
        if (optimize.refresh()) {
            refresh(new Refresh(false).force(true));
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

    @Override
    public void recover(RecoveryHandler recoveryHandler) throws EngineException {
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
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 1, "Execution failed", e);
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            if (closed) {
                e = new EngineClosedException(shardId, e);
            }
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
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
        } catch (Exception e) {
            throw new RecoveryEngineException(shardId, 3, "Execution failed", e);
        } finally {
            --disableFlushCounter;
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            if (phase3Snapshot != null) {
                phase3Snapshot.release();
            }
        }
    }

    @Override
    public List<Segment> segments() {
        rwl.readLock().lock();
        try {
            IndexWriter indexWriter = this.indexWriter;
            if (indexWriter == null) {
                throw new EngineClosedException(shardId, failedEngine);
            }
            Map<String, Segment> segments = new HashMap<String, Segment>();

            // first, go over and compute the search ones...
            Searcher searcher = searcher();
            try {
                IndexReader[] readers = searcher.reader().getSequentialSubReaders();
                for (IndexReader reader : readers) {
                    assert reader instanceof SegmentReader;
                    SegmentInfo info = Lucene.getSegmentInfo((SegmentReader) reader);
                    assert !segments.containsKey(info.name);
                    Segment segment = new Segment(info.name);
                    segment.search = true;
                    segment.docCount = reader.numDocs();
                    segment.delDocCount = reader.numDeletedDocs();
                    try {
                        segment.sizeInBytes = info.sizeInBytes(true);
                    } catch (IOException e) {
                        logger.trace("failed to get size for [{}]", e, info.name);
                    }
                    segments.put(info.name, segment);
                }
            } finally {
                searcher.release();
            }

            // now, correlate or add the committed ones...
            if (lastCommittedSegmentInfos != null) {
                SegmentInfos infos = lastCommittedSegmentInfos;
                for (SegmentInfo info : infos) {
                    Segment segment = segments.get(info.name);
                    if (segment == null) {
                        segment = new Segment(info.name);
                        segment.search = false;
                        segment.committed = true;
                        segment.docCount = info.docCount;
                        try {
                            segment.delDocCount = indexWriter.numDeletedDocs(info);
                        } catch (IOException e) {
                            logger.trace("failed to get deleted docs for committed segment", e);
                        }
                        try {
                            segment.sizeInBytes = info.sizeInBytes(true);
                        } catch (IOException e) {
                            logger.trace("failed to get size for [{}]", e, info.name);
                        }
                        segments.put(info.name, segment);
                    } else {
                        segment.committed = true;
                    }
                }
            }

            Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
            Arrays.sort(segmentsArr, new Comparator<Segment>() {
                @Override
                public int compare(Segment o1, Segment o2) {
                    return (int) (o1.generation() - o2.generation());
                }
            });

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
            if (searcherManager != null) {
                searcherManager.close();
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

    private Object dirtyLock(String id) {
        int hash = DjbHashFunction.DJB_HASH(id);
        // abs returns Integer.MIN_VALUE, so we need to protect against it...
        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        }
        return dirtyLocks[Math.abs(hash) % dirtyLocks.length];
    }

    private Object dirtyLock(Term uid) {
        return dirtyLock(uid.text());
    }

    private long loadCurrentVersionFromIndex(Term uid) {
        UnicodeUtil.UTF8Result utf8 = Unicode.fromStringAsUtf8(uid.text());
        Searcher searcher = searcher();
        try {
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
        } finally {
            searcher.release();
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

            indexWriter = new XIndexWriter(store.directory(), config, logger, bloomCache);
        } catch (IOException e) {
            safeClose(indexWriter);
            throw e;
        }
        return indexWriter;
    }

    static {
        IndexMetaData.addDynamicSettings(
                "index.term_index_interval",
                "index.term_index_divisor",
                "index.index_concurrency",
                "index.gc_deletes"
        );
    }


    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            long gcDeletesInMillis = indexSettings.getAsTime("index.gc_deletes", TimeValue.timeValueMillis(RobinEngine.this.gcDeletesInMillis)).millis();
            if (gcDeletesInMillis != RobinEngine.this.gcDeletesInMillis) {
                logger.info("updating index.gc_deletes from [{}] to [{}]", TimeValue.timeValueMillis(RobinEngine.this.gcDeletesInMillis), TimeValue.timeValueMillis(gcDeletesInMillis));
                RobinEngine.this.gcDeletesInMillis = gcDeletesInMillis;
            }

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

    private SearcherManager buildSearchManager(IndexWriter indexWriter) throws IOException {
        return new SearcherManager(indexWriter, true, searcherFactory);
    }

    static class RobinSearcher implements Searcher {

        private final IndexSearcher searcher;
        private final SearcherManager manager;

        private RobinSearcher(IndexSearcher searcher, SearcherManager manager) {
            this.searcher = searcher;
            this.manager = manager;
        }

        @Override
        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        @Override
        public ExtendedIndexSearcher searcher() {
            return (ExtendedIndexSearcher) searcher;
        }

        @Override
        public boolean release() throws ElasticSearchException {
            try {
                manager.release(searcher);
                return true;
            } catch (IOException e) {
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
            ExtendedIndexSearcher searcher = new ExtendedIndexSearcher(reader);
            searcher.setSimilarity(similarityService.defaultSearchSimilarity());
            if (warmer != null) {
                // we need to pass a custom searcher that does not release anything on Engine.Search Release,
                // we will release explicitly
                Searcher currentSearcher = null;
                ExtendedIndexSearcher newSearcher = null;
                boolean closeNewSearcher = false;
                try {
                    if (searcherManager == null) {
                        // fresh index writer, just do on all of it
                        newSearcher = searcher;
                    } else {
                        currentSearcher = searcher();
                        // figure out the newSearcher, with only the new readers that are relevant for us
                        List<IndexReader> readers = Lists.newArrayList();
                        for (IndexReader subReader : searcher.subReaders()) {
                            boolean found = false;
                            for (IndexReader currentReader : currentSearcher.searcher().subReaders()) {
                                if (currentReader.getCoreCacheKey().equals(subReader.getCoreCacheKey())) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                readers.add(subReader);
                            }
                        }
                        if (!readers.isEmpty()) {
                            // we don't want to close the inner readers, just increase ref on them
                            newSearcher = new ExtendedIndexSearcher(new MultiReader(readers.toArray(new IndexReader[readers.size()]), false));
                            closeNewSearcher = true;
                        }
                    }

                    if (newSearcher != null) {
                        IndicesWarmer.WarmerContext context = new IndicesWarmer.WarmerContext(shardId,
                                new SimpleSearcher(searcher),
                                new SimpleSearcher(newSearcher));
                        warmer.warm(context);
                    }
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to prepare/warm", e);
                    }
                } finally {
                    // no need to release the fullSearcher, nothing really is done...
                    if (currentSearcher != null) {
                        currentSearcher.release();
                    }
                    if (newSearcher != null && closeNewSearcher) {
                        try {
                            newSearcher.close();
                        } catch (Exception e) {
                            // ignore
                        }
                        try {
                            // close the reader as well, since closing the searcher does nothing
                            // and we want to decRef the inner readers
                            newSearcher.getIndexReader().close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
            }
            return searcher;
        }
    }
}

/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.ParsedDocument.DocumentSize;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL;
import static co.elastic.elasticsearch.stateless.Stateless.SHARD_READ_THREAD_POOL_SETTING;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Stateless test utility class for creating and coordinating IndexEngine and SearchEngine engines.
 */
public abstract class AbstractEngineTestCase extends ESTestCase {

    private Map<String, ThreadPool> threadPools;
    private Path blobStorePath;
    private BlobContainer blobContainer;
    protected StatelessSharedBlobCacheService sharedBlobCacheService;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPools = ConcurrentCollections.newConcurrentMap();
        sharedBlobCacheService = mock(StatelessSharedBlobCacheService.class);
        // setup `real` blob container since test expect write/read from/to compound commits
        blobStorePath = PathUtils.get(createTempDir().toString());
        blobContainer = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false),
            BlobPath.EMPTY,
            blobStorePath
        );
    }

    @Override
    public void tearDown() throws Exception {
        var iterator = threadPools.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            terminate(entry.getValue());
            iterator.remove();
        }
        assert threadPools.isEmpty() : threadPools;
        IOUtils.rm(blobStorePath);
        super.tearDown();
    }

    private ThreadPool registerThreadPool(final ThreadPool threadPool) {
        boolean success = false;
        try {
            if (threadPools.put(threadPool.toString(), threadPool) != null) {
                throw new AssertionError("Test thread pool [" + threadPool + "] already exists");
            }
            success = true;
            return threadPool;
        } finally {
            if (success == false) {
                terminate(threadPool);
            }
        }
    }

    protected IndexEngine newIndexEngine(final EngineConfig indexConfig) {
        return newIndexEngine(indexConfig, mock(TranslogReplicator.class));
    }

    protected IndexEngine newIndexEngine(final EngineConfig indexConfig, final TranslogReplicator translogReplicator) {
        StatelessCommitService commitService = mockCommitService(indexConfig.getIndexSettings().getNodeSettings());
        return newIndexEngine(indexConfig, translogReplicator, mock(ObjectStoreService.class), commitService);
    }

    protected static StatelessCommitService mockCommitService(Settings settings) {
        StatelessCommitService commitService = mock(StatelessCommitService.class);
        doAnswer(invocation -> {
            ActionListener<Void> argument = invocation.getArgument(2);
            argument.onResponse(null);
            return null;
        }).when(commitService).addListenerForUploadedGeneration(any(ShardId.class), anyLong(), any());
        when(commitService.getIndexEngineLocalReaderListenerForShard(any(ShardId.class))).thenReturn(
            (bccHoldingClosedCommit, openBCCs) -> {}
        );
        when(commitService.getCommitBCCResolverForShard(any(ShardId.class))).thenReturn(
            generation -> Set.of(new PrimaryTermAndGeneration(1, generation))
        );
        return commitService;
    }

    protected IndexEngine newIndexEngine(
        final EngineConfig indexConfig,
        final TranslogReplicator translogReplicator,
        final ObjectStoreService objectStoreService,
        final StatelessCommitService commitService
    ) {
        return newIndexEngine(
            indexConfig,
            translogReplicator,
            objectStoreService,
            commitService,
            DocumentParsingProvider.EMPTY_INSTANCE,
            TranslogRecoveryMetrics.NOOP
        );
    }

    protected IndexEngine newIndexEngine(
        EngineConfig indexConfig,
        TranslogReplicator translogReplicator,
        ObjectStoreService objectStoreService,
        StatelessCommitService commitService,
        DocumentParsingProvider documentParsingProvider,
        TranslogRecoveryMetrics translogRecoveryMetrics
    ) {
        var indexEngine = new IndexEngine(
            indexConfig,
            translogReplicator,
            objectStoreService::getTranslogBlobContainer,
            commitService,
            RefreshThrottler.Noop::new,
            commitService.getIndexEngineLocalReaderListenerForShard(indexConfig.getShardId()),
            commitService.getCommitBCCResolverForShard(indexConfig.getShardId()),
            documentParsingProvider,
            translogRecoveryMetrics
        ) {

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    indexConfig.getStore().decRef();
                }
            }
        };
        indexEngine.skipTranslogRecovery();
        return indexEngine;
    }

    protected EngineConfig indexConfig(MapperService mapperService) throws IOException {
        var primaryTerm = new AtomicLong(1L);
        return indexConfig(Settings.EMPTY, Settings.EMPTY, primaryTerm::get, newMergePolicy(), mapperService);
    }

    protected EngineConfig indexConfig() throws IOException {
        return indexConfig(Settings.EMPTY);
    }

    protected EngineConfig indexConfig(Settings settings) throws IOException {
        return indexConfig(settings, Settings.EMPTY);
    }

    protected EngineConfig indexConfig(Settings settings, Settings nodeSettings) throws IOException {
        var primaryTerm = new AtomicLong(1L);
        return indexConfig(settings, nodeSettings, primaryTerm::get);
    }

    protected EngineConfig indexConfig(Settings settings, Settings nodeSettings, LongSupplier primaryTermSupplier) throws IOException {
        return indexConfig(settings, nodeSettings, primaryTermSupplier, newMergePolicy());
    }

    protected EngineConfig indexConfig(Settings settings, Settings nodeSettings, LongSupplier primaryTermSupplier, MergePolicy mergePolicy)
        throws IOException {
        return indexConfig(settings, nodeSettings, primaryTermSupplier, mergePolicy, null);
    }

    protected EngineConfig indexConfig(
        Settings settings,
        Settings nodeSettings,
        LongSupplier primaryTermSupplier,
        MergePolicy mergePolicy,
        MapperService mapperService
    ) throws IOException {

        var shardId = new ShardId(new Index(randomAlphaOfLengthBetween(5, 10), UUIDs.randomBase64UUID(random())), randomInt(10));
        var indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings, nodeSettings);
        var translogConfig = new TranslogConfig(shardId, createTempDir(), indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        var indexWriterConfig = newIndexWriterConfig();
        var threadPool = registerThreadPool(new TestThreadPool(getTestName() + "[" + shardId + "][index]"));
        var directory = newDirectory();
        if (Lucene.indexExists(directory)) {
            throw new AssertionError("Lucene index already exist for shard " + shardId);
        }
        var store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
        store.createEmpty();
        final String translogUuid = Translog.createEmptyTranslog(
            translogConfig.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTermSupplier.getAsLong()
        );
        store.associateIndexWithNewTranslog(translogUuid);

        return new EngineConfig(
            shardId,
            threadPool,
            indexSettings,
            null,
            store,
            mergePolicy,
            indexWriterConfig.getAnalyzer(),
            indexWriterConfig.getSimilarity(),
            new CodecService(null, BigArrays.NON_RECYCLING_INSTANCE),
            new CapturingEngineEventListener(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()),
            emptyList(),
            emptyList(),
            null,
            new NoneCircuitBreakerService(),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            () -> RetentionLeases.EMPTY,
            primaryTermSupplier,
            IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
            null,
            threadPool::relativeTimeInNanos,
            new CapturingIndexCommitListener(),
            true,
            mapperService
        );
    }

    protected SearchEngine newSearchEngine() {
        return newSearchEngineFromIndexEngine(searchConfig());
    }

    protected SearchEngine newSearchEngineFromIndexEngine(final EngineConfig searchConfig) {
        return new SearchEngine(searchConfig, new ClosedShardService()) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    searchConfig.getStore().decRef();
                }
            }
        };
    }

    protected SearchEngine newSearchEngineFromIndexEngine(IndexEngine indexEngine, DeterministicTaskQueue deterministicTaskQueue)
        throws IOException {
        var shardId = indexEngine.getEngineConfig().getShardId();
        var indexSettings = indexEngine.getEngineConfig().getIndexSettings();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var nodeEnvironment = newNodeEnvironment();
        var cache = new StatelessSharedBlobCacheService(
            nodeEnvironment,
            indexSettings.getSettings(),
            threadPool,
            BlobCacheMetrics.NOOP,
            System::nanoTime
        );
        var directory = new SearchDirectory(
            cache,
            new CacheBlobReaderService(indexSettings.getSettings(), cache, mock(Client.class), threadPool),
            MutableObjectStoreUploadTracker.ALWAYS_UPLOADED,
            shardId
        );
        directory.setBlobContainer(primaryTerm -> blobContainer);
        var store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
        final EngineConfig searchConfig = new EngineConfig(
            shardId,
            threadPool,
            indexSettings,
            null,
            store,
            null,
            null,
            null,
            null,
            new CapturingEngineEventListener(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            null,
            null,
            List.of(),
            null,
            null,
            null,
            null,
            () -> {
                throw new AssertionError();
            },
            () -> 1L,
            null,
            null,
            null,
            null,
            false,
            null
        );
        return new SearchEngine(searchConfig, new ClosedShardService()) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    IOUtils.close(searchConfig.getStore()::decRef, nodeEnvironment);
                }
            }
        };

    }

    protected EngineConfig searchConfig() {
        return searchConfig(MutableObjectStoreUploadTracker.ALWAYS_UPLOADED);
    }

    protected EngineConfig searchConfig(MutableObjectStoreUploadTracker objectStoreUploadTracker) {
        var shardId = new ShardId(new Index(randomAlphaOfLengthBetween(5, 10), UUIDs.randomBase64UUID(random())), randomInt(10));
        var indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        var threadPool = registerThreadPool(
            new TestThreadPool(
                getTestName() + "[" + shardId + "][search]",
                new ScalingExecutorBuilder(
                    SHARD_READ_THREAD_POOL,
                    1,
                    2,
                    TimeValue.timeValueSeconds(30L),
                    true,
                    SHARD_READ_THREAD_POOL_SETTING
                )
            )
        );
        var directory = new SearchDirectory(
            sharedBlobCacheService,
            new CacheBlobReaderService(indexSettings.getSettings(), sharedBlobCacheService, mock(Client.class), threadPool),
            objectStoreUploadTracker,
            shardId
        );
        var store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
        return new EngineConfig(
            shardId,
            threadPool,
            indexSettings,
            null,
            store,
            null,
            null,
            null,
            null,
            new CapturingEngineEventListener(),
            null,
            IndexSearcher.getDefaultQueryCachingPolicy(),
            null,
            null,
            List.of(),
            null,
            null,
            null,
            null,
            () -> {
                throw new AssertionError();
            },
            () -> 1L,
            null,
            null,
            null,
            null,
            false,
            null
        );
    }

    protected static Engine.Index randomDoc(String id) throws IOException {
        final LuceneDocument document = new LuceneDocument();
        document.add(new StringField("_id", Uid.encodeId(id), Field.Store.YES));
        var version = new NumericDocValuesField("_version", 0);
        document.add(version);
        var seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        seqID.addFields(document);
        final BytesReference source;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.field("value", randomUnicodeOfCodepointLengthBetween(1, 10));
            builder.endObject();

            source = BytesReference.bytes(builder);
            document.add(new StoredField(SourceFieldMapper.NAME, source.toBytesRef().bytes, 0, source.length()));
        }
        final ParsedDocument doc = new ParsedDocument(
            version,
            seqID,
            id,
            null,
            List.of(document),
            source,
            XContentType.JSON,
            null,
            DocumentSize.UNKNOWN
        );
        return new Engine.Index(Uid.encodeId(id), 1L, doc);
    }

    static class CapturingEngineEventListener implements Engine.EventListener {

        final SetOnce<String> reason = new SetOnce<>();
        final SetOnce<Exception> exception = new SetOnce<>();

        @Override
        public void onFailedEngine(String reason, Exception e) {
            this.reason.set(reason);
            this.exception.set(e);
        }
    }

    class CapturingIndexCommitListener implements Engine.IndexCommitListener {

        private final Map<String, BlobLocation> uploadedBlobLocations = new HashMap<>();
        private final LinkedBlockingQueue<NewCommitNotification> notifications = new LinkedBlockingQueue<>();

        @Override
        public void onNewCommit(
            ShardId shardId,
            Store store,
            long primaryTerm,
            Engine.IndexCommitRef indexCommitRef,
            Set<String> additionalFiles
        ) {
            store.incRef();
            try {

                var vbcc = new VirtualBatchedCompoundCommit(
                    shardId,
                    "node-id",
                    primaryTerm,
                    indexCommitRef.getIndexCommit().getGeneration(),
                    fileName -> uploadedBlobLocations.get(fileName),
                    ESTestCase::randomNonNegativeLong
                );

                vbcc.appendCommit(
                    new StatelessCommitRef(
                        shardId,
                        indexCommitRef,
                        indexCommitRef.getIndexCommit().getFileNames(),
                        additionalFiles,
                        primaryTerm,
                        0   // not used, stubbing value for translogRecoveryStartFile
                    ),
                    randomBoolean()
                );

                vbcc.freeze();

                try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                    blobContainer.writeBlobAtomic(
                        OperationPurpose.INDICES,
                        vbcc.getBlobName(),
                        vbccInputStream,
                        vbcc.getTotalSizeInBytes(),
                        false
                    );
                }

                var scc = vbcc.lastCompoundCommit();

                uploadedBlobLocations.putAll(scc.commitFiles());

                notifications.add(new NewCommitNotification(scc, scc.generation(), scc.primaryTermAndGeneration(), 1L, "_node_id"));
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                try {
                    IOUtils.close(indexCommitRef, store::decRef);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        }

        @Override
        public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {}
    }

    /**
     * Retrieve all captured commits on a given {@link IndexEngine} and notify the {@link SearchEngine} of those new commits in a random
     * order.
     *
     * @param indexEngine  the index engine to retrieve the capture commits from
     * @param searchEngine the search engine to notify with new commits
     * @return the number of new commits notifications
     */
    protected static int notifyCommits(IndexEngine indexEngine, SearchEngine searchEngine) {
        var indexCommitListener = indexEngine.config().getIndexCommitListener();
        assertThat(indexCommitListener, instanceOf(CapturingIndexCommitListener.class));

        final List<NewCommitNotification> notifications = new ArrayList<>();
        final int count = ((CapturingIndexCommitListener) indexCommitListener).notifications.drainTo(notifications);
        Collections.shuffle(notifications, random());
        notifications.forEach(notif -> searchEngine.onCommitNotification(notif, ActionListener.noop()));
        return count;
    }
}

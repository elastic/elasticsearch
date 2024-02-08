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
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
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
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.index.engine.EngineTestCase.newUid;
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
    protected SharedBlobCacheService<FileCacheKey> sharedBlobCacheService;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPools = ConcurrentCollections.newConcurrentMap();
        sharedBlobCacheService = mock(SharedBlobCacheService.class);
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
        StatelessCommitService commitService = mockCommitService();
        return newIndexEngine(indexConfig, translogReplicator, mock(ObjectStoreService.class), commitService);
    }

    protected static StatelessCommitService mockCommitService() {
        StatelessCommitService commitService = mock(StatelessCommitService.class);
        doAnswer(invocation -> {
            ActionListener<Void> argument = invocation.getArgument(2);
            argument.onResponse(null);
            return null;
        }).when(commitService).addListenerForUploadedGeneration(any(ShardId.class), anyLong(), any());
        when(commitService.closedLocalReadersForGeneration(any(ShardId.class))).thenReturn(v -> {});
        return commitService;
    }

    protected IndexEngine newIndexEngine(
        final EngineConfig indexConfig,
        final TranslogReplicator translogReplicator,
        final ObjectStoreService objectStoreService,
        final StatelessCommitService commitService
    ) {
        var indexEngine = new IndexEngine(
            indexConfig,
            translogReplicator,
            objectStoreService::getTranslogBlobContainer,
            commitService,
            RefreshThrottler.Noop::new,
            commitService.closedLocalReadersForGeneration(indexConfig.getShardId())
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
            true
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

    protected SearchEngine newSearchEngineFromIndexEngine(
        IndexEngine indexEngine,
        DeterministicTaskQueue deterministicTaskQueue,
        boolean copyInitialMetadata
    ) throws IOException {
        var shardId = indexEngine.getEngineConfig().getShardId();
        var indexSettings = indexEngine.getEngineConfig().getIndexSettings();
        var threadPool = deterministicTaskQueue.getThreadPool();
        var nodeEnvironment = newNodeEnvironment();
        var cache = new SharedBlobCacheService<FileCacheKey>(
            nodeEnvironment,
            indexSettings.getSettings(),
            threadPool,
            Stateless.SHARD_READ_THREAD_POOL,
            BlobCacheMetrics.NOOP
        );
        var directory = new SearchDirectory(cache, shardId);
        directory.setBlobContainer(primaryTerm -> storeBlobContainer(indexEngine.getEngineConfig().getStore()));
        if (copyInitialMetadata) {
            Store.MetadataSnapshot latestMetadata = indexEngine.getEngineConfig().getStore().getMetadata(null);
            Map<String, BlobLocation> blobLocations = collectBlobLocations(
                indexEngine.getEngineConfig().getPrimaryTermSupplier().getAsLong(),
                latestMetadata
            );
            SearchDirectoryTestUtils.setMetadata(directory, blobLocations);
        }
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
            false
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
        var shardId = new ShardId(new Index(randomAlphaOfLengthBetween(5, 10), UUIDs.randomBase64UUID(random())), randomInt(10));
        var indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        var threadPool = registerThreadPool(new TestThreadPool(getTestName() + "[" + shardId + "][search]"));
        var directory = new SearchDirectory(sharedBlobCacheService, shardId);
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
            false
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
        final ParsedDocument doc = new ParsedDocument(version, seqID, id, null, List.of(document), source, XContentType.JSON, null);
        return new Engine.Index(newUid(id), 1L, doc);
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

    static class CapturingIndexCommitListener implements Engine.IndexCommitListener {

        final LinkedBlockingQueue<NewCommitNotificationRequest> notifications = new LinkedBlockingQueue<>();

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
                notifications.add(
                    new NewCommitNotificationRequest(
                        IndexShardRoutingTable.builder(shardId)
                            .addShard(newShardRouting(shardId, "_node", true, ShardRoutingState.STARTED))
                            .build(),
                        new StatelessCompoundCommit(
                            shardId,
                            new PrimaryTermAndGeneration(primaryTerm, indexCommitRef.getIndexCommit().getGeneration()),
                            0,
                            "fake_node_ephemeral_id",
                            collectBlobLocations(primaryTerm, store.getMetadata(indexCommitRef.getIndexCommit()))
                        )
                    )
                );
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

    private static Map<String, BlobLocation> collectBlobLocations(long primaryTerm, Store.MetadataSnapshot metadata) {
        return metadata.fileMetadataMap()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new BlobLocation(
                        primaryTerm,

                        entry.getKey(),
                        entry.getValue().length(),
                        0,
                        entry.getValue().length()
                    )
                )
            );
    }

    /**
     * Retrieve all captured commits on a given {@link IndexEngine} and notify the {@link SearchEngine} of those new commits in a random
     * order.
     *
     * @param indexEngine the index engine to retrieve the capture commits from
     * @param searchEngine the search engine to notify with new commits
     * @return the number of new commits notifications
     */
    protected static int notifyCommits(IndexEngine indexEngine, SearchEngine searchEngine) {
        var indexCommitListener = indexEngine.config().getIndexCommitListener();
        assertThat(indexCommitListener, instanceOf(CapturingIndexCommitListener.class));

        final List<NewCommitNotificationRequest> notifications = new ArrayList<>();
        final int count = ((CapturingIndexCommitListener) indexCommitListener).notifications.drainTo(notifications);
        Collections.shuffle(notifications, random());
        notifications.forEach(notif -> searchEngine.onCommitNotification(notif.getCompoundCommit(), ActionListener.noop()));
        return count;
    }

    /**
     * A {@link BlobContainer} that can read files from a {@link Store}
     */
    private static BlobContainer storeBlobContainer(final Store store) {
        return new FilterBlobContainer(new FsBlobContainer(null, BlobPath.EMPTY, null)) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                boolean success = false;
                store.incRef();
                try {
                    final IndexInput input = store.directory().openInput(blobName, IOContext.DEFAULT);
                    if (position > 0L) {
                        input.seek(position);
                    }
                    final InputStreamIndexInput stream = new InputStreamIndexInput(input, length) {
                        @Override
                        public void close() throws IOException {
                            try {
                                super.close();
                            } finally {
                                IOUtils.closeWhileHandlingException(input, store::decRef);
                            }
                        }
                    };
                    success = true;
                    return stream;
                } finally {
                    if (success == false) {
                        store.decRef();
                    }
                }
            }
        };
    }
}

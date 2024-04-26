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

package co.elastic.elasticsearch.stateless.test;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.TestUtils;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.utils.TransferableCloseables;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService.BUCKET_SETTING;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FakeStatelessNode implements Closeable {
    public final DiscoveryNode node;
    public final Path pathHome;
    public final Path repoPath;
    public final Settings nodeSettings;
    public final ClusterSettings clusterSettings;
    public final Environment environment;
    public final IndexMetadata indexMetadata;
    public final ShardId shardId;
    public final IndexSettings indexSettings;
    public final MockTransport transport;
    public final ClusterService clusterService;
    public final NodeClient client;
    public final ShardPath indexingShardPath;
    public final IndexDirectory indexingDirectory;
    public final Store indexingStore;
    public final ShardPath searchShardPath;
    public final SearchDirectory searchDirectory;
    public final Store searchStore;
    public final TransportService transportService;
    public final RepositoriesService repoService;
    public final ObjectStoreService objectStoreService;
    public final StatelessCommitService commitService;
    public final NodeEnvironment nodeEnvironment;
    public final ThreadPool threadPool;

    public final StatelessElectionStrategy electionStrategy;
    public final StatelessSharedBlobCacheService sharedCacheService;
    public final CacheBlobReaderService cacheBlobReaderService;
    private final StatelessCommitCleaner commitCleaner;

    private final Closeable closeables;
    private final long primaryTerm;

    public FakeStatelessNode(
        Function<Settings, Environment> environmentSupplier,
        CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, 1);
    }

    public FakeStatelessNode(
        Function<Settings, Environment> environmentSupplier,
        CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
        NamedXContentRegistry xContentRegistry,
        long primaryTerm
    ) throws IOException {
        this.primaryTerm = primaryTerm;
        node = DiscoveryNodeUtils.create("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        repoPath = LuceneTestCase.createTempDir();
        pathHome = LuceneTestCase.createTempDir().toAbsolutePath();
        nodeSettings = nodeSettings();
        clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        environment = environmentSupplier.apply(nodeSettings);

        indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();
        shardId = new ShardId(indexMetadata.getIndex(), 0);
        indexSettings = new IndexSettings(indexMetadata, nodeSettings);
        Path dataPath = Objects.requireNonNull(environment.dataFiles()[0]);
        indexingShardPath = new ShardPath(
            false,
            dataPath.resolve(shardId.getIndex().getUUID()).resolve("0"),
            dataPath.resolve(shardId.getIndex().getUUID()).resolve("0"),
            shardId
        );
        searchShardPath = new ShardPath(
            false,
            LuceneTestCase.createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            LuceneTestCase.createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            shardId
        );

        try (var localCloseables = new TransferableCloseables()) {

            threadPool = new TestThreadPool("test", Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
            localCloseables.add(() -> TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

            transport = localCloseables.add(new MockTransport());
            clusterService = localCloseables.add(ClusterServiceUtils.createClusterService(threadPool));
            client = createClient(nodeSettings, threadPool);
            nodeEnvironment = nodeEnvironmentSupplier.apply(nodeSettings);
            localCloseables.add(nodeEnvironment);
            sharedCacheService = createCacheService(nodeEnvironment, nodeSettings, threadPool);
            localCloseables.add(sharedCacheService);
            cacheBlobReaderService = createCacheBlobReaderService(sharedCacheService);
            indexingDirectory = localCloseables.add(
                new IndexDirectory(
                    new FsDirectoryFactory().newDirectory(indexSettings, indexingShardPath),
                    createSearchDirectory(
                        sharedCacheService,
                        shardId,
                        cacheBlobReaderService,
                        MutableObjectStoreUploadTracker.ALWAYS_UPLOADED
                    )
                )
            );
            indexingStore = localCloseables.add(new Store(shardId, indexSettings, indexingDirectory, new DummyShardLock(shardId)));
            searchDirectory = localCloseables.add(
                createSearchDirectory(
                    sharedCacheService,
                    searchShardPath.getShardId(),
                    cacheBlobReaderService,
                    new AtomicMutableObjectStoreUploadTracker()
                )
            );
            searchStore = localCloseables.add(new Store(shardId, indexSettings, searchDirectory, new DummyShardLock(shardId)));

            transportService = transport.createTransportService(
                nodeSettings,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> node,
                null,
                Set.of()
            );

            repoService = new RepositoriesService(
                nodeSettings,
                clusterService,
                transportService,
                Map.of(
                    FsRepository.TYPE,
                    metadata -> new FsRepository(
                        metadata,
                        environment,
                        xContentRegistry,
                        clusterService,
                        BigArrays.NON_RECYCLING_INSTANCE,
                        new RecoverySettings(nodeSettings, clusterSettings)
                    ) {
                        @Override
                        protected BlobStore createBlobStore() throws Exception {
                            final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
                            final Path locationFile = environment.resolveRepoFile(location);
                            return new FsBlobStore(bufferSize, locationFile, isReadOnly()) {
                                @Override
                                public BlobContainer blobContainer(BlobPath path) {
                                    return wrapBlobContainer(path, super.blobContainer(path));
                                }
                            };
                        }
                    }
                ),
                Map.of(),
                threadPool,
                List.of()
            );

            transportService.start();
            transportService.acceptIncomingRequests();
            localCloseables.add(transportService::stop);

            objectStoreService = new ObjectStoreService(nodeSettings, () -> repoService, threadPool, clusterService);
            objectStoreService.start();
            localCloseables.add(objectStoreService);
            electionStrategy = new StatelessElectionStrategy(objectStoreService::getClusterStateBlobContainer, threadPool);
            var consistencyService = new StatelessClusterConsistencyService(clusterService, electionStrategy, threadPool, nodeSettings);
            commitCleaner = createCommitCleaner(consistencyService, threadPool, objectStoreService);
            localCloseables.add(commitCleaner);
            commitService = createCommitService();
            commitService.start();
            commitService.register(shardId, getPrimaryTerm());
            localCloseables.add(commitService);
            indexingDirectory.getSearchDirectory().setBlobContainer(term -> objectStoreService.getBlobContainer(shardId, term));

            closeables = localCloseables.transfer();
        }
    }

    public StatelessCommitCleaner getCommitCleaner() {
        return commitCleaner;
    }

    protected SearchDirectory createSearchDirectory(
        StatelessSharedBlobCacheService sharedCacheService,
        ShardId shardId,
        CacheBlobReaderService cacheBlobReaderService,
        MutableObjectStoreUploadTracker objectStoreUploadTracker
    ) {
        return new SearchDirectory(sharedCacheService, cacheBlobReaderService, objectStoreUploadTracker, shardId);
    }

    protected StatelessSharedBlobCacheService createCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool
    ) {
        return TestUtils.newCacheService(nodeEnvironment, settings, threadPool);
    }

    protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
        return new CacheBlobReaderService(nodeSettings, cacheService, client);
    }

    public List<StatelessCommitRef> generateIndexCommits(int commitsNumber) throws IOException {
        return generateIndexCommits(commitsNumber, false, generation -> {});
    }

    public List<StatelessCommitRef> generateIndexCommits(int commitsNumber, boolean merge) throws IOException {
        return generateIndexCommits(commitsNumber, merge, generation -> {});
    }

    public List<StatelessCommitRef> generateIndexCommits(int commitsNumber, boolean merge, LongConsumer onCommitClosed) throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        Set<String> previousCommit;

        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        String deleteId = ESTestCase.randomAlphaOfLength(10);

        try (var indexWriter = new IndexWriter(indexingStore.directory(), indexWriterConfig)) {
            try (var indexReader = DirectoryReader.open(indexingStore.directory())) {
                IndexCommit indexCommit = indexReader.getIndexCommit();
                previousCommit = new HashSet<>(indexCommit.getFileNames());
            }
            for (int i = 0; i < commitsNumber; i++) {
                LuceneDocument document = new LuceneDocument();
                document.add(new KeywordField("field0", "term", Field.Store.YES));
                indexWriter.addDocument(document.getFields());
                if (ESTestCase.randomBoolean()) {
                    final ParsedDocument tombstone = ParsedDocument.deleteTombstone(deleteId);
                    LuceneDocument delete = tombstone.docs().get(0);
                    NumericDocValuesField field = Lucene.newSoftDeletesField();
                    delete.add(field);
                    indexWriter.softUpdateDocument(EngineTestCase.newUid(deleteId), delete.getFields(), Lucene.newSoftDeletesField());
                }
                if (merge) {
                    indexWriter.forceMerge(1, true);
                }
                indexWriter.setLiveCommitData(Map.of(SequenceNumbers.MAX_SEQ_NO, Integer.toString(i)).entrySet());
                indexWriter.commit();
                try (var indexReader = DirectoryReader.open(indexingStore.directory())) {
                    IndexCommit indexCommit = indexReader.getIndexCommit();
                    Set<String> commitFiles = new HashSet<>(indexCommit.getFileNames());
                    Set<String> additionalFiles = Sets.difference(commitFiles, previousCommit);
                    previousCommit = commitFiles;

                    StatelessCommitRef statelessCommitRef = new StatelessCommitRef(
                        shardId,
                        new Engine.IndexCommitRef(indexCommit, () -> onCommitClosed.accept(indexCommit.getGeneration())),
                        commitFiles,
                        additionalFiles,
                        primaryTerm,
                        0
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }

        return commits;
    }

    protected Settings nodeSettings() {
        return Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), pathHome)
            .put(PATH_REPO_SETTING.getKey(), repoPath)
            .put(BUCKET_SETTING.getKey(), repoPath)
            .build();
    }

    protected StatelessCommitCleaner createCommitCleaner(
        StatelessClusterConsistencyService consistencyService,
        ThreadPool threadPool,
        ObjectStoreService objectStoreService
    ) {
        return new StatelessCommitCleaner(consistencyService, threadPool, objectStoreService);
    }

    protected StatelessCommitService createCommitService() {
        return new StatelessCommitService(
            nodeSettings,
            objectStoreService,
            () -> clusterService.localNode().getEphemeralId(),
            this::getShardRoutingTable,
            clusterService.threadPool(),
            client,
            commitCleaner,
            createAllNodesResolveBCCReferencesLocallySupplier()
        );
    }

    protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
        IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.shardId()).thenReturn(shardId);
        return routingTable;
    }

    protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
        return new NoOpNodeClient(threadPool) {
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assert action == TransportNewCommitNotificationAction.TYPE;
                ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(new NewCommitNotificationResponse(Set.of()));
            }
        };
    }

    protected long getPrimaryTerm() {
        return primaryTerm;
    }

    protected BooleanSupplier createAllNodesResolveBCCReferencesLocallySupplier() {
        return () -> true;
    }

    public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
        return innerContainer;
    }

    public BlobContainer getShardContainer() {
        return objectStoreService.getBlobContainer(shardId, primaryTerm);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(closeables);
    }
}

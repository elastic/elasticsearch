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

package org.elasticsearch.xpack.stateless.test;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
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
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.action.FetchShardCommitsInUseAction;
import org.elasticsearch.xpack.stateless.action.NewCommitNotificationResponse;
import org.elasticsearch.xpack.stateless.action.TransportFetchShardCommitsInUseAction;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessClusterConsistencyService;
import org.elasticsearch.xpack.stateless.cluster.coordination.StatelessElectionStrategy;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.utils.TransferableCloseables;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService.BUCKET_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a test harness that sets up a collection of stateless components.
 * Tests can use, or add to, what it provides to set up stateless component unit tests.
 */
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
    public final IndicesService indicesService;
    public final StatelessCommitService commitService;
    public final NodeEnvironment nodeEnvironment;
    public final ThreadPool threadPool;

    public final StatelessElectionStrategy electionStrategy;
    public final StatelessSharedBlobCacheService sharedCacheService;
    public final CacheBlobReaderService cacheBlobReaderService;
    public final SharedBlobCacheWarmingService warmingService;
    public final TelemetryProvider telemetryProvider;
    public final StatelessOnlinePrewarmingService onlinePrewarmingService;
    public final RecordingMeterRegistry meterRegistry;
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
        this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
    }

    public FakeStatelessNode(
        Function<Settings, Environment> environmentSupplier,
        CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
        NamedXContentRegistry xContentRegistry,
        long primaryTerm,
        ProjectResolver projectResolver
    ) throws IOException {
        this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, primaryTerm, projectResolver, null);
    }

    @SuppressWarnings("this-escape")
    public FakeStatelessNode(
        Function<Settings, Environment> environmentSupplier,
        CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
        NamedXContentRegistry xContentRegistry,
        long primaryTerm,
        ProjectResolver projectResolver,
        RecordingMeterRegistry meterRegistry
    ) throws IOException {
        this.primaryTerm = primaryTerm;
        DiscoveryNodeUtils.create(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Map.of(),
            Set.copyOf(ESTestCase.randomSubsetOf(DiscoveryNodeRole.roles()))
        );
        node = DiscoveryNodeUtils.create("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        repoPath = LuceneTestCase.createTempDir();
        pathHome = LuceneTestCase.createTempDir().toAbsolutePath();
        nodeSettings = nodeSettings();
        clusterSettings = new ClusterSettings(
            nodeSettings,
            Set.copyOf(
                Stream.concat(
                    BUILT_IN_CLUSTER_SETTINGS.stream(),
                    Stream.of(
                        ServerlessSharedSettings.BOOST_WINDOW_SETTING,
                        ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
                        SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP,
                        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING,
                        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING
                    )
                ).toList()
            )
        );
        environment = environmentSupplier.apply(nodeSettings);
        telemetryProvider = TelemetryProvider.NOOP;

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
        Path dataPath = Objects.requireNonNull(environment.dataDirs()[0]);
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

            threadPool = new TestThreadPool("test", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
            localCloseables.add(() -> TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

            transport = localCloseables.add(new MockTransport());
            clusterService = localCloseables.add(createClusterService());
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadata, false))
                    .build()
            );
            client = createClient(nodeSettings, threadPool);
            nodeEnvironment = nodeEnvironmentSupplier.apply(nodeSettings);
            localCloseables.add(nodeEnvironment);
            sharedCacheService = createCacheService(nodeEnvironment, nodeSettings, threadPool, meterRegistry);
            this.meterRegistry = meterRegistry;
            localCloseables.add(sharedCacheService);
            cacheBlobReaderService = createCacheBlobReaderService(sharedCacheService);

            transportService = transport.createTransportService(
                nodeSettings,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> node,
                null,
                Set.of()
            );

            repoService = createRepositoryService(xContentRegistry);

            transportService.start();
            transportService.acceptIncomingRequests();
            localCloseables.add(transportService::stop);

            objectStoreService = new ObjectStoreService(nodeSettings, repoService, threadPool, clusterService, projectResolver);
            if (projectResolver.supportsMultipleProjects()) {
                clusterService.addStateApplier(objectStoreService);
            }
            objectStoreService.start();
            localCloseables.add(objectStoreService);
            indicesService = mock(IndicesService.class);
            electionStrategy = new StatelessElectionStrategy(objectStoreService::getClusterStateBlobContainer, threadPool);
            var consistencyService = new StatelessClusterConsistencyService(clusterService, electionStrategy, threadPool, nodeSettings);
            commitCleaner = createCommitCleaner(consistencyService, threadPool, objectStoreService);
            localCloseables.add(commitCleaner);
            warmingService = new SharedBlobCacheWarmingService(sharedCacheService, threadPool, telemetryProvider, clusterSettings);
            onlinePrewarmingService = new StatelessOnlinePrewarmingService(
                nodeSettings,
                threadPool,
                sharedCacheService,
                telemetryProvider.getMeterRegistry()
            );
            commitService = createCommitService();
            commitService.start();
            commitService.register(
                shardId,
                getPrimaryTerm(),
                () -> false,
                () -> MappingLookup.EMPTY,
                (checkpoint, gcpListener, timeout) -> {
                    gcpListener.accept(Long.MAX_VALUE, null);
                },
                () -> {}
            );
            localCloseables.add(commitService);
            indexingDirectory = localCloseables.add(
                new IndexDirectory(
                    new FsDirectoryFactory().newDirectory(indexSettings, indexingShardPath),
                    new IndexBlobStoreCacheDirectory(sharedCacheService, shardId),
                    commitService::onGenerationalFileDeletion,
                    true
                )
            );
            indexingStore = localCloseables.add(new Store(shardId, indexSettings, indexingDirectory, new DummyShardLock(shardId)));
            indexingDirectory.getBlobStoreCacheDirectory().setBlobContainer(term -> getBlobContainer(objectStoreService, shardId, term));
            searchDirectory = localCloseables.add(
                createSearchDirectory(sharedCacheService, shardId, cacheBlobReaderService, new AtomicMutableObjectStoreUploadTracker())
            );
            searchDirectory.setBlobContainer(term -> getBlobContainer(objectStoreService, shardId, term));
            searchStore = localCloseables.add(new Store(shardId, indexSettings, searchDirectory, new DummyShardLock(shardId)));

            closeables = localCloseables.transfer();
        }
    }

    protected RepositoriesService createRepositoryService(NamedXContentRegistry xContentRegistry) {
        return new RepositoriesService(
            nodeSettings,
            clusterService,
            Map.of(FsRepository.TYPE, (projectId, metadata) -> createFsRepository(xContentRegistry, projectId, metadata)),
            Map.of(),
            threadPool,
            client,
            List.of(),
            SnapshotMetrics.NOOP
        );
    }

    protected FsRepository createFsRepository(NamedXContentRegistry xContentRegistry, ProjectId projectId, RepositoryMetadata metadata) {
        return new FsRepository(
            projectId,
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
                final Path locationFile = environment.resolveRepoDir(location);
                return new FsBlobStore(bufferSize, locationFile, isReadOnly()) {
                    @Override
                    public BlobContainer blobContainer(BlobPath path) {
                        return wrapBlobContainer(path, super.blobContainer(path));
                    }
                };
            }
        };
    }

    protected BlobContainer getBlobContainer(ObjectStoreService objectStoreService, ShardId shardId, long term) {
        return objectStoreService.getProjectBlobContainer(shardId, term);
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
        return createCacheService(nodeEnvironment, settings, threadPool, null);
    }

    protected StatelessSharedBlobCacheService createCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        return TestUtils.newCacheService(nodeEnvironment, settings, threadPool, meterRegistry);
    }

    protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
        return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool);
    }

    public List<StatelessCommitRef> generateIndexCommits(int commitsNumber) throws IOException {
        return generateIndexCommits(commitsNumber, false, true, generation -> {});
    }

    public List<StatelessCommitRef> generateIndexCommitsWithoutMergeOrDeletion(int commitsNumber) throws IOException {
        return generateIndexCommits(commitsNumber, false, false, generation -> {});
    }

    public List<StatelessCommitRef> generateIndexCommits(int commitsNumber, boolean merge) throws IOException {
        return generateIndexCommits(commitsNumber, merge, true, generation -> {});
    }

    /**
     * Generates {@code commitsNumber} commits where each commit is composed of 1 or more segments whose cumulative files sizes is larger
     * than {@code minSize}.
     */
    public List<StatelessCommitRef> generateIndexCommitsWithMinSegmentSize(int commitsNumber, long minSize) throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        Set<String> previousCommit;
        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        indexWriterConfig.setRAMBufferSizeMB(1.0);
        var indexDirectory = IndexDirectory.unwrapDirectory(indexingStore.directory());
        try (var indexWriter = new IndexWriter(indexDirectory, indexWriterConfig)) {
            try (var indexReader = DirectoryReader.open(indexingStore.directory())) {
                previousCommit = new HashSet<>(indexReader.getIndexCommit().getFileNames());
            }
            for (int i = 0; i < commitsNumber; i++) {
                final long initialSizeInBytes = indexDirectory.estimateSizeInBytes();

                while (indexDirectory.estimateSizeInBytes() - initialSizeInBytes < minSize) {
                    // generate a segment with files ~4KiB
                    for (int doc = 0; doc < 1024; doc++) {
                        LuceneDocument document = new LuceneDocument();
                        document.add(new StringField("bytes", new BytesRef(ESTestCase.randomByteArrayOfLength(1)), Field.Store.NO));
                        indexWriter.addDocument(document.getFields());
                    }
                    indexWriter.flush();
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
                        new Engine.IndexCommitRef(indexCommit, () -> {}),
                        additionalFiles,
                        primaryTerm,
                        0,
                        0
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }
        return commits;
    }

    public List<StatelessCommitRef> generateIndexCommits(
        int commitsNumber,
        boolean merge,
        boolean includeDeletions,
        LongConsumer onCommitClosed
    ) throws IOException {
        var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        indexWriterConfig.setMergePolicy(new TieredMergePolicy().setSegmentsPerTier(10)); // lucene 10.3 changed default to 8
        return generateIndexCommits(commitsNumber, merge, includeDeletions, onCommitClosed, (commitNumber) -> {
            LuceneDocument document = new LuceneDocument();
            document.add(new KeywordField("field0", "term", Field.Store.YES));
            return document;
        }, indexWriterConfig);
    }

    public List<StatelessCommitRef> generateIndexCommits(
        int commitsNumber,
        boolean merge,
        boolean includeDeletions,
        LongConsumer onCommitClosed,
        Function<Integer, LuceneDocument> newDocForCommitFunction,
        IndexWriterConfig indexWriterConfig
    ) throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        IndexCommit previousCommit;
        try (var indexWriter = new IndexWriter(indexingStore.directory(), indexWriterConfig)) {
            try (var indexReader = DirectoryReader.open(indexingStore.directory())) {
                previousCommit = indexReader.getIndexCommit();
            }
            for (int i = 0; i < commitsNumber; i++) {
                LuceneDocument document = newDocForCommitFunction.apply(i);
                indexWriter.addDocument(document.getFields());
                if (includeDeletions && ESTestCase.randomBoolean()) {
                    String deleteId = ESTestCase.randomAlphaOfLength(10);
                    final ParsedDocument tombstone = ParsedDocument.deleteTombstone(indexSettings.seqNoIndexOptions(), deleteId);
                    LuceneDocument delete = tombstone.docs().get(0);
                    NumericDocValuesField field = Lucene.newSoftDeletesField();
                    delete.add(field);
                    indexWriter.softUpdateDocument(
                        new Term(IdFieldMapper.NAME, Uid.encodeId(deleteId)),
                        delete.getFields(),
                        Lucene.newSoftDeletesField()
                    );
                }
                if (merge) {
                    indexWriter.forceMerge(1, true);
                }
                indexWriter.setLiveCommitData(Map.of(SequenceNumbers.MAX_SEQ_NO, Integer.toString(i)).entrySet());
                indexWriter.commit();
                try (var indexReader = DirectoryReader.open(indexingStore.directory())) {
                    IndexCommit indexCommit = indexReader.getIndexCommit();
                    final Set<String> additionalFiles = Lucene.additionalFileNames(previousCommit, indexCommit);
                    previousCommit = indexCommit;

                    StatelessCommitRef statelessCommitRef = new StatelessCommitRef(
                        shardId,
                        new Engine.IndexCommitRef(indexCommit, () -> onCommitClosed.accept(indexCommit.getGeneration())),
                        additionalFiles,
                        primaryTerm,
                        0,
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
            clusterService,
            objectStoreService,
            indicesService,
            () -> clusterService.localNode().getEphemeralId(),
            this::getShardRoutingTable,
            clusterService.threadPool(),
            client,
            commitCleaner,
            sharedCacheService,
            warmingService,
            telemetryProvider
        );
    }

    protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
        IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.shardId()).thenReturn(shardId);
        return Optional.of(routingTable);
    }

    protected ClusterService createClusterService() {
        // TODO: stateless enabled should be part of nodeSettings
        final Settings settings = Settings.builder().put(nodeSettings).put(StatelessPlugin.STATELESS_ENABLED.getKey(), true).build();
        return ClusterServiceUtils.createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            settings,
            new ClusterSettings(settings, BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
        return new NoOpNodeClient(threadPool) {
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assert action == TransportNewCommitNotificationAction.TYPE || action == TransportFetchShardCommitsInUseAction.TYPE;
                if (action == TransportNewCommitNotificationAction.TYPE) {
                    ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(new NewCommitNotificationResponse(Set.of()));
                } else if (action == TransportFetchShardCommitsInUseAction.TYPE) {
                    ((ActionListener<FetchShardCommitsInUseAction.Response>) listener).onResponse(
                        new FetchShardCommitsInUseAction.Response(new ClusterName("fake-cluster-name"), List.of(), List.of())
                    );
                } else {
                    assert false : "Unexpected request type: " + action;
                }
            }
        };
    }

    protected long getPrimaryTerm() {
        return primaryTerm;
    }

    public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
        return innerContainer;
    }

    public BlobContainer getShardContainer() {
        return objectStoreService.getProjectBlobContainer(shardId, primaryTerm);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(closeables);
    }
}

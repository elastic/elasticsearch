/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.StorageClass;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.InvalidRepository;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.VerifyNodeRepositoryCoordinationAction;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class S3RepositoryTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private ProjectId projectId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(), boundAddress.publishAddress()),
            null,
            Collections.emptySet()
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        projectId = randomProjectIdOrDefault();
        if (ProjectId.DEFAULT.equals(projectId) == false) {
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).build()
            );
        }
        DiscoveryNode localNode = DiscoveryNodeUtils.builder("local").name("local").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow());
        client.initialize(
            Map.of(
                VerifyNodeRepositoryCoordinationAction.TYPE,
                new VerifyNodeRepositoryCoordinationAction.LocalAction(
                    new ActionFilters(Set.of()),
                    transportService,
                    clusterService,
                    client
                )
            ),
            transportService.getTaskManager(),
            localNode::getId,
            transportService.getLocalNodeConnection(),
            null
        );
        Map<String, Repository.Factory> s3Registry = Map.of(S3Repository.TYPE, (pid, metadata) -> createS3Repo(pid, metadata));
        repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            s3Registry,
            s3Registry,
            threadPool,
            client,
            List.of(),
            SnapshotMetrics.NOOP
        );
        clusterService.start();
        repositoriesService.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        repositoriesService.stop();
        clusterService.stop();
        threadPool.shutdownNow();
    }

    private static class DummyS3Client implements S3Client {

        @Override
        public void close() {
            // TODO check is closed
        }

        @Override
        public String serviceName() {
            return "DummyS3Client";
        }
    }

    private static class DummyS3Service extends S3Service {

        DummyS3Service(
            Environment environment,
            ClusterService clusterService,
            ProjectResolver projectResolver,
            ResourceWatcherService resourceWatcherService
        ) {
            super(environment, clusterService, projectResolver, resourceWatcherService, () -> Region.of(randomIdentifier()));
        }

        @Override
        public AmazonS3Reference client(@Nullable ProjectId projectId, RepositoryMetadata repositoryMetadata) {
            return new AmazonS3Reference(new DummyS3Client(), mock(SdkHttpClient.class));
        }

        @Override
        public void refreshAndClearCache(Map<String, S3ClientSettings> clientsSettings) {}

        @Override
        public void doClose() {
            // nothing to clean up
        }
    }

    public void testInvalidChunkBufferSizeSettings() {
        // chunk < buffer should fail
        final Settings s1 = bufferAndChunkSettings(10, 5);
        final Exception e1 = expectThrows(RepositoryException.class, () -> createS3Repo(getRepositoryMetadata(s1)));
        assertThat(e1.getMessage(), containsString("chunk_size (5mb) can't be lower than buffer_size (10mb)"));
        // chunk > buffer should pass
        final Settings s2 = bufferAndChunkSettings(5, 10);
        createS3Repo(getRepositoryMetadata(s2)).close();
        // chunk = buffer should pass
        final Settings s3 = bufferAndChunkSettings(5, 5);
        createS3Repo(getRepositoryMetadata(s3)).close();
        // buffer < 5mb should fail
        final Settings s4 = bufferAndChunkSettings(4, 10);
        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> createS3Repo(getRepositoryMetadata(s4)).close()
        );
        assertThat(e2.getMessage(), containsString("failed to parse value [4mb] for setting [buffer_size], must be >= [5mb]"));
        final Settings s5 = bufferAndChunkSettings(5, 6000000);
        final IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> createS3Repo(getRepositoryMetadata(s5)).close()
        );
        assertThat(e3.getMessage(), containsString("failed to parse value [6000000mb] for setting [chunk_size], must be <= [5tb]"));
    }

    private Settings bufferAndChunkSettings(long buffer, long chunk) {
        return Settings.builder()
            .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
            .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), ByteSizeValue.of(buffer, ByteSizeUnit.MB).getStringRep())
            .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), ByteSizeValue.of(chunk, ByteSizeUnit.MB).getStringRep())
            .build();
    }

    private RepositoryMetadata getRepositoryMetadata(Settings settings) {
        return new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(settings).build());
    }

    public void testBasePathSetting() {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.BASE_PATH_SETTING.getKey(), "foo/bar")
                .build()
        );
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertEquals("foo/bar/", s3repo.basePath().buildAsString());
        }
    }

    public void testDefaultBufferSize() {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "bucket").build()
        );
        try (S3Repository s3repo = createS3Repo(metadata)) {
            assertThat(s3repo.getBlobStore(), is(nullValue()));
            s3repo.start();
            final long defaultBufferSize = ((S3BlobStore) s3repo.blobStore()).bufferSizeInBytes();
            assertThat(s3repo.getBlobStore(), not(nullValue()));
            assertThat(defaultBufferSize, Matchers.lessThanOrEqualTo(100L * 1024 * 1024));
            assertThat(defaultBufferSize, Matchers.greaterThanOrEqualTo(5L * 1024 * 1024));
        }
    }

    public void testMissingBucketName() {
        final var metadata = new RepositoryMetadata("repo", "mock", Settings.EMPTY);
        assertThrows(IllegalArgumentException.class, () -> createS3Repo(metadata));
    }

    public void testEmptyBucketName() {
        final var settings = Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "").build();
        final var metadata = new RepositoryMetadata("repo", "mock", settings);
        assertThrows(IllegalArgumentException.class, () -> createS3Repo(metadata));
    }

    private S3Repository createS3Repo(RepositoryMetadata metadata) {
        return createS3Repo(ProjectId.DEFAULT, metadata);
    }

    private S3Repository createS3Repo(ProjectId pid, RepositoryMetadata metadata) {
        return new S3Repository(
            pid,
            metadata,
            NamedXContentRegistry.EMPTY,
            new DummyS3Service(
                mock(Environment.class),
                ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                mock(ResourceWatcherService.class)
            ),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            S3RepositoriesMetrics.NOOP,
            SnapshotMetrics.NOOP
        );
    }

    public void testAnalysisFailureDetail() {
        try (
            S3Repository s3repo = createS3Repo(
                new RepositoryMetadata("dummy-repo", "mock", Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "bucket").build())
            )
        ) {
            assertThat(
                s3repo.getAnalysisFailureExtraDetail(),
                allOf(
                    containsString("storage system underneath this repository behaved incorrectly"),
                    containsString("incorrectly claims to be S3-compatible"),
                    containsString("report this incompatibility to your storage supplier"),
                    containsString("unless you can demonstrate that the same issue exists when using a genuine AWS S3 repository"),
                    containsString(ReferenceDocs.SNAPSHOT_REPOSITORY_ANALYSIS.toString()),
                    containsString(ReferenceDocs.S3_COMPATIBLE_REPOSITORIES.toString())
                )
            );
        }
    }

    // ensures that chunkSize is limited to chunk_size setting, when buffer_size * parts_num is bigger
    public void testChunkSizeLimit() {
        var meta = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), "1GB")
                .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), "100MB")
                .put(S3Repository.MAX_MULTIPART_PARTS.getKey(), 10_000) // ~1TB
                .build()
        );
        try (var repo = createS3Repo(meta)) {
            assertEquals(ByteSizeValue.ofGb(1), repo.chunkSize());
        }
    }

    // ensures that chunkSize is limited to buffer_size * parts_num, when chunk_size setting is bigger
    public void testPartsNumLimit() {
        var meta = new RepositoryMetadata(
            "dummy-repo",
            "mock",
            Settings.builder()
                .put(S3Repository.BUCKET_SETTING.getKey(), "bucket")
                .put(S3Repository.CHUNK_SIZE_SETTING.getKey(), "5TB")
                .put(S3Repository.BUFFER_SIZE_SETTING.getKey(), "100MB")
                .put(S3Repository.MAX_MULTIPART_PARTS.getKey(), 10_000)
                .build()
        );
        try (var repo = createS3Repo(meta)) {
            assertEquals(ByteSizeValue.ofMb(1_000_000), repo.chunkSize());
        }
    }

    public void testStorageClassFallbackMatrix() {
        // Case 1: no storage-class settings → everything resolves to STANDARD.
        assertResolvedStorageClasses(
            storageClassSettings(null, null, null),
            StorageClass.STANDARD,
            StorageClass.STANDARD,
            StorageClass.STANDARD
        );

        // Case 2: only legacy storage_class set → existing behaviour preserved, all three purposes get it.
        assertResolvedStorageClasses(
            storageClassSettings("onezone_ia", null, null),
            StorageClass.ONEZONE_IA,
            StorageClass.ONEZONE_IA,
            StorageClass.ONEZONE_IA
        );

        // Case 3: only data_storage_class set → data uses it; metadata + others fall back to STANDARD (storage_class unset).
        assertResolvedStorageClasses(
            storageClassSettings(null, "onezone_ia", null),
            StorageClass.STANDARD,
            StorageClass.ONEZONE_IA,
            StorageClass.STANDARD
        );

        // Case 4: only metadata_storage_class set → metadata uses it; data + others fall back to STANDARD (storage_class unset).
        assertResolvedStorageClasses(
            storageClassSettings(null, null, "onezone_ia"),
            StorageClass.STANDARD,
            StorageClass.STANDARD,
            StorageClass.ONEZONE_IA
        );

        // Case 5: storage_class + metadata_storage_class set → data falls back to storage_class; metadata uses its override.
        assertResolvedStorageClasses(
            storageClassSettings("standard_ia", null, "onezone_ia"),
            StorageClass.STANDARD_IA,
            StorageClass.STANDARD_IA,
            StorageClass.ONEZONE_IA
        );

        // Case 6: storage_class + data_storage_class set → metadata falls back to storage_class; data uses its override.
        assertResolvedStorageClasses(
            storageClassSettings("standard_ia", "onezone_ia", null),
            StorageClass.STANDARD_IA,
            StorageClass.ONEZONE_IA,
            StorageClass.STANDARD_IA
        );
    }

    public void testInvalidDataStorageClassFailsAtConstruction() {
        RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> createS3Repo(getRepositoryMetadata(storageClassSettings(null, "whatever", null)))
        );
        assertThat(ex.getMessage(), containsString(S3Repository.DATA_STORAGE_CLASS_SETTING.getKey()));
        assertThat(ex.getCause(), Matchers.instanceOf(BlobStoreException.class));
        assertThat(ex.getCause().getMessage(), equalTo("`whatever` is not a known S3 Storage Class."));
    }

    public void testInvalidMetadataStorageClassFailsAtConstruction() {
        RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> createS3Repo(getRepositoryMetadata(storageClassSettings(null, null, "whatever")))
        );
        assertThat(ex.getMessage(), containsString(S3Repository.METADATA_STORAGE_CLASS_SETTING.getKey()));
        assertThat(ex.getCause(), Matchers.instanceOf(BlobStoreException.class));
        assertThat(ex.getCause().getMessage(), equalTo("`whatever` is not a known S3 Storage Class."));
    }

    public void testInvalidDataStorageClassProducesInvalidRepository() {
        final String repoName = randomAlphaOfLength(10);
        repositoriesService.applyClusterState(
            new ClusterChangedEvent("test", clusterStateWithS3Repo(repoName, storageClassSettings(null, "whatever", null)), emptyState())
        );
        assertThat(repositoriesService.repository(projectId, repoName), instanceOf(InvalidRepository.class));
    }

    public void testInvalidMetadataStorageClassProducesInvalidRepository() {
        final String repoName = randomAlphaOfLength(10);
        repositoriesService.applyClusterState(
            new ClusterChangedEvent("test", clusterStateWithS3Repo(repoName, storageClassSettings(null, null, "whatever")), emptyState())
        );
        assertThat(repositoriesService.repository(projectId, repoName), instanceOf(InvalidRepository.class));
    }

    public void testResolveStorageClassRouting() {
        // Three distinct classes so we can tell which lookup fires for which purpose.
        try (var repo = createS3Repo(getRepositoryMetadata(storageClassSettings("standard_ia", "onezone_ia", "reduced_redundancy")))) {
            repo.start();
            final var blobStore = (S3BlobStore) repo.blobStore();
            assertThat(blobStore.resolveStorageClass(OperationPurpose.SNAPSHOT_DATA), equalTo(StorageClass.ONEZONE_IA));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.SNAPSHOT_METADATA), equalTo(StorageClass.REDUCED_REDUNDANCY));
            // All "other" purposes fall through to the legacy storage_class.
            assertThat(blobStore.resolveStorageClass(OperationPurpose.REPOSITORY_ANALYSIS), equalTo(StorageClass.STANDARD_IA));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.CLUSTER_STATE), equalTo(StorageClass.STANDARD_IA));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.INDICES), equalTo(StorageClass.STANDARD_IA));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.TRANSLOG), equalTo(StorageClass.STANDARD_IA));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.RESHARDING), equalTo(StorageClass.STANDARD_IA));
        }
    }

    private void assertResolvedStorageClasses(
        Settings settings,
        StorageClass expectedDefault,
        StorageClass expectedData,
        StorageClass expectedMetadata
    ) {
        try (var repo = createS3Repo(getRepositoryMetadata(settings))) {
            repo.start();
            final var blobStore = (S3BlobStore) repo.blobStore();
            assertThat(blobStore.resolveStorageClass(OperationPurpose.INDICES), equalTo(expectedDefault));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.SNAPSHOT_DATA), equalTo(expectedData));
            assertThat(blobStore.resolveStorageClass(OperationPurpose.SNAPSHOT_METADATA), equalTo(expectedMetadata));
        }
    }

    private static Settings storageClassSettings(@Nullable String legacy, @Nullable String data, @Nullable String metadata) {
        final Settings.Builder builder = Settings.builder().put(S3Repository.BUCKET_SETTING.getKey(), "bucket");
        if (legacy != null) {
            builder.put(S3Repository.FALLBACK_STORAGE_CLASS_SETTING.getKey(), legacy);
        }
        if (data != null) {
            builder.put(S3Repository.DATA_STORAGE_CLASS_SETTING.getKey(), data);
        }
        if (metadata != null) {
            builder.put(S3Repository.METADATA_STORAGE_CLASS_SETTING.getKey(), metadata);
        }
        return builder.build();
    }

    private ClusterState clusterStateWithS3Repo(String repoName, Settings repoSettings) {
        return ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(
                            Collections.singletonList(new RepositoryMetadata(repoName, S3Repository.TYPE, repoSettings))
                        )
                    )
            )
            .build();
    }

    private static ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }
}

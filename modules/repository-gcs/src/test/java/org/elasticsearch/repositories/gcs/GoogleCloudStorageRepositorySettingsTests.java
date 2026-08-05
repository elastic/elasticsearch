/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.InvalidRepository;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class GoogleCloudStorageRepositorySettingsTests extends ESTestCase {

    private TestThreadPool repositoryServiceThreadPool;
    private ClusterService repositoryServiceClusterService;
    private RepositoriesService repositoriesService;
    private ProjectId repositoryServiceProjectId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        repositoryServiceThreadPool = new TestThreadPool(getClass().getName());
        repositoryServiceClusterService = ClusterServiceUtils.createClusterService(repositoryServiceThreadPool);
        repositoryServiceProjectId = randomProjectIdOrDefault();
        if (ProjectId.DEFAULT.equals(repositoryServiceProjectId) == false) {
            ClusterServiceUtils.setState(
                repositoryServiceClusterService,
                ClusterState.builder(repositoryServiceClusterService.state())
                    .putProjectMetadata(ProjectMetadata.builder(repositoryServiceProjectId))
                    .build()
            );
        }
        final NodeClient client = new NodeClient(Settings.EMPTY, repositoryServiceThreadPool, TestProjectResolvers.alwaysThrow());
        repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            repositoryServiceClusterService,
            Map.of(GoogleCloudStorageRepository.TYPE, this::createGcsRepo),
            Map.of(),
            repositoryServiceThreadPool,
            client,
            List.of(),
            SnapshotMetrics.NOOP
        );
        repositoryServiceClusterService.start();
        repositoriesService.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        repositoriesService.stop();
        repositoryServiceClusterService.stop();
        repositoryServiceThreadPool.shutdownNow();
    }

    private GoogleCloudStorageRepository createGcsRepo(ProjectId projectId, RepositoryMetadata metadata) {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(metadata.settings())
            .build();
        return new GoogleCloudStorageRepository(
            projectId,
            new RepositoryMetadata(metadata.name(), metadata.type(), internalSettings),
            NamedXContentRegistry.EMPTY,
            mock(GoogleCloudStorageService.class),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(internalSettings, new ClusterSettings(internalSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            new GcsRepositoryStatsCollector(),
            SnapshotMetrics.NOOP
        );
    }

    private ClusterState clusterStateWithGcsRepo(String repoName, Settings repoSettings) {
        return ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(
                ProjectMetadata.builder(repositoryServiceProjectId)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(
                            Collections.singletonList(new RepositoryMetadata(repoName, GoogleCloudStorageRepository.TYPE, repoSettings))
                        )
                    )
            )
            .build();
    }

    private static ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private GoogleCloudStorageRepository gcsRepository(Settings settings) {
        // A valid bucket must always be configured, otherwise construction fails for that reason instead of the one under test.
        Settings repoSettings = Settings.builder()
            .put(GoogleCloudStorageRepository.BUCKET.getKey(), randomIdentifier())
            .put(settings)
            .build();
        return createGcsRepo(randomProjectIdOrDefault(), new RepositoryMetadata("foo", GoogleCloudStorageRepository.TYPE, repoSettings));
    }

    public void testInvalidDataStorageClassFailsAtConstruction() {
        RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> gcsRepository(Settings.builder().put(GoogleCloudStorageRepository.DATA_STORAGE_CLASS.getKey(), "whatever").build())
        );
        assertThat(ex.getMessage(), containsString(GoogleCloudStorageRepository.DATA_STORAGE_CLASS.getKey()));
        assertThat(ex.getCause(), instanceOf(BlobStoreException.class));
        assertThat(ex.getCause().getMessage(), equalTo("`whatever` is not an allowed GCS Storage Class."));
    }

    public void testInvalidMetadataStorageClassFailsAtConstruction() {
        RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> gcsRepository(Settings.builder().put(GoogleCloudStorageRepository.METADATA_STORAGE_CLASS.getKey(), "whatever").build())
        );
        assertThat(ex.getMessage(), containsString(GoogleCloudStorageRepository.METADATA_STORAGE_CLASS.getKey()));
        assertThat(ex.getCause(), instanceOf(BlobStoreException.class));
        assertThat(ex.getCause().getMessage(), equalTo("`whatever` is not an allowed GCS Storage Class."));
    }

    public void testInvalidDataStorageClassProducesInvalidRepository() {
        final String repoName = randomAlphaOfLength(10);
        repositoriesService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                clusterStateWithGcsRepo(
                    repoName,
                    Settings.builder()
                        .put(GoogleCloudStorageRepository.BUCKET.getKey(), randomIdentifier())
                        .put(GoogleCloudStorageRepository.DATA_STORAGE_CLASS.getKey(), "whatever")
                        .build()
                ),
                emptyState()
            )
        );
        assertThat(repositoriesService.repository(repositoryServiceProjectId, repoName), instanceOf(InvalidRepository.class));
    }

    public void testInvalidMetadataStorageClassProducesInvalidRepository() {
        final String repoName = randomAlphaOfLength(10);
        repositoriesService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                clusterStateWithGcsRepo(
                    repoName,
                    Settings.builder()
                        .put(GoogleCloudStorageRepository.BUCKET.getKey(), randomIdentifier())
                        .put(GoogleCloudStorageRepository.METADATA_STORAGE_CLASS.getKey(), "whatever")
                        .build()
                ),
                emptyState()
            )
        );
        assertThat(repositoriesService.repository(repositoryServiceProjectId, repoName), instanceOf(InvalidRepository.class));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.InvalidRepository;
import org.elasticsearch.repositories.RepositoriesMetrics;
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

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class AzureRepositorySettingsTests extends ESTestCase {

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
            Map.of(AzureRepository.TYPE, (pid, metadata) -> createAzureRepo(pid, metadata)),
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

    private AzureRepository createAzureRepo(ProjectId projectId, RepositoryMetadata metadata) {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(metadata.settings())
            .build();
        return new AzureRepository(
            projectId,
            new RepositoryMetadata(metadata.name(), metadata.type(), internalSettings),
            NamedXContentRegistry.EMPTY,
            mock(AzureStorageService.class),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(internalSettings, new ClusterSettings(internalSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            RepositoriesMetrics.NOOP,
            SnapshotMetrics.NOOP
        );
    }

    private ClusterState clusterStateWithAzureRepo(String repoName, Settings repoSettings) {
        return ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(
                ProjectMetadata.builder(repositoryServiceProjectId)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(
                            Collections.singletonList(new RepositoryMetadata(repoName, AzureRepository.TYPE, repoSettings))
                        )
                    )
            )
            .build();
    }

    private static ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private AzureRepository azureRepository(Settings settings) {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings)
            .build();
        final ProjectId projectId = randomProjectIdOrDefault();
        final AzureRepository azureRepository = new AzureRepository(
            projectId,
            new RepositoryMetadata("foo", "azure", internalSettings),
            NamedXContentRegistry.EMPTY,
            mock(AzureStorageService.class),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            RepositoriesMetrics.NOOP,
            SnapshotMetrics.NOOP
        );
        assertThat(azureRepository.getProjectId(), equalTo(projectId));
        assertThat(azureRepository.getBlobStore(), is(nullValue()));
        return azureRepository;
    }

    public void testReadonlyDefault() {
        assertThat(azureRepository(Settings.EMPTY).isReadOnly(), is(false));
    }

    public void testReadonlyDefaultAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder().put(READONLY_SETTING_KEY, true).build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithPrimaryOnly() {
        assertThat(
            azureRepository(
                Settings.builder().put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name()).build()
            ).isReadOnly(),
            is(false)
        );
    }

    public void testReadonlyWithPrimaryOnlyAndReadonlyOn() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
                    .put(READONLY_SETTING_KEY, true)
                    .build()
            ).isReadOnly(),
            is(true)
        );
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOn() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
                    .put(READONLY_SETTING_KEY, true)
                    .build()
            ).isReadOnly(),
            is(true)
        );
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOff() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
                    .put(READONLY_SETTING_KEY, false)
                    .build()
            ).isReadOnly(),
            is(false)
        );
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOn() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
                    .put(READONLY_SETTING_KEY, true)
                    .build()
            ).isReadOnly(),
            is(true)
        );
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOff() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
                    .put(READONLY_SETTING_KEY, false)
                    .build()
            ).isReadOnly(),
            is(false)
        );
    }

    public void testInvalidDataAccessTierFailsAtConstruction() {
        RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> azureRepository(Settings.builder().put(AzureRepository.Repository.DATA_ACCESS_TIER_SETTING.getKey(), "whatever").build())
        );
        assertThat(ex.getMessage(), containsString(AzureRepository.Repository.DATA_ACCESS_TIER_SETTING.getKey()));
        assertThat(ex.getCause(), instanceOf(BlobStoreException.class));
        assertThat(ex.getCause().getMessage(), equalTo("`whatever` is not an allowed Azure Access Tier."));
    }

    public void testInvalidMetadataAccessTierFailsAtConstruction() {
        RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> azureRepository(
                Settings.builder().put(AzureRepository.Repository.METADATA_ACCESS_TIER_SETTING.getKey(), "whatever").build()
            )
        );
        assertThat(ex.getMessage(), containsString(AzureRepository.Repository.METADATA_ACCESS_TIER_SETTING.getKey()));
        assertThat(ex.getCause(), instanceOf(BlobStoreException.class));
        assertThat(ex.getCause().getMessage(), equalTo("`whatever` is not an allowed Azure Access Tier."));
    }

    public void testInvalidDataAccessTierProducesInvalidRepository() {
        final String repoName = randomAlphaOfLength(10);
        repositoriesService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                clusterStateWithAzureRepo(
                    repoName,
                    Settings.builder().put(AzureRepository.Repository.DATA_ACCESS_TIER_SETTING.getKey(), "whatever").build()
                ),
                emptyState()
            )
        );
        assertThat(repositoriesService.repository(repositoryServiceProjectId, repoName), instanceOf(InvalidRepository.class));
    }

    public void testInvalidMetadataAccessTierProducesInvalidRepository() {
        final String repoName = randomAlphaOfLength(10);
        repositoriesService.applyClusterState(
            new ClusterChangedEvent(
                "test",
                clusterStateWithAzureRepo(
                    repoName,
                    Settings.builder().put(AzureRepository.Repository.METADATA_ACCESS_TIER_SETTING.getKey(), "whatever").build()
                ),
                emptyState()
            )
        );
        assertThat(repositoriesService.repository(repositoryServiceProjectId, repoName), instanceOf(InvalidRepository.class));
    }

    public void testChunkSize() {
        // default chunk size
        AzureRepository azureRepository = azureRepository(Settings.EMPTY);
        assertEquals(AzureStorageService.MAX_CHUNK_SIZE, azureRepository.chunkSize());

        // chunk size in settings
        int size = randomIntBetween(1, 256);
        azureRepository = azureRepository(Settings.builder().put("chunk_size", size + "mb").build());
        assertEquals(ByteSizeValue.of(size, ByteSizeUnit.MB), azureRepository.chunkSize());

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> azureRepository(Settings.builder().put("chunk_size", "0").build())
        );
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> azureRepository(Settings.builder().put("chunk_size", "-1").build()));
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> azureRepository(Settings.builder().put("chunk_size", "6tb").build()));
        assertEquals(
            "failed to parse value [6tb] for setting [chunk_size], must be <= [" + AzureStorageService.MAX_CHUNK_SIZE.getStringRep() + "]",
            e.getMessage()
        );
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.nio.file.Path;

import static org.elasticsearch.test.ESTestCase.indexSettings;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;

public class TestUtils {

    record RepositoryAndClusterService(Repository repository, ClusterService clusterService) {}

    /** Create a {@link Repository} with a random name **/
    static Repository createRepository(ProjectId projectId, Path tempDir, NamedXContentRegistry xContentRegistry, Index... indices) {
        return createRepositoryAndClusterService(projectId, tempDir, xContentRegistry, indices).repository;
    }

    static RepositoryAndClusterService createRepositoryAndClusterService(
        ProjectId projectId,
        Path tempDir,
        NamedXContentRegistry xContentRegistry,
        Index... indices
    ) {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(projectId, repositoryMetadata);
        if (indices != null && indices.length > 0) {
            createIndices(clusterService, projectId, indices);
        }
        final FsRepository repository = new FsRepository(
            projectId,
            repositoryMetadata,
            createEnvironment(tempDir),
            xContentRegistry,
            clusterService,
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return new RepositoryAndClusterService(repository, clusterService);
    }

    private static void createIndices(ClusterService clusterService, ProjectId projectId, Index... indices) {
        clusterService.submitUnbatchedStateUpdateTask("create-indices", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                var projectBuilder = ProjectMetadata.builder(currentState.metadata().getProject(projectId));
                for (Index index : indices) {
                    projectBuilder.put(
                        IndexMetadata.builder(index.getName())
                            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID()))
                            .build(),
                        false
                    );
                }
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).put(projectBuilder.build()).build())
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    private static Environment createEnvironment(Path tempDir) {
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), tempDir.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
                .build()
        );
    }
}

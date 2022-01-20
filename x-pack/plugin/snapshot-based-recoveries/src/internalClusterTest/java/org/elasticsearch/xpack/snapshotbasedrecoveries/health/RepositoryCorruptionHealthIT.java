/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.health;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.xpack.snapshotbasedrecoveries.SnapshotBasedRecoveriesPlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class RepositoryCorruptionHealthIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SnapshotBasedRecoveriesPlugin.class);
    }

    public void testCorruptedRepository() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNodes(1);

        createIndexWithContent("index");
        final String repository = "corrupted-repository";
        final Path repositoryPath = randomRepoPath();
        createRepository(repository, FsRepository.TYPE, repositoryPath);
        final String snapshot = "snapshot";
        createFullSnapshot(repository, snapshot);

        final long generation = getRepositoryData(repository).getGenId();
        final Path indexN = repositoryPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation);
        final Path renamedIndexN = repositoryPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + (generation + 1));
        Files.move(indexN, renamedIndexN, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        final ActionFuture<CreateSnapshotResponse> future = client().admin()
            .cluster()
            .prepareCreateSnapshot(repository, "other")
            .setWaitForCompletion(true)
            .execute();
        awaitClusterState(
            state -> state.metadata()
                .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                .repository(repository)
                .generation() == RepositoryData.CORRUPTED_REPO_GEN
        );
        expectThrows(SnapshotException.class, future::actionGet);

        final Client client = internalCluster().masterClient();
        GetHealthAction.Response response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();
        assertThat(response.getHealthIndicators().size(), equalTo(1));

        String output = """
                {
                  "timed_out" : false,
                  "status" : "green",
                  "cluster_name" : "...",
                  "impacts" : [
                    [ ]
                  ],
                  "components" : {
                    "snapshots" : [
                      {
                        "indicator" : "repository-corruption",
                        "status" : "red",
                        "description" : "Repository is marked as corrupted",
                        "meta" : {
                          "repository_name" : "corrupted-repository",
                          "repository_uuid" : "uFyvLMewQdKTacDSANQPKw",
                          "corrupted_at" : "2022-01-20T13:52:38.050Z"
                        }
                      }
                    ]
                  }
                }
            """;

        assertAcked(client().admin().cluster().prepareDeleteRepository(repository));
        awaitClusterState(
            state -> state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY).repository(repository) == null
        );

        Files.move(renamedIndexN, indexN, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        createRepository(repository, FsRepository.TYPE, repositoryPath);
        awaitClusterState(
            state -> state.metadata()
                .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                .repository(repository)
                .generation() == generation
        );

        response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();
        assertThat(response.getHealthIndicators().size(), equalTo(0));
    }
}

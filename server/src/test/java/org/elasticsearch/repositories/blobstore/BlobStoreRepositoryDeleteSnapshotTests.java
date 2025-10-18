/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Tests whether we can create and then delete snapshots, even when the repository is deleted and recreated (ES-12539)
 */
public class BlobStoreRepositoryDeleteSnapshotTests extends ESSingleNodeTestCase {

    private static final String TEST_REPO_TYPE = "delete-snapshot-fs";
    private static final String TEST_REPO_NAME = "test-repo";

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DeleteSnapshotFsRepositoryPlugin.class);
    }

    public static class DeleteSnapshotFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Collections.singletonMap(
                TEST_REPO_TYPE,
                (projectId, metadata) -> new FsRepository(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }
    }

    /**
     * Creates M indices each with N snapshots, and deletes them all in one delete snapshot request
     */
    public void testDeleteMultipleSnapshotsConcurrently() {
        final var repoPath = ESIntegTestCase.randomRepoPath(node().settings());

        int numberOfIndices = randomIntBetween(3, 10);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "index-" + i;
            int shardCount = between(1, 3);
            logger.warn("index " + indexName + " has " + shardCount + " shards");
            createIndex(indexName, indexSettings(shardCount, 0).build());
            ensureGreen(indexName);
        }

        // Set up our test repo
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(TEST_REPO_TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );

        int numberOfSnapshots = randomIntBetween(3, 10);
        List<String> snapshotNames = new ArrayList<>();
        for (int i = 0; i < numberOfSnapshots; i++) {
            String snapshotName = "snapshot-" + i;
            snapshotNames.add(snapshotName);
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName)
                .setWaitForCompletion(true)
                .get();
        }

        // We want to avoid deleting all snapshots since this would invoke cleanup code and bulk snapshot deletion
        // which is out of scope of this test
        List<String> snapshotsToDelete = randomSubsetOf(randomIntBetween(1, numberOfSnapshots - 1), snapshotNames);

        assertAcked(
            client().admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotsToDelete.toArray(new String[0]))
                .get()
        );

        // Cleanup
        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));
    }

    /**
     * Creates M indices each with N snapshots, and deletes each snapshot sequentially within its own command
     */
    public void testDeleteSnapshotsSequentially() {
        final var repoPath = ESIntegTestCase.randomRepoPath(node().settings());

        int numberOfIndices = randomIntBetween(3, 10);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "index-" + i;
            createIndex(indexName, indexSettings(between(1, 3), 0).build());
            ensureGreen(indexName);
        }

        // Set up our test repo
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(TEST_REPO_TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );

        int numberOfSnapshots = randomIntBetween(3, 10);
        List<String> snapshotNames = new ArrayList<>();
        for (int i = 0; i < numberOfSnapshots; i++) {
            String snapshotName = "snapshot-" + i;
            snapshotNames.add(snapshotName);
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName)
                .setWaitForCompletion(true)
                .get();
        }

        // We want to avoid deleting all snapshots since this would invoke cleanup code and bulk snapshot deletion
        // which is out of scope of this test
        snapshotNames.removeLast();

        for (String snapshotName : snapshotNames) {
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName).get());
        }

        // Cleanup
        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));
    }

    /**
     * Creates M indices each with N snapshots.
     * The repository is deleted and recreated.
     * The snapshots are deleted sequentially.
     * This tests whether BlobStoreRepository can load a repository instance from disc successfully
     */
    public void testDeleteSnapshotsAfterRepositoryIsDeletedAndRecreated() {
        final var repoPath = ESIntegTestCase.randomRepoPath(node().settings());

        int numberOfIndices = randomIntBetween(3, 10);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = "index-" + i;
            createIndex(indexName, indexSettings(between(1, 3), 0).build());
            ensureGreen(indexName);
        }

        // Set up our test repo
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(TEST_REPO_TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );

        int numberOfSnapshots = randomIntBetween(3, 10);
        List<String> snapshotNames = new ArrayList<>();
        for (int i = 0; i < numberOfSnapshots; i++) {
            String snapshotName = "snapshot-" + i;
            snapshotNames.add(snapshotName);
            client().admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName)
                .setWaitForCompletion(true)
                .get();
        }

        // We want to avoid deleting all snapshots since this would invoke cleanup code and bulk snapshot deletion
        // which is out of scope of this test
        snapshotNames.removeLast();

        // Now delete the repository which will delete the RepositoryData.indexShardCounts cache
        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));

        // Now recreate the repository
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(TEST_REPO_TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );

        // Delete M - 1 snapshots for each index
        for (String snapshotName : snapshotNames) {
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName).get());
        }

        // Cleanup
        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));
    }
}

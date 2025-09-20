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
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
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
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * This test ensures that we only load each IndexMetaData object into memory once and then store the shard count result (ES-12539)
 */
public class BlobStoreRepositoryShardCountComputedOncePerIndexTests extends ESSingleNodeTestCase {

    private static final String TEST_REPO_TYPE = "shard-count-computed-once-fs";
    private static final String TEST_REPO_NAME = "test-repo";
    private static AtomicInteger INDEX_LOADED_COUNT;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        INDEX_LOADED_COUNT = new AtomicInteger();
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ShardCountComputedOncePerIndexFsRepositoryPlugin.class);
    }

    public static class ShardCountComputedOncePerIndexFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
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
                ) {
                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        return new ShardCountComputedOncePerIndexBlobStore(super.createBlobStore());
                    }
                }
            );
        }
    }

    private static class ShardCountComputedOncePerIndexBlobStore implements BlobStore {
        private final BlobStore delegate;

        private ShardCountComputedOncePerIndexBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new ShardCountComputedOncePerIndexBlobContainer(delegate.blobContainer(path));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class ShardCountComputedOncePerIndexBlobContainer extends FilterBlobContainer {
        ShardCountComputedOncePerIndexBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new ShardCountComputedOncePerIndexBlobContainer(child);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            final var pathParts = path().parts();
            // Increment the count only when an index metadata is loaded into heap
            if (pathParts.size() == 2
                && pathParts.getFirst().equals("indices")
                && blobName.startsWith(BlobStoreRepository.METADATA_PREFIX)) {
                INDEX_LOADED_COUNT.incrementAndGet();
            }

            return super.readBlob(purpose, blobName);
        }
    }

    /*
        This test generates N indices, and each index has M snapshots.
        When deleting multiple snapshots within one request, each including the same index,
        we expect each indices metadata to only be loaded once
     */
    public void testShardCountComputedOncePerIndexWhenDeletingMultipleSnapshotsConcurrently() {
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
        List<String> snapshotsToDelete = randomSubsetOf(randomIntBetween(1, numberOfSnapshots - 1), snapshotNames);

        assertAcked(
            client().admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotsToDelete.toArray(new String[0]))
                .get()
        );

        // Each index metadata should only be loaded into heap memory once
        assertEquals(numberOfIndices, INDEX_LOADED_COUNT.get());
        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));
    }

    /*
        This test generates N indices, and each index has M snapshots.
        When deleting multiple snapshots sequentially, even if they include the same index,
        we expect each indices metadata to be loaded each time
     */
    public void testShardCountComputedOncePerIndexWhenDeletingMultipleSnapshotsSequentially() {
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

        // Each index metadata is loaded into heap for each snapshot deletion request
        assertEquals(numberOfIndices * (numberOfSnapshots - 1), INDEX_LOADED_COUNT.get());
        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));
    }
}

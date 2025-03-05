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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlobStoreRepositoryDeleteThrottlingTests extends ESSingleNodeTestCase {

    // This test ensures that we appropriately throttle the per-index activity when deleting a snapshot by marking an index as "active" when
    // its index metadata is read, and then as "inactive" when it updates the shard-level metadata. Without throttling, we would pretty much
    // read all the index metadata first, and then update all the shard-level metadata. With too much throttling, we would work one index at
    // a time and would not fully utilize all snapshot threads. This test shows that we do neither of these things.

    private static final String TEST_REPO_TYPE = "concurrency-limiting-fs";
    private static final String TEST_REPO_NAME = "test-repo";
    private static final int MAX_SNAPSHOT_THREADS = 3;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("thread_pool.snapshot.max", MAX_SNAPSHOT_THREADS).build();
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ConcurrencyLimitingFsRepositoryPlugin.class);
    }

    public static class ConcurrencyLimitingFsRepositoryPlugin extends Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Collections.singletonMap(
                TEST_REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings) {
                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        return new ConcurrencyLimitingBlobStore(super.createBlobStore());
                    }
                }
            );
        }
    }

    private static class ConcurrencyLimitingBlobStore implements BlobStore {
        private final BlobStore delegate;
        private final Set<String> activeIndices = ConcurrentCollections.newConcurrentSet();
        private final CountDownLatch countDownLatch = new CountDownLatch(MAX_SNAPSHOT_THREADS);

        private ConcurrencyLimitingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new ConcurrencyLimitingBlobContainer(delegate.blobContainer(path), activeIndices, countDownLatch);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class ConcurrencyLimitingBlobContainer extends FilterBlobContainer {
        private final Set<String> activeIndices;
        private final CountDownLatch countDownLatch;

        ConcurrencyLimitingBlobContainer(BlobContainer delegate, Set<String> activeIndices, CountDownLatch countDownLatch) {
            super(delegate);
            this.activeIndices = activeIndices;
            this.countDownLatch = countDownLatch;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new ConcurrencyLimitingBlobContainer(child, activeIndices, countDownLatch);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            final var pathParts = path().parts();
            if (pathParts.size() == 2 && pathParts.get(0).equals("indices") && blobName.startsWith(BlobStoreRepository.METADATA_PREFIX)) {
                // reading index metadata, so mark index as active
                assertTrue(activeIndices.add(pathParts.get(1)));
                assertThat(activeIndices.size(), lessThanOrEqualTo(MAX_SNAPSHOT_THREADS));
                countDownLatch.countDown();
                safeAwait(countDownLatch); // ensure that we do use all the threads
            }
            return super.readBlob(purpose, blobName);
        }

        @Override
        public void writeMetadataBlob(
            OperationPurpose purpose,
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) throws IOException {
            final var pathParts = path().parts();
            if (pathParts.size() == 3
                && pathParts.get(0).equals("indices")
                && pathParts.get(2).equals("0")
                && blobName.startsWith(BlobStoreRepository.SNAPSHOT_INDEX_PREFIX)) {
                // writing shard-level BlobStoreIndexShardSnapshots, mark index as inactive again
                assertTrue(activeIndices.remove(pathParts.get(1)));
            }
            super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer);
        }
    }

    public void testDeleteThrottling() {
        final var repoPath = ESIntegTestCase.randomRepoPath(node().settings());

        // Create enough indices that we cannot process them all at once

        for (int i = 0; i < 3 * MAX_SNAPSHOT_THREADS; i++) {
            createIndex("index-" + i, indexSettings(between(1, 3), 0).build());
        }

        // Set up the repository contents including containing a couple of snapshots, using a regular 'fs' repo

        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );

        client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, "snapshot-1")
            .setWaitForCompletion(true)
            .get();
        client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, "snapshot-2")
            .setWaitForCompletion(true)
            .get();

        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));

        // Now delete one of the snapshots using the test repo implementation which verifies the throttling behaviour

        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(TEST_REPO_TYPE)
                .setSettings(Settings.builder().put("location", repoPath))
        );

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, "snapshot-1").get());

        assertAcked(client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME));
    }
}

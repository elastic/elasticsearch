/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class BlobStoreRepositoryOperationPurposeIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    public void testSnapshotOperationPurposes() throws Exception {
        // Perform some simple operations on the repository in order to exercise the checks that the purpose is set correctly for various
        // operations

        final var repoName = randomIdentifier();
        createRepository(repoName, TestPlugin.ASSERTING_REPO_TYPE);

        final var count = between(1, 3);

        for (int i = 0; i < count; i++) {
            createIndexWithContent("index-" + i);
            createFullSnapshot(repoName, "snap-" + i);
        }

        final var timeout = TimeValue.timeValueSeconds(10);
        clusterAdmin().prepareCleanupRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).get(timeout);
        clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-0", "clone-0").setIndices("index-0").get(timeout);

        // restart to ensure that the reads which happen when starting a node on a nonempty repository use the expected purposes
        internalCluster().fullRestart();

        clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).get(timeout);

        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "clone-0")
            .setRenamePattern("index-0")
            .setRenameReplacement("restored-0")
            .setWaitForCompletion(true)
            .get(timeout);

        for (int i = 0; i < count; i++) {
            assertTrue(startDeleteSnapshot(repoName, "snap-" + i).get(10, TimeUnit.SECONDS).isAcknowledged());
        }

        clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).get(timeout);
    }

    public static class TestPlugin extends Plugin implements RepositoryPlugin {
        static final String ASSERTING_REPO_TYPE = "asserting";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Map.of(
                ASSERTING_REPO_TYPE,
                metadata -> new AssertingRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }
    }

    private static class AssertingRepository extends FsRepository {
        AssertingRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new AssertingBlobStore(super.createBlobStore());
        }
    }

    private static class AssertingBlobStore implements BlobStore {
        private final BlobStore delegateBlobStore;

        AssertingBlobStore(BlobStore delegateBlobStore) {
            this.delegateBlobStore = delegateBlobStore;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new AssertingBlobContainer(delegateBlobStore.blobContainer(path));
        }

        @Override
        public void close() throws IOException {
            delegateBlobStore.close();
        }
    }

    private static class AssertingBlobContainer extends FilterBlobContainer {

        AssertingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new AssertingBlobContainer(child);
        }

        @Override
        public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
            throws IOException {
            assertPurposeConsistency(purpose, blobName);
            super.writeBlob(purpose, blobName, bytes, failIfAlreadyExists);
        }

        @Override
        public void writeBlob(
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            assertPurposeConsistency(purpose, blobName);
            super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeMetadataBlob(
            OperationPurpose purpose,
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) throws IOException {
            assertEquals(blobName, OperationPurpose.SNAPSHOT_METADATA, purpose);
            assertPurposeConsistency(purpose, blobName);
            super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer);
        }

        @Override
        public void writeBlobAtomic(
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            assertEquals(blobName, OperationPurpose.SNAPSHOT_METADATA, purpose);
            assertPurposeConsistency(purpose, blobName);
            super.writeBlobAtomic(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
            throws IOException {
            assertEquals(blobName, OperationPurpose.SNAPSHOT_METADATA, purpose);
            assertPurposeConsistency(purpose, blobName);
            super.writeBlobAtomic(purpose, blobName, bytes, failIfAlreadyExists);
        }

        @Override
        public boolean blobExists(OperationPurpose purpose, String blobName) throws IOException {
            assertEquals(blobName, OperationPurpose.SNAPSHOT_METADATA, purpose);
            assertPurposeConsistency(purpose, blobName);
            return super.blobExists(purpose, blobName);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            assertPurposeConsistency(purpose, blobName);
            return super.readBlob(purpose, blobName);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            assertPurposeConsistency(purpose, blobName);
            return super.readBlob(purpose, blobName, position, length);
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
            assertEquals(OperationPurpose.SNAPSHOT_METADATA, purpose);
            return super.listBlobsByPrefix(purpose, blobNamePrefix);
        }
    }

    private static void assertPurposeConsistency(OperationPurpose purpose, String blobName) {
        if (blobName.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)) {
            assertEquals(blobName, OperationPurpose.SNAPSHOT_DATA, purpose);
        } else {
            assertThat(
                blobName,
                anyOf(
                    startsWith(BlobStoreRepository.INDEX_FILE_PREFIX),
                    startsWith(BlobStoreRepository.METADATA_PREFIX),
                    startsWith(BlobStoreRepository.SNAPSHOT_PREFIX),
                    equalTo(BlobStoreRepository.INDEX_LATEST_BLOB),
                    // verification
                    equalTo("master.dat"),
                    startsWith("data-")
                )
            );
            assertEquals(blobName, OperationPurpose.SNAPSHOT_METADATA, purpose);
        }
    }
}

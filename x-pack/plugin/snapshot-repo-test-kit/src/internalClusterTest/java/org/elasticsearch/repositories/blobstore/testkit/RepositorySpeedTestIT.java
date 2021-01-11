/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class RepositorySpeedTestIT extends AbstractSnapshotIntegTestCase {

    @Before
    public void suppressConsistencyChecks() {
        disableRepoConsistencyCheck("repository is not used for snapshots");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class, LocalStateCompositeXPackPlugin.class, SnapshotRepositoryTestKit.class);
    }

    public void testFoo() {

        createRepository("test-repo", TestPlugin.DISRUPTABLE_REPO_TYPE);

        final DisruptableBlobStore blobStore = new DisruptableBlobStore();
        for (final RepositoriesService repositoriesService : internalCluster().getInstances(RepositoriesService.class)) {
            try {
                ((DisruptableRepository) repositoriesService.repository("test-repo")).setBlobStore(blobStore);
            } catch (RepositoryMissingException e) {
                // it's only present on voting masters and data nodes
            }
        }

        final RepositorySpeedTestAction.Request request = new RepositorySpeedTestAction.Request("test-repo");

        if (randomBoolean()) {
            request.concurrency(between(1, 5));
            blobStore.ensureMaxWriteConcurrency(request.getConcurrency());
        }

        if (randomBoolean()) {
            request.blobCount(between(1, 100));
            blobStore.ensureMaxBlobCount(request.getBlobCount());
        }

        if (request.getBlobCount() > 3 || randomBoolean()) {
            // only use the default blob size of 10MB if writing a small number of blobs, since this is all in-memory
            request.maxBlobSize(new ByteSizeValue(between(1, 2048)));
            blobStore.ensureMaxBlobSize(request.getMaxBlobSize().getBytes());
        }

        request.timeout(TimeValue.timeValueSeconds(5));

        final RepositorySpeedTestAction.Response response = client().execute(RepositorySpeedTestAction.INSTANCE, request).actionGet();

        assertThat(response.status(), equalTo(RestStatus.OK));
    }

    public static class TestPlugin extends Plugin implements RepositoryPlugin {

        static final String DISRUPTABLE_REPO_TYPE = "disruptable";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Map.of(
                DISRUPTABLE_REPO_TYPE,
                metadata -> new DisruptableRepository(
                    metadata,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    new BlobPath()
                )
            );
        }
    }

    static class DisruptableRepository extends BlobStoreRepository {

        private final AtomicReference<BlobStore> blobStoreRef = new AtomicReference<>();

        DisruptableRepository(
            RepositoryMetadata metadata,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            BlobPath basePath
        ) {
            super(metadata, namedXContentRegistry, clusterService, bigArrays, recoverySettings, basePath);
        }

        void setBlobStore(BlobStore blobStore) {
            assertTrue(blobStoreRef.compareAndSet(null, blobStore));
        }

        @Override
        protected BlobStore createBlobStore() {
            final BlobStore blobStore = blobStoreRef.get();
            assertNotNull(blobStore);
            return blobStore;
        }

        @Override
        public String startVerification() {
            return null; // suppress verify-on-register since we need to wire up all the nodes' repositories first
        }
    }

    static class DisruptableBlobStore implements BlobStore {

        private final Map<String, DisruptableBlobContainer> blobContainers = ConcurrentCollections.newConcurrentMap();
        private Semaphore writeSemaphore = new Semaphore(new RepositorySpeedTestAction.Request("dummy").getConcurrency());
        private int maxBlobCount = new RepositorySpeedTestAction.Request("dummy").getBlobCount();
        private long maxBlobSize = new RepositorySpeedTestAction.Request("dummy").getMaxBlobSize().getBytes();

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            assertThat(path.buildAsString(), startsWith("temp-speed-test-"));
            return blobContainers.computeIfAbsent(
                path.buildAsString(),
                ignored -> new DisruptableBlobContainer(path, this::deleteContainer, writeSemaphore, maxBlobCount, maxBlobSize)
            );
        }

        private void deleteContainer(DisruptableBlobContainer container) {
            assertNotNull("container " + container.path() + " not found", blobContainers.remove(container.path().buildAsString()));
        }

        @Override
        public void close() {}

        public void ensureMaxWriteConcurrency(int concurrency) {
            writeSemaphore = new Semaphore(concurrency);
        }

        public void ensureMaxBlobCount(int maxBlobCount) {
            this.maxBlobCount = maxBlobCount;
        }

        public void ensureMaxBlobSize(long maxBlobSize) {
            this.maxBlobSize = maxBlobSize;
        }
    }

    static class DisruptableBlobContainer implements BlobContainer {

        private static final byte[] EMPTY = new byte[0];

        private final BlobPath path;
        private final Consumer<DisruptableBlobContainer> deleteContainer;
        private final Semaphore writeSemaphore;
        private final int maxBlobCount;
        private final long maxBlobSize;
        private final Map<String, byte[]> blobs = ConcurrentCollections.newConcurrentMap();

        DisruptableBlobContainer(
            BlobPath path,
            Consumer<DisruptableBlobContainer> deleteContainer,
            Semaphore writeSemaphore,
            int maxBlobCount,
            long maxBlobSize
        ) {
            this.path = path;
            this.deleteContainer = deleteContainer;
            this.writeSemaphore = writeSemaphore;
            this.maxBlobCount = maxBlobCount;
            this.maxBlobSize = maxBlobSize;
        }

        @Override
        public BlobPath path() {
            return path;
        }

        @Override
        public boolean blobExists(String blobName) {
            return blobs.containsKey(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            final byte[] contents = blobs.get(blobName);
            if (contents == null) {
                throw new FileNotFoundException(blobName + " not found");
            }
            return new ByteArrayInputStream(contents);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            final byte[] contents = blobs.get(blobName);
            if (contents == null) {
                throw new FileNotFoundException(blobName + " not found");
            }
            final int truncatedLength = Math.toIntExact(Math.min(length, contents.length - position));
            return new ByteArrayInputStream(contents, Math.toIntExact(position), truncatedLength);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            assertTrue("must only write blob [" + blobName + "] non-atomically if it doesn't already exist", failIfAlreadyExists);
            assertNull("blob [" + blobName + "] must not exist", blobs.get(blobName));

            blobs.put(blobName, EMPTY);
            writeBlobAtomic(blobName, inputStream, blobSize, false);
        }

        @Override
        public void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
            writeBlob(blobName, bytes.streamInput(), bytes.length(), failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
            writeBlobAtomic(blobName, bytes.streamInput(), bytes.length(), failIfAlreadyExists);
        }

        private void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {

            if (failIfAlreadyExists) {
                assertNull("blob [" + blobName + "] must not exist", blobs.get(blobName));
            }

            assertThat(blobSize, lessThanOrEqualTo(maxBlobSize));

            assertTrue(writeSemaphore.tryAcquire());
            try {
                final byte[] contents = inputStream.readAllBytes();
                assertThat((long) contents.length, equalTo(blobSize));
                blobs.put(blobName, contents);
                assertThat(blobs.size(), lessThanOrEqualTo(maxBlobCount));
            } finally {
                writeSemaphore.release();
            }
        }

        @Override
        public DeleteResult delete() {
            deleteContainer.accept(this);
            return new DeleteResult(blobs.size(), blobs.values().stream().mapToLong(b -> b.length).sum());
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
            blobs.keySet().removeAll(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() {
            return blobs.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new PlainBlobMetadata(e.getKey(), e.getValue().length)));
        }

        @Override
        public Map<String, BlobContainer> children() {
            return Map.of();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
            final Map<String, BlobMetadata> blobMetadataByName = listBlobs();
            blobMetadataByName.keySet().removeIf(s -> s.startsWith(blobNamePrefix) == false);
            return blobMetadataByName;
        }
    }

}

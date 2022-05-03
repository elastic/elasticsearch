/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class RepositoryAnalysisSuccessIT extends AbstractSnapshotIntegTestCase {

    private static final String BASE_PATH_SETTING_KEY = "base_path";

    @Before
    public void suppressConsistencyChecks() {
        disableRepoConsistencyCheck("repository is not used for snapshots");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class, LocalStateCompositeXPackPlugin.class, SnapshotRepositoryTestKit.class);
    }

    public void testRepositoryAnalysis() {

        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(BASE_PATH_SETTING_KEY, randomAlphaOfLength(10));
        }

        assertAcked(
            clusterAdmin().preparePutRepository("test-repo").setVerify(false).setType(TestPlugin.ASSERTING_REPO_TYPE).setSettings(settings)
        );

        final AssertingBlobStore blobStore = new AssertingBlobStore(settings.get(BASE_PATH_SETTING_KEY));
        for (final RepositoriesService repositoriesService : internalCluster().getInstances(RepositoriesService.class)) {
            try {
                ((AssertingRepository) repositoriesService.repository("test-repo")).setBlobStore(blobStore);
            } catch (RepositoryMissingException e) {
                // nbd, it's only present on voting masters and data nodes
            }
        }

        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");

        if (randomBoolean()) {
            request.concurrency(between(1, 5));
            blobStore.setMaxWriteConcurrency(request.getConcurrency());
        }

        if (randomBoolean()) {
            request.blobCount(between(1, 100));
            blobStore.setMaxBlobCount(request.getBlobCount());
        }

        if (request.getBlobCount() > 3 || randomBoolean()) {
            // only use the default blob size of 10MB if writing a small number of blobs, since this is all in-memory
            request.maxBlobSize(new ByteSizeValue(between(1, 2048)));
            blobStore.setMaxBlobSize(request.getMaxBlobSize().getBytes());
        }

        if (usually()) {
            request.maxTotalDataSize(
                new ByteSizeValue(request.getMaxBlobSize().getBytes() + request.getBlobCount() - 1 + between(0, 1 << 20))
            );
            blobStore.setMaxTotalBlobSize(request.getMaxTotalDataSize().getBytes());
        }

        request.timeout(TimeValue.timeValueSeconds(20));

        final RepositoryAnalyzeAction.Response response = client().execute(RepositoryAnalyzeAction.INSTANCE, request)
            .actionGet(30L, TimeUnit.SECONDS);

        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(blobStore.currentPath, nullValue());
    }

    public static class TestPlugin extends Plugin implements RepositoryPlugin {

        static final String ASSERTING_REPO_TYPE = "asserting";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Map.of(
                ASSERTING_REPO_TYPE,
                metadata -> new AssertingRepository(
                    metadata,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    buildBlobPath(metadata.settings())
                )
            );
        }
    }

    private static BlobPath buildBlobPath(Settings settings) {
        final String basePath = settings.get(BASE_PATH_SETTING_KEY);
        if (basePath == null) {
            return BlobPath.EMPTY;
        } else {
            return BlobPath.EMPTY.add(basePath);
        }
    }

    static class AssertingRepository extends BlobStoreRepository {

        private final AtomicReference<BlobStore> blobStoreRef = new AtomicReference<>();

        AssertingRepository(
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
    }

    static class AssertingBlobStore implements BlobStore {
        private final String pathPrefix;

        @Nullable // if no current blob container
        private String currentPath;

        @Nullable // if no current blob container
        private AssertingBlobContainer currentBlobContainer;

        private Semaphore writeSemaphore = new Semaphore(new RepositoryAnalyzeAction.Request("dummy").getConcurrency());
        private int maxBlobCount = new RepositoryAnalyzeAction.Request("dummy").getBlobCount();
        private long maxBlobSize = new RepositoryAnalyzeAction.Request("dummy").getMaxBlobSize().getBytes();
        private long maxTotalBlobSize = new RepositoryAnalyzeAction.Request("dummy").getMaxTotalDataSize().getBytes();

        AssertingBlobStore(@Nullable String basePath) {
            this.pathPrefix = basePath == null ? "" : basePath + "/";
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            assertThat(path.buildAsString(), startsWith(pathPrefix + "temp-analysis-"));

            synchronized (this) {
                if (currentPath == null) {
                    currentPath = path.buildAsString();
                    currentBlobContainer = new AssertingBlobContainer(
                        path,
                        this::deleteContainer,
                        writeSemaphore,
                        maxBlobCount,
                        maxBlobSize,
                        maxTotalBlobSize
                    );
                }
                assertThat(path.buildAsString(), equalTo(currentPath));
                return currentBlobContainer;
            }
        }

        private void deleteContainer(AssertingBlobContainer container) {
            synchronized (this) {
                assertThat(currentPath, equalTo(container.path.buildAsString()));
                currentPath = null;
                currentBlobContainer = null;
            }
        }

        @Override
        public void close() {}

        public void setMaxWriteConcurrency(int concurrency) {
            this.writeSemaphore = new Semaphore(concurrency);
        }

        public void setMaxBlobCount(int maxBlobCount) {
            this.maxBlobCount = maxBlobCount;
        }

        public void setMaxBlobSize(long maxBlobSize) {
            this.maxBlobSize = maxBlobSize;
        }

        public void setMaxTotalBlobSize(long maxTotalBlobSize) {
            this.maxTotalBlobSize = maxTotalBlobSize;
        }
    }

    static class AssertingBlobContainer implements BlobContainer {

        private static final byte[] EMPTY = new byte[0];

        private final BlobPath path;
        private final Consumer<AssertingBlobContainer> deleteContainer;
        private final Semaphore writeSemaphore;
        private final int maxBlobCount;
        private final long maxBlobSize;
        private final long maxTotalBlobSize;
        private final Map<String, byte[]> blobs = ConcurrentCollections.newConcurrentMap();
        private final AtomicLong totalBytesWritten = new AtomicLong();

        AssertingBlobContainer(
            BlobPath path,
            Consumer<AssertingBlobContainer> deleteContainer,
            Semaphore writeSemaphore,
            int maxBlobCount,
            long maxBlobSize,
            long maxTotalBlobSize
        ) {
            this.path = path;
            this.deleteContainer = deleteContainer;
            this.writeSemaphore = writeSemaphore;
            this.maxBlobCount = maxBlobCount;
            this.maxBlobSize = maxBlobSize;
            this.maxTotalBlobSize = maxTotalBlobSize;
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
        public void writeBlob(
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) throws IOException {
            final BytesStreamOutput out = new BytesStreamOutput();
            writer.accept(out);
            if (atomic) {
                writeBlobAtomic(blobName, out.bytes(), failIfAlreadyExists);
            } else {
                writeBlob(blobName, out.bytes(), failIfAlreadyExists);
            }
        }

        @Override
        public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
            writeBlobAtomic(blobName, bytes.streamInput(), bytes.length(), failIfAlreadyExists);
        }

        private void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {

            final byte[] existingBlob = blobs.get(blobName);
            if (failIfAlreadyExists) {
                assertNull("blob [" + blobName + "] must not exist", existingBlob);
            }
            final int existingSize = existingBlob == null ? 0 : existingBlob.length;

            assertThat(blobSize, lessThanOrEqualTo(maxBlobSize));

            assertTrue(writeSemaphore.tryAcquire());
            try {
                final byte[] contents = inputStream.readAllBytes();
                assertThat((long) contents.length, equalTo(blobSize));
                blobs.put(blobName, contents);
                assertThat(blobs.size(), lessThanOrEqualTo(maxBlobCount));
                final long currentTotal = totalBytesWritten.addAndGet(blobSize - existingSize);
                assertThat(currentTotal, lessThanOrEqualTo(maxTotalBlobSize));
            } finally {
                writeSemaphore.release();
            }
        }

        @Override
        public DeleteResult delete() {
            deleteContainer.accept(this);
            final DeleteResult deleteResult = new DeleteResult(blobs.size(), blobs.values().stream().mapToLong(b -> b.length).sum());
            blobs.clear();
            return deleteResult;
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) {
            blobNames.forEachRemaining(blobs.keySet()::remove);
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

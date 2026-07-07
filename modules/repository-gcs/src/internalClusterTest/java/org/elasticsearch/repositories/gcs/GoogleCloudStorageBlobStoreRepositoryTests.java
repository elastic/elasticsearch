/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.common.io.Streams.readFully;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomRetryingPurpose;

/// GCS blob-store repository integration tests that exercise the production-default 16MB resumable
/// write buffer. Shared infrastructure lives in [AbstractGoogleCloudStorageBlobStoreRepositoryTestCase];
/// tests that need a fixed small buffer to assert exact chunk sizes live in
/// [GoogleCloudStorageResumableWriteBufferTests].
public class GoogleCloudStorageBlobStoreRepositoryTests extends AbstractGoogleCloudStorageBlobStoreRepositoryTestCase {

    public void testDeleteItems() throws IOException {
        final var repoName = createRepository(randomRepositoryName(), false);
        final var repositoriesService = internalCluster().getAnyMasterNodeInstance(RepositoriesService.class);
        final var repository = (BlobStoreRepository) repositoriesService.repository(repoName);
        final var blobStore = repository.blobStore();
        final var container = blobStore.blobContainer(repository.basePath());

        final var purpose = randomPurpose();
        final var blobNamePrefix = "delete-blob-";
        final int numberOfBlobs = between(1, GoogleCloudStorageBlobStore.MAX_DELETES_PER_BATCH * 10);
        final List<String> blobNames = IntStream.range(0, numberOfBlobs).mapToObj(n -> blobNamePrefix + n).toList();

        // randomly skips blob creation to exercise deletion if blob not exists
        int created = 0;
        for (var blob : blobNames) {
            if (randomBoolean()) {
                container.writeBlob(purpose, blob, randomBytesReference(between(1, 10)), false);
                created += 1;
            }
        }
        assertEquals("should write all blobs", created, container.listBlobsByPrefix(purpose, blobNamePrefix).size());

        container.deleteBlobsIgnoringIfNotExists(purpose, blobNames.iterator());
        assertEquals("should delete all blobs", 0, container.listBlobsByPrefix(purpose, blobNamePrefix).size());
    }

    public void testChunkSize() {
        // default chunk size
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE, Settings.EMPTY);
        ByteSizeValue chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetadata);
        assertEquals(GoogleCloudStorageRepository.MAX_CHUNK_SIZE, chunkSize);

        // chunk size in settings
        final int size = randomIntBetween(1, 100);
        repositoryMetadata = new RepositoryMetadata(
            "repo",
            GoogleCloudStorageRepository.TYPE,
            Settings.builder().put("chunk_size", size + "mb").build()
        );
        chunkSize = GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repositoryMetadata);
        assertEquals(ByteSizeValue.of(size, ByteSizeUnit.MB), chunkSize);

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata(
                "repo",
                GoogleCloudStorageRepository.TYPE,
                Settings.builder().put("chunk_size", "0").build()
            );
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata(
                "repo",
                GoogleCloudStorageRepository.TYPE,
                Settings.builder().put("chunk_size", "-1").build()
            );
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> {
            final RepositoryMetadata repoMetadata = new RepositoryMetadata(
                "repo",
                GoogleCloudStorageRepository.TYPE,
                Settings.builder().put("chunk_size", "6tb").build()
            );
            GoogleCloudStorageRepository.getSetting(GoogleCloudStorageRepository.CHUNK_SIZE, repoMetadata);
        });
        assertEquals("failed to parse value [6tb] for setting [chunk_size], must be <= [5tb]", e.getMessage());
    }

    public void testWriteReadLarge() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            byte[] data = randomBytes(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE + 1);
            writeBlob(container, "foobar", new BytesArray(data), randomBoolean());
            if (randomBoolean()) {
                // override file, to check if we get latest contents
                random().nextBytes(data);
                writeBlob(container, "foobar", new BytesArray(data), false);
            }
            try (InputStream stream = container.readBlob(randomRetryingPurpose(), "foobar")) {
                BytesRefBuilder target = new BytesRefBuilder();
                while (target.length() < data.length) {
                    byte[] buffer = new byte[scaledRandomIntBetween(1, data.length - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = stream.read(buffer, offset, buffer.length - offset);
                    target.append(new BytesRef(buffer, offset, read));
                }
                assertEquals(data.length, target.length());
                assertArrayEquals(data, Arrays.copyOfRange(target.bytes(), 0, target.length()));
            }
            container.delete(randomPurpose());
        }
    }

    /// Exercises the production-default 16MB resumable write buffer by uploading a blob that is an exact
    /// multiple of the SDK default chunk size (so it splits into 2-4 full chunks). Assertions about exact
    /// per-chunk sizes under a pinned small buffer live in [GoogleCloudStorageResumableWriteBufferTests].
    public void testWriteFileMultipleOfChunkSize() throws IOException {
        final int uploadSize = randomIntBetween(2, 4) * GoogleCloudStorageBlobStore.SDK_DEFAULT_CHUNK_SIZE;
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            final String key = randomIdentifier();
            byte[] initialValue = randomByteArrayOfLength(uploadSize);
            container.writeBlob(randomPurpose(), key, new BytesArray(initialValue), true);

            BytesReference reference = readFully(container.readBlob(randomRetryingPurpose(), key));
            assertThat(reference, equalBytes(new BytesArray(initialValue)));

            container.deleteBlobsIgnoringIfNotExists(randomPurpose(), Iterators.single(key));
        }
    }

    public void testCopy() throws Exception {
        final var sourceBlobName = randomIdentifier();
        final var repoName = createRepository(randomRepositoryName(), false);
        final var destinationBlobName = randomIdentifier();
        final var repositoriesService = internalCluster().getAnyMasterNodeInstance(RepositoriesService.class);
        final var repository = (BlobStoreRepository) repositoriesService.repository(ProjectId.DEFAULT, repoName);
        final var blobStore = repository.blobStore();
        final var sourceBlobContainer = blobStore.blobContainer(repository.basePath());
        final var blobBytes = randomBytesReference(randomIntBetween(100, 2_000_000));
        sourceBlobContainer.writeBlob(randomPurpose(), sourceBlobName, blobBytes, true);
        assertBusy(() -> assertTrue(sourceBlobContainer.blobExists(randomPurpose(), sourceBlobName)));

        final var destinationBlobContainer = repository.blobStore().blobContainer(repository.basePath().add("target"));
        destinationBlobContainer.copyBlob(randomPurpose(), sourceBlobContainer, sourceBlobName, destinationBlobName, blobBytes.length());
        assertThat(readFully(destinationBlobContainer.readBlob(randomRetryingPurpose(), destinationBlobName)), equalBytes(blobBytes));

        sourceBlobContainer.delete(randomPurpose());
        destinationBlobContainer.delete(randomPurpose());
    }

    @Override
    public void testRequestStats() throws Exception {
        super.testRequestStats();
    }
}

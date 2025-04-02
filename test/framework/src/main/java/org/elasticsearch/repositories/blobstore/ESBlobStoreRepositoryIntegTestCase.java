/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_INDEX_NAME_FORMAT;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_NAME_FORMAT;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomNonDataPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for {@link BlobStoreRepository} implementations.
 */
public abstract class ESBlobStoreRepositoryIntegTestCase extends ESIntegTestCase {

    public static RepositoryData getRepositoryData(Repository repository) {
        return AbstractSnapshotIntegTestCase.getRepositoryData(repository);
    }

    protected abstract String repositoryType();

    protected Settings repositorySettings(String repoName) {
        return Settings.builder().put("compress", randomBoolean()).build();
    }

    protected final String createRepository(final String name) {
        return createRepository(name, true);
    }

    protected final String createRepository(final String name, final boolean verify) {
        return createRepository(name, repositorySettings(name), verify);
    }

    protected final String createRepository(final String name, final Settings settings, final boolean verify) {
        logger.info("-->  creating repository [name: {}, verify: {}, settings: {}]", name, verify, settings);
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
                .setType(repositoryType())
                .setVerify(verify)
                .setSettings(settings)
        );

        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            assertThat(repositories.repository(name), notNullValue());
            assertThat(repositories.repository(name), instanceOf(BlobStoreRepository.class));
            assertThat(repositories.repository(name).isReadOnly(), is(settings.getAsBoolean(READONLY_SETTING_KEY, false)));
            BlobStore blobStore = ((BlobStoreRepository) repositories.repository(name)).getBlobStore();
            assertThat("blob store has to be lazy initialized", blobStore, verify ? is(notNullValue()) : is(nullValue()));
        });

        return name;
    }

    protected final void deleteRepository(final String name) {
        logger.debug("-->  deleting repository [name: {}]", name);
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name));
        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            RepositoryMissingException e = expectThrows(RepositoryMissingException.class, () -> repositories.repository(name));
            assertThat(e.repository(), equalTo(name));
        });
    }

    public void testReadNonExistingPath() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            expectThrows(NoSuchFileException.class, () -> {
                try (InputStream is = container.readBlob(randomPurpose(), "non-existing")) {
                    is.read();
                }
            });
        }
    }

    public void testWriteMaybeCopyRead() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final String blobName = randomAlphaOfLengthBetween(8, 12);
            String readBlobName = blobName;
            writeBlob(container, blobName, new BytesArray(data), randomBoolean());
            if (randomBoolean()) {
                // override file, to check if we get latest contents
                data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
                writeBlob(container, blobName, new BytesArray(data), false);
            }
            if (randomBoolean()) {
                // server-side copy if supported
                try {
                    final var destinationBlobName = blobName + "_copy";
                    container.copyBlob(randomPurpose(), container, blobName, destinationBlobName, data.length);
                    readBlobName = destinationBlobName;
                } catch (UnsupportedOperationException ignored) {}
            }
            try (InputStream stream = container.readBlob(randomPurpose(), readBlobName)) {
                BytesRefBuilder target = new BytesRefBuilder();
                while (target.length() < data.length) {
                    byte[] buffer = new byte[scaledRandomIntBetween(1, data.length - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = stream.read(buffer, offset, buffer.length - offset);
                    if (read >= 0) {
                        target.append(new BytesRef(buffer, offset, read));
                    } else {
                        fail("Expected [" + (data.length - target.length()) + "] more bytes to be readable but reached EOF");
                    }
                }
                assertEquals(data.length, target.length());
                assertArrayEquals(data, Arrays.copyOfRange(target.bytes(), 0, target.length()));
            }
            container.delete(randomPurpose());
        }
    }

    public void testList() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            assertThat(container.listBlobs(randomPurpose()).size(), CoreMatchers.equalTo(0));
            int numberOfFooBlobs = randomIntBetween(0, 10);
            int numberOfBarBlobs = randomIntBetween(3, 20);
            Map<String, Long> generatedBlobs = new HashMap<>();
            for (int i = 0; i < numberOfFooBlobs; i++) {
                int length = randomIntBetween(10, 100);
                String name = "foo-" + i + "-";
                generatedBlobs.put(name, (long) length);
                writeRandomBlob(container, name, length);
            }
            for (int i = 1; i < numberOfBarBlobs; i++) {
                int length = randomIntBetween(10, 100);
                String name = "bar-" + i + "-";
                generatedBlobs.put(name, (long) length);
                writeRandomBlob(container, name, length);
            }
            int length = randomIntBetween(10, 100);
            String name = "bar-0-";
            generatedBlobs.put(name, (long) length);
            writeRandomBlob(container, name, length);

            Map<String, BlobMetadata> blobs = container.listBlobs(randomPurpose());
            assertThat(blobs.size(), CoreMatchers.equalTo(numberOfFooBlobs + numberOfBarBlobs));
            for (Map.Entry<String, Long> generated : generatedBlobs.entrySet()) {
                BlobMetadata blobMetadata = blobs.get(generated.getKey());
                assertThat(generated.getKey(), blobMetadata, CoreMatchers.notNullValue());
                assertThat(blobMetadata.name(), CoreMatchers.equalTo(generated.getKey()));
                assertThat(blobMetadata.length(), CoreMatchers.equalTo(blobLengthFromContentLength(generated.getValue())));
            }

            assertThat(container.listBlobsByPrefix(randomPurpose(), "foo-").size(), CoreMatchers.equalTo(numberOfFooBlobs));
            assertThat(container.listBlobsByPrefix(randomPurpose(), "bar-").size(), CoreMatchers.equalTo(numberOfBarBlobs));
            assertThat(container.listBlobsByPrefix(randomPurpose(), "baz-").size(), CoreMatchers.equalTo(0));
            container.delete(randomPurpose());
        }
    }

    public void testDeleteBlobs() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final List<String> blobNames = Arrays.asList("foobar", "barfoo");
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            container.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobNames.iterator()); // does not raise when blobs
            // don't exist
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final BytesArray bytesArray = new BytesArray(data);
            for (String blobName : blobNames) {
                writeBlob(container, blobName, bytesArray, randomBoolean());
            }
            assertEquals(container.listBlobs(randomPurpose()).size(), 2);
            container.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobNames.iterator());
            assertTrue(container.listBlobs(randomPurpose()).isEmpty());
            container.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobNames.iterator()); // does not raise when blobs
            // don't exist
        }
    }

    public static void writeBlob(
        final BlobContainer container,
        final String blobName,
        final BytesArray bytesArray,
        boolean failIfAlreadyExists
    ) throws IOException {
        if (randomBoolean()) {
            container.writeBlob(randomPurpose(), blobName, bytesArray, failIfAlreadyExists);
        } else {
            if (randomBoolean()) {
                container.writeBlobAtomic(randomNonDataPurpose(), blobName, bytesArray, failIfAlreadyExists);
            } else {
                container.writeBlobAtomic(
                    randomNonDataPurpose(),
                    blobName,
                    bytesArray.streamInput(),
                    bytesArray.length(),
                    failIfAlreadyExists
                );
            }
        }
    }

    public void testContainerCreationAndDeletion() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer containerFoo = store.blobContainer(BlobPath.EMPTY.add("foo"));
            final BlobContainer containerBar = store.blobContainer(BlobPath.EMPTY.add("bar"));
            byte[] data1 = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            byte[] data2 = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            writeBlob(containerFoo, "test", new BytesArray(data1));
            writeBlob(containerBar, "test", new BytesArray(data2));

            assertArrayEquals(readBlobFully(containerFoo, "test", data1.length), data1);
            assertArrayEquals(readBlobFully(containerBar, "test", data2.length), data2);

            assertTrue(containerFoo.blobExists(randomPurpose(), "test"));
            assertTrue(containerBar.blobExists(randomPurpose(), "test"));
            containerBar.delete(randomPurpose());
            containerFoo.delete(randomPurpose());
        }
    }

    public static byte[] writeRandomBlob(BlobContainer container, String name, int length) throws IOException {
        byte[] data = randomBytes(length);
        writeBlob(container, name, new BytesArray(data));
        return data;
    }

    public static byte[] readBlobFully(BlobContainer container, String name, int length) throws IOException {
        byte[] data = new byte[length];
        try (InputStream inputStream = container.readBlob(randomPurpose(), name)) {
            assertThat(Streams.readFully(inputStream, data), CoreMatchers.equalTo(length));
            assertThat(inputStream.read(), CoreMatchers.equalTo(-1));
        }
        return data;
    }

    public static byte[] randomBytes(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        return data;
    }

    protected static void writeBlob(BlobContainer container, String blobName, BytesArray bytesArray) throws IOException {
        container.writeBlob(randomPurpose(), blobName, bytesArray, true);
    }

    protected BlobStore newBlobStore() {
        final String repository = createRepository(randomRepositoryName());
        return newBlobStore(repository);
    }

    protected BlobStore newBlobStore(String repository) {
        return asInstanceOf(
            BlobStoreRepository.class,
            internalCluster().getAnyMasterNodeInstance(RepositoriesService.class).repository(repository)
        ).blobStore();
    }

    public void testSnapshotAndRestore() throws Exception {
        testSnapshotAndRestore(randomBoolean());
    }

    protected void testSnapshotAndRestore(boolean recreateRepositoryBeforeRestore) throws Exception {
        final String repoName = randomRepositoryName();
        final Settings repoSettings = repositorySettings(repoName);
        createRepository(repoName, repoSettings, randomBoolean());
        int indexCount = randomIntBetween(1, 5);
        int[] docCounts = new int[indexCount];
        String[] indexNames = generateRandomNames(indexCount);
        for (int i = 0; i < indexCount; i++) {
            docCounts[i] = iterations(10, 1000);
            logger.info("-->  create random index {} with {} records", indexNames[i], docCounts[i]);
            addRandomDocuments(indexNames[i], docCounts[i]);
            assertHitCount(prepareSearch(indexNames[i]).setSize(0), docCounts[i]);
        }

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexNames)
        );

        List<String> deleteIndices = randomSubsetOf(randomIntBetween(0, indexCount), indexNames);
        if (deleteIndices.size() > 0) {
            logger.info("-->  delete indices {}", deleteIndices);
            assertAcked(client().admin().indices().prepareDelete(deleteIndices.toArray(new String[deleteIndices.size()])));
        }

        Set<String> closeIndices = new HashSet<>(Arrays.asList(indexNames));
        closeIndices.removeAll(deleteIndices);

        if (closeIndices.size() > 0) {
            for (String index : closeIndices) {
                if (randomBoolean()) {
                    logger.info("--> add random documents to {}", index);
                    addRandomDocuments(index, randomIntBetween(10, 1000));
                } else {
                    final int docCount = (int) SearchResponseUtils.getTotalHitsValue(prepareSearch(index).setSize(0));
                    deleteRandomDocs(index, docCount);
                }
            }

            // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
            ensureGreen();
            logger.info("-->  close indices {}", closeIndices);
            assertAcked(client().admin().indices().prepareClose(closeIndices.toArray(new String[closeIndices.size()])));
        }

        if (recreateRepositoryBeforeRestore) {
            deleteRepository(repoName);
            createRepository(repoName, repoSettings, randomBoolean());
        }

        logger.info("--> restore all indices from the snapshot");
        assertSuccessfulRestore(
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName).setWaitForCompletion(true)
        );

        // higher timeout since we can have quite a few shards and a little more data here
        ensureGreen(TimeValue.timeValueSeconds(120));

        for (int i = 0; i < indexCount; i++) {
            assertHitCount(prepareSearch(indexNames[i]).setSize(0), docCounts[i]);
        }

        logger.info("-->  delete snapshot {}:{}", repoName, snapshotName);
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName).get());

        expectThrows(
            SnapshotMissingException.class,
            () -> clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).setSnapshots(snapshotName).get()
        );

        expectThrows(
            SnapshotMissingException.class,
            () -> clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName).get()
        );

        expectThrows(
            SnapshotRestoreException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setWaitForCompletion(randomBoolean())
                .get()
        );
    }

    public void testMultipleSnapshotAndRollback() throws Exception {
        final String repoName = createRepository(randomRepositoryName());
        int iterationCount = randomIntBetween(2, 5);
        int[] docCounts = new int[iterationCount];
        String indexName = randomName();
        String snapshotName = randomName();
        assertAcked(client().admin().indices().prepareCreate(indexName).get());
        for (int i = 0; i < iterationCount; i++) {
            if (randomBoolean() && i > 0) { // don't delete on the first iteration
                int docCount = docCounts[i - 1];
                if (docCount > 0) {
                    deleteRandomDocs(indexName, docCount);
                }
            } else {
                int docCount = randomIntBetween(10, 1000);
                logger.info("--> add {} random documents to {}", docCount, indexName);
                addRandomDocuments(indexName, docCount);
            }
            // Check number of documents in this iteration
            docCounts[i] = (int) SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setSize(0));
            logger.info("-->  create snapshot {}:{} with {} documents", repoName, snapshotName + "-" + i, docCounts[i]);
            assertSuccessfulSnapshot(
                clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName + "-" + i)
                    .setWaitForCompletion(true)
                    .setIndices(indexName)
            );
        }

        int restoreOperations = randomIntBetween(1, 3);
        for (int i = 0; i < restoreOperations; i++) {
            int iterationToRestore = randomIntBetween(0, iterationCount - 1);
            logger.info("-->  performing restore of the iteration {}", iterationToRestore);

            // Wait for green so the close does not fail in the edge case of coinciding with a shard recovery that hasn't fully synced yet
            ensureGreen();
            logger.info("-->  close index");
            assertAcked(client().admin().indices().prepareClose(indexName));

            logger.info("--> restore index from the snapshot");
            assertSuccessfulRestore(
                clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName + "-" + iterationToRestore)
                    .setWaitForCompletion(true)
            );

            ensureGreen();
            assertHitCount(prepareSearch(indexName).setSize(0), docCounts[iterationToRestore]);
        }

        for (int i = 0; i < iterationCount; i++) {
            logger.info("-->  delete snapshot {}:{}", repoName, snapshotName + "-" + i);
            assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName + "-" + i).get());
        }
    }

    private void deleteRandomDocs(String indexName, int existingDocCount) {
        int deleteCount = randomIntBetween(1, existingDocCount);
        logger.info("--> delete {} random documents from {}", deleteCount, indexName);
        for (int j = 0; j < deleteCount; j++) {
            int doc = randomIntBetween(0, existingDocCount - 1);
            client().prepareDelete(indexName, Integer.toString(doc)).get();
        }
        client().admin().indices().prepareRefresh(indexName).get();
    }

    public void testIndicesDeletedFromRepository() throws Exception {
        final String repoName = createRepository(randomRepositoryName());
        Client client = client();
        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 20; i++) {
            indexDoc("test-idx-1", Integer.toString(i), "foo", "bar" + i);
            indexDoc("test-idx-2", Integer.toString(i), "foo", "baz" + i);
            indexDoc("test-idx-3", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        logger.info("--> take a snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "test-snap")
            .setWaitForCompletion(true)
            .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> indexing more data");
        for (int i = 20; i < 40; i++) {
            indexDoc("test-idx-1", Integer.toString(i), "foo", "bar" + i);
            indexDoc("test-idx-2", Integer.toString(i), "foo", "baz" + i);
            indexDoc("test-idx-3", Integer.toString(i), "foo", "baz" + i);
        }

        logger.info("--> take another snapshot with only 2 of the 3 indices");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "test-snap2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-1", "test-idx-2")
            .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> delete a snapshot");
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "test-snap").get());

        logger.info("--> verify index folder deleted from blob container");
        RepositoriesService repositoriesSvc = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesSvc.repository(repoName);

        final SetOnce<BlobContainer> indicesBlobContainer = new SetOnce<>();
        final PlainActionFuture<RepositoryData> repositoryData = new PlainActionFuture<>();
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            indicesBlobContainer.set(repository.blobStore().blobContainer(repository.basePath().add("indices")));
            repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, repositoryData);
        });

        for (IndexId indexId : repositoryData.actionGet().getIndices().values()) {
            if (indexId.getName().equals("test-idx-3")) {
                assertFalse(indicesBlobContainer.get().blobExists(randomPurpose(), indexId.getId())); // deleted index
            }
        }

        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "test-snap2").get());
    }

    public void testDanglingShardLevelBlobCleanup() throws Exception {
        final var repoName = createRepository(randomRepositoryName());
        final var client = client();
        final var indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        addRandomDocuments(indexName, between(1, 10));

        // Create a snapshot
        assertEquals(
            SnapshotState.SUCCESS,
            client.admin()
                .cluster()
                .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snapshot-1")
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo()
                .state()
        );

        final var repo = asInstanceOf(
            BlobStoreRepository.class,
            internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName)
        );
        final var indexId = getRepositoryData(repo).resolveIndexId(indexName);
        final var shardContainer = repo.shardContainer(indexId, 0);

        // Create an extra dangling blob as if from an earlier snapshot that failed to clean up
        shardContainer.writeBlob(
            OperationPurpose.SNAPSHOT_DATA,
            BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + UUIDs.randomBase64UUID(random()),
            BytesArray.EMPTY,
            true
        );

        // Create another snapshot with distinct blobs (by adding some docs and force-merging to a single segment)
        addRandomDocuments(indexName, between(1, 10));
        assertEquals(
            1,
            client.admin().indices().prepareForceMerge(indexName).setFlush(true).setMaxNumSegments(1).get().getSuccessfulShards()
        );
        final var snapshot2Info = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snapshot-2")
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertEquals(SnapshotState.SUCCESS, snapshot2Info.state());

        // Delete the first snapshot, which should leave only the blobs from snapshot-2
        assertAcked(client.admin().cluster().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snapshot-1"));

        // Retrieve the blobs actually present
        final var actualBlobs = shardContainer.listBlobs(randomPurpose())
            .keySet()
            .stream()
            .filter(f -> ExtrasFS.isExtra(f) == false)
            .collect(Collectors.toSet());

        // Prepare to compute the expected blobs
        final var shardGeneration = Objects.requireNonNull(getRepositoryData(repo).shardGenerations().getShardGen(indexId, 0));
        final var snapBlob = Strings.format(SNAPSHOT_NAME_FORMAT, snapshot2Info.snapshotId().getUUID());
        final var indexBlob = Strings.format(SNAPSHOT_INDEX_NAME_FORMAT, shardGeneration.getGenerationUUID());

        for (var fileInfos : List.of(
            // The expected blobs according to the BlobStoreIndexShardSnapshot (snap-UUID.dat) blob
            repo.loadShardSnapshot(shardContainer, snapshot2Info.snapshotId()).indexFiles(),
            // The expected blobs according to the BlobStoreIndexShardSnapshots (index-UUID) blob
            repo.getBlobStoreIndexShardSnapshots(indexId, 0, shardGeneration)
                .snapshots()
                .stream()
                .flatMap(s -> s.indexFiles().stream())
                .toList()
        )) {
            assertEquals(
                Stream.concat(
                    Stream.of(snapBlob, indexBlob),
                    fileInfos.stream()
                        .flatMap(
                            f -> f.metadata().hashEqualsContents()
                                ? Stream.empty()
                                : IntStream.range(0, f.numberOfParts()).mapToObj(f::partName)
                        )
                ).collect(Collectors.toSet()),
                actualBlobs
            );
        }

        assertAcked(client.admin().cluster().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snapshot-2"));
    }

    protected void addRandomDocuments(String name, int numDocs) throws InterruptedException {
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = prepareIndex(name).setId(Integer.toString(i))
                .setRouting(randomAlphaOfLength(randomIntBetween(1, 10)))
                .setSource("field", "value");
        }
        indexRandom(true, indexRequestBuilders);
    }

    private String[] generateRandomNames(int num) {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < num; i++) {
            String name;
            do {
                name = randomName();
            } while (names.contains(name));
            names.add(name);
        }
        return names.toArray(new String[num]);
    }

    protected static void assertSuccessfulSnapshot(CreateSnapshotRequestBuilder requestBuilder) {
        CreateSnapshotResponse response = requestBuilder.get();
        assertSuccessfulSnapshot(response);
    }

    private static void assertSuccessfulSnapshot(CreateSnapshotResponse response) {
        assertThat(response.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(response.getSnapshotInfo().successfulShards(), equalTo(response.getSnapshotInfo().totalShards()));
    }

    protected static void assertSuccessfulRestore(RestoreSnapshotRequestBuilder requestBuilder) {
        RestoreSnapshotResponse response = requestBuilder.get();
        assertSuccessfulRestore(response);
    }

    private static void assertSuccessfulRestore(RestoreSnapshotResponse response) {
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(response.getRestoreInfo().totalShards()));
    }

    protected String randomName() {
        return randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
    }

    protected String randomRepositoryName() {
        return randomName();
    }

    protected long blobLengthFromContentLength(long contentLength) {
        return contentLength;
    }
}

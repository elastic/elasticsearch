/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
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
import java.util.Set;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
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
        return PlainActionFuture.get(repository::getRepositoryData);
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
            client().admin().cluster().preparePutRepository(name).setType(repositoryType()).setVerify(verify).setSettings(settings)
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
        assertAcked(client().admin().cluster().prepareDeleteRepository(name));
        internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class).forEach(repositories -> {
            RepositoryMissingException e = expectThrows(RepositoryMissingException.class, () -> repositories.repository(name));
            assertThat(e.repository(), equalTo(name));
        });
    }

    public void testReadNonExistingPath() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            expectThrows(NoSuchFileException.class, () -> {
                try (InputStream is = container.readBlob("non-existing")) {
                    is.read();
                }
            });
        }
    }

    public void testWriteRead() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            writeBlob(container, "foobar", new BytesArray(data), randomBoolean());
            if (randomBoolean()) {
                // override file, to check if we get latest contents
                data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
                writeBlob(container, "foobar", new BytesArray(data), false);
            }
            try (InputStream stream = container.readBlob("foobar")) {
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
            container.delete();
        }
    }

    public void testList() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            assertThat(container.listBlobs().size(), CoreMatchers.equalTo(0));
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

            Map<String, BlobMetadata> blobs = container.listBlobs();
            assertThat(blobs.size(), CoreMatchers.equalTo(numberOfFooBlobs + numberOfBarBlobs));
            for (Map.Entry<String, Long> generated : generatedBlobs.entrySet()) {
                BlobMetadata blobMetadata = blobs.get(generated.getKey());
                assertThat(generated.getKey(), blobMetadata, CoreMatchers.notNullValue());
                assertThat(blobMetadata.name(), CoreMatchers.equalTo(generated.getKey()));
                assertThat(blobMetadata.length(), CoreMatchers.equalTo(blobLengthFromContentLength(generated.getValue())));
            }

            assertThat(container.listBlobsByPrefix("foo-").size(), CoreMatchers.equalTo(numberOfFooBlobs));
            assertThat(container.listBlobsByPrefix("bar-").size(), CoreMatchers.equalTo(numberOfBarBlobs));
            assertThat(container.listBlobsByPrefix("baz-").size(), CoreMatchers.equalTo(0));
            container.delete();
        }
    }

    public void testDeleteBlobs() throws IOException {
        try (BlobStore store = newBlobStore()) {
            final List<String> blobNames = Arrays.asList("foobar", "barfoo");
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            container.deleteBlobsIgnoringIfNotExists(blobNames.iterator()); // does not raise when blobs don't exist
            byte[] data = randomBytes(randomIntBetween(10, scaledRandomIntBetween(1024, 1 << 16)));
            final BytesArray bytesArray = new BytesArray(data);
            for (String blobName : blobNames) {
                writeBlob(container, blobName, bytesArray, randomBoolean());
            }
            assertEquals(container.listBlobs().size(), 2);
            container.deleteBlobsIgnoringIfNotExists(blobNames.iterator());
            assertTrue(container.listBlobs().isEmpty());
            container.deleteBlobsIgnoringIfNotExists(blobNames.iterator()); // does not raise when blobs don't exist
        }
    }

    public static void writeBlob(
        final BlobContainer container,
        final String blobName,
        final BytesArray bytesArray,
        boolean failIfAlreadyExists
    ) throws IOException {
        if (randomBoolean()) {
            container.writeBlob(blobName, bytesArray, failIfAlreadyExists);
        } else {
            container.writeBlobAtomic(blobName, bytesArray, failIfAlreadyExists);
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

            assertTrue(containerFoo.blobExists("test"));
            assertTrue(containerBar.blobExists("test"));
            containerBar.delete();
            containerFoo.delete();
        }
    }

    public static byte[] writeRandomBlob(BlobContainer container, String name, int length) throws IOException {
        byte[] data = randomBytes(length);
        writeBlob(container, name, new BytesArray(data));
        return data;
    }

    public static byte[] readBlobFully(BlobContainer container, String name, int length) throws IOException {
        byte[] data = new byte[length];
        try (InputStream inputStream = container.readBlob(name)) {
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
        container.writeBlob(blobName, bytesArray, true);
    }

    protected BlobStore newBlobStore() {
        final String repository = createRepository(randomRepositoryName());
        return newBlobStore(repository);
    }

    protected BlobStore newBlobStore(String repository) {
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) internalCluster().getAnyMasterNodeInstance(
            RepositoriesService.class
        ).repository(repository);
        return PlainActionFuture.get(
            f -> blobStoreRepository.threadPool().generic().execute(ActionRunnable.supply(f, blobStoreRepository::blobStore))
        );
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
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(0).get(), docCounts[i]);
        }

        final String snapshotName = randomName();
        logger.info("-->  create snapshot {}:{}", repoName, snapshotName);
        assertSuccessfulSnapshot(
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).setIndices(indexNames)
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
                    int docCount = (int) client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, index);
                    for (int i = 0; i < deleteCount; i++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(index, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(index).get();
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
        assertSuccessfulRestore(client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true));

        // higher timeout since we can have quite a few shards and a little more data here
        ensureGreen(TimeValue.timeValueSeconds(120));

        for (int i = 0; i < indexCount; i++) {
            assertHitCount(client().prepareSearch(indexNames[i]).setSize(0).get(), docCounts[i]);
        }

        logger.info("-->  delete snapshot {}:{}", repoName, snapshotName);
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());

        expectThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareGetSnapshots(repoName).setSnapshots(snapshotName).execute().actionGet()
        );

        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).get());

        expectThrows(
            SnapshotRestoreException.class,
            () -> client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(randomBoolean()).get()
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
                    int deleteCount = randomIntBetween(1, docCount);
                    logger.info("--> delete {} random documents from {}", deleteCount, indexName);
                    for (int j = 0; j < deleteCount; j++) {
                        int doc = randomIntBetween(0, docCount - 1);
                        client().prepareDelete(indexName, Integer.toString(doc)).get();
                    }
                    client().admin().indices().prepareRefresh(indexName).get();
                }
            } else {
                int docCount = randomIntBetween(10, 1000);
                logger.info("--> add {} random documents to {}", docCount, indexName);
                addRandomDocuments(indexName, docCount);
            }
            // Check number of documents in this iteration
            docCounts[i] = (int) client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits().value;
            logger.info("-->  create snapshot {}:{} with {} documents", repoName, snapshotName + "-" + i, docCounts[i]);
            assertSuccessfulSnapshot(
                client().admin()
                    .cluster()
                    .prepareCreateSnapshot(repoName, snapshotName + "-" + i)
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
                client().admin()
                    .cluster()
                    .prepareRestoreSnapshot(repoName, snapshotName + "-" + iterationToRestore)
                    .setWaitForCompletion(true)
            );

            ensureGreen();
            assertHitCount(client().prepareSearch(indexName).setSize(0).get(), docCounts[iterationToRestore]);
        }

        for (int i = 0; i < iterationCount; i++) {
            logger.info("-->  delete snapshot {}:{}", repoName, snapshotName + "-" + i);
            assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName + "-" + i).get());
        }
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
            .prepareCreateSnapshot(repoName, "test-snap")
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
            .prepareCreateSnapshot(repoName, "test-snap2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-1", "test-idx-2")
            .get();
        assertEquals(createSnapshotResponse.getSnapshotInfo().successfulShards(), createSnapshotResponse.getSnapshotInfo().totalShards());

        logger.info("--> delete a snapshot");
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "test-snap").get());

        logger.info("--> verify index folder deleted from blob container");
        RepositoriesService repositoriesSvc = internalCluster().getInstance(RepositoriesService.class, internalCluster().getMasterName());
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesSvc.repository(repoName);

        final SetOnce<BlobContainer> indicesBlobContainer = new SetOnce<>();
        final PlainActionFuture<RepositoryData> repositoryData = PlainActionFuture.newFuture();
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            indicesBlobContainer.set(repository.blobStore().blobContainer(repository.basePath().add("indices")));
            repository.getRepositoryData(repositoryData);
        });

        for (IndexId indexId : repositoryData.actionGet().getIndices().values()) {
            if (indexId.getName().equals("test-idx-3")) {
                assertFalse(indicesBlobContainer.get().blobExists(indexId.getId())); // deleted index
            }
        }

        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repoName, "test-snap2").get());
    }

    protected void addRandomDocuments(String name, int numDocs) throws InterruptedException {
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders[i] = client().prepareIndex(name)
                .setId(Integer.toString(i))
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

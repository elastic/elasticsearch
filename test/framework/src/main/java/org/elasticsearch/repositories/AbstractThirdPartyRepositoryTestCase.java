/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Streams;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.RequestedRangeNotSatisfiedException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomNonDataPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public abstract class AbstractThirdPartyRepositoryTestCase extends ESSingleNodeTestCase {

    protected final String TEST_REPO_NAME = "test-repo";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).setSecureSettings(credentials()).build();
    }

    protected abstract SecureSettings credentials();

    protected abstract void createRepository(String repoName);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createRepository(TEST_REPO_NAME);
        deleteAndAssertEmpty(getRepository().basePath());
    }

    @Override
    public void tearDown() throws Exception {
        deleteAndAssertEmpty(getRepository().basePath());
        clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME).get();
        super.tearDown();
    }

    private void deleteAndAssertEmpty(BlobPath path) {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        repo.threadPool().generic().execute(ActionRunnable.run(future, () -> repo.blobStore().blobContainer(path).delete(randomPurpose())));
        future.actionGet();
        final BlobPath parent = path.parent();
        if (parent == null) {
            assertChildren(path, Collections.emptyList());
        } else {
            assertThat(listChildren(parent), not(contains(path.parts().get(path.parts().size() - 1))));
        }
    }

    public void testCreateSnapshot() {
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            TEST_REPO_NAME,
            snapshotName
        ).setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-3").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots()
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );

        assertTrue(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName).get().isAcknowledged());
    }

    public void testListChildren() {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        final Executor genericExec = repo.threadPool().generic();
        final int testBlobLen = randomIntBetween(1, 100);
        genericExec.execute(ActionRunnable.run(future, () -> {
            final BlobStore blobStore = repo.blobStore();
            blobStore.blobContainer(repo.basePath().add("foo"))
                .writeBlob(
                    randomPurpose(),
                    "nested-blob",
                    new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)),
                    testBlobLen,
                    false
                );
            blobStore.blobContainer(repo.basePath().add("foo").add("nested"))
                .writeBlob(randomPurpose(), "bar", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
            blobStore.blobContainer(repo.basePath().add("foo").add("nested2"))
                .writeBlob(randomPurpose(), "blub", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
        }));
        future.actionGet();
        assertChildren(repo.basePath(), Collections.singleton("foo"));
        BlobStoreTestUtil.assertBlobsByPrefix(repo, repo.basePath(), "fo", Collections.emptyMap());
        assertChildren(repo.basePath().add("foo"), List.of("nested", "nested2"));
        BlobStoreTestUtil.assertBlobsByPrefix(
            repo,
            repo.basePath().add("foo"),
            "nest",
            Collections.singletonMap("nested-blob", new BlobMetadata("nested-blob", testBlobLen))
        );
        assertChildren(repo.basePath().add("foo").add("nested"), Collections.emptyList());
        if (randomBoolean()) {
            deleteAndAssertEmpty(repo.basePath());
        } else {
            deleteAndAssertEmpty(repo.basePath().add("foo"));
        }
    }

    public void testCleanup() throws Exception {
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            TEST_REPO_NAME,
            snapshotName
        ).setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-3").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots()
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );

        final BlobStoreRepository repo = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(TEST_REPO_NAME);
        final Executor genericExec = repo.threadPool().executor(ThreadPool.Names.GENERIC);

        logger.info("--> creating a dangling index folder");

        createDanglingIndex(repo, genericExec);

        logger.info("--> deleting a snapshot to trigger repository cleanup");
        clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, snapshotName).get();

        BlobStoreTestUtil.assertConsistency(repo);

        logger.info("--> Create dangling index");
        createDanglingIndex(repo, genericExec);

        logger.info("--> Execute repository cleanup");
        final CleanupRepositoryResponse response = clusterAdmin().prepareCleanupRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            TEST_REPO_NAME
        ).get();
        assertCleanupResponse(response, 3L, 1L);
    }

    public void testIndexLatest() throws Exception {
        // This test verifies that every completed snapshot operation updates a blob called literally 'index.latest' (by default at least),
        // which is important because some external systems use the freshness of this specific blob as an indicator of whether a repository
        // is in use. Most notably, ESS checks this blob as an extra layer of protection against a bug in the delete-old-repositories
        // process incorrectly deleting repositories that have seen recent writes. It's possible that some future development might change
        // the meaning of this blob, and that's ok, but we must continue to update it to keep those external systems working.

        createIndex("test-idx-1");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }

        final var repository = getRepository();
        final var blobContents = new HashSet<BytesReference>();

        final var createSnapshot1Response = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, randomIdentifier())
            .setWaitForCompletion(true)
            .get();
        assertTrue(blobContents.add(readIndexLatest(repository)));

        clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME).get();
        assertFalse(blobContents.add(readIndexLatest(repository)));

        final var createSnapshot2Response = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, randomIdentifier())
            .setWaitForCompletion(true)
            .get();
        assertTrue(blobContents.add(readIndexLatest(repository)));

        assertAcked(
            clusterAdmin().prepareDeleteSnapshot(
                TEST_REQUEST_TIMEOUT,
                TEST_REPO_NAME,
                createSnapshot1Response.getSnapshotInfo().snapshotId().getName()
            )
        );
        assertTrue(blobContents.add(readIndexLatest(repository)));

        assertAcked(
            clusterAdmin().prepareDeleteSnapshot(
                TEST_REQUEST_TIMEOUT,
                TEST_REPO_NAME,
                createSnapshot2Response.getSnapshotInfo().snapshotId().getName()
            )
        );
        assertTrue(blobContents.add(readIndexLatest(repository)));
    }

    public void testReadFromPositionWithLength() {
        final var blobName = randomIdentifier();
        final var blobBytes = randomBytesReference(randomIntBetween(100, 2_000));

        final var repository = getRepository();
        executeOnBlobStore(repository, blobStore -> {
            blobStore.writeBlob(randomPurpose(), blobName, blobBytes, true);
            return null;
        });

        {
            assertThat("Exact Range", readBlob(repository, blobName, 0L, blobBytes.length()), equalTo(blobBytes));
        }
        {
            int position = randomIntBetween(0, blobBytes.length() - 1);
            int length = randomIntBetween(1, blobBytes.length() - position);
            assertThat(
                "Random Range: " + position + '-' + (position + length),
                readBlob(repository, blobName, position, length),
                equalTo(blobBytes.slice(position, length))
            );
        }
        {
            int position = randomIntBetween(0, blobBytes.length() - 1);
            long length = randomLongBetween(1L, Long.MAX_VALUE - position - 1L);
            assertThat(
                "Random Larger Range: " + position + '-' + (position + length),
                readBlob(repository, blobName, position, length),
                equalTo(blobBytes.slice(position, Math.toIntExact(Math.min(length, blobBytes.length() - position))))
            );
        }
    }

    public void testSkipBeyondBlobLengthShouldThrowEOFException() throws IOException {
        final var blobName = randomIdentifier();
        final int blobLength = randomIntBetween(100, 2_000);
        final var blobBytes = randomBytesReference(blobLength);

        final var repository = getRepository();
        executeOnBlobStore(repository, blobStore -> {
            blobStore.writeBlob(randomPurpose(), blobName, blobBytes, true);
            return null;
        });

        var blobContainer = repository.blobStore().blobContainer(repository.basePath());
        try (var input = blobContainer.readBlob(randomPurpose(), blobName, 0, blobLength); var output = new BytesStreamOutput()) {
            Streams.copy(input, output, false);
            expectThrows(EOFException.class, () -> input.skipNBytes(randomLongBetween(1, 1000)));
        }

        try (var input = blobContainer.readBlob(randomPurpose(), blobName, 0, blobLength); var output = new BytesStreamOutput()) {
            final int capacity = between(1, blobLength);
            final ByteBuffer byteBuffer = randomBoolean() ? ByteBuffer.allocate(capacity) : ByteBuffer.allocateDirect(capacity);
            Streams.read(input, byteBuffer, capacity);
            expectThrows(EOFException.class, () -> input.skipNBytes((blobLength - capacity) + randomLongBetween(1, 1000)));
        }
    }

    protected void testReadFromPositionLargerThanBlobLength(Predicate<RequestedRangeNotSatisfiedException> responseCodeChecker) {
        final var blobName = randomIdentifier();
        final var blobBytes = randomBytesReference(randomIntBetween(100, 2_000));

        final var repository = getRepository();
        executeOnBlobStore(repository, blobStore -> {
            blobStore.writeBlob(randomPurpose(), blobName, blobBytes, true);
            return null;
        });

        long position = randomLongBetween(blobBytes.length(), Long.MAX_VALUE - 1L);
        long length = randomLongBetween(1L, Long.MAX_VALUE - position);

        var exception = expectThrows(UncategorizedExecutionException.class, () -> readBlob(repository, blobName, position, length));
        assertThat(exception.getCause(), instanceOf(ExecutionException.class));
        assertThat(exception.getCause().getCause(), instanceOf(RequestedRangeNotSatisfiedException.class));
        assertThat(
            exception.getCause().getCause().getMessage(),
            containsString(
                "Requested range [position="
                    + position
                    + ", length="
                    + length
                    + "] cannot be satisfied for ["
                    + repository.basePath().buildAsString()
                    + blobName
                    + ']'
            )
        );
        var rangeNotSatisfiedException = (RequestedRangeNotSatisfiedException) exception.getCause().getCause();
        assertThat(rangeNotSatisfiedException.getPosition(), equalTo(position));
        assertThat(rangeNotSatisfiedException.getLength(), equalTo(length));
        assertThat(responseCodeChecker.test(rangeNotSatisfiedException), is(true));
    }

    protected static <T> T executeOnBlobStore(BlobStoreRepository repository, CheckedFunction<BlobContainer, T, IOException> fn) {
        final var future = new PlainActionFuture<T>();
        repository.threadPool().generic().execute(ActionRunnable.supply(future, () -> {
            var blobContainer = repository.blobStore().blobContainer(repository.basePath());
            return fn.apply(blobContainer);
        }));
        return future.actionGet();
    }

    protected static BytesReference readBlob(BlobStoreRepository repository, String blobName, long position, long length) {
        return executeOnBlobStore(repository, blobContainer -> {
            try (var input = blobContainer.readBlob(randomPurpose(), blobName, position, length); var output = new BytesStreamOutput()) {
                Streams.copy(input, output);
                return output.bytes();
            }
        });
    }

    private static BytesReference readIndexLatest(BlobStoreRepository repository) throws IOException {
        try (var baos = new BytesStreamOutput()) {
            Streams.copy(
                repository.blobStore()
                    .blobContainer(repository.basePath())
                    .readBlob(
                        randomPurpose(),
                        // Deliberately not using BlobStoreRepository#INDEX_LATEST_BLOB here, it's important for external systems that a
                        // blob with literally this name is updated on each write:
                        "index.latest"
                    ),
                baos
            );
            return baos.bytes();
        }
    }

    protected void assertCleanupResponse(CleanupRepositoryResponse response, long bytes, long blobs) {
        assertThat(response.result().blobs(), equalTo(1L + 2L));
        assertThat(response.result().bytes(), equalTo(3L + 2 * 3L));
    }

    private static void createDanglingIndex(final BlobStoreRepository repo, final Executor genericExec) throws Exception {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        genericExec.execute(ActionRunnable.run(future, () -> {
            final BlobStore blobStore = repo.blobStore();
            blobStore.blobContainer(repo.basePath().add("indices").add("foo"))
                .writeBlob(randomPurpose(), "bar", new ByteArrayInputStream(new byte[3]), 3, false);
            for (String prefix : Arrays.asList("snap-", "meta-")) {
                blobStore.blobContainer(repo.basePath())
                    .writeBlob(randomNonDataPurpose(), prefix + "foo.dat", new ByteArrayInputStream(new byte[3]), 3, false);
            }
        }));
        future.get();

        final PlainActionFuture<Boolean> corruptionFuture = new PlainActionFuture<>();
        genericExec.execute(ActionRunnable.supply(corruptionFuture, () -> {
            final BlobStore blobStore = repo.blobStore();
            return blobStore.blobContainer(repo.basePath().add("indices")).children(randomPurpose()).containsKey("foo")
                && blobStore.blobContainer(repo.basePath().add("indices").add("foo")).blobExists(randomPurpose(), "bar")
                && blobStore.blobContainer(repo.basePath()).blobExists(randomNonDataPurpose(), "meta-foo.dat")
                && blobStore.blobContainer(repo.basePath()).blobExists(randomNonDataPurpose(), "snap-foo.dat");
        }));
        assertTrue(corruptionFuture.get());
    }

    private void assertChildren(BlobPath path, Collection<String> children) {
        listChildren(path);
        final Set<String> foundChildren = listChildren(path);
        if (children.isEmpty()) {
            assertThat(foundChildren, empty());
        } else {
            assertThat(foundChildren, containsInAnyOrder(children.toArray(Strings.EMPTY_ARRAY)));
        }
    }

    private Set<String> listChildren(BlobPath path) {
        final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        final BlobStoreRepository repository = getRepository();
        repository.threadPool()
            .generic()
            .execute(ActionRunnable.supply(future, () -> repository.blobStore().blobContainer(path).children(randomPurpose()).keySet()));
        return future.actionGet();
    }

    protected BlobStoreRepository getRepository() {
        return (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(TEST_REPO_NAME);
    }
}

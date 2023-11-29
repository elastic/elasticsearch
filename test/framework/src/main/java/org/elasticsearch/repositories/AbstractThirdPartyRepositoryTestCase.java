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
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Streams;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        clusterAdmin().prepareDeleteRepository(TEST_REPO_NAME).get();
        super.tearDown();
    }

    private void deleteAndAssertEmpty(BlobPath path) {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        repo.threadPool()
            .generic()
            .execute(ActionRunnable.run(future, () -> repo.blobStore().blobContainer(path).delete(OperationPurpose.SNAPSHOT)));
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
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REPO_NAME, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REPO_NAME).setSnapshots(snapshotName).get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );

        assertTrue(clusterAdmin().prepareDeleteSnapshot(TEST_REPO_NAME, snapshotName).get().isAcknowledged());
    }

    public void testListChildren() throws Exception {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        final Executor genericExec = repo.threadPool().generic();
        final int testBlobLen = randomIntBetween(1, 100);
        genericExec.execute(ActionRunnable.run(future, () -> {
            final BlobStore blobStore = repo.blobStore();
            blobStore.blobContainer(repo.basePath().add("foo"))
                .writeBlob(
                    OperationPurpose.SNAPSHOT,
                    "nested-blob",
                    new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)),
                    testBlobLen,
                    false
                );
            blobStore.blobContainer(repo.basePath().add("foo").add("nested"))
                .writeBlob(
                    OperationPurpose.SNAPSHOT,
                    "bar",
                    new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)),
                    testBlobLen,
                    false
                );
            blobStore.blobContainer(repo.basePath().add("foo").add("nested2"))
                .writeBlob(
                    OperationPurpose.SNAPSHOT,
                    "blub",
                    new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)),
                    testBlobLen,
                    false
                );
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
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REPO_NAME, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REPO_NAME).setSnapshots(snapshotName).get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );

        final BlobStoreRepository repo = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(TEST_REPO_NAME);
        final Executor genericExec = repo.threadPool().executor(ThreadPool.Names.GENERIC);

        logger.info("--> creating a dangling index folder");

        createDanglingIndex(repo, genericExec);

        logger.info("--> deleting a snapshot to trigger repository cleanup");
        clusterAdmin().prepareDeleteSnapshot(TEST_REPO_NAME, snapshotName).get();

        BlobStoreTestUtil.assertConsistency(repo);

        logger.info("--> Create dangling index");
        createDanglingIndex(repo, genericExec);

        logger.info("--> Execute repository cleanup");
        final CleanupRepositoryResponse response = clusterAdmin().prepareCleanupRepository(TEST_REPO_NAME).get();
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

        final var createSnapshot1Response = clusterAdmin().prepareCreateSnapshot(TEST_REPO_NAME, randomIdentifier())
            .setWaitForCompletion(true)
            .get();
        assertTrue(blobContents.add(readIndexLatest(repository)));

        clusterAdmin().prepareGetSnapshots(TEST_REPO_NAME).get();
        assertFalse(blobContents.add(readIndexLatest(repository)));

        final var createSnapshot2Response = clusterAdmin().prepareCreateSnapshot(TEST_REPO_NAME, randomIdentifier())
            .setWaitForCompletion(true)
            .get();
        assertTrue(blobContents.add(readIndexLatest(repository)));

        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REPO_NAME, createSnapshot1Response.getSnapshotInfo().snapshotId().getName()));
        assertTrue(blobContents.add(readIndexLatest(repository)));

        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REPO_NAME, createSnapshot2Response.getSnapshotInfo().snapshotId().getName()));
        assertTrue(blobContents.add(readIndexLatest(repository)));
    }

    private static BytesReference readIndexLatest(BlobStoreRepository repository) throws IOException {
        try (var baos = new BytesStreamOutput()) {
            Streams.copy(
                repository.blobStore()
                    .blobContainer(repository.basePath())
                    .readBlob(
                        OperationPurpose.SNAPSHOT,
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
                .writeBlob(OperationPurpose.SNAPSHOT, "bar", new ByteArrayInputStream(new byte[3]), 3, false);
            for (String prefix : Arrays.asList("snap-", "meta-")) {
                blobStore.blobContainer(repo.basePath())
                    .writeBlob(OperationPurpose.SNAPSHOT, prefix + "foo.dat", new ByteArrayInputStream(new byte[3]), 3, false);
            }
        }));
        future.get();

        final PlainActionFuture<Boolean> corruptionFuture = new PlainActionFuture<>();
        genericExec.execute(ActionRunnable.supply(corruptionFuture, () -> {
            final BlobStore blobStore = repo.blobStore();
            return blobStore.blobContainer(repo.basePath().add("indices")).children(OperationPurpose.SNAPSHOT).containsKey("foo")
                && blobStore.blobContainer(repo.basePath().add("indices").add("foo")).blobExists(OperationPurpose.SNAPSHOT, "bar")
                && blobStore.blobContainer(repo.basePath()).blobExists(OperationPurpose.SNAPSHOT, "meta-foo.dat")
                && blobStore.blobContainer(repo.basePath()).blobExists(OperationPurpose.SNAPSHOT, "snap-foo.dat");
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
            .execute(
                ActionRunnable.supply(future, () -> repository.blobStore().blobContainer(path).children(OperationPurpose.SNAPSHOT).keySet())
            );
        return future.actionGet();
    }

    protected BlobStoreRepository getRepository() {
        return (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(TEST_REPO_NAME);
    }
}

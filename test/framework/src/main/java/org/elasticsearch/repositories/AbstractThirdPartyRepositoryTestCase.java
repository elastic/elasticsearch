/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public abstract class AbstractThirdPartyRepositoryTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .setSecureSettings(credentials())
            .build();
    }

    protected abstract SecureSettings credentials();

    protected abstract void createRepository(String repoName);

    protected BlobStoreTestUtil createUtil(BlobStoreRepository repository) {
        return new BlobStoreTestUtil(repository);
    }

    private BlobStoreTestUtil util;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createRepository("test-repo");
        util = createUtil(getRepository());
        util.deleteAndAssertEmpty(getRepository().basePath());
    }

    public void testCreateSnapshot() {
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client().admin()
                .cluster()
                .prepareGetSnapshots("test-repo")
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots("test-repo")
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS));

        assertTrue(client().admin()
                .cluster()
                .prepareDeleteSnapshot("test-repo", snapshotName)
                .get()
                .isAcknowledged());
    }

    public void testListChildren() throws Exception {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        final Executor genericExec = repo.threadPool().generic();
        final int testBlobLen = randomIntBetween(1, 100);
        genericExec.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repo.blobStore();
                blobStore.blobContainer(repo.basePath().add("foo"))
                    .writeBlob("nested-blob", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
                blobStore.blobContainer(repo.basePath().add("foo").add("nested"))
                    .writeBlob("bar", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
                blobStore.blobContainer(repo.basePath().add("foo").add("nested2"))
                    .writeBlob("blub", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
                future.onResponse(null);
            }
        });
        future.actionGet();
        util.assertChildren(repo.basePath(), Collections.singleton("foo"));
        util.assertBlobsByPrefix(repo.basePath(), "fo", Collections.emptyMap());
        util.assertChildren(repo.basePath().add("foo"), List.of("nested", "nested2"));
        util.assertBlobsByPrefix(repo.basePath().add("foo"), "nest",
            Collections.singletonMap("nested-blob", new PlainBlobMetaData("nested-blob", testBlobLen)));
        util.assertChildren(repo.basePath().add("foo").add("nested"), Collections.emptyList());
        if (randomBoolean()) {
            util.deleteAndAssertEmpty(repo.basePath());
        } else {
            util.deleteAndAssertEmpty(repo.basePath().add("foo"));
        }
    }

    public void testCleanup() throws Exception {
        createRepository("test-repo");

        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3", "doc", Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client().admin()
                .cluster()
                .prepareGetSnapshots("test-repo")
                .setSnapshots(snapshotName)
                .get()
                .getSnapshots("test-repo")
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS));

        logger.info("--> creating a dangling index folder");
        util.createDanglingIndex("foo", Set.of("bar"));
        util.assertCorruptionVisible(Map.of("foo", Set.of("bar")));

        logger.info("--> deleting a snapshot to trigger repository cleanup");
        client().admin().cluster().deleteSnapshot(new DeleteSnapshotRequest("test-repo", snapshotName)).actionGet();

        util.assertConsistency();
    }

    private BlobStoreRepository getRepository() {
        return (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
    }
}

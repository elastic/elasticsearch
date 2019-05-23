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

package org.elasticsearch.repositories.azure;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class AzureStorageCleanupThirdPartyTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(AzureRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        assertThat(System.getProperty("test.azure.account"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.key"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.container"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.base"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.default.account", System.getProperty("test.azure.account"));
        secureSettings.setString("azure.client.default.key", System.getProperty("test.azure.key"));
        return Settings.builder()
            .put(super.nodeSettings())
            .setSecureSettings(secureSettings)
            .build();
    }

    public void testCleanup() {
        Client client = client();

        AcknowledgedResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
            .setType("azure")
            .setSettings(Settings.builder()
                .put("container", System.getProperty("test.azure.container"))
                .put("base_path", System.getProperty("test.azure.base"))
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

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
        assertThat(count(client, "test-idx-1"), equalTo(100L));
        assertThat(count(client, "test-idx-2"), equalTo(100L));
        assertThat(count(client, "test-idx-3"), equalTo(100L));

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client.admin()
                .cluster()
                .prepareGetSnapshots("test-repo")
                .setSnapshots("test-snap")
                .get()
                .getSnapshots()
                .get(0)
                .state(),
            equalTo(SnapshotState.SUCCESS));

        logger.info("--> creating a dangling index folder");
        final AzureRepository repo =
            (AzureRepository) getInstanceFromNode(RepositoriesService.class).repository("test-repo");
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        final Executor genericExec = repo.threadPool().executor(ThreadPool.Names.GENERIC);
        genericExec.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() throws Exception {
                final BlobStore blobStore = repo.blobStore();
                blobStore.blobContainer(BlobPath.cleanPath().add("indices").add("foo"))
                    .writeBlob("bar", new ByteArrayInputStream(new byte[0]), 0, false);
                future.onResponse(null);
            }
        });
        future.actionGet();

        logger.info("--> deleting a snapshot to trigger repository cleanup");
        client.admin().cluster().deleteSnapshot(new DeleteSnapshotRequest("test-repo", "test-snap")).actionGet();

        BlobStoreTestUtil.assertConsistency(repo, genericExec);
    }

    private long count(Client client, String index) {
        return client.prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
    }
}

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

package org.elasticsearch.snapshots;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.mockstore.MockRepositoryModule;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DedicatedClusterSnapshotRestoreTests extends AbstractSnapshotTests {

    @Test
    public void restorePersistentSettingsTest() throws Exception {
        logger.info("--> start node");
        cluster().startNode(settingsBuilder().put("gateway.type", "local"));
        Client client = client();

        // Add dummy persistent setting
        logger.info("--> set test persistent setting");
        String settingValue = "test-" + randomInt();
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(ImmutableSettings.settingsBuilder().put(ThreadPool.THREADPOOL_GROUP + "dummy.value", settingValue)).execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().get(ThreadPool.THREADPOOL_GROUP + "dummy.value"), equalTo(settingValue));

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", newTempDir())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute().actionGet().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> clean the test persistent setting");
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(ImmutableSettings.settingsBuilder().put(ThreadPool.THREADPOOL_GROUP + "dummy.value", "")).execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().get(ThreadPool.THREADPOOL_GROUP + "dummy.value"), equalTo(""));

        logger.info("--> restore snapshot");
        client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setRoutingTable(false).setNodes(false).execute().actionGet().getState()
                .getMetaData().persistentSettings().get(ThreadPool.THREADPOOL_GROUP + "dummy.value"), equalTo(settingValue));
    }

    @Test
    public void snapshotDuringNodeShutdownTest() throws Exception {
        logger.info("--> start 2 nodes");
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0).put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType(MockRepositoryModule.class.getCanonicalName()).setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("location", newTempDir(LifecycleScope.TEST))
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
        unblockNode(blockedNode);

        logger.info("--> stopping node", blockedNode);
        stopNode(blockedNode);
        logger.info("--> waiting for completion");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(60));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");
    }

    @Test
    public void snapshotWithStuckNodeTest() throws Exception {
        logger.info("--> start 2 nodes");
        ArrayList<String> nodes = newArrayList();
        nodes.add(cluster().startNode());
        nodes.add(cluster().startNode());
        Client client = client();

        assertAcked(prepareCreate("test-idx", 2, settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0).put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client.prepareCount("test-idx").get().getCount(), equalTo(100L));

        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType(MockRepositoryModule.class.getCanonicalName()).setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("location", newTempDir(LifecycleScope.TEST))
                                .put("random", randomAsciiOfLength(10))
                                .put("wait_after_unblock", 200)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-idx");
        // Remove it from the list of available nodes
        nodes.remove(blockedNode);

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], aborting snapshot", blockedNode);

        ListenableActionFuture<DeleteSnapshotResponse> deleteSnapshotResponseFuture = cluster().client(nodes.get(0)).admin().cluster().prepareDeleteSnapshot("test-repo", "test-snap").execute();
        // Make sure that abort makes some progress
        Thread.sleep(100);
        unblockNode(blockedNode);
        logger.info("--> stopping node", blockedNode);
        stopNode(blockedNode);
        DeleteSnapshotResponse deleteSnapshotResponse = deleteSnapshotResponseFuture.get();
        assertThat(deleteSnapshotResponse.isAcknowledged(), equalTo(true));

        logger.info("--> making sure that snapshot no longer exists");
        assertThrows(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").execute(), SnapshotMissingException.class);
        logger.info("--> done");
    }

    @Test
    @TestLogging("snapshots:TRACE")
    public void restoreIndexWithMissingShards() throws Exception {
        logger.info("--> start 2 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local"));
        cluster().startNode(settingsBuilder().put("gateway.type", "local"));
        immutableCluster().wipeIndices("_all");

        assertAcked(prepareCreate("test-idx-1", 2, settingsBuilder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)
                .put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx-1");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareCount("test-idx-1").get().getCount(), equalTo(100L));

        logger.info("--> shutdown one of the nodes");
        cluster().stopRandomDataNode();
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("1m").setWaitForNodes("<2").execute().actionGet().isTimedOut(), equalTo(false));

        assertAcked(prepareCreate("test-idx-2", 1, settingsBuilder().put("number_of_shards", 6)
                .put("number_of_replicas", 0)
                .put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)));
        ensureGreen("test-idx-2");

        logger.info("--> indexing some data into test-idx-2");
        for (int i = 0; i < 100; i++) {
            index("test-idx-2", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareCount("test-idx-2").get().getCount(), equalTo(100L));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", newTempDir())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> start snapshot with default settings - should fail");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).execute().actionGet();

        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.FAILED));

        createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2").setWaitForCompletion(true).setPartial(true).execute().actionGet();
        logger.info("State: [{}], Reason: [{}]", createSnapshotResponse.getSnapshotInfo().state(), createSnapshotResponse.getSnapshotInfo().reason());
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(12));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), lessThan(12));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(6));
        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-2").execute().actionGet().getSnapshots().get(0).state(), equalTo(SnapshotState.PARTIAL));

        assertAcked(client().admin().indices().prepareClose("test-idx-1", "test-idx-2").execute().actionGet());

        logger.info("--> restore incomplete snapshot - should fail");
        assertThrows(client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2").setRestoreGlobalState(false).setWaitForCompletion(true).execute(), SnapshotRestoreException.class);

        logger.info("--> restore snapshot for the index that was snapshotted completely");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2").setRestoreGlobalState(false).setIndices("test-idx-2").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo(), notNullValue());
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(6));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        ensureGreen("test-idx-2");

        assertThat(client().prepareCount("test-idx-2").get().getCount(), equalTo(100L));

    }
}

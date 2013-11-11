/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.mockstore.MockRepositoryModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numNodes = 0)
public class DedicatedClusterSnapshotRestoreTests extends AbstractSnapshotTests {

    @Test
    public void restorePersistentSettingsTest() throws Exception {
        logger.info("--> start node");
        cluster().startNode(settingsBuilder().put("gateway.type", "local"));
        Client client = cluster().client();

        // Add dummy persistent setting
        logger.info("--> set test persistent setting");
        String settingValue = "test-" + randomInt();
        client.admin().cluster().prepareUpdateSettings().setPersistentSettings(ImmutableSettings.settingsBuilder().put(ThreadPool.THREADPOOL_GROUP + "dummy.value", settingValue)).execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setFilterRoutingTable(true).setFilterNodes(true).execute().actionGet().getState()
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
        assertThat(client.admin().cluster().prepareState().setFilterRoutingTable(true).setFilterNodes(true).execute().actionGet().getState()
                .getMetaData().persistentSettings().get(ThreadPool.THREADPOOL_GROUP + "dummy.value"), equalTo(""));

        logger.info("--> restore snapshot");
        client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true).setWaitForCompletion(true).execute().actionGet();
        assertThat(client.admin().cluster().prepareState().setFilterRoutingTable(true).setFilterNodes(true).execute().actionGet().getState()
                .getMetaData().persistentSettings().get(ThreadPool.THREADPOOL_GROUP + "dummy.value"), equalTo(settingValue));
    }

    @Test
    public void snapshotDuringNodeShutdownTest() throws Exception {
        logger.info("--> start 2 nodes");
        ArrayList<String> nodes = newArrayList();
        nodes.add(cluster().startNode());
        nodes.add(cluster().startNode());
        Client client = cluster().client();

        assertAcked(prepareCreate("test-idx", 2, settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0).put(MockDirectoryHelper.RANDOM_NO_DELETE_OPEN_FILE, false)));
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(run(client.prepareCount("test-idx")).getCount(), equalTo(100L));

        logger.info("--> create repository");
        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse = run(client.admin().cluster().preparePutRepository("test-repo")
                .setType(MockRepositoryModule.class.getCanonicalName()).setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("location", newTempDir(LifecycleScope.TEST))
                                .put("random", randomAsciiOfLength(10))
                                .put("random_data_file_blocking_rate", 0.1)
                                .put("wait_after_unblock", 200)
                ));
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> snapshot");
        run(client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx"));

        String blockedNode = waitForCompletionOrBlock(nodes, "test-repo", "test-snap", TimeValue.timeValueSeconds(60));
        if (blockedNode != null) {
            logger.info("--> execution was blocked on node [{}], shutting it down", blockedNode);
            unblock("test-repo");
            logger.info("--> stopping node", blockedNode);
            stopNode(blockedNode);
            logger.info("--> waiting for completion");
            SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(60));
            logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
            logger.info("--> done");
        } else {
            logger.info("--> done without blocks");
            ImmutableList<SnapshotInfo> snapshotInfos = run(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap")).getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfos.get(0).shardFailures().size(), equalTo(0));

        }
    }
}

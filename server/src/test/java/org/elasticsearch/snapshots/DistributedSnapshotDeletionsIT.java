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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class DistributedSnapshotDeletionsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, org.elasticsearch.test.transport.MockTransportService.TestPlugin.class);
    }

    public void testDeleteSnapshot() throws Exception {
        logger.info("--> start 1 master and 3 data nodes");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode(); // Start an additional data-node
        internalCluster().startDataOnlyNode(); // Start an additional data-node
        Client client = client();

        String repoName = "test-repo";
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("mock").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-3", "_doc").setSource("foo", "bar"));

        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap-1")
                                                                      .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete snapshot");
        ActionFuture<AcknowledgedResponse> future = client.admin().cluster().prepareDeleteSnapshot(repoName,"test-snap-1").execute();
        future.actionGet(1, TimeUnit.MINUTES);

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots(repoName).addSnapshots("test-snap-1"), SnapshotMissingException.class);

        // Subtract four files that will remain in the repository:
        //   (1) index-1
        //   (2) index-0 (because we keep the previous version) and
        //   (3) index-latest
        //   (4) incompatible-snapshots
        assertThat("not all files were deleted during snapshot cancellation",
            numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 4));
        logger.info("--> done");
    }

    public void testDeleteSnapshotWithoutData() throws Exception {
        logger.info("--> start 1 master and 3 data nodes");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode(); // Start an additional data-node
        internalCluster().startDataOnlyNode(); // Start an additional data-node
        Client client = client();

        String repoName = "test-repo";
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("mock").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);

        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap-1")
                                                                      .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));

        logger.info("--> delete snapshot");
        ActionFuture<AcknowledgedResponse> future = client.admin().cluster().prepareDeleteSnapshot(repoName,"test-snap-1").execute();
        future.actionGet(1, TimeUnit.MINUTES);

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots(repoName).addSnapshots("test-snap-1"), SnapshotMissingException.class);

        // Subtract four files that will remain in the repository:
        //   (1) index-1
        //   (2) index-0 (because we keep the previous version) and
        //   (3) index-latest
        //   (4) incompatible-snapshots
        assertThat("not all files were deleted during snapshot cancellation",
            numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 4));
        logger.info("--> done");
    }

    public void testDeleteSnapshotWithDataNodeOutage() throws Exception {
        logger.info("--> start 1 master and 3 data nodes");
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode(); // Start an additional data-node
        internalCluster().startDataOnlyNode(); // Start an additional data-node
        Client client = client();

        String repoName = "test-repo";
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("mock").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-3", "_doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap-1")
                                                                      .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, dataNode).repository(repoName)).blockOnDataFiles(true);

        logger.info("--> delete snapshot");
        ActionFuture<AcknowledgedResponse> future = client.admin().cluster().prepareDeleteSnapshot(repoName,"test-snap-1").execute();

        logger.info("--> waiting for block to kick in on node [{}]", dataNode);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(10));

        logger.info("--> stopping node [{}]", dataNode);
        stopNode(dataNode);

        try {
            future.actionGet(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.info("--> the node where the client is connected is down, sleep for 10 seconds before proceeding");
            Thread.sleep(10 * 1000);
        }

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client().admin().cluster().prepareGetSnapshots(repoName).addSnapshots("test-snap-1"), SnapshotMissingException.class);

        // Subtract four files that will remain in the repository:
        //   (1) index-1
        //   (2) index-0 (because we keep the previous version) and
        //   (3) index-latest
        //   (4) incompatible-snapshots
        assertThat("not all files were deleted during snapshot cancellation",
            numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 4));
        logger.info("--> done");
    }

    public void testDeleteSnapshotWithMasterNodeOutage() throws Exception {
        logger.info("--> start 3 master and 3 data nodes");
        internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        Client client = client();

        String repoName = "test-repo";
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("mock").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-3", "_doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap-1")
                                                                      .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, dataNode).repository(repoName)).blockOnDataFiles(true);

        logger.info("--> delete snapshot");
        ActionFuture<AcknowledgedResponse> future = client.admin().cluster().prepareDeleteSnapshot(repoName,"test-snap-1").execute();

        logger.info("--> waiting for block to kick in on node [{}]", dataNode);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(10));

        String masterNode = internalCluster().getMasterName();
        logger.info("--> stopping current master [{}]", masterNode);
        stopNode(masterNode);

        logger.info("--> unblocking blocked node [{}]", dataNode);
        unblockNode(repoName, dataNode);

        try {
            future.actionGet(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.info("--> new master may not be elected, sleep for 10 seconds");
            Thread.sleep(10 * 1000);
        }

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client().admin().cluster().prepareGetSnapshots(repoName).addSnapshots("test-snap-1"), SnapshotMissingException.class);

        // Subtract four files that will remain in the repository:
        //   (1) index-1
        //   (2) index-0 (because we keep the previous version) and
        //   (3) index-latest
        //   (4) incompatible-snapshots
        assertThat("not all files were deleted during snapshot cancellation",
            numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 4));
        logger.info("--> done");
    }

    public void testDeleteSnapshotWithBothMasterAndDataNodeOutage() throws Exception {
        logger.info("--> start 3 master and 3 data nodes");
        internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        Client client = client();

        String repoName = "test-repo";
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("mock").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-3", "_doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap-1")
                                                                      .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, dataNode).repository(repoName)).blockOnDataFiles(true);

        logger.info("--> delete snapshot");
        ActionFuture<AcknowledgedResponse> future = client.admin().cluster().prepareDeleteSnapshot(repoName,"test-snap-1").execute();

        logger.info("--> waiting for block to kick in on node [{}]", dataNode);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(10));

        String masterNode = internalCluster().getMasterName();
        logger.info("--> stopping current master [{}] and data node [{}]", masterNode, dataNode);
        stopNode(masterNode);
        stopNode(dataNode);

        try {
            future.actionGet(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.info("--> new master may not be elected, sleep for 10 seconds");
            Thread.sleep(10 * 1000);
        }

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client().admin().cluster().prepareGetSnapshots(repoName).addSnapshots("test-snap-1"), SnapshotMissingException.class);

        // Subtract four files that will remain in the repository:
        //   (1) index-1
        //   (2) index-0 (because we keep the previous version) and
        //   (3) index-latest
        //   (4) incompatible-snapshots
        assertThat("not all files were deleted during snapshot cancellation",
            numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 4));
        logger.info("--> done");
    }

    public void testDeleteSnapshotWithOneNode() throws Exception {
        logger.info("--> start 1 node");
        internalCluster().startNode(Settings.builder()
            .put("thread_pool.snapshot.core", 1)
            .put("thread_pool.snapshot.max", 1)
            .build());
        Client client = client();

        String repoName = "test-repo";
        Path repo = randomRepoPath();
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(client.admin().cluster().preparePutRepository(repoName)
            .setType("mock").setSettings(Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));
        int numberOfFilesBeforeSnapshot = numberOfFiles(repo);

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(true,
            client().prepareIndex("test-idx-1", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2", "_doc").setSource("foo", "bar"),
            client().prepareIndex("test-idx-3", "_doc").setSource("foo", "bar"));

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot(repoName, "test-snap-1")
                                                                      .setWaitForCompletion(true).setIndices("test-idx-*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
                   equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        logger.info("--> delete snapshot");
        ActionFuture<AcknowledgedResponse> future = client.admin().cluster().prepareDeleteSnapshot(repoName,"test-snap-1").execute();
        future.actionGet(1, TimeUnit.MINUTES);

        logger.info("--> make sure snapshot doesn't exist");
        assertThrows(client.admin().cluster().prepareGetSnapshots(repoName).addSnapshots("test-snap-1"), SnapshotMissingException.class);

        // Subtract four files that will remain in the repository:
        //   (1) index-1
        //   (2) index-0 (because we keep the previous version) and
        //   (3) index-latest
        //   (4) incompatible-snapshots
        assertThat("not all files were deleted during snapshot cancellation",
            numberOfFilesBeforeSnapshot, equalTo(numberOfFiles(repo) - 4));
        logger.info("--> done");
    }

}

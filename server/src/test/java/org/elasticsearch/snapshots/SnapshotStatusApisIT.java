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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SnapshotStatusApisIT extends AbstractSnapshotIntegTestCase {

    public void testStatusApiConsistency() {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(
            Settings.builder().put("location", randomRepoPath()).build()));

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "_doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true).get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        List<SnapshotInfo> snapshotInfos =
            client.admin().cluster().prepareGetSnapshots("test-repo").get().getSnapshots("test-repo");
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        final List<SnapshotStatus> snapshotStatus = client.admin().cluster().snapshotsStatus(
            new SnapshotsStatusRequest("test-repo", new String[]{"test-snap"})).actionGet().getSnapshots();
        assertThat(snapshotStatus.size(), equalTo(1));
        final SnapshotStatus snStatus = snapshotStatus.get(0);
        assertEquals(snStatus.getStats().getStartTime(), snapshotInfo.startTime());
        assertEquals(snStatus.getStats().getTime(), snapshotInfo.endTime() - snapshotInfo.startTime());
    }

    public void testStatusAPICallInProgressSnapshot() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo").setType("mock").setSettings(
            Settings.builder().put("location", randomRepoPath()).put("block_on_data", true)));

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture =
            client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute();

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));

        final List<SnapshotStatus> snapshotStatus = client.admin().cluster().snapshotsStatus(
            new SnapshotsStatusRequest("test-repo", new String[]{"test-snap"})).actionGet().getSnapshots();
        assertBusy(() -> assertEquals(SnapshotsInProgress.State.STARTED, snapshotStatus.get(0).getState()), 1L, TimeUnit.MINUTES);

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }
}

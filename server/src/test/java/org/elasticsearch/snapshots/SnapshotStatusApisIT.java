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
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class SnapshotStatusApisIT extends AbstractSnapshotIntegTestCase {

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

        assertBusy(() -> assertEquals(SnapshotsInProgress.State.STARTED, client.admin().cluster().snapshotsStatus(
            new SnapshotsStatusRequest("test-repo", new String[]{"test-snap"})).actionGet().getSnapshots().get(0).getState()), 1L,
            TimeUnit.MINUTES);

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }

    public void testExceptionOnMissingSnapBlob() throws IOException {

        logger.info("--> creating repository");
        final Path repoPath = randomRepoPath();
        assertAcked(client().admin().cluster().preparePutRepository("test-repo").setType("fs").setSettings(
            Settings.builder().put("location", repoPath).build()));

        logger.info("--> snapshot");
        final CreateSnapshotResponse response =
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).get();

        logger.info("--> delete snap-${uuid}.dat file for this snapshot to simulate concurrent delete");
        IOUtils.rm(repoPath.resolve(BlobStoreRepository.SNAPSHOT_PREFIX + response.getSnapshotInfo().snapshotId().getUUID() + ".dat"));

        expectThrows(SnapshotMissingException.class, () -> client().admin().cluster()
            .getSnapshots(new GetSnapshotsRequest("test-repo", new String[] {"test-snap"})).actionGet());
    }
}

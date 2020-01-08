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

package org.elasticsearch.upgrades;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests that verify that a snapshot repository is not getting corrupted and continues to function properly when accessed by multiple
 * clusters of different versions. Concretely this test suit is simulating the following scenario:
 * <ul>
 *     <li>Start a cluster in an old version: {@link TestStep#STEP1_OLD_CLUSTER}</li>
 *     <li>Start a cluster running the current version: {@link TestStep#STEP2_NEW_CLUSTER}</li>
 *     <li>Again start a cluster in an old version: {@link TestStep#STEP3_OLD_CLUSTER}</li>
 *     <li>Once again start a cluster running the current version: {@link TestStep#STEP4_NEW_CLUSTER}</li>
 * </ul>
 * TODO: Add two more steps: delete all old version snapshots from the repository, then downgrade again and verify that the repository
 *       is not being corrupted. This requires first merging the logic for reading the min_version field in RepositoryData back to 7.6.
 */
public class MultiVersionRepositoryAccessIT extends ESRestTestCase {

    private enum TestStep {
        STEP1_OLD_CLUSTER("step1"),
        STEP2_NEW_CLUSTER("step2"),
        STEP3_OLD_CLUSTER("step3"),
        STEP4_NEW_CLUSTER("step4");

        private final String name;

        TestStep(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        public static TestStep parse(String value) {
            switch (value) {
                case "step1":
                    return STEP1_OLD_CLUSTER;
                case "step2":
                    return STEP2_NEW_CLUSTER;
                case "step3":
                    return STEP3_OLD_CLUSTER;
                case "step4":
                    return STEP4_NEW_CLUSTER;
                default:
                    throw new AssertionError("unknown test step: " + value);
            }
        }
    }

    protected static final TestStep TEST_STEP = TestStep.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    public void testCreateAndRestoreSnapshot() throws IOException {
        final String repoName = "testCreateAndRestoreSnapshot";
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(adminClient().getNodes().toArray(new Node[0])))) {
            final int shards = 3;
            createIndex(client, "test-index", shards);
            createRepository(client, repoName, false);
            createSnapshot(client, repoName, "snapshot-" + TEST_STEP);
            final String snapshotToDeleteName = "snapshot-to-delete";
            // Create a snapshot and delete it right away again to test the impact of each version's cleanup functionality that is run
            // as part of the snapshot delete
            createSnapshot(client, repoName, snapshotToDeleteName);
            deleteSnapshot(client, repoName, snapshotToDeleteName);
            final List<Map<String, Object>> snapshots = readSnapshotsList(
                client.getLowLevelClient().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all")));
            // Every step creates one snapshot
            assertThat(snapshots, hasSize(TEST_STEP.ordinal() + 1));
            final SnapshotsStatusResponse statusResponse = client.snapshot().status(new SnapshotsStatusRequest(repoName,
                snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new)), RequestOptions.DEFAULT);
            for (SnapshotStatus status : statusResponse.getSnapshots()) {
                assertThat(status.getShardsStats().getFailedShards(), is(0));
            }
            if (TEST_STEP == TestStep.STEP3_OLD_CLUSTER) {
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards);
            } else if (TEST_STEP == TestStep.STEP4_NEW_CLUSTER) {
                for (TestStep value : TestStep.values()) {
                    ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + value, shards);
                }
            }
        } finally {
            deleteRepository(repoName);
        }
    }

    public void testReadOnlyRepo() throws IOException {
        final String repoName = "testReadOnlyRepo";
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(adminClient().getNodes().toArray(new Node[0])))) {
            final int shards = 3;
            final boolean readOnly = TEST_STEP.ordinal() > 1; // only restore from read-only repo in steps 3 and 4
            createRepository(client, repoName, readOnly);
            if (readOnly == false) {
                createIndex(client, "test-index", shards);
                createSnapshot(client, repoName, "snapshot-" + TEST_STEP);
            }
            final List<Map<String, Object>> snapshots = readSnapshotsList(
                client.getLowLevelClient().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all")));
            switch (TEST_STEP) {
                case STEP1_OLD_CLUSTER:
                    assertThat(snapshots, hasSize(1));
                    break;
                case STEP2_NEW_CLUSTER:
                case STEP4_NEW_CLUSTER:
                case STEP3_OLD_CLUSTER:
                    assertThat(snapshots, hasSize(2));
                    break;
            }
            final SnapshotsStatusResponse statusResponse = client.snapshot().status(new SnapshotsStatusRequest(repoName,
                snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new)), RequestOptions.DEFAULT);
            for (SnapshotStatus status : statusResponse.getSnapshots()) {
                assertThat(status.getShardsStats().getFailedShards(), is(0));
            }
            if (TEST_STEP == TestStep.STEP3_OLD_CLUSTER) {
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards);
            } else if (TEST_STEP == TestStep.STEP4_NEW_CLUSTER) {
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards);
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER, shards);
            }
        }
    }

    public void testUpgradeMovesRepoToNewMetaVersion() throws IOException {
        if (TEST_STEP.ordinal() > 1) {
            // Only testing the first 2 steps here
            return;
        }
        final String repoName = "testUpgradeMovesRepoToNewMetaVersion";
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(adminClient().getNodes().toArray(new Node[0])))) {
            final int shards = 3;
            createIndex(client, "test-index", shards);
            createRepository(client, repoName, false);
            createSnapshot(client, repoName, "snapshot-" + TEST_STEP);
            final List<Map<String, Object>> snapshots = readSnapshotsList(
                client.getLowLevelClient().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all")));
            // Every step creates one snapshot
            assertThat(snapshots, hasSize(TEST_STEP.ordinal() + 1));
            final SnapshotsStatusResponse statusResponse = client.snapshot().status(new SnapshotsStatusRequest(repoName,
                snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new)), RequestOptions.DEFAULT);
            for (SnapshotStatus status : statusResponse.getSnapshots()) {
                assertThat(status.getShardsStats().getFailedShards(), is(0));
            }
            if (TEST_STEP == TestStep.STEP1_OLD_CLUSTER) {
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER, shards);
            } else {
                deleteSnapshot(client, repoName, "snapshot-" + TestStep.STEP1_OLD_CLUSTER);
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER, shards);
                createSnapshot(client, repoName, "snapshot-1");
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-1", shards);
                deleteSnapshot(client, repoName, "snapshot-" + TestStep.STEP2_NEW_CLUSTER);
                createSnapshot(client, repoName, "snapshot-2");
                ensureSnapshotRestoreWorks(client, repoName, "snapshot-2", shards);
            }
        } finally {
            deleteRepository(repoName);
        }
    }

    private void deleteSnapshot(RestHighLevelClient client, String repoName, String name) throws IOException {
        client.snapshot().delete(new DeleteSnapshotRequest(repoName, name), RequestOptions.DEFAULT);
    }

    private static void ensureSnapshotRestoreWorks(RestHighLevelClient client, String repoName, String name,
                                                   int shards) throws IOException {
        wipeAllIndices();
        final RestoreInfo restoreInfo =
            client.snapshot().restore(new RestoreSnapshotRequest().repository(repoName).snapshot(name).waitForCompletion(true),
                RequestOptions.DEFAULT).getRestoreInfo();
        assertThat(restoreInfo.failedShards(), is(0));
        assertThat(restoreInfo.successfulShards(), equalTo(shards));
    }

    private static void createRepository(RestHighLevelClient client, String repoName, boolean readOnly) throws IOException {
        assertThat(client.snapshot().createRepository(new PutRepositoryRequest(repoName).type("fs").settings(
            Settings.builder().put("location", "./" + repoName).put("readonly", readOnly)), RequestOptions.DEFAULT).isAcknowledged(),
            is(true));
    }

    private static void createSnapshot(RestHighLevelClient client, String repoName, String name) throws IOException {
        client.snapshot().create(new CreateSnapshotRequest(repoName, name).waitForCompletion(true), RequestOptions.DEFAULT);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readSnapshotsList(Response response) throws IOException {
        final List<Map<String, Object>> snapshots;
        try (InputStream entity = response.getEntity().getContent();
             XContentParser parser = JsonXContent.jsonXContent.createParser(
                 xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, entity)) {
            final Map<String, Object> raw = parser.map();
            // Bwc lookup since the format of the snapshots response changed between versions
            if (raw.containsKey("snapshots")) {
                snapshots = (List<Map<String, Object>>) raw.get("snapshots");
            } else {
                snapshots = (List<Map<String, Object>>) ((List<Map<?, ?>>) raw.get("responses")).get(0).get("snapshots");
            }
        }
        return snapshots;
    }

    private void createIndex(RestHighLevelClient client, String name, int shards) throws IOException {
        final Request putIndexRequest = new Request("PUT", "/" + name);
        putIndexRequest.setJsonEntity("{\n" +
            "    \"settings\" : {\n" +
            "        \"index\" : {\n" +
            "            \"number_of_shards\" : " + shards + ", \n" +
            "            \"number_of_replicas\" : 0 \n" +
            "        }\n" +
            "    }\n" +
            "}");
        final Response response = client.getLowLevelClient().performRequest(putIndexRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(HttpURLConnection.HTTP_OK));
    }
}

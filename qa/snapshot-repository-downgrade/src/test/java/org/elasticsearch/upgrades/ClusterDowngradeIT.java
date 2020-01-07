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
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Downgrade tests that verify that a snapshot repository is not getting corrupted and continues to function properly during cluster
 * downgrades. Concretely this test suit is simulating the following scenario:
 * <ul>
 *     <li>Start from a cluster in an old version: {@link TestStep#OLD}</li>
 *     <li>Upgrade the cluster to the current version: {@link TestStep#UPGRADED}</li>
 *     <li>Downgrade the cluster back to the old version: {@link TestStep#DOWNGRADED}</li>
 *     <li>Once again upgrade the cluster to the current version: {@link TestStep#RE_UPGRADED}</li>
 * </ul>
 * TODO: Add two more steps: delete all old version snapshots from the repository, then downgrade again and verify that the repository
 *       is not being corrupted. This requires first merging the logic for reading the min_version field in RepositoryData back to 7.6.
 */
public class ClusterDowngradeIT extends ESRestTestCase {

    protected enum TestStep {
        OLD,
        UPGRADED,
        DOWNGRADED,
        RE_UPGRADED;

        public static TestStep parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "upgraded_cluster":
                    return UPGRADED;
                case "downgraded_cluster":
                    return DOWNGRADED;
                case "re_upgraded_cluster":
                    return RE_UPGRADED;
                default:
                    throw new AssertionError("unknown test step: " + value);
            }
        }
    }

    protected static final TestStep TEST_STEP = TestStep.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSLMPoliciesUponCompletion() {
        return true;
    }

    public void testCreateSnapshot() throws IOException {
        final String repoName = "repo";
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(adminClient().getNodes().toArray(new Node[0])))) {
            final int shards = 3;
            if (TEST_STEP == TestStep.OLD || TEST_STEP == TestStep.DOWNGRADED) {
                createIndex(client, "test-index", shards);
                assertThat(client.snapshot().createRepository(new PutRepositoryRequest(repoName).type("fs").settings(
                    Settings.builder().put("location", ".")), RequestOptions.DEFAULT).isAcknowledged(), is(true));
            }
            createSnapshot(client, repoName, "snapshot-" + TEST_STEP.toString().toLowerCase(Locale.ROOT));
            final String snapshotToDeleteName = "snapshot-to-delete";
            // Create a snapshot and delete it right away again to test the impact of each version's cleanup functionality that is run
            // as part of the snapshot delete
            createSnapshot(client, repoName, snapshotToDeleteName);
            client.snapshot().delete(new DeleteSnapshotRequest(repoName, snapshotToDeleteName), RequestOptions.DEFAULT);
            final List<Map<String, Object>> snapshots = readSnapshotsList(
                client.getLowLevelClient().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all")));
            switch (TEST_STEP) {
                case OLD:
                    assertThat(snapshots, hasSize(1));
                    break;
                case UPGRADED:
                    assertThat(snapshots, hasSize(2));
                    break;
                case DOWNGRADED:
                    assertThat(snapshots, hasSize(3));
                    break;
                case RE_UPGRADED:
                    assertThat(snapshots, hasSize(4));
            }
            final SnapshotsStatusResponse statusResponse = client.snapshot().status(new SnapshotsStatusRequest(repoName,
                snapshots.stream().map(sn -> (String) sn.get("snapshot")).toArray(String[]::new)), RequestOptions.DEFAULT);
            for (SnapshotStatus status : statusResponse.getSnapshots()) {
                assertThat(status.getShardsStats().getFailedShards(), is(0));
            }
            if (TEST_STEP == TestStep.DOWNGRADED) {
                wipeAllIndices();
                final RestoreInfo restoreInfo = restoreSnapshot(client, repoName, "snapshot-old");
                assertThat(restoreInfo.failedShards(), is(0));
                assertThat(restoreInfo.successfulShards(), equalTo(shards));
            } else if (TEST_STEP == TestStep.RE_UPGRADED) {
                for (TestStep value : TestStep.values()) {
                    wipeAllIndices();
                    final RestoreInfo restoreInfo =
                        restoreSnapshot(client, repoName, "snapshot-" + value.toString().toLowerCase(Locale.ROOT));
                    assertThat(restoreInfo.failedShards(), is(0));
                    assertThat(restoreInfo.successfulShards(), equalTo(shards));
                }
            }
        }
    }

    private static RestoreInfo restoreSnapshot(RestHighLevelClient client, String repoName, String name) throws IOException {
        return client.snapshot().restore(new RestoreSnapshotRequest().repository(repoName).snapshot(name).waitForCompletion(true),
            RequestOptions.DEFAULT).getRestoreInfo();
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

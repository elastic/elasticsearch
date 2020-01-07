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
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
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
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Downgrade tests that verify that a snapshot repository is not getting corrupted and continues to function properly for downgrades.
 */
public class ClusterDowngradeIT extends ESRestTestCase {

    protected enum ClusterType {
        OLD,
        UPGRADED,
        DOWNGRADED,
        RE_UPGRADED;

        public static ClusterType parse(String value) {
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
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    protected static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.suite"));

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

    @SuppressWarnings("unchecked")
    public void testCreateSnapshot() throws IOException {
        final String repoName = "repo";
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(adminClient().getNodes().toArray(new Node[0])))) {
            if (CLUSTER_TYPE == ClusterType.OLD || CLUSTER_TYPE == ClusterType.DOWNGRADED) {
                createIndex(client, "idx-1", 3);
            }
            if (CLUSTER_TYPE == ClusterType.OLD || CLUSTER_TYPE == ClusterType.DOWNGRADED) {
                assertThat(client.snapshot().createRepository(new PutRepositoryRequest(repoName).type("fs").settings(
                    Settings.builder().put("location", ".")), RequestOptions.DEFAULT).isAcknowledged(), is(true));
            }
            client.snapshot().create(
                new CreateSnapshotRequest(repoName, "snapshot-" + CLUSTER_TYPE.toString().toLowerCase(Locale.ROOT))
                    .waitForCompletion(true), RequestOptions.DEFAULT);
            client.snapshot().create(
                new CreateSnapshotRequest(repoName, "snapshot-to-delete").waitForCompletion(true), RequestOptions.DEFAULT);
            client.snapshot().delete(new DeleteSnapshotRequest(repoName, "snapshot-to-delete"), RequestOptions.DEFAULT);
            final List<Map<String, Object>> snapshots;
            Response response = client.getLowLevelClient().performRequest(new Request("GET", "/_snapshot/" + repoName + "/_all"));
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
            switch (CLUSTER_TYPE) {
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
            final SnapshotsStatusResponse statusResponse = client.snapshot().status(
                new SnapshotsStatusRequest(repoName, new String[]{"snapshot-old"}), RequestOptions.DEFAULT);
            for (SnapshotStatus status : statusResponse.getSnapshots()) {
                assertThat(status.getShardsStats().getFailedShards(), is(0));
            }
            if (CLUSTER_TYPE == ClusterType.DOWNGRADED) {
                wipeAllIndices();
                final RestoreSnapshotResponse restoreSnapshotResponse = client.snapshot().restore(
                    new RestoreSnapshotRequest().repository(repoName).snapshot("snapshot-old").waitForCompletion(true),
                    RequestOptions.DEFAULT);
                assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), is(0));
                assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), greaterThanOrEqualTo(3));
            } else if (CLUSTER_TYPE == ClusterType.RE_UPGRADED) {
                for (ClusterType value : ClusterType.values()) {
                    wipeAllIndices();
                    final RestoreSnapshotResponse restoreSnapshotResponse = client.snapshot().restore(
                        new RestoreSnapshotRequest().repository(repoName)
                            .snapshot("snapshot-" + value.toString().toLowerCase(Locale.ROOT)).waitForCompletion(true),
                        RequestOptions.DEFAULT);
                    assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), is(0));
                    assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), greaterThanOrEqualTo(3));
                }
            }
        }
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

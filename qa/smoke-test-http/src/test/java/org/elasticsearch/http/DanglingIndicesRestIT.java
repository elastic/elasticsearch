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

package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.indices.IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING;
import static org.elasticsearch.rest.RestStatus.ACCEPTED;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.test.XContentTestUtils.createJsonMapView;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * This class tests the dangling indices REST API.  These tests are here
 * today so they have access to a proper REST client. They cannot be in
 * :server:integTest since the REST client needs a proper transport
 * implementation, and they cannot be REST tests today since they need to
 * restart nodes. Really, though, this test should live elsewhere.
 *
 * @see org.elasticsearch.action.admin.indices.dangling
 */
@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST, autoManageMasterNodes = false)
public class DanglingIndicesRestIT extends HttpSmokeTestCase {
    private static final String INDEX_NAME = "test-idx-1";
    private static final String OTHER_INDEX_NAME = INDEX_NAME + "-other";

    private Settings buildSettings(int maxTombstones) {
        return Settings.builder()
            // Limit the indices kept in the graveyard. This can be set to zero, so that
            // when we delete an index, it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), maxTombstones)
            .put(WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), true)
            .build();
    }

    /**
     * Check that when dangling indices are discovered, then they can be listed via the REST API.
     */
    public void testDanglingIndicesCanBeListed() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(1);
        internalCluster().startNodes(3, buildSettings(0));

        final DanglingIndexDetails danglingIndexDetails = createDanglingIndices(INDEX_NAME);
        final String stoppedNodeId = mapNodeNameToId(danglingIndexDetails.stoppedNodeName);

        final RestClient restClient = getRestClient();

        final Response listResponse = restClient.performRequest(new Request("GET", "/_dangling"));
        assertOK(listResponse);

        final XContentTestUtils.JsonMapView mapView = createJsonMapView(listResponse.getEntity().getContent());

        assertThat(mapView.get("_nodes.total"), equalTo(3));
        assertThat(mapView.get("_nodes.successful"), equalTo(3));
        assertThat(mapView.get("_nodes.failed"), equalTo(0));

        List<Object> indices = mapView.get("dangling_indices");
        assertThat(indices, hasSize(1));

        assertThat(mapView.get("dangling_indices.0.index_name"), equalTo(INDEX_NAME));
        assertThat(mapView.get("dangling_indices.0.index_uuid"), equalTo(danglingIndexDetails.indexToUUID.get(INDEX_NAME)));
        assertThat(mapView.get("dangling_indices.0.creation_date_millis"), instanceOf(Long.class));
        assertThat(mapView.get("dangling_indices.0.node_ids.0"), equalTo(stoppedNodeId));
    }

    /**
     * Check that dangling indices can be imported.
     */
    public void testDanglingIndicesCanBeImported() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(1);
        internalCluster().startNodes(3, buildSettings(0));

        createDanglingIndices(INDEX_NAME);

        final RestClient restClient = getRestClient();

        final List<String> danglingIndexIds = listDanglingIndexIds();
        assertThat(danglingIndexIds, hasSize(1));

        final Request importRequest = new Request("POST", "/_dangling/" + danglingIndexIds.get(0));
        importRequest.addParameter("accept_data_loss", "true");
        // Ensure this parameter is accepted
        importRequest.addParameter("timeout", "20s");
        importRequest.addParameter("master_timeout", "20s");
        final Response importResponse = restClient.performRequest(importRequest);
        assertThat(importResponse.getStatusLine().getStatusCode(), equalTo(ACCEPTED.getStatus()));

        final XContentTestUtils.JsonMapView mapView = createJsonMapView(importResponse.getEntity().getContent());
        assertThat(mapView.get("acknowledged"), equalTo(true));

        assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME));
    }

    /**
     * Check that dangling indices can be deleted. Since this requires that
     * we add an entry to the index graveyard, the graveyard size must be
     * greater than 1. To test deletes, we set the index graveyard size to
     * 1, then create two indices and delete them both while one node in
     * the cluster is stopped. The deletion of the second pushes the deletion
     * of the first out of the graveyard. When the stopped node is resumed,
     * only the second index will be found into the graveyard and the the
     * other will be considered dangling, and can therefore be listed and
     * deleted through the API
     */
    public void testDanglingIndicesCanBeDeleted() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(1);
        internalCluster().startNodes(3, buildSettings(1));

        createDanglingIndices(INDEX_NAME, OTHER_INDEX_NAME);

        final RestClient restClient = getRestClient();

        final List<String> danglingIndexIds = listDanglingIndexIds();
        assertThat(danglingIndexIds, hasSize(1));

        final Request deleteRequest = new Request("DELETE", "/_dangling/" + danglingIndexIds.get(0));
        deleteRequest.addParameter("accept_data_loss", "true");
        // Ensure these parameters is accepted
        deleteRequest.addParameter("timeout", "20s");
        deleteRequest.addParameter("master_timeout", "20s");
        final Response deleteResponse = restClient.performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), equalTo(ACCEPTED.getStatus()));

        final XContentTestUtils.JsonMapView mapView = createJsonMapView(deleteResponse.getEntity().getContent());
        assertThat(mapView.get("acknowledged"), equalTo(true));

        assertBusy(() -> assertThat("Expected dangling index to be deleted", listDanglingIndexIds(), hasSize(0)));

        // The dangling index that we deleted ought to have been removed from disk. Check by
        // creating and deleting another index, which creates a new tombstone entry, which should
        // not cause the deleted dangling index to be considered "live" again, just because its
        // tombstone has been pushed out of the graveyard.
        createIndex("additional");
        deleteIndex("additional");
        assertThat(listDanglingIndexIds(), is(empty()));
    }

    private List<String> listDanglingIndexIds() throws IOException {
        final Response response = getRestClient().performRequest(new Request("GET", "/_dangling"));
        assertOK(response);

        final XContentTestUtils.JsonMapView mapView = createJsonMapView(response.getEntity().getContent());

        assertThat(mapView.get("_nodes.total"), equalTo(3));
        assertThat(mapView.get("_nodes.successful"), equalTo(3));
        assertThat(mapView.get("_nodes.failed"), equalTo(0));

        List<Object> indices = mapView.get("dangling_indices");

        List<String> danglingIndexIds = new ArrayList<>();

        for (int i = 0; i < indices.size(); i++) {
            danglingIndexIds.add(mapView.get("dangling_indices." + i + ".index_uuid"));
        }

        return danglingIndexIds;
    }

    private void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
    }

    /**
     * Given a node name, finds the corresponding node ID.
     */
    private String mapNodeNameToId(String nodeName) throws IOException {
        final Response catResponse = getRestClient().performRequest(new Request("GET", "/_cat/nodes?full_id&h=id,name"));
        assertOK(catResponse);

        for (String nodeLine : Streams.readAllLines(catResponse.getEntity().getContent())) {
            String[] elements = nodeLine.split(" ");
            if (elements[1].equals(nodeName)) {
                return elements[0];
            }
        }

        throw new AssertionError("Failed to map node name [" + nodeName + "] to node ID");
    }

    /**
     * Helper that creates one or more indices, and importantly,
     * checks that they are green before proceeding. This is important
     * because the tests in this class stop and restart nodes, assuming
     * that each index has a primary or replica shard on every node, and if
     * a node is stopped prematurely, this assumption is broken.
     *
     * @return a mapping from each created index name to its UUID
     */
    private Map<String, String> createIndices(String... indices) throws IOException {
        assert indices.length > 0;

        for (String index : indices) {
            String indexSettings = "{"
                + "  \"settings\": {"
                + "    \"index\": {"
                + "      \"number_of_shards\": 1,"
                + "      \"number_of_replicas\": 2,"
                + "      \"routing\": {"
                + "        \"allocation\": {"
                + "          \"total_shards_per_node\": 1"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";
            Request request = new Request("PUT", "/" + index);
            request.setJsonEntity(indexSettings);
            assertOK(getRestClient().performRequest(request));
        }
        ensureGreen(indices);

        final Response catResponse = getRestClient().performRequest(new Request("GET", "/_cat/indices?h=index,uuid"));
        assertOK(catResponse);

        final Map<String, String> createdIndexIDs = new HashMap<>();

        final List<String> indicesAsList = Arrays.asList(indices);

        for (String indexLine : Streams.readAllLines(catResponse.getEntity().getContent())) {
            String[] elements = indexLine.split(" +");
            if (indicesAsList.contains(elements[0])) {
                createdIndexIDs.put(elements[0], elements[1]);
            }
        }

        assertThat("Expected to find as many index UUIDs as created indices", createdIndexIDs.size(), equalTo(indices.length));

        return createdIndexIDs;
    }

    private void deleteIndex(String indexName) throws IOException {
        Response deleteResponse = getRestClient().performRequest(new Request("DELETE", "/" + indexName));
        assertOK(deleteResponse);
    }

    private DanglingIndexDetails createDanglingIndices(String... indices) throws Exception {
        ensureStableCluster(3);
        final Map<String, String> indexToUUID = createIndices(indices);

        final AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        assertBusy(
            () -> internalCluster().getInstances(IndicesService.class)
                .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
        );

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureClusterSizeConsistency();
                stoppedNodeName.set(nodeName);
                for (String index : indices) {
                    deleteIndex(index);
                }
                return super.onNodeStopped(nodeName);
            }
        });

        ensureStableCluster(3);

        return new DanglingIndexDetails(stoppedNodeName.get(), indexToUUID);
    }

    private static class DanglingIndexDetails {
        private final String stoppedNodeName;
        private final Map<String, String> indexToUUID;

        DanglingIndexDetails(String stoppedNodeName, Map<String, String> indexToUUID) {
            this.stoppedNodeName = stoppedNodeName;
            this.indexToUUID = indexToUUID;
        }
    }
}

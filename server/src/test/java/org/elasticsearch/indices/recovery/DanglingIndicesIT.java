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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.indices.dangling.DeleteDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.ListDanglingIndicesRequest;
import org.elasticsearch.action.admin.indices.dangling.ListDanglingIndicesResponse;
import org.elasticsearch.action.admin.indices.dangling.NodeDanglingIndicesResponse;
import org.elasticsearch.action.admin.indices.dangling.RestoreDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.RestoreDanglingIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.common.util.CollectionUtils.map;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * This class tests how dangling indices are handled, in terms of how they
 * are discovered, and how they can be accessed and manipulated through the
 * API.
 */
@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class DanglingIndicesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-idx-1";

    private Settings buildSettings(int maxTombstones, boolean importDanglingIndices) {
        return Settings.builder()
            // Limit the indices kept in the graveyard. This can be set to zero, so that
            // when we delete an index, it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), maxTombstones)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), importDanglingIndices)
            .build();
    }

    /**
     * Check that when the auto-recovery setting is enabled and a dangling index is
     * discovered, then that index is recovered into the cluster.
     */
    public void testDanglingIndicesAreRecoveredWhenSettingIsEnabled() throws Exception {
        final Settings settings = buildSettings(0, true);
        internalCluster().startNodes(3, settings);

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME)));
    }

    /**
     * Check that when dangling indices are discovered, then they are not recovered into
     * the cluster when the recovery setting is disabled.
     */
    public void testDanglingIndicesAreNotRecoveredWhenSettingIsDisabled() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, false));

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        // Since index recovery is async, we can't prove index recovery will never occur, just that it doesn't occur within some reasonable
        // amount of time
        assertFalse(
            "Did not expect dangling index " + INDEX_NAME + " to be recovered",
            waitUntil(() -> indexExists(INDEX_NAME), 1, TimeUnit.SECONDS)
        );
    }

    /**
     * Check that when dangling indices are discovered, then they can be listed.
     */
    public void testDanglingIndicesCanBeListed() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, false));

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());

        final AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                stoppedNodeName.set(nodeName);
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> {
            final ListDanglingIndicesResponse response = client().admin()
                .cluster()
                .listDanglingIndices(new ListDanglingIndicesRequest())
                .actionGet();
            assertThat(response.status(), equalTo(RestStatus.OK));

            final List<NodeDanglingIndicesResponse> nodeResponses = response.getNodes();
            assertThat("Didn't get responses from all nodes", nodeResponses, hasSize(3));

            for (NodeDanglingIndicesResponse nodeResponse : nodeResponses) {
                if (nodeResponse.getNode().getName().equals(stoppedNodeName.get())) {
                    assertThat("Expected node that was stopped to have one dangling index", nodeResponse.getDanglingIndices(), hasSize(1));

                    final IndexMetaData danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                    assertThat(danglingIndexInfo.getIndex().getName(), equalTo(INDEX_NAME));
                } else {
                    assertThat(
                        "Expected node that was never stopped to have no dangling indices",
                        nodeResponse.getDanglingIndices(),
                        empty()
                    );
                }
            }
        });
    }

    /**
     * Check that dangling indices can be restored.
     */
    public void testDanglingIndicesCanBeRestored() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, false));

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());

        final AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                stoppedNodeName.set(nodeName);
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        final AtomicReference<String> danglingIndexUUID = new AtomicReference<>();

        // Wait for the dangling index to be noticed
        assertBusy(() -> {
            final ListDanglingIndicesResponse response = client().admin()
                .cluster()
                .listDanglingIndices(new ListDanglingIndicesRequest())
                .actionGet();
            assertThat(response.status(), equalTo(RestStatus.OK));

            final List<NodeDanglingIndicesResponse> nodeResponses = response.getNodes();

            for (NodeDanglingIndicesResponse nodeResponse : nodeResponses) {
                if (nodeResponse.getNode().getName().equals(stoppedNodeName.get())) {
                    final IndexMetaData danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                    assertThat(danglingIndexInfo.getIndex().getName(), equalTo(INDEX_NAME));

                    danglingIndexUUID.set(danglingIndexInfo.getIndexUUID());
                    break;
                }
            }
        });

        final RestoreDanglingIndexRequest request = new RestoreDanglingIndexRequest();
        request.setIndexUuid(danglingIndexUUID.get());

        final RestoreDanglingIndexResponse restoreResponse = client().admin().cluster().restoreDanglingIndex(request).actionGet();

        assertThat(restoreResponse.status(), equalTo(RestStatus.ACCEPTED));

        assertBusy(() -> assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME)));
    }

    /**
     * Check that the when sending a restore-dangling-indices request, the specified UUIDs are validated as
     * being dangling.
     */
    public void testDanglingIndicesMustExistToBeRestored() {
        internalCluster().startNodes(1, buildSettings(0, false));

        final RestoreDanglingIndexRequest request = new RestoreDanglingIndexRequest();
        request.setIndexUuid("NonExistentUUID");

        boolean noExceptionThrown = false;
        try {
            client().admin().cluster().restoreDanglingIndex(request).actionGet();
            noExceptionThrown = true;
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("No dangling index found for UUID [NonExistentUUID]"));
        }

        assertFalse("No exception thrown", noExceptionThrown);
    }

    /**
     * Check that dangling indices can be deleted. To do this, we set the index graveyard size to 1, then create
     * two indices and delete them both while one node in the cluster is stopped. When the stopped node is resumed,
     * only one index will be found into the graveyard, while the the other will be considered dangling, and can
     * therefore be listed and deleted through the API
     */
    public void testDanglingIndexCanBeDeleted() throws Exception {
        final Settings settings = buildSettings(1, false);
        internalCluster().startNodes(3, settings);

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());
        createIndex(INDEX_NAME + "-other", Settings.builder().put("number_of_replicas", 2).build());

        final AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                stoppedNodeName.set(nodeName);
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME + "-other"));
                return super.onNodeStopped(nodeName);
            }
        });

        final AtomicReference<IndexMetaData> danglingIndex = new AtomicReference<>();

        // Wait for the dangling index to be noticed
        assertBusy(() -> {
            final ListDanglingIndicesResponse response = client().admin()
                .cluster()
                .listDanglingIndices(new ListDanglingIndicesRequest())
                .actionGet();
            assertThat(response.status(), equalTo(RestStatus.OK));

            final List<NodeDanglingIndicesResponse> nodeResponses = response.getNodes();

            for (NodeDanglingIndicesResponse nodeResponse : nodeResponses) {
                if (nodeResponse.getNode().getName().equals(stoppedNodeName.get())) {
                    final IndexMetaData danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                    assertThat(danglingIndexInfo.getIndex().getName(), equalTo(INDEX_NAME));

                    danglingIndex.set(danglingIndexInfo);
                    break;
                }
            }
        });

        final IndexMetaData indexMetaData = danglingIndex.get();

        client().admin().cluster().deleteDanglingIndex(new DeleteDanglingIndexRequest(indexMetaData.getIndexUUID(), true)).actionGet();

        // Check that the dangling index goes away
        assertBusy(() -> {
            final ListDanglingIndicesResponse response = client().admin()
                .cluster()
                .listDanglingIndices(new ListDanglingIndicesRequest())
                .actionGet();
            assertThat(response.status(), equalTo(RestStatus.OK));

            final List<NodeDanglingIndicesResponse> nodeResponses = response.getNodes();

            for (NodeDanglingIndicesResponse nodeResponse : nodeResponses) {
                assertThat(
                    map(nodeResponse.getDanglingIndices(), each -> each.getIndex().getName()).toString(),
                    nodeResponse.getDanglingIndices(),
                    empty()
                );
            }
        });
    }

    public void testDanglingIndexOverMultipleNodesCanBeDeleted() throws Exception {
        final Settings settings = buildSettings(1, false);
        internalCluster().startNodes(5, settings);

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 4).build());
        createIndex(INDEX_NAME + "-other", Settings.builder().put("number_of_replicas", 4).build());

        final AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNodes(2, new InternalTestCluster.RestartCallback() {

            @Override
            public void onAllNodesStopped() {
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME + "-other"));
            }

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // We don't care which node name we store
                stoppedNodeName.set(nodeName);
                return super.onNodeStopped(nodeName);
            }
        });

        final AtomicReference<List<IndexMetaData>> danglingIndices = new AtomicReference<>();

        // Wait for the dangling indices to be noticed
        assertBusy(() -> {
            final List<IndexMetaData> results = listDanglingIndices();

            assertThat(results, not(empty()));
            danglingIndices.set(results);
        });

        // Both the stopped nodes should have found a dangling index.
        assertThat(danglingIndices.get(), hasSize(2));

        // Try to delete the index - this request should succeed
        client().admin()
            .cluster()
            .deleteDanglingIndex(new DeleteDanglingIndexRequest(danglingIndices.get().get(0).getIndexUUID(), true))
            .actionGet();

        assertBusy(() -> assertThat(listDanglingIndices(), empty()));
    }

    /**
     * Check that when deleting a dangling index, it is required that the "accept_data_loss" flag is set.
     */
    public void testDeleteDanglingIndicesRequiresDataLossFlagToBeTrue() {
        final Settings settings = buildSettings(1, false);
        internalCluster().startNodes(1, settings);

        Exception caughtException = null;

        try {
            internalCluster().client().admin().cluster().deleteDanglingIndex(new DeleteDanglingIndexRequest("fred", false)).actionGet();
        } catch (Exception e) {
            caughtException = e;
        }

        assertNotNull("No exception thrown", caughtException);

        assertThat(caughtException, instanceOf(IllegalArgumentException.class));
        assertThat(caughtException.getMessage(), containsString("accept_data_loss must be true"));
    }

    private List<IndexMetaData> listDanglingIndices() {
        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeDanglingIndicesResponse> nodeResponses = response.getNodes();

        final List<IndexMetaData> results = new ArrayList<>();

        for (NodeDanglingIndicesResponse nodeResponse : nodeResponses) {
            results.addAll(nodeResponse.getDanglingIndices());
        }

        return results;
    }
}

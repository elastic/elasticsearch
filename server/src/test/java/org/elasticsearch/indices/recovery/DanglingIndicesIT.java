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

import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.admin.indices.dangling.DeleteDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.ImportDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.ListDanglingIndicesRequest;
import org.elasticsearch.action.admin.indices.dangling.ListDanglingIndicesResponse;
import org.elasticsearch.action.admin.indices.dangling.NodeListDanglingIndicesResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
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
import static org.hamcrest.Matchers.is;

/**
 * This class tests how dangling indices are handled, in terms of how they
 * are discovered, and how they can be accessed and manipulated through the
 * API.
 *
 * <p>See also <code>DanglingIndicesRestIT</code> in the <code>qa:smoke-test-http</code>
 * project.
 *
 * @see org.elasticsearch.action.admin.indices.dangling
 */
@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class DanglingIndicesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-idx-1";
    private static final String OTHER_INDEX_NAME = INDEX_NAME + "-other";

    private Settings buildSettings(int maxTombstones, boolean writeDanglingIndices, boolean importDanglingIndices) {
        return Settings.builder()
            // Limit the indices kept in the graveyard. This can be set to zero, so that
            // when we delete an index, it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), maxTombstones)
            .put(IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), writeDanglingIndices)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), importDanglingIndices)
            .build();
    }

    /**
     * Check that when the auto-recovery setting is enabled and a dangling index is
     * discovered, then that index is recovered into the cluster.
     */
    public void testDanglingIndicesAreRecoveredWhenSettingIsEnabled() throws Exception {
        final Settings settings = buildSettings(0, true, true);
        internalCluster().startNodes(3, settings);

        createIndices(INDEX_NAME);
        assertBusy(
            () -> internalCluster().getInstances(IndicesService.class)
                .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
        );

        boolean refreshIntervalChanged = randomBoolean();
        if (refreshIntervalChanged) {
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("index.refresh_interval", "42s").build())
                .get();
            assertBusy(
                () -> internalCluster().getInstances(IndicesService.class)
                    .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
            );
        }

        if (randomBoolean()) {
            client().admin().indices().prepareClose(INDEX_NAME).get();
        }
        ensureGreen(INDEX_NAME);

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureClusterSizeConsistency();
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME)));
        if (refreshIntervalChanged) {
            assertThat(
                client().admin().indices().prepareGetSettings(INDEX_NAME).get().getSetting(INDEX_NAME, "index.refresh_interval"),
                equalTo("42s")
            );
        }
        ensureGreen(INDEX_NAME);
    }

    /**
     * Check that when dangling indices are discovered, then they are not recovered into
     * the cluster when the recovery setting is disabled.
     */
    public void testDanglingIndicesAreNotRecoveredWhenSettingIsDisabled() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, true, false));

        createDanglingIndices(INDEX_NAME);

        // Since index recovery is async, we can't prove index recovery will never occur, just that it doesn't occur within some reasonable
        // amount of time
        assertFalse(
            "Did not expect dangling index " + INDEX_NAME + " to be recovered",
            waitUntil(() -> indexExists(INDEX_NAME), 1, TimeUnit.SECONDS)
        );
    }

    /**
     * Check that when dangling indices are not written, then they cannot be recovered into the cluster.
     */
    public void testDanglingIndicesAreNotRecoveredWhenNotWritten() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, false, true));

        createDanglingIndices(INDEX_NAME);

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
        internalCluster().startNodes(3, buildSettings(0, true, false));

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME);

        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeListDanglingIndicesResponse> nodeResponses = response.getNodes();
        assertThat("Didn't get responses from all nodes", nodeResponses, hasSize(3));

        for (NodeListDanglingIndicesResponse nodeResponse : nodeResponses) {
            if (nodeResponse.getNode().getName().equals(stoppedNodeName)) {
                assertThat("Expected node that was stopped to have one dangling index", nodeResponse.getDanglingIndices(), hasSize(1));

                final DanglingIndexInfo danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                assertThat(danglingIndexInfo.getIndexName(), equalTo(INDEX_NAME));
            } else {
                assertThat("Expected node that was never stopped to have no dangling indices", nodeResponse.getDanglingIndices(), empty());
            }
        }
    }

    /**
     * Check that dangling indices can be imported.
     */
    public void testDanglingIndicesCanBeImported() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, true, false));

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME);

        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        final ImportDanglingIndexRequest request = new ImportDanglingIndexRequest(danglingIndexUUID, true);

        final AcknowledgedResponse importResponse = client().admin().cluster().importDanglingIndex(request).actionGet();

        assertThat(importResponse.isAcknowledged(), equalTo(true));

        assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME));
    }

    /**
     * Check that the when sending an import-dangling-indices request, the specified UUIDs are validated as
     * being dangling.
     */
    public void testDanglingIndicesMustExistToBeImported() {
        internalCluster().startNodes(1, buildSettings(0, true, false));

        final ImportDanglingIndexRequest request = new ImportDanglingIndexRequest("NonExistentUUID", true);

        boolean noExceptionThrown = false;
        try {
            client().admin().cluster().importDanglingIndex(request).actionGet();
            noExceptionThrown = true;
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("No dangling index found for UUID [NonExistentUUID]"));
        }

        assertFalse("No exception thrown", noExceptionThrown);
    }

    /**
     * Check that a dangling index can only be imported if "accept_data_loss" is set to true.
     */
    public void testMustAcceptDataLossToImportDanglingIndex() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, true, false));

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        final ImportDanglingIndexRequest request = new ImportDanglingIndexRequest(danglingIndexUUID, false);

        Exception caughtException = null;

        try {
            client().admin().cluster().importDanglingIndex(request).actionGet();
        } catch (Exception e) {
            caughtException = e;
        }

        assertNotNull("No exception thrown", caughtException);
        assertThat(caughtException.getMessage(), containsString("accept_data_loss must be set to true"));
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
    public void testDanglingIndexCanBeDeleted() throws Exception {
        final Settings settings = buildSettings(1, true, false);
        internalCluster().startNodes(3, settings);

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME, OTHER_INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        client().admin().cluster().deleteDanglingIndex(new DeleteDanglingIndexRequest(danglingIndexUUID, true)).actionGet();

        final List<DanglingIndexInfo> danglingIndices = listDanglingIndices();
        assertThat(map(danglingIndices, DanglingIndexInfo::getIndexName).toString(), danglingIndices, is(empty()));
    }

    /**
     * Check that when a index is found to be dangling on more than one node, it can be deleted.
     */
    public void testDanglingIndexOverMultipleNodesCanBeDeleted() throws Exception {
        final Settings settings = buildSettings(1, true, false);
        internalCluster().startNodes(3, settings);

        createIndices(INDEX_NAME, OTHER_INDEX_NAME);

        // Restart 2 nodes, deleting the indices in their absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

                    @Override
                    public Settings onNodeStopped(String nodeName) throws Exception {
                        ensureClusterSizeConsistency();
                        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                        assertAcked(client().admin().indices().prepareDelete(OTHER_INDEX_NAME));
                        return super.onNodeStopped(nodeName);
                    }
                });

                return super.onNodeStopped(nodeName);
            }
        });

        final AtomicReference<List<DanglingIndexInfo>> danglingIndices = new AtomicReference<>();

        final List<DanglingIndexInfo> results = listDanglingIndices();

        // Both the stopped nodes should have found a dangling index.
        assertThat(results, hasSize(2));
        danglingIndices.set(results);

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
    public void testDeleteDanglingIndicesRequiresDataLossFlagToBeTrue() throws Exception {
        final Settings settings = buildSettings(1, true, false);
        internalCluster().startNodes(3, settings);

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME, OTHER_INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        Exception caughtException = null;

        try {
            client().admin().cluster().deleteDanglingIndex(new DeleteDanglingIndexRequest(danglingIndexUUID, false)).actionGet();
        } catch (Exception e) {
            caughtException = e;
        }

        assertNotNull("No exception thrown", caughtException);
        assertThat(caughtException.getMessage(), containsString("accept_data_loss must be set to true"));
    }

    /**
     * Helper that fetches the current list of dangling indices.
     */
    private List<DanglingIndexInfo> listDanglingIndices() {
        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeListDanglingIndicesResponse> nodeResponses = response.getNodes();

        final List<DanglingIndexInfo> results = new ArrayList<>();

        for (NodeListDanglingIndicesResponse nodeResponse : nodeResponses) {
            results.addAll(nodeResponse.getDanglingIndices());
        }

        return results;
    }

    /**
     * Simple helper that creates one or more indices, and importantly,
     * checks that they are green before proceeding. This is important
     * because the tests in this class stop and restart nodes, assuming
     * that each index has a primary or replica shard on every node, and if
     * a node is stopped prematurely, this assumption is broken.
     */
    private void createIndices(String... indices) {
        assert indices.length > 0;
        for (String index : indices) {
            createIndex(index, Settings.builder().put("number_of_replicas", 2).build());
        }
        ensureGreen(indices);
    }

    private String createDanglingIndices(String... indices) throws Exception {
        createIndices(indices);

        assertBusy(
            () -> internalCluster().getInstances(IndicesService.class)
                .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
        );

        AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        // Restart node, deleting the indices in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureClusterSizeConsistency();
                stoppedNodeName.set(nodeName);
                for (String index : indices) {
                    assertAcked(client().admin().indices().prepareDelete(index));
                }
                return super.onNodeStopped(nodeName);
            }
        });

        return stoppedNodeName.get();
    }

    private String findDanglingIndexForNode(String stoppedNodeName, String indexName) {
        String danglingIndexUUID = null;

        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeListDanglingIndicesResponse> nodeResponses = response.getNodes();

        for (NodeListDanglingIndicesResponse nodeResponse : nodeResponses) {
            if (nodeResponse.getNode().getName().equals(stoppedNodeName)) {
                final DanglingIndexInfo danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                assertThat(danglingIndexInfo.getIndexName(), equalTo(indexName));

                danglingIndexUUID = danglingIndexInfo.getIndexUUID();
                break;
            }
        }

        assertNotNull("Failed to find a dangling index UUID for node [" + stoppedNodeName + "]", danglingIndexUUID);

        return danglingIndexUUID;
    }
}

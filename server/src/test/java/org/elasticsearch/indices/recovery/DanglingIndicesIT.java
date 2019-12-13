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

import org.elasticsearch.action.admin.indices.dangling.ListDanglingIndicesRequest;
import org.elasticsearch.action.admin.indices.dangling.ListDanglingIndicesResponse;
import org.elasticsearch.action.admin.indices.dangling.NodeDanglingIndicesResponse;
import org.elasticsearch.action.admin.indices.dangling.RestoreDanglingIndicesRequest;
import org.elasticsearch.action.admin.indices.dangling.RestoreDanglingIndicesResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class DanglingIndicesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-idx-1";

    private Settings buildSettings(boolean importDanglingIndices) {
        return Settings.builder()
            // Don't keep any indices in the graveyard, so that when we delete an index,
            // it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), 0)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), importDanglingIndices)
            .build();
    }

    /**
     * Check that when dangling indices are discovered, then they are recovered into
     * the cluster, so long as the recovery setting is enabled.
     */
    public void testDanglingIndicesAreRecoveredWhenSettingIsEnabled() throws Exception {
        final Settings settings = buildSettings(true);
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
        internalCluster().startNodes(3, buildSettings(false));

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
        internalCluster().startNodes(3, buildSettings(false));

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
        internalCluster().startNodes(3, buildSettings(false));

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

        final RestoreDanglingIndicesRequest request = new RestoreDanglingIndicesRequest();
        request.setIndexIds(new String[] { danglingIndexUUID.get() });

        final RestoreDanglingIndicesResponse restoreResponse = client().admin().cluster().restoreDanglingIndices(request).actionGet();

        assertThat(restoreResponse.status(), equalTo(RestStatus.ACCEPTED));

        assertBusy(() -> assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME)));
    }

    /**
     * Check that the when sending a restore-dangling-indices request, the specified UUIDs are validated as
     * being dangling.
     */
    public void testDanglingIndicesMustExistToBeRestored() {
        internalCluster().startNodes(1, buildSettings(false));

        final RestoreDanglingIndicesRequest request = new RestoreDanglingIndicesRequest();
        request.setIndexIds(new String[] { "NonExistentUUID", "AnotherNonExistentUUID" });

        boolean noExceptionThrown = false;
        try {
            client().admin().cluster().restoreDanglingIndices(request).actionGet();
            noExceptionThrown = true;
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(
                e.getMessage(),
                containsString("Dangling index list missing some specified UUIDs: [NonExistentUUID, AnotherNonExistentUUID]")
            );
        }

        assertFalse("No exception thrown", noExceptionThrown);
    }
}

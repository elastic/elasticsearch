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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class DanglingIndicesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-idx-1";

    private Settings buildSettings(boolean writeDanglingIndices, boolean importDanglingIndices) {
        return Settings.builder()
            // Don't keep any indices in the graveyard, so that when we delete an index,
            // it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), 0)
            .put(IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), writeDanglingIndices)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), importDanglingIndices)
            .build();
    }

    /**
     * Check that when dangling indices are discovered, then they are recovered into
     * the cluster, so long as the recovery setting is enabled.
     */
    public void testDanglingIndicesAreRecoveredWhenSettingIsEnabled() throws Exception {
        final Settings settings = buildSettings(true, true);
        internalCluster().startNodes(3, settings);

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());
        ensureGreen(INDEX_NAME);
        assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));

        boolean refreshIntervalChanged = randomBoolean();
        if (refreshIntervalChanged) {
            client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", "42s").build()).get();
            assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
                indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));
        }

        if (randomBoolean()) {
            client().admin().indices().prepareClose(INDEX_NAME).get();
        }
        ensureGreen(INDEX_NAME);

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().validateClusterFormed();
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        assertBusy(() -> assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME)));
        if (refreshIntervalChanged) {
            assertThat(client().admin().indices().prepareGetSettings(INDEX_NAME).get().getSetting(INDEX_NAME, "index.refresh_interval"),
                equalTo("42s"));
        }
        ensureGreen(INDEX_NAME);
    }

    /**
     * Check that when dangling indices are discovered, then they are not recovered into
     * the cluster when the recovery setting is disabled.
     */
    public void testDanglingIndicesAreNotRecoveredWhenSettingIsDisabled() throws Exception {
        internalCluster().startNodes(3, buildSettings(true, false));

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());
        ensureGreen(INDEX_NAME);
        assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().validateClusterFormed();
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
     * Check that when dangling indices are not written, then they cannot be recovered into the cluster.
     */
    public void testDanglingIndicesAreNotRecoveredWhenNotWritten() throws Exception {
        internalCluster().startNodes(3, buildSettings(false, true));

        createIndex(INDEX_NAME, Settings.builder().put("number_of_replicas", 2).build());
        ensureGreen(INDEX_NAME);
        internalCluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()));

        // Restart node, deleting the index in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().validateClusterFormed();
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
}

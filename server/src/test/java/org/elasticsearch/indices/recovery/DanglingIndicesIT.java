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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

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
        logger.info("--> starting cluster");
        final Settings settings = buildSettings(true);
        internalCluster().startNodes(3, settings);

        // Create an index and distribute it across the 3 nodes
        createAndPopulateIndex(INDEX_NAME, 1, 2);
        ensureGreen();

        // This is so that when then node comes back up, we have a dangling index that can be recovered.
        logger.info("--> restarted a random node and deleting the index while it's down");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                logger.info("--> deleting test index: {}", INDEX_NAME);
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();

        assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME));
    }

    /**
     * Check that when dangling indices are discovered, then they are not recovered into
     * the cluster when the recovery setting is disabled.
     */
    public void testDanglingIndicesAreNotRecoveredWhenSettingIsDisabled() throws Exception {
        logger.info("--> starting cluster");
        internalCluster().startNodes(3, buildSettings(false));

        // Create an index and distribute it across the 3 nodes
        createAndPopulateIndex(INDEX_NAME, 1, 2);

        // Create another index so that once we drop the first index, we
        // can still assert that the cluster is green.
        createAndPopulateIndex(INDEX_NAME + "-other", 1, 2);

        ensureGreen();

        // This is so that when then node comes back up, we have a dangling index that could
        // be recovered, but shouldn't be.
        logger.info("--> restarted a random node and deleting the index while it's down");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                logger.info("--> deleting test index: {}", INDEX_NAME);
                assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();

        assertFalse("Did not expect dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME));
    }

    private void createAndPopulateIndex(String name, int shardCount, int replicaCount) {
        logger.info("--> creating test index: {}", name);
        assertAcked(prepareCreate(name, Settings.builder().put("number_of_shards", shardCount).put("number_of_replicas", replicaCount)));
        ensureGreen();
    }
}

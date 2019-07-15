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
package org.elasticsearch.env;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.contains;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeRepurposeCommandIT extends ESIntegTestCase {

    public void testRepurpose() throws Exception {
        final String indexName = "test-repurpose";

        logger.info("--> starting two nodes");
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        logger.info("--> creating index");
        prepareCreate(indexName, Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
        ).get();
        final String indexUUID = resolveIndex(indexName).getUUID();

        logger.info("--> indexing a simple document");
        client().prepareIndex(indexName, "type1", "1").setSource("field1", "value1").get();

        ensureGreen();

        assertTrue(client().prepareGet(indexName, "type1", "1").get().isExists());

        final Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        final Settings dataNodeDataPathSettings = internalCluster().dataPathSettings(dataNode);

        final Settings noMasterNoDataSettings = Settings.builder()
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();

        final Settings noMasterNoDataSettingsForMasterNode = Settings.builder()
            .put(noMasterNoDataSettings)
            .put(masterNodeDataPathSettings)
            .build();

        final Settings noMasterNoDataSettingsForDataNode = Settings.builder()
            .put(noMasterNoDataSettings)
            .put(dataNodeDataPathSettings)
            .build();

        internalCluster().stopRandomDataNode();

        // verify test setup
        logger.info("--> restarting node with node.data=false and node.master=false");
        IllegalStateException ex = expectThrows(IllegalStateException.class,
            "Node started with node.data=false and node.master=false while having existing index metadata must fail",
            () -> internalCluster().startCoordinatingOnlyNode(dataNodeDataPathSettings)
        );

        logger.info("--> Repurposing node 1");
        executeRepurposeCommand(noMasterNoDataSettingsForDataNode, indexUUID, 1);

        ElasticsearchException lockedException = expectThrows(ElasticsearchException.class,
            () -> executeRepurposeCommand(noMasterNoDataSettingsForMasterNode, indexUUID, 1)
        );

        assertThat(lockedException.getMessage(), containsString(NodeRepurposeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG));

        logger.info("--> Starting node after repurpose");
        internalCluster().startCoordinatingOnlyNode(dataNodeDataPathSettings);

        assertTrue(indexExists(indexName));
        expectThrows(NoShardAvailableActionException.class, () -> client().prepareGet(indexName, "type1", "1").get());

        logger.info("--> Restarting and repurposing other node");

        internalCluster().stopRandomNode(s -> true);
        internalCluster().stopRandomNode(s -> true);

        executeRepurposeCommand(noMasterNoDataSettingsForMasterNode, indexUUID, 0);

        // by restarting as master and data node, we can check that the index definition was really deleted and also that the tool
        // does not mess things up so much that the nodes cannot boot as master or data node any longer.
        internalCluster().startMasterOnlyNode(masterNodeDataPathSettings);
        internalCluster().startDataOnlyNode(dataNodeDataPathSettings);

        ensureGreen();

        // index is gone.
        assertFalse(indexExists(indexName));
    }

    private void executeRepurposeCommand(Settings settings, String indexUUID, int expectedShardCount) throws Exception {
        boolean verbose = randomBoolean();
        Settings settingsWithPath = Settings.builder().put(internalCluster().getDefaultSettings()).put(settings).build();
        int expectedIndexCount = TestEnvironment.newEnvironment(settingsWithPath).dataFiles().length;
        Matcher<String> matcher = allOf(
            containsString(NodeRepurposeCommand.noMasterMessage(1, expectedShardCount, expectedIndexCount)),
            not(contains(NodeRepurposeCommand.PRE_V7_MESSAGE)),
            NodeRepurposeCommandTests.conditionalNot(containsString(indexUUID), verbose == false));
        NodeRepurposeCommandTests.verifySuccess(settingsWithPath, matcher,
            verbose);
    }
}

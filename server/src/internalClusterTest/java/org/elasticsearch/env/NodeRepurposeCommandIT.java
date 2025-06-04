/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.env;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeRepurposeCommandIT extends ESIntegTestCase {

    public void testRepurpose() throws Exception {
        final String indexName = "test-repurpose";

        logger.info("--> starting two nodes");
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), false).build()
        );

        logger.info("--> creating index");
        prepareCreate(indexName, indexSettings(1, 0)).get();

        logger.info("--> indexing a simple document");
        prepareIndex(indexName).setId("1").setSource("field1", "value1").get();

        ensureGreen();

        assertTrue(client().prepareGet(indexName, "1").get().isExists());

        final Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        final Settings dataNodeDataPathSettings = internalCluster().dataPathSettings(dataNode);

        // put some unknown role here to make sure the tool does not bark when encountering an unknown role
        final Settings noMasterNoDataSettings = nonMasterNode(nonDataNode());

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
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            "Node started with node.data=false and node.master=false while having existing index metadata must fail",
            () -> internalCluster().startCoordinatingOnlyNode(dataNodeDataPathSettings)
        );

        logger.info("--> Repurposing node 1");
        executeRepurposeCommand(noMasterNoDataSettingsForDataNode, 1, 1);

        ElasticsearchException lockedException = expectThrows(
            ElasticsearchException.class,
            () -> executeRepurposeCommand(noMasterNoDataSettingsForMasterNode, 1, 1)
        );

        assertThat(lockedException.getMessage(), containsString(NodeRepurposeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG));

        logger.info("--> Starting node after repurpose");
        internalCluster().startCoordinatingOnlyNode(dataNodeDataPathSettings);

        assertTrue(indexExists(indexName));
        expectThrows(NoShardAvailableActionException.class, client().prepareGet(indexName, "1"));

        logger.info("--> Restarting and repurposing other node");

        internalCluster().stopNode(internalCluster().getRandomNodeName());
        internalCluster().stopNode(internalCluster().getRandomNodeName());

        executeRepurposeCommand(noMasterNoDataSettingsForMasterNode, 1, 0);

        // by restarting as master and data node, we can check that the index definition was really deleted and also that the tool
        // does not mess things up so much that the nodes cannot boot as master or data node any longer.
        internalCluster().startMasterOnlyNode(masterNodeDataPathSettings);
        internalCluster().startDataOnlyNode(dataNodeDataPathSettings);

        ensureGreen();

        // index is gone.
        assertFalse(indexExists(indexName));
    }

    private void executeRepurposeCommand(Settings settings, int expectedIndexCount, int expectedShardCount) throws Exception {
        boolean verbose = randomBoolean();
        Settings settingsWithPath = Settings.builder().put(internalCluster().getDefaultSettings()).put(settings).build();
        Matcher<String> matcher = allOf(
            containsString(NodeRepurposeCommand.noMasterMessage(expectedIndexCount, expectedShardCount, 0)),
            NodeRepurposeCommandTests.conditionalNot(containsString("test-repurpose"), verbose == false)
        );
        NodeRepurposeCommandTests.verifySuccess(settingsWithPath, matcher, verbose);
    }
}

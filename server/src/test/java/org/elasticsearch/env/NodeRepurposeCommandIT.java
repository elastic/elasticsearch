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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.contains;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeRepurposeCommandIT extends ESIntegTestCase {

    public void testRepurpose() throws Exception {
        final String indexName = "test-repurpose";

        logger.info("--> starting two nodes");
        final List<String> nodes = internalCluster().startNodes(2);

        logger.info("--> creating index");
        prepareCreate(indexName, Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
        ).get();
        final String indexUUID = resolveIndex(indexName).getUUID();

        logger.info("--> indexing a simple document");
        client().prepareIndex(indexName, "type1", "1").setSource("field1", "value1").get();

        ensureGreen();

        assertTrue(client().prepareGet(indexName, "type1", "1").get().isExists());

        final Settings noMasterNoDataSettings = Settings.builder()
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();

        internalCluster().stopRandomDataNode();

        // verify test setup
        logger.info("--> restarting node with node.data=false and node.master=false");
        IllegalStateException ex = expectThrows(IllegalStateException.class,
            "Node started with node.data=false and node.master=false while having existing index metadata must fail",
            () -> internalCluster().startNode(noMasterNoDataSettings)
        );

        int runningOrdinal = nodes.indexOf(internalCluster().getNodeNames()[0]);
        int stoppedOrdinal = 1-runningOrdinal;

        logger.info("--> Repurposing node");
        executeRepurposeCommandForOrdinal(noMasterNoDataSettings, indexUUID, stoppedOrdinal);

        ElasticsearchException lockedException = expectThrows(ElasticsearchException.class,
            () -> executeRepurposeCommandForOrdinal(noMasterNoDataSettings, indexUUID, runningOrdinal)
        );

        assertThat(lockedException.getMessage(), containsString(NodeRepurposeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG));

        logger.info("--> Starting node after repurpose");
        nodes.set(stoppedOrdinal, internalCluster().startNode(noMasterNoDataSettings));

        ensureYellow();

        assertTrue(client().prepareGet(indexName, "type1", "1").get().isExists());
        assertTrue(indexExists(indexName));

        logger.info("--> Restarting and repurposing other node");

        internalCluster().stopRandomNode(s -> true);
        internalCluster().stopRandomNode(s -> true);

        executeRepurposeCommandForOrdinal(noMasterNoDataSettings, indexUUID, runningOrdinal);

        internalCluster().startNodes(2);
        ensureGreen();

        // indexes and docs gone.
        assertFalse(indexExists(indexName));
    }

    private void executeRepurposeCommandForOrdinal(Settings settings, String indexUUID, int ordinal) throws Exception {
        boolean verbose = randomBoolean();
        Settings settingsWithPath = Settings.builder().put(internalCluster().getDefaultSettings()).put(settings).build();
        int expectedIndexCount = TestEnvironment.newEnvironment(settingsWithPath).dataFiles().length;
        Matcher<String> matcher = allOf(
            containsString(NodeRepurposeCommand.noMasterMessage(1, 1, expectedIndexCount)),
            not(contains(NodeRepurposeCommand.PRE_V7_MESSAGE)),
            NodeRepurposeCommandTests.conditionalNot(containsString(indexUUID), verbose == false));
        NodeRepurposeCommandTests.verifySuccess(settingsWithPath, matcher,
            verbose, ordinal);
    }
}

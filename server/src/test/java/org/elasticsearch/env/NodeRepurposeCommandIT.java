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

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.contains;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeRepurposeCommandIT extends ESIntegTestCase {

    public void testRepurpose() throws Exception {
        final String indexName = "test-repurpose";

        logger.info("--> starting two nodes");
        internalCluster().startNodes(2);

        logger.info("--> creating index");
        prepareCreate(indexName, Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
        ).get();
        final String indexUUID = resolveIndex(indexName).getUUID();

        logger.info("--> indexing a simple document");
        client().prepareIndex(indexName, "type1", "1").setSource("field1", "value1").get();

        ensureGreen();

        final Settings noMasterNoDataSettings = Settings.builder()
            .put(internalCluster().getDefaultSettings())
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

        logger.info("--> Repurposing node");
        executeRepurposeCommand(noMasterNoDataSettings, indexUUID);

        logger.info("--> Starting node after repurpose");
        internalCluster().startNode(noMasterNoDataSettings);
    }

    private void executeRepurposeCommand(Settings settings, String indexUUID) throws Exception {
        assertEquals("Exactly one node folder/ordinal must succeed", 1,
            IntStream.range(0, 2).filter(i -> executeRepurposeCommandForOrdinal(settings, indexUUID, i)).count());
    }

    private boolean executeRepurposeCommandForOrdinal(Settings settings, String indexUUID, int ordinal) {
        try {
            boolean verbose = randomBoolean();
            Matcher<String> matcher = allOf(
                containsString(NodeRepurposeCommand.noMasterMessage(1, 1, TestEnvironment.newEnvironment(settings).dataFiles().length)),
                not(contains(NodeRepurposeCommand.PRE_V7_MESSAGE)),
                NodeRepurposeCommandTests.conditionalNot(containsString(indexUUID), verbose == false));
            NodeRepurposeCommandTests.verifySuccess(settings, matcher, verbose, ordinal);
            return true;
        } catch (ElasticsearchException e) {
            if (e.getMessage().contains(NodeRepurposeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG))
                return false;
            else
                throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

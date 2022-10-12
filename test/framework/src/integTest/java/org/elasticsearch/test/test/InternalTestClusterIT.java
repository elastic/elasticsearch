/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.test;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class InternalTestClusterIT extends ESIntegTestCase {

    public void testStartingAndStoppingNodes() throws IOException {
        logger.info("--> cluster has [{}] nodes", internalCluster().size());
        if (internalCluster().size() < 5) {
            final int nodesToStart = randomIntBetween(Math.max(2, internalCluster().size() + 1), 5);
            logger.info("--> growing to [{}] nodes", nodesToStart);
            internalCluster().startNodes(nodesToStart);
        }
        ensureGreen();

        while (internalCluster().size() > 1) {
            final int nodesToRemain = randomIntBetween(1, internalCluster().size() - 1);
            logger.info("--> reducing to [{}] nodes", nodesToRemain);
            internalCluster().ensureAtMostNumDataNodes(nodesToRemain);
            assertThat(internalCluster().size(), lessThanOrEqualTo(nodesToRemain));
        }

        ensureGreen();
    }

    public void testStoppingNodesOneByOne() throws IOException {
        // In a 5+ node cluster there must be at least one reconfiguration as the nodes are shut down one-by-one before we drop to 2 nodes.
        // If the nodes shut down too quickly then this reconfiguration does not have time to occur and the quorum is lost in the 3->2
        // transition, even though in a stable cluster the 3->2 transition requires no special treatment.

        internalCluster().startNodes(5);
        ensureGreen();

        while (internalCluster().size() > 1) {
            internalCluster().stopNode(internalCluster().getRandomNodeName());

        }

        ensureGreen();
    }

    public void testOperationsDuringRestart() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureGreen();
                internalCluster().validateClusterFormed();
                assertNotNull(internalCluster().getInstance(NodeClient.class));
                internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) throws Exception {
                        ensureGreen();
                        internalCluster().validateClusterFormed();
                        return super.onNodeStopped(nodeName);
                    }
                });
                return super.onNodeStopped(nodeName);
            }
        });
    }
}

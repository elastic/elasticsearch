/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.readiness;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.readiness.ReadinessClientProbe;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class ReadinessClusterIT extends ESIntegTestCase implements ReadinessClientProbe {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());
        return settings.build();
    }

    private void assertMasterNode(Client client, String node) {
        assertThat(
            client.admin().cluster().prepareState().execute().actionGet().getState().nodes().getMasterNode().getName(),
            equalTo(node)
        );
    }

    private void expectMasterNotFound() {
        expectThrows(
            MasterNotDiscoveredException.class,
            () -> client().admin()
                .cluster()
                .prepareState()
                .setMasterNodeTimeout("100ms")
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getMasterNodeId()
        );
    }

    public void testReadinessDuringRestarts() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));

        expectMasterNotFound();
        assertFalse(internalCluster().getInstance(ReadinessService.class, dataNode).ready());

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();

        assertMasterNode(internalCluster().nonMasterClient(), masterNode);
        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, dataNode));
        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, masterNode));

        Integer masterPort = internalCluster().getInstance(ReadinessService.class, internalCluster().getMasterName())
            .boundAddress()
            .publishAddress()
            .getPort();

        assertMasterNode(internalCluster().nonMasterClient(), masterNode);

        logger.info("--> stop master node");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
        internalCluster().stopCurrentMasterNode();
        expectMasterNotFound();

        tcpReadinessProbeFalse(masterPort);

        logger.info("--> start previous master node again");
        final String nextMasterEligibleNodeName = internalCluster().startNode(
            Settings.builder().put(nonDataNode(masterNode())).put(masterDataPathSettings)
        );

        assertMasterNode(internalCluster().nonMasterClient(), nextMasterEligibleNodeName);
        assertMasterNode(internalCluster().masterClient(), nextMasterEligibleNodeName);
        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, nextMasterEligibleNodeName));
    }

    public void testReadinessDuringRestartsNormalOrder() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node");
        String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().validateClusterFormed();

        assertMasterNode(internalCluster().masterClient(), masterNode);

        logger.info("--> start 2 data nodes");
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, masterNode));

        for (String dataNode : dataNodes) {
            ReadinessService s = internalCluster().getInstance(ReadinessService.class, dataNode);
            tcpReadinessProbeTrue(s);
        }

        logger.info("--> restart data node 1");
        internalCluster().restartNode(dataNodes.get(0), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, masterNode));
                tcpReadinessProbeTrue(internalCluster().getInstance(ReadinessService.class, dataNodes.get(1)));

                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> restart master");

        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                expectMasterNotFound();

                for (String dataNode : dataNodes) {
                    ReadinessService s = internalCluster().getInstance(ReadinessService.class, dataNode);
                    s.listenerThreadLatch.await(10, TimeUnit.SECONDS);
                    tcpReadinessProbeFalse(s);
                }

                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();
        for (String dataNode : dataNodes) {
            ReadinessService s = internalCluster().getInstance(ReadinessService.class, dataNode);
            tcpReadinessProbeTrue(s);
        }
    }
}

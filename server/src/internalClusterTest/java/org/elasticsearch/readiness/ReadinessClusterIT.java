/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.readiness;

import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class ReadinessClusterIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));

        settings.put(ReadinessService.PORT.getKey(), Integer.toString(9400 + nodeOrdinal));
        return settings.build();
    }

    public void testReadinessDuringRestarts() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        String dataNode = internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(
                client().admin()
                    .cluster()
                    .prepareState()
                    .setMasterNodeTimeout("100ms")
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getMasterNodeId(),
                nullValue()
            );
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        assertFalse(getStatus(internalCluster().getInstance(Environment.class, dataNode)));

        logger.info("--> start master node");
        final String masterNode = internalCluster().startMasterOnlyNode();
        assertThat(
            internalCluster().nonMasterClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getMasterNode()
                .getName(),
            equalTo(masterNode)
        );
        assertTrue(getStatus(internalCluster().getInstance(Environment.class, dataNode)));
        assertTrue(getStatus(internalCluster().getInstance(Environment.class, masterNode)));

        assertThat(
            internalCluster().masterClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getMasterNode()
                .getName(),
            equalTo(masterNode)
        );

        logger.info("--> stop master node");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
        internalCluster().stopCurrentMasterNode();

        assertFalse(getStatus(internalCluster().getInstance(Environment.class, dataNode)));

        try {
            assertThat(
                client().admin()
                    .cluster()
                    .prepareState()
                    .setMasterNodeTimeout("100ms")
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getMasterNodeId(),
                nullValue()
            );
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start previous master node again");
        final String nextMasterEligibleNodeName = internalCluster().startNode(
            Settings.builder().put(nonDataNode(masterNode())).put(masterDataPathSettings)
        );
        assertThat(
            internalCluster().nonMasterClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getMasterNode()
                .getName(),
            equalTo(nextMasterEligibleNodeName)
        );
        assertThat(
            internalCluster().masterClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getMasterNode()
                .getName(),
            equalTo(nextMasterEligibleNodeName)
        );
    }

    public void testReadinessDuringRestartsNormalOrder() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node");
        String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().validateClusterFormed();

        assertThat(
            internalCluster().masterClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getMasterNode()
                .getName(),
            equalTo(masterNode)
        );

        logger.info("--> start 2 data nodes");
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        internalCluster().validateClusterFormed();

        assertTrue(getStatus(internalCluster().getInstance(Environment.class, masterNode)));

        for (String dataNode : dataNodes) {
            Environment env = internalCluster().getInstance(Environment.class, dataNode);
            assertTrue(getStatus(env));
        }

        logger.info("--> restart data node 1");
        internalCluster().restartNode(dataNodes.get(0), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertTrue(getStatus(internalCluster().getInstance(Environment.class, masterNode)));
                assertTrue(getStatus(internalCluster().getInstance(Environment.class, dataNodes.get(1))));

                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> restart master");

        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {

                try {
                    assertThat(
                        client().admin()
                            .cluster()
                            .prepareState()
                            .setMasterNodeTimeout("100ms")
                            .execute()
                            .actionGet()
                            .getState()
                            .nodes()
                            .getMasterNodeId(),
                        nullValue()
                    );
                    fail("should not be able to find master");
                } catch (MasterNotDiscoveredException e) {
                    // all is well, no master elected
                }

                for (String dataNode : dataNodes) {
                    Environment env = internalCluster().getInstance(Environment.class, dataNode);
                    assertFalse(getStatus(env));
                }

                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();
        for (String dataNode : dataNodes) {
            Environment env = internalCluster().getInstance(Environment.class, dataNode);
            assertTrue(getStatus(env));
        }
    }

    private boolean getStatus(Environment env) throws Exception {
        return getStatus(ReadinessService.PORT.get(env.settings()));
    }

    @SuppressForbidden(reason = "Intentional socket open")
    private boolean getStatus(String port) throws Exception {
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), Integer.parseInt(port));

        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.INET)) {
            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                try {
                    channel.connect(socketAddress);
                    BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
                    String message = reader.readLine();
                    assertNotNull(message);
                    return message.startsWith("true,");
                } catch (IOException ignored) {}

                return false;
            });
        }
    }
}

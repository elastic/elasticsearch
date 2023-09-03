/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.NetworkInterface;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cloud.gce.GceInstancesService;
import org.elasticsearch.cloud.gce.util.Access;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.discovery.gce.GceDiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.singletonList;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 0, numClientNodes = 0)
public class GceDiscoverTests extends ESIntegTestCase {

    /** Holds a list of the current discovery nodes started in tests **/
    private static final Map<String, DiscoveryNode> nodes = new ConcurrentHashMap<>();

    @After
    public void clearGceNodes() {
        nodes.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "gce")
            .put("cloud.gce.project_id", "test")
            .put("cloud.gce.zone", "test")
            .build();
    }

    public void testJoin() {
        // start master node
        final String masterNode = internalCluster().startMasterOnlyNode();
        registerGceNode(masterNode);

        ClusterStateResponse clusterStateResponse = client(masterNode).admin()
            .cluster()
            .prepareState()
            .setMasterNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getMasterNodeId());

        // start another node
        final String secondNode = internalCluster().startNode();
        registerGceNode(secondNode);
        clusterStateResponse = client(secondNode).admin()
            .cluster()
            .prepareState()
            .setMasterNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .setLocal(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getMasterNodeId());

        // wait for the cluster to form
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(2)).get());
        assertNumberOfNodes(2);

        // add one more node and wait for it to join
        final String thirdNode = internalCluster().startDataOnlyNode();
        registerGceNode(thirdNode);
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(3)).get());
        assertNumberOfNodes(3);
    }

    /**
     * Register an existing node as a GCE node
     *
     * @param nodeName the name of the node
     */
    private static void registerGceNode(final String nodeName) {
        final TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        assertNotNull(transportService);
        final DiscoveryNode discoveryNode = transportService.getLocalNode();
        assertNotNull(discoveryNode);
        if (nodes.put(discoveryNode.getName(), discoveryNode) != null) {
            throw new IllegalArgumentException("Node [" + discoveryNode.getName() + "] cannot be registered twice");
        }
    }

    /**
     * Asserts that the cluster nodes info contains an expected number of node
     *
     * @param expected the expected number of nodes
     */
    private static void assertNumberOfNodes(final int expected) {
        assertEquals(expected, clusterAdmin().prepareNodesInfo().clear().get().getNodes().size());
    }

    /**
     * Test plugin that exposes internal test cluster nodes as if they were real GCE nodes.
     * Use {@link #registerGceNode(String)} method to expose nodes in the tests.
     */
    public static class TestPlugin extends GceDiscoveryPlugin {

        public TestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GceInstancesService createGceInstancesService() {
            return new GceInstancesService() {
                @Override
                public Collection<Instance> instances() {
                    return Access.doPrivileged(() -> {
                        final List<Instance> instances = new ArrayList<>();

                        for (DiscoveryNode discoveryNode : nodes.values()) {
                            Instance instance = new Instance();
                            instance.setName(discoveryNode.getName());
                            instance.setStatus("STARTED");

                            NetworkInterface networkInterface = new NetworkInterface();
                            networkInterface.setNetworkIP(discoveryNode.getAddress().toString());
                            instance.setNetworkInterfaces(singletonList(networkInterface));

                            instances.add(instance);
                        }

                        return instances;
                    });
                }

                @Override
                public String projectId() {
                    return PROJECT_SETTING.get(settings);
                }

                @Override
                public List<String> zones() {
                    return ZONE_SETTING.get(settings);
                }

                @Override
                public void close() throws IOException {}
            };
        }
    }
}

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

@Ignore
public abstract class AbstractNodesTests extends ElasticsearchTestCase {
    private static Map<String, Node> nodes = newHashMap();

    private static Map<String, Client> clients = newHashMap();
    
    private static final Settings defaultSettings = ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress().getHostName() + "CHILD_VM=[" + CHILD_VM_ID +"]")
            .build();


    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node startNode(String id, Settings.Builder settings) {
        return startNode(id, settings.build());
    }

    public Node startNode(String id, Settings settings) {
        return buildNode(id, settings).start();
    }

    public Node buildNode(String id) {
        return buildNode(id, EMPTY_SETTINGS);
    }

    public Node buildNode(String id, Settings.Builder settings) {
        return buildNode(id, settings.build());
    }

    public Node buildNode(String id, Settings settings) {
        synchronized (AbstractNodesTests.class) {
            if (nodes.containsKey(id)) {
                throw new IllegalArgumentException("Node with id ["+ id + "] already exists");
            }
            assert !nodes.containsKey(id);
            assert !clients.containsKey(id);
                
            String settingsSource = getClass().getName().replace('.', '/') + ".yml";
            Settings finalSettings = settingsBuilder()
                    .loadFromClasspath(settingsSource)
                    .put(defaultSettings)
                    .put(getClassDefaultSettings())
                    .put(settings)
                    .put("name", id)
                    .build();
    
            if (finalSettings.get("gateway.type") == null) {
                // default to non gateway
                finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
            }
            if (finalSettings.get("cluster.routing.schedule") != null) {
                // decrease the routing schedule so new nodes will be added quickly
                finalSettings = settingsBuilder().put(finalSettings).put("cluster.routing.schedule", "50ms").build();
            }
            Node node = nodeBuilder()
                    .settings(finalSettings)
                    .build();
            logger.info("Build Node [{}] with settings [{}]", id, finalSettings.toDelimitedString(','));
            nodes.put(id, node);
            clients.put(id, node.client());
            return node;
        }
    }

    public void closeNode(String id) {
        Client client;
        Node node;
        synchronized (AbstractNodesTests.class) {
            client = clients.remove(id);
            node = nodes.remove(id);
        }
        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }

    }

    public Node node(String id) {
        synchronized (AbstractNodesTests.class) {
            return nodes.get(id);
        }
    }

    public Client client(String id) {
        synchronized (AbstractNodesTests.class) {
            return clients.get(id);
        }
    }
    public void closeAllNodes() {
        closeAllNodes(false);
    }
    public void closeAllNodes(boolean preventRelocation) {
        synchronized (AbstractNodesTests.class) {
            if (preventRelocation) {
                Settings build = ImmutableSettings.builder().put("cluster.routing.allocation.disable_allocation", true).build();
                Client aClient = client();
                if (aClient != null) {
                        aClient.admin().cluster().prepareUpdateSettings().setTransientSettings(build).execute().actionGet();
                }
            }
            for (Client client : clients.values()) {
                client.close();
            }
            clients.clear();
            for (Node node : nodes.values()) {
                node.close();
            }
            nodes.clear();
        }
    }

    public ImmutableSet<ClusterBlock> waitForNoBlocks(TimeValue timeout, String node) throws InterruptedException {
        long start = System.currentTimeMillis();
        ImmutableSet<ClusterBlock> blocks;
        do {
            blocks = client(node).admin().cluster().prepareState().setLocal(true).execute().actionGet()
                    .getState().blocks().global(ClusterBlockLevel.METADATA);
        }
        while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
        return blocks;
    }

    public void createIndices(Client client, String... indices) {
        for (String index : indices) {
            client.admin().indices().prepareCreate(index).execute().actionGet();
        }
    }

    public void wipeIndices(Client client, String... names) {
        try {
            client.admin().indices().prepareDelete(names).execute().actionGet();
        } catch (IndexMissingException e) {
            // ignore
        }
    }
    
    private static volatile AbstractNodesTests testInstance; // this test class only works once per JVM
    
    @BeforeClass
    public static void tearDownOnce() throws Exception {
        synchronized (AbstractNodesTests.class) {
            if (testInstance != null) {
                testInstance.afterClass();
                testInstance.closeAllNodes();
                testInstance = null;
            }
        }
    }
    
    @Before
    public final void setUp() throws Exception {
        synchronized (AbstractNodesTests.class) {
            if (testInstance == null) {
                testInstance = this;
                testInstance.beforeClass();
                
            } else {
                assert testInstance.getClass() == this.getClass();
            }
        }
    }
    
    public Client client() {
        synchronized (AbstractNodesTests.class) {
            if (clients.isEmpty()) {
                return null;
            }
            return clients.values().iterator().next();
        }
    }
    
    protected void afterClass() throws Exception {
    }
    
    protected Settings getClassDefaultSettings() {
        return ImmutableSettings.EMPTY;
    }
    
    protected void beforeClass() throws Exception {
    }
}
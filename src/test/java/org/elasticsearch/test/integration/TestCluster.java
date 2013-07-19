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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class TestCluster {
    
    /* some random options to consider
     *  "action.auto_create_index"
     *  "node.local"
     */
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Map<String, NodeAndClient> nodes = newHashMap();

    private final String clusterName;
    
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    

    private final Settings defaultSettings;

    
    private NodeAndClient clientNode;

    private Random random;
    private ClientFactory clientFactory;


    public TestCluster(Random random) {
        
        this(random, "shared-test-cluster-" + NetworkUtils.getLocalAddress().getHostName() + "CHILD_VM=[" + ElasticsearchTestCase.CHILD_VM_ID + "]"+ "_" + System.currentTimeMillis(), ImmutableSettings.settingsBuilder().build());
    }

    private TestCluster(Random random, String clusterName, Settings defaultSettings) {
        this.random = new Random(random.nextLong());
        clientFactory = new RandomClientFactory(random);
        this.clusterName = clusterName;
        if (defaultSettings.get("gateway.type") == null) {
            // default to non gateway
            defaultSettings = settingsBuilder().put(defaultSettings).put("gateway.type", "none").build();
        }
        if (defaultSettings.get("cluster.routing.schedule") != null) {
            // decrease the routing schedule so new nodes will be added quickly
            defaultSettings = settingsBuilder().put(defaultSettings).put("cluster.routing.schedule", "50ms").build();
        }
        this.defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put("cluster.name", clusterName).build();
    }


    private void ensureOpen() {
        if (!open.get()) {
            throw new RuntimeException("Cluster is already closed");
        }
    }

    public Node getOneNode() {
        ensureOpen();
        Collection<NodeAndClient> values = nodes.values();
        for (NodeAndClient nodeAndClient : values) {
            return nodeAndClient.node();
        }
        return buildNode().start();
    }

    public void ensureAtLeastNumNodes(int num) {
        int size = nodes.size();
        for (int i = size; i < num; i++) {
            buildNode().start();
        }
    }

    public void ensureAtMostNumNodes(int num) {
        if (nodes.size() <= num) {
            return;
        }
        Collection<NodeAndClient> values = nodes.values();
        Iterator<NodeAndClient> limit = Iterators.limit(values.iterator(),  nodes.size() - num);
        while(limit.hasNext()) {
            NodeAndClient next = limit.next();
            limit.remove();
            next.close();
        }
    }

    public Node startNode(Settings.Builder settings) {
        ensureOpen();
        return startNode(settings.build());
    }

    public Node startNode(Settings settings) {
        ensureOpen();
        return buildNode(settings).start();
    }

    public Node buildNode() {
        ensureOpen();
        return buildNode(EMPTY_SETTINGS);
    }

    public Node buildNode(Settings.Builder settings) {
        ensureOpen();
        return buildNode(settings.build());
    }

    public Node buildNode(Settings settings) {
        ensureOpen();
        String name = UUID.randomUUID().toString();
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder().loadFromClasspath(settingsSource).put(defaultSettings).put(settings).put("name", name)
                .build();
        Node node = nodeBuilder().settings(finalSettings).build();
        nodes.put(name, new NodeAndClient(name, node, clientFactory));
        return node;
    }

    public void setClientFactory(ClientFactory factory) {
        this.clientFactory = factory;
    }

    public void closeNode(Node node) {
        ensureOpen();
        NodeAndClient remove = nodes.remove(node.settings().get("name"));
        IOUtils.closeWhileHandlingException(remove); // quiet
    }

    public Client client() {
        ensureOpen();
        return getOneNode().client();
    }

    public void close() {
        ensureOpen();
        if (this.open.compareAndSet(true, false)) {
            IOUtils.closeWhileHandlingException(nodes.values());
            nodes.clear();
            if (clientNode != null) {
                IOUtils.closeWhileHandlingException(clientNode);
            }
        }
    }

    public ImmutableSet<ClusterBlock> waitForNoBlocks(TimeValue timeout, Node node) throws InterruptedException {
        ensureOpen();
        long start = System.currentTimeMillis();
        ImmutableSet<ClusterBlock> blocks;
        do {
            blocks = node.client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks()
                    .global(ClusterBlockLevel.METADATA);
        } while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
        return blocks;
    }

    public class NodeAndClient implements Closeable {
        final Node node;
        Client client;
        final AtomicBoolean closed = new AtomicBoolean(false);
        final ClientFactory clientFactory;
        final String name;

        public NodeAndClient(String name, Node node, ClientFactory factory) {
            this.node = node;
            this.name = name;
            this.clientFactory = factory;
        }

        public Node node() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return node;
        }

        public Client client() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (client != null) {
                return client;
            }
            return client = clientFactory.client(node, clusterName);
        }

        @Override
        public void close() {
            closed.set(true);
            if (client != null) {
                client.close();
            }
            node.close();

        }
    }

    public static class ClientFactory {

        public Client client(Node node, String clusterName) {
            return node.client();
        }
    }

    public static class TransportClientFactory extends ClientFactory {

        private boolean sniff;
        public static TransportClientFactory NO_SNIFF_CLIENT_FACTORY = new TransportClientFactory(false);
        public static TransportClientFactory SNIFF_CLIENT_FACTORY = new TransportClientFactory(true);

        public TransportClientFactory(boolean sniff) {
            this.sniff = sniff;
        }

        @Override
        public Client client(Node node, String clusterName) {
            TransportAddress addr = ((InternalNode) node).injector().getInstance(TransportService.class).boundAddress().publishAddress();
            TransportClient client = new TransportClient(settingsBuilder().put("client.transport.nodes_sampler_interval", "30s")
                    .put("cluster.name", clusterName).put("client.transport.sniff", sniff).build());
            client.addTransportAddress(addr);
            return client;
        }
    }

    public static class RandomClientFactory extends ClientFactory {
        private final Random random;
        
        public RandomClientFactory(Random random) {
            this.random = random;
        }

        @Override
        public Client client(Node node, String clusterName) {
            switch (random.nextInt(10)) {
            case 5:
                return TransportClientFactory.NO_SNIFF_CLIENT_FACTORY.client(node, clusterName);
            case 3:
                return TransportClientFactory.SNIFF_CLIENT_FACTORY.client(node, clusterName);
            default:
                return node.client();
            }
        }
    }

    void reset(Random random) {
        this.random = new Random(random.nextLong());
        this.clientFactory = new RandomClientFactory(this.random);
    }

    public ClusterService clusterService() {
        return ((InternalNode) getOneNode()).injector().getInstance(ClusterService.class);
    }

    public int numNodes() {
        return this.nodes.size();
    }

    public void stopRandomNode() {
        ensureOpen();

        // TODO randomize
        Set<Entry<String, NodeAndClient>> entrySet = nodes.entrySet();
        if (entrySet.isEmpty()) {
            return;
        }
        Entry<String, NodeAndClient> next = entrySet.iterator().next();
        nodes.remove(next.getKey());
        next.getValue().close();
    }

    public Iterable<Client> clients() {
        final Map<String, NodeAndClient> nodes = this.nodes;
        return new Iterable<Client>() {

            @Override
            public Iterator<Client> iterator() {
                final Iterator<NodeAndClient> iterator = nodes.values().iterator();
                return new Iterator<Client>() {

                    @Override
                    public boolean hasNext() {

                        return iterator.hasNext();
                    }

                    @Override
                    public Client next() {
                        return iterator.next().client();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("");
                    }

                };
            }
        };

    }
    
    public Set<String> allButN(int numNodes) {
        return nRandomNodes(numNodes() - numNodes);
    }

    public Set<String> nRandomNodes(int numNodes) {
        assert numNodes() >= numNodes;
        return Sets.newHashSet(Iterators.limit(this.nodes.keySet().iterator(), numNodes));
    }

    public Client nodeClient() {
        ensureOpen();
        if (clientNode == null) {
            String name = UUID.randomUUID().toString();
            String settingsSource = getClass().getName().replace('.', '/') + ".yml";
            Settings finalSettings = settingsBuilder().loadFromClasspath(settingsSource).put(defaultSettings).put("node.client", true).put("name", name)
                    .build();
            Node node = nodeBuilder().settings(finalSettings).build();
            node.start();
            this.clientNode = new NodeAndClient(name, node, clientFactory);
            
        }
        return clientNode.client();
    }

    public Set<String> nodesInclude(String index) {
        if (clusterService().state().routingTable().hasIndex(index)) {
        List<ShardRouting> allShards = clusterService().state().routingTable().allShards(index);
        DiscoveryNodes discoveryNodes = clusterService().state().getNodes();
        Set<String> nodes = new HashSet<String>();
        for (ShardRouting shardRouting : allShards) {
            if (shardRouting.assignedToNode()) {
                DiscoveryNode discoveryNode = discoveryNodes.get(shardRouting.currentNodeId());
                nodes.add(discoveryNode.getName());
            }
        }
        return nodes;
        } 
        return Collections.emptySet();
    }
    
    
    public Set<String> nodeExclude(String index) {
        final Set<String> nodesInclude = nodesInclude(index);
        return Sets.newHashSet(Iterators.transform(Iterators.filter(nodes.values().iterator(), new Predicate<NodeAndClient>() {
            @Override
            public boolean apply(NodeAndClient nodeAndClient) {
                return !nodesInclude.contains(nodeAndClient.name);
            }
            
        }), new Function<NodeAndClient, String>() {
            @Override
            public String apply(NodeAndClient nodeAndClient) {
                return nodeAndClient.name;
            }
        }));
    }
}

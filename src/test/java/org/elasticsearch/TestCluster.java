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
package org.elasticsearch;

import com.carrotsearch.randomizedtesting.SeedUtils;
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
import org.elasticsearch.index.store.mock.MockFSIndexStoreModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.newTreeMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class TestCluster {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    /* sorted map to make traverse order reproducible */
    private final TreeMap<String, NodeAndClient> nodes = newTreeMap(); 

    private final String clusterName;

    private final AtomicBoolean open = new AtomicBoolean(true);

    private final Settings defaultSettings;

    private NodeAndClient clientNode; // currently unused
    
    private Random random;
    
    private AtomicInteger nextNodeId = new AtomicInteger(0);
    
    /* We have a fixed number of shared nodes that we keep around across tests */
    private final int numSharedNodes;
    
    /* Each shared node has a node seed that is used to start up the node and get default settings
     * this is important if a node is randomly shut down in a test since the next test relies on a
     * fully shared cluster to be more reproducible */
    private final long[] sharedNodesSeeds;

    public TestCluster(long clusterSeed, String clusterName) {
        this(clusterSeed, clusterName, ImmutableSettings.EMPTY);
    }

    private TestCluster(long clusterSeed, String clusterName, Settings defaultSettings) {
        this.clusterName = clusterName;
        Random random = new Random(clusterSeed);
        numSharedNodes = 2 + random.nextInt(4); // at least 2 nodes
        /*
         *  TODO 
         *  - we might want start some master only nodes?
         *  - we could add a flag that returns a client to the master all the time?
         *  - we could add a flag that never returns a client to the master 
         *  - along those lines use a dedicated node that is master eligible and let all other nodes be only data nodes
         */
        sharedNodesSeeds = new long[numSharedNodes];
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            sharedNodesSeeds[i] = random.nextLong();
        }
        logger.info("Started TestCluster with seed [{}] using [{}] nodes" , SeedUtils.formatSeed(clusterSeed), numSharedNodes);

        if (defaultSettings.get("gateway.type") == null) {
            // default to non gateway
            defaultSettings = settingsBuilder().put(defaultSettings).put("gateway.type", "none").build();
        }
        if (defaultSettings.get("cluster.routing.schedule") != null) {
            // decrease the routing schedule so new nodes will be added quickly - some random value between 30 and 80 ms
            defaultSettings = settingsBuilder().put(defaultSettings).put("cluster.routing.schedule", (30 + random.nextInt(50)) + "ms").build();
        }
        this.defaultSettings = ImmutableSettings.settingsBuilder()
                /* use RAM directories in 10% of the runs */
//                .put("index.store.type", random.nextInt(10) == 0 ? MockRamIndexStoreModule.class.getName() : MockFSIndexStoreModule.class.getName())
                .put("index.store.type", MockFSIndexStoreModule.class.getName()) // no RAM dir for now!
                .put(defaultSettings)
                .put("cluster.name", clusterName).build();
    }
    
    public static String clusterName(String prefix, String childVMId, long clusterSeed) {
        StringBuilder builder = new StringBuilder(prefix);
        builder.append('-').append(NetworkUtils.getLocalAddress().getHostName());
        builder.append("-CHILD_VM=[").append(childVMId).append(']');
        builder.append("-CLUSTER_SEED=[").append(clusterSeed).append(']');
        // if multiple maven task run on a single host we better have an identifier that doesn't rely on input params
        builder.append("-HASH=[").append(SeedUtils.formatSeed(System.nanoTime())).append(']');
        return builder.toString();
    }

    private void ensureOpen() {
        if (!open.get()) {
            throw new RuntimeException("Cluster is already closed");
        }
    }

    private synchronized Node getOrBuildRandomNode() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.node();
        }
        NodeAndClient buildNode = buildNode();
        nodes.put(buildNode.name, buildNode);
        return buildNode.node().start();
    }
    
    private synchronized NodeAndClient getRandomNodeAndClient() {
        ensureOpen();
        Collection<NodeAndClient> values = nodes.values();
        int whichOne = random.nextInt(values.size());
        for (NodeAndClient nodeAndClient : values) {
            if (whichOne-- == 0) {
                return nodeAndClient;
            }
        }
        return null;
    }

    public synchronized void ensureAtLeastNumNodes(int num) {
        int size = nodes.size();
        for (int i = size; i < num; i++) {
            logger.info("increasing cluster size from {} to {}", size, num);
            NodeAndClient buildNode = buildNode();
            buildNode.node().start(); 
            nodes.put(buildNode.name, buildNode);
        }
    }

    public synchronized void ensureAtMostNumNodes(int num) {
        if (nodes.size() <= num) {
            return;
        }
        Collection<NodeAndClient> values = nodes.values();
        Iterator<NodeAndClient> limit = Iterators.limit(values.iterator(), nodes.size() - num);
        logger.info("reducing cluster size from {} to {}", nodes.size() - num, num);
        while (limit.hasNext()) {
            NodeAndClient next = limit.next();
            limit.remove();
            next.close();
        }
    }
    
    private NodeAndClient buildNode() {
        return buildNode(nextNodeId.getAndIncrement(), random.nextLong());
    }

    private NodeAndClient buildNode(int nodeId, long seed) {
        ensureOpen();
        String name = buildNodeName(nodeId);
        assert !nodes.containsKey(name);
        Settings finalSettings = settingsBuilder()
                .put(defaultSettings)
                .put("name", name)
                .put("discovery.id.seed", seed)
                .build();
        Node node = nodeBuilder().settings(finalSettings).build();
        return new NodeAndClient(name, node, new RandomClientFactory());
    }

    private String buildNodeName(int id) {
        return "node_" + id;
    }

    public synchronized Client client() {
        ensureOpen();
        /* Randomly return a client to one of the nodes in the cluster */
        return getOrBuildRandomNode().client();
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

    public synchronized ImmutableSet<ClusterBlock> waitForNoBlocks(TimeValue timeout, Node node) throws InterruptedException {
        ensureOpen();
        long start = System.currentTimeMillis();
        ImmutableSet<ClusterBlock> blocks;
        do {
            blocks = node.client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks()
                    .global(ClusterBlockLevel.METADATA);
        } while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
        return blocks;
    }

    private final class NodeAndClient implements Closeable {
        private final Node node;
        private Client client;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ClientFactory clientFactory;
        private final String name;

        NodeAndClient(String name, Node node, ClientFactory factory) {
            this.node = node;
            this.name = name;
            this.clientFactory = factory;
        }

        Node node() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return node;
        }

        Client client(Random random) {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (client != null) {
                return client;
            }
            return client = clientFactory.client(node, clusterName, random);
        }
        
        void resetClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (client != null) {
                client.close();
                client = null;
            }
        }

        @Override
        public void close() {
            closed.set(true);
            if (client != null) {
                client.close();
                client = null;
            }
            node.close();

        }
    }

    public static class ClientFactory {

        public Client client(Node node, String clusterName, Random random) {
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
        public Client client(Node node, String clusterName, Random random) {
            TransportAddress addr = ((InternalNode) node).injector().getInstance(TransportService.class).boundAddress().publishAddress();
            TransportClient client = new TransportClient(settingsBuilder().put("client.transport.nodes_sampler_interval", "30s")
                    .put("cluster.name", clusterName).put("client.transport.sniff", sniff).build());
            client.addTransportAddress(addr);
            return client;
        }
    }

    public static class RandomClientFactory extends ClientFactory {

        @Override
        public Client client(Node node, String clusterName,  Random random) {
            switch (random.nextInt(10)) {
                case 5:
                    return TransportClientFactory.NO_SNIFF_CLIENT_FACTORY.client(node, clusterName, random);
                case 3:
                    return TransportClientFactory.SNIFF_CLIENT_FACTORY.client(node, clusterName, random);
                default:
                    return node.client();
            }
        }
    }

    public synchronized void beforeTest(Random random) {
        this.random = new Random(random.nextLong());
        resetClients(); /* reset all clients - each test gets it's own client based on the Random instance created above. */
        if (nextNodeId.get() == sharedNodesSeeds.length && nodes.size() == sharedNodesSeeds.length) {
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);

        if (nodes.size() > 0) {
            client().admin().cluster().prepareHealth().setWaitForNodes(""+nodes.size()).get();
        }
        Set<NodeAndClient> sharedNodes = new HashSet<NodeAndClient>();
        boolean changed = false;
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            String buildNodeName = buildNodeName(i);
            NodeAndClient nodeAndClient = nodes.get(buildNodeName);
            if (nodeAndClient == null) {
                changed = true;
                nodeAndClient = buildNode(i, sharedNodesSeeds[i]);
                nodeAndClient.node.start();
                logger.info("Start Shared Node [{}] not shared", nodeAndClient.name);
            }
            sharedNodes.add(nodeAndClient);
        }
        if (!changed && sharedNodes.size() == nodes.size()) {
            logger.debug("Cluster is consistent - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            return; // we are consistent - return
        }
        for (NodeAndClient nodeAndClient : sharedNodes) {
            nodes.remove(nodeAndClient.name);
        }
        
        // trash the remaining nodes
        final Collection<NodeAndClient> toShutDown = nodes.values();
        for (NodeAndClient nodeAndClient : toShutDown) {
            logger.debug("Close Node [{}] not shared", nodeAndClient.name);
            nodeAndClient.close();
        }
        nodes.clear();
        for (NodeAndClient nodeAndClient : sharedNodes) {
            nodes.put(nodeAndClient.name, nodeAndClient);
        }
        nextNodeId.set(sharedNodesSeeds.length);
        assert numNodes() == sharedNodesSeeds.length;
        client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(sharedNodesSeeds.length)).get();
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
    }
    
    private void resetClients() {
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            nodeAndClient.resetClient();
        }
    }

    public synchronized ClusterService clusterService() {
        return ((InternalNode) getOrBuildRandomNode()).injector().getInstance(ClusterService.class);
    }

    public synchronized int numNodes() {
        return this.nodes.size();
    }

    public synchronized void stopRandomNode() {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient();
        if (nodeAndClient != null) {
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    public synchronized Iterable<Client> clients() {
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
                        return iterator.next().client(random);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("");
                    }

                };
            }
        };

    }

    public synchronized Set<String> allButN(int numNodes) {
        return nRandomNodes(numNodes() - numNodes);
    }

    public synchronized Set<String> nRandomNodes(int numNodes) {
        assert numNodes() >= numNodes;
        return Sets.newHashSet(Iterators.limit(this.nodes.keySet().iterator(), numNodes));
    }

    private synchronized Client nodeClient() {
        ensureOpen(); // currently unused
        if (clientNode == null) {
            String name = "client_node";
            Settings finalSettings = settingsBuilder().put(defaultSettings).put("name", name)
                    .build();
            Node node = nodeBuilder().settings(finalSettings).client(true).build();
            node.start();
            this.clientNode = new NodeAndClient(name, node, new ClientFactory());

        }
        return clientNode.client(random);
        
    }

    public synchronized Set<String> nodesInclude(String index) {
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


    public synchronized Set<String> nodeExclude(String index) {
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

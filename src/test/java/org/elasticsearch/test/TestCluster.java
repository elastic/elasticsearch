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
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.SeedUtils;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.engine.MockEngineModule;
import org.elasticsearch.test.store.mock.MockFSIndexStoreModule;
import org.elasticsearch.test.transport.AssertingLocalTransportModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;

import java.io.Closeable;
import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.newTreeMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class TestCluster implements Closeable, Iterable<Client> {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    /* sorted map to make traverse order reproducible */
    private final TreeMap<String, NodeAndClient> nodes = newTreeMap();

    private final Set<File> dataDirToClean = new HashSet<File>();

    private final String clusterName;

    private final AtomicBoolean open = new AtomicBoolean(true);

    private final Settings defaultSettings;

    private Random random;

    private AtomicInteger nextNodeId = new AtomicInteger(0);

    /* We have a fixed number of shared nodes that we keep around across tests */
    private final int numSharedNodes;

    /* Each shared node has a node seed that is used to start up the node and get default settings
     * this is important if a node is randomly shut down in a test since the next test relies on a
     * fully shared cluster to be more reproducible */
    private final long[] sharedNodesSeeds;

    private double transportClientRatio = 0.0;

    private final NodeSettingsSource nodeSettingsSource;

    public TestCluster(long clusterSeed, String clusterName) {
        this(clusterSeed, -1, clusterName, NodeSettingsSource.EMPTY);
    }

    public TestCluster(long clusterSeed, int numNodes, String clusterName, NodeSettingsSource nodeSettingsSource) {
        this.clusterName = clusterName;
        Random random = new Random(clusterSeed);
        numSharedNodes = numNodes == -1 ? 2 + random.nextInt(4) : numNodes; // at least 2 nodes if randomized
        assert numSharedNodes >= 0;
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
        logger.info("Setup TestCluster [{}] with seed [{}] using [{}] nodes", clusterName, SeedUtils.formatSeed(clusterSeed), numSharedNodes);
        Builder builder = ImmutableSettings.settingsBuilder()
        /* use RAM directories in 10% of the runs */
//        .put("index.store.type", random.nextInt(10) == 0 ? MockRamIndexStoreModule.class.getName() : MockFSIndexStoreModule.class.getName())
        .put("index.store.type", MockFSIndexStoreModule.class.getName()) // no RAM dir for now!
        .put(IndexEngineModule.EngineSettings.ENGINE_TYPE, MockEngineModule.class.getName())
        .put("cluster.name", clusterName)
                // decrease the routing schedule so new nodes will be added quickly - some random value between 30 and 80 ms
        .put("cluster.routing.schedule", (30 + random.nextInt(50)) + "ms")
                // default to non gateway
        .put("gateway.type", "none");
        if (isLocalTransportConfigured()) {
            builder.put(TransportModule.TRANSPORT_TYPE_KEY, AssertingLocalTransportModule.class.getName());
        }
        this.defaultSettings = builder.build();
        this.nodeSettingsSource = nodeSettingsSource;
    }

    private static boolean isLocalTransportConfigured() {
        if ("local".equals(System.getProperty("es.node.mode", "network"))) {
           return true;
        }
        return Boolean.parseBoolean(System.getProperty("es.node.local", "false"));
    }

    private Settings getSettings(int nodeOrdinal, Settings others) {
        Builder builder = ImmutableSettings.settingsBuilder().put(defaultSettings);
        Settings settings = nodeSettingsSource.settings(nodeOrdinal);
        if (settings != null) {
            builder.put(settings);
        }
        if (others != null) {
            builder.put(others);
        }
        return builder.build();
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

    private synchronized NodeAndClient getOrBuildRandomNode() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient;
        }
        NodeAndClient buildNode = buildNode();
        buildNode.node().start();
        publishNode(buildNode);
        return buildNode;
    }

    private synchronized NodeAndClient getRandomNodeAndClient() {
        Predicate<NodeAndClient> all = Predicates.alwaysTrue();
        return getRandomNodeAndClient(all);
    }


    private synchronized NodeAndClient getRandomNodeAndClient(Predicate<NodeAndClient> predicate) {
        ensureOpen();
        Collection<NodeAndClient> values = Collections2.filter(nodes.values(), predicate);
        if (!values.isEmpty()) {
            int whichOne = random.nextInt(values.size());
            for (NodeAndClient nodeAndClient : values) {
                if (whichOne-- == 0) {
                    return nodeAndClient;
                }
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
            publishNode(buildNode);
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

    private NodeAndClient buildNode(Settings settings) {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), settings);
    }

    private NodeAndClient buildNode() {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), null);
    }

    private NodeAndClient buildNode(int nodeId, long seed, Settings settings) {
        ensureOpen();
        settings = getSettings(nodeId, settings);
        String name = buildNodeName(nodeId);
        assert !nodes.containsKey(name);
        Settings finalSettings = settingsBuilder()
                .put(settings)
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
        return getOrBuildRandomNode().client(random);
    }

    public synchronized Client masterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client master is requested
        }
        Assert.fail("No master client found");
        return null; // can't happen
    }

    public synchronized Client nonMasterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(Predicates.not(new MasterNodePredicate(getMasterName())));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client non-master is requested
        }
        Assert.fail("No non-master client found");
        return null; // can't happen
    }

    public synchronized Client clientNodeClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new ClientNodePredicate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        startNodeClient(ImmutableSettings.EMPTY);
        return getRandomNodeAndClient(new ClientNodePredicate()).client(random);
    }

    public synchronized Client clientNodeClient(String nodeName) {
        ensureOpen();
        NodeAndClient randomNodeAndClient = nodes.get(nodeName);
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        return null;
    }

    public synchronized Client smartClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        Assert.fail("No smart client found");
        return null; // can't happen
    }

    public synchronized Client client(final Predicate<Settings> filterPredicate) {
        ensureOpen();
        final NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new Predicate<NodeAndClient>() {
            @Override
            public boolean apply(NodeAndClient nodeAndClient) {
                return filterPredicate.apply(nodeAndClient.node.settings());
            }
        });
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        return null;
    }

    public void close() {
        ensureOpen();
        if (this.open.compareAndSet(true, false)) {
            IOUtils.closeWhileHandlingException(nodes.values());
            nodes.clear();
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
        private InternalNode node;
        private Client client;
        private Client nodeClient;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ClientFactory clientFactory;
        private final String name;

        NodeAndClient(String name, Node node, ClientFactory factory) {
            this.node = (InternalNode) node;
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

        Client nodeClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (nodeClient == null) {
                nodeClient = node.client();
            }
            return nodeClient;
        }

        void resetClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            if (client != null) {
                client.close();
                client = null;
            }
            if (nodeClient != null) {
                nodeClient.close();
                nodeClient = null;
            }
        }

        void restart(RestartCallback callback) throws Exception {
            assert callback != null;
            if (!node.isClosed()) {
                node.close();
            }
            Settings newSettings = callback.onNodeStopped(name);
            if (newSettings == null) {
                newSettings = ImmutableSettings.EMPTY;
            }
            if (callback.clearData(name)) {
                NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, node);
                if (nodeEnv.hasNodeFile()) {
                    FileSystemUtils.deleteRecursively(nodeEnv.nodeDataLocations());
                }
            }
            node = (InternalNode) nodeBuilder().settings(node.settings()).settings(newSettings).node();
            resetClient();
        }
        
        

        @Override
        public void close() {
            closed.set(true);
            if (client != null) {
                client.close();
                client = null;
            }
            if (nodeClient != null) {
                nodeClient.close();
                nodeClient = null;
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
            TransportClient client = new TransportClient(settingsBuilder().put("client.transport.nodes_sampler_interval", "1s")
                    .put("name", "transport_client_" + node.settings().get("name"))
                    .put("cluster.name", clusterName).put("client.transport.sniff", sniff).build());
            client.addTransportAddress(addr);
            return client;
        }
    }

    public class RandomClientFactory extends ClientFactory {

        @Override
        public Client client(Node node, String clusterName, Random random) {
            double nextDouble = random.nextDouble();
            if (nextDouble < transportClientRatio) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Using transport client for node [{}] sniff: [{}]", node.settings().get("name"), false);
                } 
                /* no sniff client for now - doesn't work will all tests since it might throw NoNodeAvailableException if nodes are shut down.
                 * we first need support of transportClientRatio as annotations or so
                 */
                return TransportClientFactory.NO_SNIFF_CLIENT_FACTORY.client(node, clusterName, random);
            } else {
                return node.client();
            }
        }
    }

    public synchronized void beforeTest(Random random, double transportClientRatio) {
        reset(random, true, transportClientRatio);
    }

    private synchronized void reset(Random random, boolean wipeData, double transportClientRatio) {
        assert transportClientRatio >= 0.0 && transportClientRatio <= 1.0;
        logger.debug("Reset test cluster with transport client ratio: [{}]", transportClientRatio);
        this.transportClientRatio = transportClientRatio;
        this.random = new Random(random.nextLong());
        resetClients(); /* reset all clients - each test gets it's own client based on the Random instance created above. */
        if (wipeData) {
            wipeDataDirectories();
        }
        if (nextNodeId.get() == sharedNodesSeeds.length && nodes.size() == sharedNodesSeeds.length) {
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);


        Set<NodeAndClient> sharedNodes = new HashSet<NodeAndClient>();
        boolean changed = false;
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            String buildNodeName = buildNodeName(i);
            NodeAndClient nodeAndClient = nodes.get(buildNodeName);
            if (nodeAndClient == null) {
                changed = true;
                nodeAndClient = buildNode(i, sharedNodesSeeds[i], defaultSettings);
                nodeAndClient.node.start();
                logger.info("Start Shared Node [{}] not shared", nodeAndClient.name);
            }
            sharedNodes.add(nodeAndClient);
        }
        if (!changed && sharedNodes.size() == nodes.size()) {
            logger.debug("Cluster is consistent - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            if (numNodes() > 0) {
                client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(sharedNodesSeeds.length)).get();
            }
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
            publishNode(nodeAndClient);
        }
        nextNodeId.set(sharedNodesSeeds.length);
        assert numNodes() == sharedNodesSeeds.length;
        if (numNodes() > 0) {
            client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(sharedNodesSeeds.length)).get();
        }
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
    }

    public synchronized void afterTest() {
        wipeDataDirectories();
        resetClients(); /* reset all clients - each test gets it's own client based on the Random instance created above. */

    }

    private void resetClients() {
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            nodeAndClient.resetClient();
        }
    }

    private void wipeDataDirectories() {
        if (!dataDirToClean.isEmpty()) {
            logger.info("Wipe data directory for all nodes locations: {}", this.dataDirToClean);
            try {
                FileSystemUtils.deleteRecursively(dataDirToClean.toArray(new File[0]));
            } finally {
                this.dataDirToClean.clear();
            }
        }
    }

    public synchronized ClusterService clusterService() {
        return getInstance(ClusterService.class);
    }

    public synchronized <T> Iterable<T> getInstances(Class<T> clazz) {
        List<T> instances = new ArrayList<T>(nodes.size());
        for (NodeAndClient nodeAndClient : nodes.values()) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    public synchronized <T> T getInstance(Class<T> clazz, final String node) {
        final Predicate<TestCluster.NodeAndClient> predicate;
        if (node != null) {
            predicate = new Predicate<TestCluster.NodeAndClient>() {
                public boolean apply(NodeAndClient nodeAndClient) {
                    return node.equals(nodeAndClient.name);
                }
            };
        } else {
            predicate = Predicates.alwaysTrue();
        }
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(predicate);
        assert randomNodeAndClient != null;
        return getInstanceFromNode(clazz, randomNodeAndClient.node);
    }

    public synchronized <T> T getInstance(Class<T> clazz) {
        return getInstance(clazz, null);
    }

    private synchronized <T> T getInstanceFromNode(Class<T> clazz, InternalNode node) {
        return node.injector().getInstance(clazz);
    }

    public synchronized int numNodes() {
        return this.nodes.size();
    }

    public synchronized void stopRandomNode() {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient();
        if (nodeAndClient != null) {
            logger.info("Closing random node [{}] ", nodeAndClient.name);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    public synchronized void stopRandomNode(final Predicate<Settings> filter) {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new Predicate<TestCluster.NodeAndClient>() {
            @Override
            public boolean apply(NodeAndClient nodeAndClient) {
                return filter.apply(nodeAndClient.node.settings());
            }
        });
        if (nodeAndClient != null) {
            logger.info("Closing filtered random node [{}] ", nodeAndClient.name);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }


    public synchronized void stopCurrentMasterNode() {
        ensureOpen();
        assert numNodes() > 0;
        String masterNodeName = getMasterName();
        assert nodes.containsKey(masterNodeName);
        logger.info("Closing master node [{}] ", masterNodeName);
        NodeAndClient remove = nodes.remove(masterNodeName);
        remove.close();
    }

    public void stopRandomNonMasterNode() {
        NodeAndClient nodeAndClient = getRandomNodeAndClient(Predicates.not(new MasterNodePredicate(getMasterName())));
        if (nodeAndClient != null) {
            logger.info("Closing random non master node [{}] current master [{}] ", nodeAndClient.name, getMasterName());
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }
    public void restartRandomNode() throws Exception {
        restartRandomNode(EMPTY_CALLBACK);
        
    }

    public void restartRandomNode(RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient();
        if (nodeAndClient != null) {
            logger.info("Restarting random node [{}] ", nodeAndClient.name);
            nodeAndClient.restart(callback);
        }
    }
   
    private void restartAllNodes(boolean rollingRestart, RestartCallback callback) throws Exception {
        ensureOpen();
        List<NodeAndClient> toRemove = new ArrayList<TestCluster.NodeAndClient>();
        try {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                if (!callback.doRestart(nodeAndClient.name)) {
                    logger.info("Closing node [{}] during restart", nodeAndClient.name);
                    toRemove.add(nodeAndClient);
                    nodeAndClient.close();
                }
            }
        } finally {
            for (NodeAndClient nodeAndClient : toRemove) {
                nodes.remove(nodeAndClient.name);
            }
        }
        logger.info("Restarting remaining nodes rollingRestart [{}]", rollingRestart);
        if (rollingRestart) {
            int numNodesRestarted = 0;
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Restarting node [{}] ", nodeAndClient.name);
                nodeAndClient.restart(callback);
            }
        } else {
            int numNodesRestarted = 0;
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Stopping node [{}] ", nodeAndClient.name);
                nodeAndClient.node.close();
            }
            for (NodeAndClient nodeAndClient : nodes.values()) {
                logger.info("Starting node [{}] ", nodeAndClient.name);
                nodeAndClient.restart(callback);
            }
        }
    }
    public static final RestartCallback EMPTY_CALLBACK = new RestartCallback() {
        public Settings onNodeStopped(String node) {
            return null;
        }
    };
    
    public void fullRestart() throws Exception {
        fullRestart(EMPTY_CALLBACK);
    }
    
    public void rollingRestart() throws Exception {
        rollingRestart(EMPTY_CALLBACK);
    }
    
    public void rollingRestart(RestartCallback function) throws Exception {
        restartAllNodes(true, function);
    }
    
    public void fullRestart(RestartCallback function) throws Exception {
        restartAllNodes(false, function);
    }
    

    private String getMasterName() {
        try {
            ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
            return state.nodes().masterNode().name();
        } catch (Throwable e) {
            logger.warn("Can't fetch cluster state", e);
            throw new RuntimeException("Can't get master node " + e.getMessage(), e);
        }
    }

    public synchronized Set<String> allButN(int numNodes) {
        return nRandomNodes(numNodes() - numNodes);
    }

    public synchronized Set<String> nRandomNodes(int numNodes) {
        assert numNodes() >= numNodes;
        return Sets.newHashSet(Iterators.limit(this.nodes.keySet().iterator(), numNodes));
    }

    public synchronized void startNodeClient(Settings settings) {
        ensureOpen(); // currently unused
        startNode(settingsBuilder().put(settings).put("node.client", true));
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

    public String startNode() {
        return startNode(ImmutableSettings.EMPTY);
    }

    public String startNode(Settings.Builder settings) {
        return startNode(settings.build());
    }

    public String startNode(Settings settings) {
        NodeAndClient buildNode = buildNode(settings);
        buildNode.node().start();
        publishNode(buildNode);
        return buildNode.name;
    }

    private void publishNode(NodeAndClient nodeAndClient) {
        assert !nodeAndClient.node().isClosed();
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, nodeAndClient.node);
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataLocations()));
        }
        nodes.put(nodeAndClient.name, nodeAndClient);

    }

    public void resetAllGateways() throws Exception {
        Collection<NodeAndClient> values = this.nodes.values();
        for (NodeAndClient nodeAndClient : values) {
            getInstanceFromNode(Gateway.class, ((InternalNode) nodeAndClient.node)).reset();
        }
    }

    public void closeNonSharedNodes(boolean wipeData) {
        reset(random, wipeData, transportClientRatio);
    }


    private static final class MasterNodePredicate implements Predicate<NodeAndClient> {
        private final String masterNodeName;

        public MasterNodePredicate(String masterNodeName) {
            this.masterNodeName = masterNodeName;
        }

        @Override
        public boolean apply(NodeAndClient nodeAndClient) {
            return masterNodeName.equals(nodeAndClient.name);
        }
    }

    private static final class ClientNodePredicate implements Predicate<NodeAndClient> {

        @Override
        public boolean apply(NodeAndClient nodeAndClient) {
            return nodeAndClient.node.settings().getAsBoolean("node.client", false);
        }
    }

    @Override
    public synchronized Iterator<Client> iterator() {
        ensureOpen();
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
    
    public static Predicate<Settings> nameFilter(String... nodeName) {
        return new NodeNamePredicate(new HashSet<String>(Arrays.asList(nodeName)));
    }
    
    private static final class NodeNamePredicate implements Predicate<Settings> {
        private final HashSet<String> nodeNames;


        public NodeNamePredicate(HashSet<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        @Override
        public boolean apply(Settings settings) {
            return nodeNames.contains(settings.get("name"));
            		
        }
    }
    
    public static abstract class RestartCallback {
        
        public Settings onNodeStopped(String nodeName) throws Exception {
            return ImmutableSettings.EMPTY;
        }
        
        public void doAfterNodes(int numNodes, Client client) throws Exception {
        }
        
        public boolean clearData(String nodeName) {
            return false;
        }
        
        public boolean doRestart(String nodeName) {
            return true;
        }
        
    }

}

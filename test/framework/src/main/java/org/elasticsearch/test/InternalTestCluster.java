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
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.SysGlobals;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.util.LuceneTestCase.TEST_NIGHTLY;
import static org.apache.lucene.util.LuceneTestCase.rarely;
import static org.elasticsearch.discovery.DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.awaitBusy;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.getTestTransportType;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * InternalTestCluster manages a set of JVM private nodes and allows convenient access to them.
 * The cluster supports randomized configuration such that nodes started in the cluster will
 * automatically load asserting services tracking resources like file handles or open searchers.
 * <p>
 * The Cluster is bound to a test lifecycle where tests must call {@link #beforeTest(java.util.Random, double)} and
 * {@link #afterTest()} to initialize and reset the cluster in order to be more reproducible. The term "more" relates
 * to the async nature of Elasticsearch in combination with randomized testing. Once Threads and asynchronous calls
 * are involved reproducibility is very limited. This class should only be used through {@link ESIntegTestCase}.
 * </p>
 */
public final class InternalTestCluster extends TestCluster {

    private final Logger logger = Loggers.getLogger(getClass());


    private static final AtomicInteger clusterOrdinal = new AtomicInteger();


    public static final int DEFAULT_LOW_NUM_MASTER_NODES = 1;
    public static final int DEFAULT_HIGH_NUM_MASTER_NODES = 3;

    static final int DEFAULT_MIN_NUM_DATA_NODES = 1;
    static final int DEFAULT_MAX_NUM_DATA_NODES = TEST_NIGHTLY ? 6 : 3;

    static final int DEFAULT_NUM_CLIENT_NODES = -1;
    static final int DEFAULT_MIN_NUM_CLIENT_NODES = 0;
    static final int DEFAULT_MAX_NUM_CLIENT_NODES = 1;

    static final boolean DEFAULT_ENABLE_HTTP_PIPELINING = true;

    /* sorted map to make traverse order reproducible, concurrent since we do checks on it not within a sync block */
    private final NavigableMap<String, NodeAndClient> nodes = new TreeMap<>();

    private final Set<Path> dataDirToClean = new HashSet<>();

    private final String clusterName;

    private final AtomicBoolean open = new AtomicBoolean(true);

    private final Settings defaultSettings;

    private AtomicInteger nextNodeId = new AtomicInteger(0);

    /* Each shared node has a node seed that is used to start up the node and get default settings
     * this is important if a node is randomly shut down in a test since the next test relies on a
     * fully shared cluster to be more reproducible */
    private final long[] sharedNodesSeeds;


    // if set to 0, data nodes will also assume the master role
    private final int numSharedDedicatedMasterNodes;

    private final int numSharedDataNodes;

    private final int numSharedCoordOnlyNodes;

    private final NodeConfigurationSource nodeConfigurationSource;

    private final ExecutorService executor;

    private final boolean autoManageMinMasterNodes;

    private final Collection<Class<? extends Plugin>> mockPlugins;

    /**
     * All nodes started by the cluster will have their name set to nodePrefix followed by a positive number
     */
    private final String nodePrefix;
    private final Path baseDir;

    private ServiceDisruptionScheme activeDisruptionScheme;
    private Function<Client, Client> clientWrapper;

    public InternalTestCluster(long clusterSeed, Path baseDir,
                               boolean randomlyAddDedicatedMasters,
                               boolean autoManageMinMasterNodes, int minNumDataNodes, int maxNumDataNodes, String clusterName, NodeConfigurationSource nodeConfigurationSource, int numClientNodes,
                               boolean enableHttpPipelining, String nodePrefix, Collection<Class<? extends Plugin>> mockPlugins, Function<Client, Client> clientWrapper) {
        super(clusterSeed);
        this.autoManageMinMasterNodes = autoManageMinMasterNodes;
        this.clientWrapper = clientWrapper;
        this.baseDir = baseDir;
        this.clusterName = clusterName;
        if (minNumDataNodes < 0 || maxNumDataNodes < 0) {
            throw new IllegalArgumentException("minimum and maximum number of data nodes must be >= 0");
        }

        if (maxNumDataNodes < minNumDataNodes) {
            throw new IllegalArgumentException("maximum number of data nodes must be >= minimum number of  data nodes");
        }

        Random random = new Random(clusterSeed);

        boolean useDedicatedMasterNodes = randomlyAddDedicatedMasters ? random.nextBoolean() : false;

        this.numSharedDataNodes = RandomNumbers.randomIntBetween(random, minNumDataNodes, maxNumDataNodes);
        assert this.numSharedDataNodes >= 0;

        if (numSharedDataNodes == 0) {
            this.numSharedCoordOnlyNodes = 0;
            this.numSharedDedicatedMasterNodes = 0;
        } else {
            if (useDedicatedMasterNodes) {
                if (random.nextBoolean()) {
                    // use a dedicated master, but only low number to reduce overhead to tests
                    this.numSharedDedicatedMasterNodes = DEFAULT_LOW_NUM_MASTER_NODES;
                } else {
                    this.numSharedDedicatedMasterNodes = DEFAULT_HIGH_NUM_MASTER_NODES;
                }
            } else {
                this.numSharedDedicatedMasterNodes = 0;
            }
            if (numClientNodes < 0) {
                this.numSharedCoordOnlyNodes = RandomNumbers.randomIntBetween(random, DEFAULT_MIN_NUM_CLIENT_NODES, DEFAULT_MAX_NUM_CLIENT_NODES);
            } else {
                this.numSharedCoordOnlyNodes = numClientNodes;
            }
        }
        assert this.numSharedCoordOnlyNodes >= 0;

        this.nodePrefix = nodePrefix;

        assert nodePrefix != null;
        ArrayList<Class<? extends Plugin>> tmpMockPlugins = new ArrayList<>(mockPlugins);


        this.mockPlugins = mockPlugins;


        sharedNodesSeeds = new long[numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes];
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            sharedNodesSeeds[i] = random.nextLong();
        }

        logger.info("Setup InternalTestCluster [{}] with seed [{}] using [{}] dedicated masters, " +
                "[{}] (data) nodes and [{}] coord only nodes (min_master_nodes are [{}])",
            clusterName, SeedUtils.formatSeed(clusterSeed),
            numSharedDedicatedMasterNodes, numSharedDataNodes, numSharedCoordOnlyNodes,
            autoManageMinMasterNodes ? "auto-managed" : "manual");
        this.nodeConfigurationSource = nodeConfigurationSource;
        Builder builder = Settings.builder();
        if (random.nextInt(5) == 0) { // sometimes set this
            // randomize (multi/single) data path, special case for 0, don't set it at all...
            final int numOfDataPaths = random.nextInt(5);
            if (numOfDataPaths > 0) {
                StringBuilder dataPath = new StringBuilder();
                for (int i = 0; i < numOfDataPaths; i++) {
                    dataPath.append(baseDir.resolve("d" + i).toAbsolutePath()).append(',');
                }
                builder.put(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString());
            }
        }
        builder.put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE);
        builder.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), baseDir.resolve("custom"));
        builder.put(Environment.PATH_HOME_SETTING.getKey(), baseDir);
        builder.put(Environment.PATH_REPO_SETTING.getKey(), baseDir.resolve("repos"));
        builder.put(TcpTransport.PORT.getKey(), 0);
        builder.put("http.port", 0);
        builder.put("http.pipelining", enableHttpPipelining);
        if (Strings.hasLength(System.getProperty("tests.es.logger.level"))) {
            builder.put("logger.level", System.getProperty("tests.es.logger.level"));
        }
        if (Strings.hasLength(System.getProperty("es.logger.prefix"))) {
            builder.put("logger.prefix", System.getProperty("es.logger.prefix"));
        }
        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b");
        // Some tests make use of scripting quite a bit, so increase the limit for integration tests
        builder.put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "1000/1m");
        builder.put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), random.nextBoolean());
        if (TEST_NIGHTLY) {
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 5, 10));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 5, 10));
        } else if (random.nextInt(100) <= 90) {
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 2, 5));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 2, 5));
        }
        // always reduce this - it can make tests really slow
        builder.put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), TimeValue.timeValueMillis(RandomNumbers.randomIntBetween(random, 20, 50)));
        defaultSettings = builder.build();
        executor = EsExecutors.newScaling("test runner", 0, Integer.MAX_VALUE, 0, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory("test_" + clusterName), new ThreadContext(Settings.EMPTY));
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    /** returns true if the {@link ElectMasterService#DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING} setting is auto managed by this cluster */
    public boolean getAutoManageMinMasterNode() {
        return autoManageMinMasterNodes;
    }

    public String[] getNodeNames() {
        return nodes.keySet().toArray(Strings.EMPTY_ARRAY);
    }

    private Settings getSettings(int nodeOrdinal, long nodeSeed, Settings others) {
        Builder builder = Settings.builder().put(defaultSettings)
            .put(getRandomNodeSettings(nodeSeed));
        Settings settings = nodeConfigurationSource.nodeSettings(nodeOrdinal);
        if (settings != null) {
            if (settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) != null) {
                throw new IllegalStateException("Tests must not set a '" + ClusterName.CLUSTER_NAME_SETTING.getKey() + "' as a node setting set '" + ClusterName.CLUSTER_NAME_SETTING.getKey() + "': [" + settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) + "]");
            }
            builder.put(settings);
        }
        if (others != null) {
            builder.put(others);
        }
        builder.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName);
        return builder.build();
    }

    public Collection<Class<? extends Plugin>> getPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(nodeConfigurationSource.nodePlugins());
        plugins.addAll(mockPlugins);
        return plugins;
    }

    private Settings getRandomNodeSettings(long seed) {
        Random random = new Random(seed);
        Builder builder = Settings.builder();
        builder.put(Transport.TRANSPORT_TCP_COMPRESS.getKey(), rarely(random));
        if (random.nextBoolean()) {
            builder.put("cache.recycler.page.type", RandomPicks.randomFrom(random, PageCacheRecycler.Type.values()));
        }
        if (random.nextInt(10) == 0) { // 10% of the nodes have a very frequent check interval
            builder.put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10 + random.nextInt(2000)).getStringRep());
        } else if (random.nextInt(10) != 0) { // 90% of the time - 10% of the time we don't set anything
            builder.put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(10 + random.nextInt(5 * 60)).getStringRep());
        }
        if (random.nextBoolean()) { // sometimes set a
            builder.put(SearchService.DEFAULT_KEEPALIVE_SETTING.getKey(), TimeValue.timeValueSeconds(100 + random.nextInt(5 * 60)).getStringRep());
        }

        builder.put(EsExecutors.PROCESSORS_SETTING.getKey(), 1 + random.nextInt(3));
        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                builder.put("indices.fielddata.cache.size", 1 + random.nextInt(1000), ByteSizeUnit.MB);
            }
        }

        // randomize tcp settings
        if (random.nextBoolean()) {
            builder.put(TcpTransport.CONNECTIONS_PER_NODE_RECOVERY.getKey(), random.nextInt(2) + 1);
            builder.put(TcpTransport.CONNECTIONS_PER_NODE_BULK.getKey(), random.nextInt(3) + 1);
            builder.put(TcpTransport.CONNECTIONS_PER_NODE_REG.getKey(), random.nextInt(6) + 1);
        }

        if (random.nextBoolean()) {
            builder.put(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey(), new TimeValue(RandomNumbers.randomIntBetween(random, 10, 30), TimeUnit.SECONDS));
        }

        if (random.nextInt(10) == 0) {
            builder.put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop");
            builder.put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop");
        }

        if (random.nextBoolean()) {
            if (random.nextInt(10) == 0) { // do something crazy slow here
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(RandomNumbers.randomIntBetween(random, 1, 10), ByteSizeUnit.MB));
            } else {
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(RandomNumbers.randomIntBetween(random, 10, 200), ByteSizeUnit.MB));
            }
        }

        if (random.nextBoolean()) {
            builder.put(TcpTransport.PING_SCHEDULE.getKey(), RandomNumbers.randomIntBetween(random, 100, 2000) + "ms");
        }

        if (random.nextBoolean()) {
            builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 0, 2000));
        }
        if (random.nextBoolean()) {
            builder.put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getKey(), TimeValue.timeValueMillis(RandomNumbers.randomIntBetween(random, 750, 10000000)).getStringRep());
        }

        return builder.build();
    }

    public static String clusterName(String prefix, long clusterSeed) {
        StringBuilder builder = new StringBuilder(prefix);
        final int childVM = RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0);
        builder.append("-CHILD_VM=[").append(childVM).append(']');
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
        NodeAndClient buildNode = buildNode(1);
        buildNode.startNode();
        publishNode(buildNode);
        return buildNode;
    }

    private synchronized NodeAndClient getRandomNodeAndClient() {
        return getRandomNodeAndClient(nc -> true);
    }


    private synchronized NodeAndClient getRandomNodeAndClient(Predicate<NodeAndClient> predicate) {
        ensureOpen();
        Collection<NodeAndClient> values = nodes.values().stream().filter(predicate).collect(Collectors.toCollection(ArrayList::new));
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

    /**
     * Ensures that at least <code>n</code> data nodes are present in the cluster.
     * if more nodes than <code>n</code> are present this method will not
     * stop any of the running nodes.
     */
    public synchronized void ensureAtLeastNumDataNodes(int n) {
        boolean added = false;
        int size = numDataNodes();
        for (int i = size; i < n; i++) {
            logger.info("increasing cluster size from {} to {}", size, n);
            added = true;
            if (numSharedDedicatedMasterNodes > 0) {
                startDataOnlyNode(Settings.EMPTY);
            } else {
                startNode(Settings.EMPTY);
            }
        }
        if (added) {
            validateClusterFormed();
        }
    }

    /**
     * Ensures that at most <code>n</code> are up and running.
     * If less nodes that <code>n</code> are running this method
     * will not start any additional nodes.
     */
    public synchronized void ensureAtMostNumDataNodes(int n) throws IOException {
        int size = numDataNodes();
        if (size <= n) {
            return;
        }
        // prevent killing the master if possible and client nodes
        final Stream<NodeAndClient> collection =
            n == 0 ? nodes.values().stream() : nodes.values().stream().filter(new DataNodePredicate().and(new MasterNodePredicate(getMasterName()).negate()));
        final Iterator<NodeAndClient> values = collection.iterator();

        logger.info("changing cluster size from {} data nodes to {}", size, n);
        Set<NodeAndClient> nodesToRemove = new HashSet<>();
        int numNodesAndClients = 0;
        while (values.hasNext() && numNodesAndClients++ < size - n) {
            NodeAndClient next = values.next();
            nodesToRemove.add(next);
        }

        stopNodesAndClients(nodesToRemove);
        if (!nodesToRemove.isEmpty() && size() > 0) {
            validateClusterFormed();
        }
    }

    /**
     * builds a new node given the settings.
     *
     * @param settings              the settings to use
     * @param defaultMinMasterNodes min_master_nodes value to use if min_master_nodes is auto managed
     */
    private NodeAndClient buildNode(Settings settings, int defaultMinMasterNodes) {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), settings, false, defaultMinMasterNodes);
    }

    /**
     * builds a new node with default settings
     *
     * @param defaultMinMasterNodes min_master_nodes value to use if min_master_nodes is auto managed
     */
    private NodeAndClient buildNode(int defaultMinMasterNodes) {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), null, false, defaultMinMasterNodes);
    }

    /**
     * builds a new node
     *
     * @param nodeId                the node internal id (see {@link NodeAndClient#nodeAndClientId()}
     * @param seed                  the node's random seed
     * @param settings              the settings to use
     * @param reuseExisting         if a node with the same name is already part of {@link #nodes}, no new node will be built and
     *                              the method will return the existing one
     * @param defaultMinMasterNodes min_master_nodes value to use if min_master_nodes is auto managed
     */
    private NodeAndClient buildNode(int nodeId, long seed, Settings settings,
                                    boolean reuseExisting, int defaultMinMasterNodes) {
        assert Thread.holdsLock(this);
        ensureOpen();
        settings = getSettings(nodeId, seed, settings);
        Collection<Class<? extends Plugin>> plugins = getPlugins();
        String name = buildNodeName(nodeId, settings);
        if (reuseExisting && nodes.containsKey(name)) {
            return nodes.get(name);
        } else {
            assert reuseExisting == true || nodes.containsKey(name) == false :
                "node name [" + name + "] already exists but not allowed to use it";
        }
        Settings.Builder finalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), baseDir) // allow overriding path.home
            .put(settings)
            .put("node.name", name)
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), seed);

        final boolean usingSingleNodeDiscovery = DiscoveryModule.DISCOVERY_TYPE_SETTING.get(finalSettings.build()).equals("single-node");
        if (!usingSingleNodeDiscovery && autoManageMinMasterNodes) {
            assert finalSettings.get(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()) == null :
                "min master nodes may not be set when auto managed";
            assert finalSettings.get(INITIAL_STATE_TIMEOUT_SETTING.getKey()) == null :
                "automatically managing min master nodes require nodes to complete a join cycle" +
                    " when starting";
            finalSettings
                // don't wait too long not to slow down tests
                .put(ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING.getKey(), "5s")
                .put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), defaultMinMasterNodes);
        } else if (!usingSingleNodeDiscovery && finalSettings.get(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()) == null) {
            throw new IllegalArgumentException(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey() + " must be configured");
        }
        SecureSettings secureSettings = finalSettings.getSecureSettings();
        if (secureSettings instanceof MockSecureSettings) {
            // we clone this here since in the case of a node restart we might need it again
            secureSettings = ((MockSecureSettings) secureSettings).clone();
        }
        MockNode node = new MockNode(finalSettings.build(), plugins, nodeConfigurationSource.nodeConfigPath(nodeId));
        try {
            IOUtils.close(secureSettings);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new NodeAndClient(name, node, nodeId);
    }

    private String buildNodeName(int id, Settings settings) {
        String prefix = nodePrefix;
        prefix = prefix + getRoleSuffix(settings);
        return prefix + id;
    }

    /**
     * returns a suffix string based on the node role. If no explicit role is defined, the suffix will be empty
     */
    private String getRoleSuffix(Settings settings) {
        String suffix = "";
        if (Node.NODE_MASTER_SETTING.exists(settings) && Node.NODE_MASTER_SETTING.get(settings)) {
            suffix = suffix + Role.MASTER.getAbbreviation();
        }
        if (Node.NODE_DATA_SETTING.exists(settings) && Node.NODE_DATA_SETTING.get(settings)) {
            suffix = suffix + Role.DATA.getAbbreviation();
        }
        if (Node.NODE_MASTER_SETTING.exists(settings) && Node.NODE_MASTER_SETTING.get(settings) == false &&
            Node.NODE_DATA_SETTING.exists(settings) && Node.NODE_DATA_SETTING.get(settings) == false
            ) {
            suffix = suffix + "c";
        }
        return suffix;
    }

    /**
     * Returns the common node name prefix for this test cluster.
     */
    public String nodePrefix() {
        return nodePrefix;
    }

    @Override
    public synchronized Client client() {
        ensureOpen();
        /* Randomly return a client to one of the nodes in the cluster */
        return getOrBuildRandomNode().client(random);
    }

    /**
     * Returns a node client to a data node in the cluster.
     * Note: use this with care tests should not rely on a certain nodes client.
     */
    public synchronized Client dataNodeClient() {
        ensureOpen();
        /* Randomly return a client to one of the nodes in the cluster */
        return getRandomNodeAndClient(new DataNodePredicate()).client(random);
    }

    /**
     * Returns a node client to the current master node.
     * Note: use this with care tests should not rely on a certain nodes client.
     */
    public synchronized Client masterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client master is requested
        }
        Assert.fail("No master client found");
        return null; // can't happen
    }

    /**
     * Returns a node client to random node but not the master. This method will fail if no non-master client is available.
     */
    public synchronized Client nonMasterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()).negate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client non-master is requested
        }
        Assert.fail("No non-master client found");
        return null; // can't happen
    }

    /**
     * Returns a client to a coordinating only node
     */
    public synchronized Client coordOnlyNodeClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new NoDataNoMasterNodePredicate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        int nodeId = nextNodeId.getAndIncrement();
        Settings settings = getSettings(nodeId, random.nextLong(), Settings.EMPTY);
        startCoordinatingOnlyNode(settings);
        return getRandomNodeAndClient(new NoDataNoMasterNodePredicate()).client(random);
    }

    public synchronized String startCoordinatingOnlyNode(Settings settings) {
        ensureOpen(); // currently unused
        Builder builder = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_INGEST_SETTING.getKey(), false);
        return startNode(builder);
    }

    /**
     * Returns a transport client
     */
    public synchronized Client transportClient() {
        ensureOpen();
        // randomly return a transport client going to one of the nodes in the cluster
        return getOrBuildRandomNode().transportClient();
    }

    /**
     * Returns a node client to a given node.
     */
    public synchronized Client client(String nodeName) {
        ensureOpen();
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            return nodeAndClient.client(random);
        }
        Assert.fail("No node found with name: [" + nodeName + "]");
        return null; // can't happen
    }


    /**
     * Returns a "smart" node client to a random node in the cluster
     */
    public synchronized Client smartClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        Assert.fail("No smart client found");
        return null; // can't happen
    }

    /**
     * Returns a random node that applies to the given predicate.
     * The predicate can filter nodes based on the nodes settings.
     * If all nodes are filtered out this method will return <code>null</code>
     */
    public synchronized Client client(final Predicate<Settings> filterPredicate) {
        ensureOpen();
        final NodeAndClient randomNodeAndClient = getRandomNodeAndClient(nodeAndClient -> filterPredicate.test(nodeAndClient.node.settings()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        return null;
    }

    @Override
    public synchronized void close() {
        if (this.open.compareAndSet(true, false)) {
            if (activeDisruptionScheme != null) {
                activeDisruptionScheme.testClusterClosed();
                activeDisruptionScheme = null;
            }
            IOUtils.closeWhileHandlingException(nodes.values());
            nodes.clear();
            executor.shutdownNow();
        }
    }

    private final class NodeAndClient implements Closeable {
        private MockNode node;
        private Client nodeClient;
        private Client transportClient;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final String name;
        private final int nodeAndClientId;

        NodeAndClient(String name, MockNode node, int nodeAndClientId) {
            this.node = node;
            this.name = name;
            this.nodeAndClientId = nodeAndClientId;
            markNodeDataDirsAsNotEligableForWipe(node);
        }

        Node node() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return node;
        }

        public int nodeAndClientId() {
            return nodeAndClientId;
        }

        public String getName() {
            return name;
        }

        public boolean isMasterEligible() {
            return Node.NODE_MASTER_SETTING.get(node.settings());
        }

        Client client(Random random) {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            double nextDouble = random.nextDouble();
            if (nextDouble < transportClientRatio) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Using transport client for node [{}] sniff: [{}]", node.settings().get("node.name"), false);
                }
                return getOrBuildTransportClient();
            } else {
                return getOrBuildNodeClient();
            }
        }

        Client nodeClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return getOrBuildNodeClient();
        }

        Client transportClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return getOrBuildTransportClient();
        }

        private Client getOrBuildNodeClient() {
            if (nodeClient == null) {
                nodeClient = node.client();
            }
            return clientWrapper.apply(nodeClient);
        }

        private Client getOrBuildTransportClient() {
            if (transportClient == null) {
                /* no sniff client for now - doesn't work will all tests since it might throw NoNodeAvailableException if nodes are shut down.
                 * we first need support of transportClientRatio as annotations or so
                 */
                transportClient = new TransportClientFactory(false, nodeConfigurationSource.transportClientSettings(), baseDir, nodeConfigurationSource.transportClientPlugins()).client(node, clusterName);
            }
            return clientWrapper.apply(transportClient);
        }

        void resetClient() throws IOException {
            if (closed.get() == false) {
                Releasables.close(nodeClient, transportClient);
                nodeClient = null;
                transportClient = null;
            }
        }

        void startNode() {
            try {
                node.start();
            } catch (NodeValidationException e) {
                throw new RuntimeException(e);
            }
        }

        void closeNode() throws IOException {
            markNodeDataDirsAsPendingForWipe(node);
            node.close();
        }

        /**
         * closes the current node if not already closed, builds a new node object using the current node settings and starts it
         */
        void restart(RestartCallback callback, boolean clearDataIfNeeded, int minMasterNodes) throws Exception {
            if (!node.isClosed()) {
                closeNode();
            }
            recreateNodeOnRestart(callback, clearDataIfNeeded, minMasterNodes);
            startNode();
        }

        /**
         * rebuilds a new node object using the current node settings and starts it
         */
        void recreateNodeOnRestart(RestartCallback callback, boolean clearDataIfNeeded, int minMasterNodes) throws Exception {
            assert callback != null;
            Settings callbackSettings = callback.onNodeStopped(name);
            Settings.Builder newSettings = Settings.builder();
            if (callbackSettings != null) {
                newSettings.put(callbackSettings);
            }
            if (minMasterNodes >= 0) {
                assert DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.exists(newSettings.build()) == false : "min master nodes is auto managed";
                newSettings.put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minMasterNodes).build();
            }
            if (clearDataIfNeeded) {
                clearDataIfNeeded(callback);
            }
            createNewNode(newSettings.build());
            // make sure cached client points to new node
            resetClient();
        }

        private void clearDataIfNeeded(RestartCallback callback) throws IOException {
            if (callback.clearData(name)) {
                NodeEnvironment nodeEnv = node.getNodeEnvironment();
                if (nodeEnv.hasNodeFile()) {
                    final Path[] locations = nodeEnv.nodeDataPaths();
                    logger.debug("removing node data paths: [{}]", Arrays.toString(locations));
                    IOUtils.rm(locations);
                }
            }
        }

        private void createNewNode(final Settings newSettings) {
            final long newIdSeed = NodeEnvironment.NODE_ID_SEED_SETTING.get(node.settings()) + 1; // use a new seed to make sure we have new node id
            Settings finalSettings = Settings.builder().put(node.settings()).put(newSettings).put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), newIdSeed).build();
            if (DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.exists(finalSettings) == false) {
                throw new IllegalStateException(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey() +
                    " is not configured after restart of [" + name + "]");
            }
            Collection<Class<? extends Plugin>> plugins = node.getClasspathPlugins();
            node = new MockNode(finalSettings, plugins);
            markNodeDataDirsAsNotEligableForWipe(node);
        }

        @Override
        public void close() throws IOException {
            try {
                resetClient();
            } finally {
                closed.set(true);
                closeNode();
            }
        }
    }

    public static final String TRANSPORT_CLIENT_PREFIX = "transport_client_";

    static class TransportClientFactory {
        private final boolean sniff;
        private final Settings settings;
        private final Path baseDir;
        private final Collection<Class<? extends Plugin>> plugins;

        TransportClientFactory(boolean sniff, Settings settings, Path baseDir, Collection<Class<? extends Plugin>> plugins) {
            this.sniff = sniff;
            this.settings = settings != null ? settings : Settings.EMPTY;
            this.baseDir = baseDir;
            this.plugins = plugins;
        }

        public Client client(Node node, String clusterName) {
            TransportAddress addr = node.injector().getInstance(TransportService.class).boundAddress().publishAddress();
            Settings nodeSettings = node.settings();
            Builder builder = Settings.builder()
                .put("client.transport.nodes_sampler_interval", "1s")
                .put(Environment.PATH_HOME_SETTING.getKey(), baseDir)
                .put("node.name", TRANSPORT_CLIENT_PREFIX + node.settings().get("node.name"))
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName).put("client.transport.sniff", sniff)
                .put("logger.prefix", nodeSettings.get("logger.prefix", ""))
                .put("logger.level", nodeSettings.get("logger.level", "INFO"))
                .put(settings);
            if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings)) {
                builder.put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), NetworkModule.TRANSPORT_TYPE_SETTING.get(settings));
            } else {
                builder.put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), getTestTransportType());
            }
            TransportClient client = new MockTransportClient(builder.build(), plugins);
            client.addTransportAddress(addr);
            return client;
        }
    }

    @Override
    public synchronized void beforeTest(Random random, double transportClientRatio) throws IOException, InterruptedException {
        super.beforeTest(random, transportClientRatio);
        reset(true);
    }

    private synchronized void reset(boolean wipeData) throws IOException {
        // clear all rules for mock transport services
        for (NodeAndClient nodeAndClient : nodes.values()) {
            TransportService transportService = nodeAndClient.node.injector().getInstance(TransportService.class);
            if (transportService instanceof MockTransportService) {
                final MockTransportService mockTransportService = (MockTransportService) transportService;
                mockTransportService.clearAllRules();
                mockTransportService.clearTracers();
            }
        }
        randomlyResetClients();
        final int newSize = sharedNodesSeeds.length;
        if (nextNodeId.get() == newSize && nodes.size() == newSize) {
            if (wipeData) {
                wipePendingDataDirectories();
            }
            if (nodes.size() > 0 && autoManageMinMasterNodes) {
                updateMinMasterNodes(getMasterNodesCount());
            }
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), newSize);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), newSize);

        // trash all nodes with id >= sharedNodesSeeds.length - they are non shared
        final List<NodeAndClient> toClose = new ArrayList<>();
        for (Iterator<NodeAndClient> iterator = nodes.values().iterator(); iterator.hasNext();) {
            NodeAndClient nodeAndClient = iterator.next();
            if (nodeAndClient.nodeAndClientId() >= sharedNodesSeeds.length) {
                logger.debug("Close Node [{}] not shared", nodeAndClient.name);
                toClose.add(nodeAndClient);
            }
        }
        stopNodesAndClients(toClose);

        // clean up what the nodes left that is unused
        if (wipeData) {
            wipePendingDataDirectories();
        }

        // start any missing node
        assert newSize == numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes;
        final int numberOfMasterNodes = numSharedDedicatedMasterNodes > 0 ? numSharedDedicatedMasterNodes : numSharedDataNodes;
        final int defaultMinMasterNodes = (numberOfMasterNodes / 2) + 1;
        final List<NodeAndClient> toStartAndPublish = new ArrayList<>(); // we want to start nodes in one go due to min master nodes
        for (int i = 0; i < numSharedDedicatedMasterNodes; i++) {
            final Settings.Builder settings = Settings.builder();
            settings.put(Node.NODE_MASTER_SETTING.getKey(), true);
            settings.put(Node.NODE_DATA_SETTING.getKey(), false);
            NodeAndClient nodeAndClient = buildNode(i, sharedNodesSeeds[i], settings.build(), true, defaultMinMasterNodes);
            toStartAndPublish.add(nodeAndClient);
        }
        for (int i = numSharedDedicatedMasterNodes; i < numSharedDedicatedMasterNodes + numSharedDataNodes; i++) {
            final Settings.Builder settings = Settings.builder();
            if (numSharedDedicatedMasterNodes > 0) {
                // if we don't have dedicated master nodes, keep things default
                settings.put(Node.NODE_MASTER_SETTING.getKey(), false).build();
                settings.put(Node.NODE_DATA_SETTING.getKey(), true).build();
            }
            NodeAndClient nodeAndClient = buildNode(i, sharedNodesSeeds[i], settings.build(), true, defaultMinMasterNodes);
            toStartAndPublish.add(nodeAndClient);
        }
        for (int i = numSharedDedicatedMasterNodes + numSharedDataNodes;
             i < numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes; i++) {
            final Builder settings = Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_INGEST_SETTING.getKey(), false);
            NodeAndClient nodeAndClient = buildNode(i, sharedNodesSeeds[i], settings.build(), true, defaultMinMasterNodes);
            toStartAndPublish.add(nodeAndClient);
        }

        startAndPublishNodesAndClients(toStartAndPublish);

        nextNodeId.set(newSize);
        assert size() == newSize;
        if (newSize > 0) {
            validateClusterFormed();
        }
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), newSize);
    }

    /** ensure a cluster is formed with all published nodes. */
    public synchronized void validateClusterFormed() {
        String name = randomFrom(random, getNodeNames());
        validateClusterFormed(name);
    }

    /** ensure a cluster is formed with all published nodes, but do so by using the client of the specified node */
    public synchronized void validateClusterFormed(String viaNode) {
        Set<DiscoveryNode> expectedNodes = new HashSet<>();
        for (NodeAndClient nodeAndClient : nodes.values()) {
            expectedNodes.add(getInstanceFromNode(ClusterService.class, nodeAndClient.node()).localNode());
        }
        logger.trace("validating cluster formed via [{}], expecting {}", viaNode, expectedNodes);
        final Client client = client(viaNode);
        try {
            if (awaitBusy(() -> {
                DiscoveryNodes discoveryNodes = client.admin().cluster().prepareState().get().getState().nodes();
                if (discoveryNodes.getSize() != expectedNodes.size()) {
                    return false;
                }
                for (DiscoveryNode expectedNode : expectedNodes) {
                    if (discoveryNodes.nodeExists(expectedNode) == false) {
                        return false;
                    }
                }
                return true;
            }, 30, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("cluster failed to form with expected nodes " + expectedNodes + " and actual nodes " +
                    client.admin().cluster().prepareState().get().getState().nodes());
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public synchronized void afterTest() throws IOException {
        wipePendingDataDirectories();
        randomlyResetClients(); /* reset all clients - each test gets its own client based on the Random instance created above. */
    }

    @Override
    public void beforeIndexDeletion() throws Exception {
        // Check that the operations counter on index shard has reached 0.
        // The assumption here is that after a test there are no ongoing write operations.
        // test that have ongoing write operations after the test (for example because ttl is used
        // and not all docs have been purged after the test) and inherit from
        // ElasticsearchIntegrationTest must override beforeIndexDeletion() to avoid failures.
        assertShardIndexCounter();
        //check that shards that have same sync id also contain same number of documents
        assertSameSyncIdSameDocs();
        assertOpenTranslogReferences();
    }

    private void assertSameSyncIdSameDocs() {
        Map<String, Long> docsOnShards = new HashMap<>();
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
            for (IndexService indexService : indexServices) {
                for (IndexShard indexShard : indexService) {
                    CommitStats commitStats = indexShard.commitStats();
                    if (commitStats != null) { // null if the engine is closed or if the shard is recovering
                        String syncId = commitStats.getUserData().get(Engine.SYNC_COMMIT_ID);
                        if (syncId != null) {
                            long liveDocsOnShard = commitStats.getNumDocs();
                            if (docsOnShards.get(syncId) != null) {
                                assertThat("sync id is equal but number of docs does not match on node " + nodeAndClient.name + ". expected " + docsOnShards.get(syncId) + " but got " + liveDocsOnShard, docsOnShards.get(syncId), equalTo(liveDocsOnShard));
                            } else {
                                docsOnShards.put(syncId, liveDocsOnShard);
                            }
                        }
                    }
                }
            }
        }
    }

    private void assertShardIndexCounter() throws Exception {
        assertBusy(() -> {
            final Collection<NodeAndClient> nodesAndClients = nodes.values();
            for (NodeAndClient nodeAndClient : nodesAndClients) {
                IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
                for (IndexService indexService : indexServices) {
                    for (IndexShard indexShard : indexService) {
                        int activeOperationsCount = indexShard.getActiveOperationsCount();
                        if (activeOperationsCount > 0) {
                            TaskManager taskManager = getInstance(TransportService.class, nodeAndClient.name).getTaskManager();
                            DiscoveryNode localNode = getInstance(ClusterService.class, nodeAndClient.name).localNode();
                            List<TaskInfo> taskInfos = taskManager.getTasks().values().stream()
                                    .filter(task -> task instanceof ReplicationTask)
                                    .map(task -> task.taskInfo(localNode.getId(), true))
                                    .collect(Collectors.toList());
                            ListTasksResponse response = new ListTasksResponse(taskInfos, Collections.emptyList(), Collections.emptyList());
                            try {
                                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().value(response);
                                throw new AssertionError("expected index shard counter on shard " + indexShard.shardId() + " on node " +
                                        nodeAndClient.name + " to be 0 but was " + activeOperationsCount + ". Current replication tasks on node:\n" +
                                        builder.string());
                            } catch (IOException e) {
                                throw new RuntimeException("caught exception while building response [" + response + "]", e);
                            }
                        }
                    }
                }
            }
        });
    }

    private void assertOpenTranslogReferences() throws Exception {
        assertBusy(() -> {
            final Collection<NodeAndClient> nodesAndClients = nodes.values();
            for (NodeAndClient nodeAndClient : nodesAndClients) {
                IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
                for (IndexService indexService : indexServices) {
                    for (IndexShard indexShard : indexService) {
                        try {
                            indexShard.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                        } catch (AlreadyClosedException ok) {
                            // all good
                        }
                    }
                }
            }
        });
    }

    private void randomlyResetClients() throws IOException {
        // only reset the clients on nightly tests, it causes heavy load...
        if (RandomizedTest.isNightly() && rarely(random)) {
            final Collection<NodeAndClient> nodesAndClients = nodes.values();
            for (NodeAndClient nodeAndClient : nodesAndClients) {
                nodeAndClient.resetClient();
            }
        }
    }

    private void wipePendingDataDirectories() {
        assert Thread.holdsLock(this);
        if (!dataDirToClean.isEmpty()) {
            try {
                for (Path path : dataDirToClean) {
                    try {
                        FileSystemUtils.deleteSubDirectories(path);
                        logger.info("Successfully wiped data directory for node location: {}", path);
                    } catch (IOException e) {
                        logger.info("Failed to wipe data directory for node location: {}", path);
                    }
                }
            } finally {
                dataDirToClean.clear();
            }
        }
    }

    private void markNodeDataDirsAsPendingForWipe(Node node) {
        assert Thread.holdsLock(this);
        NodeEnvironment nodeEnv = node.getNodeEnvironment();
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataPaths()));
        }
    }

    private void markNodeDataDirsAsNotEligableForWipe(Node node) {
        assert Thread.holdsLock(this);
        NodeEnvironment nodeEnv = node.getNodeEnvironment();
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.removeAll(Arrays.asList(nodeEnv.nodeDataPaths()));
        }
    }

    /**
     * Returns a reference to a random node's {@link ClusterService}
     */
    public ClusterService clusterService() {
        return clusterService(null);
    }

    /**
     * Returns a reference to a node's {@link ClusterService}. If the given node is null, a random node will be selected.
     */
    public synchronized ClusterService clusterService(@Nullable String node) {
        return getInstance(ClusterService.class, node);
    }

    /**
     * Returns an Iterable to all instances for the given class &gt;T&lt; across all nodes in the cluster.
     */
    public synchronized <T> Iterable<T> getInstances(Class<T> clazz) {
        List<T> instances = new ArrayList<>(nodes.size());
        for (NodeAndClient nodeAndClient : nodes.values()) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    /**
     * Returns an Iterable to all instances for the given class &gt;T&lt; across all data nodes in the cluster.
     */
    public synchronized <T> Iterable<T> getDataNodeInstances(Class<T> clazz) {
        return getInstances(clazz, new DataNodePredicate());
    }

    /**
     * Returns an Iterable to all instances for the given class &gt;T&lt; across all data and master nodes
     * in the cluster.
     */
    public synchronized <T> Iterable<T> getDataOrMasterNodeInstances(Class<T> clazz) {
        return getInstances(clazz, new DataOrMasterNodePredicate());
    }

    private synchronized <T> Iterable<T> getInstances(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        Iterable<NodeAndClient> filteredNodes = nodes.values().stream().filter(predicate)::iterator;
        List<T> instances = new ArrayList<>();
        for (NodeAndClient nodeAndClient : filteredNodes) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    /**
     * Returns a reference to the given nodes instances of the given class &gt;T&lt;
     */
    public synchronized <T> T getInstance(Class<T> clazz, final String node) {
        return getInstance(clazz, nc -> node == null || node.equals(nc.name));
    }

    public synchronized <T> T getDataNodeInstance(Class<T> clazz) {
        return getInstance(clazz, new DataNodePredicate());
    }

    private synchronized <T> T getInstance(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(predicate);
        assert randomNodeAndClient != null;
        return getInstanceFromNode(clazz, randomNodeAndClient.node);
    }

    /**
     * Returns a reference to a random nodes instances of the given class &gt;T&lt;
     */
    public synchronized <T> T getInstance(Class<T> clazz) {
        return getInstance(clazz, nc -> true);
    }

    private synchronized <T> T getInstanceFromNode(Class<T> clazz, Node node) {
        return node.injector().getInstance(clazz);
    }

    @Override
    public synchronized int size() {
        return this.nodes.size();
    }

    @Override
    public InetSocketAddress[] httpAddresses() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (HttpServerTransport httpServerTransport : getInstances(HttpServerTransport.class)) {
            addresses.add(httpServerTransport.boundAddress().publishAddress().address());
        }
        return addresses.toArray(new InetSocketAddress[addresses.size()]);
    }

    /**
     * Stops a random data node in the cluster. Returns true if a node was found to stop, false otherwise.
     */
    public synchronized boolean stopRandomDataNode() throws IOException {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new DataNodePredicate());
        if (nodeAndClient != null) {
            logger.info("Closing random node [{}] ", nodeAndClient.name);
            stopNodesAndClient(nodeAndClient);
            return true;
        }
        return false;
    }

    /**
     * Stops a random node in the cluster that applies to the given filter or non if the non of the nodes applies to the
     * filter.
     */
    public synchronized void stopRandomNode(final Predicate<Settings> filter) throws IOException {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(nc -> filter.test(nc.node.settings()));
        if (nodeAndClient != null) {
            logger.info("Closing filtered random node [{}] ", nodeAndClient.name);
            stopNodesAndClient(nodeAndClient);
        }
    }

    /**
     * Stops the current master node forcefully
     */
    public synchronized void stopCurrentMasterNode() throws IOException {
        ensureOpen();
        assert size() > 0;
        String masterNodeName = getMasterName();
        assert nodes.containsKey(masterNodeName);
        logger.info("Closing master node [{}] ", masterNodeName);
        stopNodesAndClient(nodes.get(masterNodeName));
    }

    /**
     * Stops any of the current nodes but not the master node.
     */
    public synchronized void stopRandomNonMasterNode() throws IOException {
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()).negate());
        if (nodeAndClient != null) {
            logger.info("Closing random non master node [{}] current master [{}] ", nodeAndClient.name, getMasterName());
            stopNodesAndClient(nodeAndClient);
        }
    }

    private synchronized void startAndPublishNodesAndClients(List<NodeAndClient> nodeAndClients) {
        if (nodeAndClients.size() > 0) {
            final int newMasters = (int) nodeAndClients.stream().filter(NodeAndClient::isMasterEligible)
                .filter(nac -> nodes.containsKey(nac.name) == false) // filter out old masters
                .count();
            final int currentMasters = getMasterNodesCount();
            if (autoManageMinMasterNodes && currentMasters > 1 && newMasters > 0) {
                // special case for 1 node master - we can't update the min master nodes before we add more nodes.
                updateMinMasterNodes(currentMasters + newMasters);
            }
            List<Future<?>> futures = nodeAndClients.stream().map(node -> executor.submit(node::startNode)).collect(Collectors.toList());
            try {
                for (Future<?> future : futures) {
                    future.get();
                }
            } catch (InterruptedException e) {
                throw new AssertionError("interrupted while starting nodes", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("failed to start nodes", e);
            }
            nodeAndClients.forEach(this::publishNode);

            if (autoManageMinMasterNodes && currentMasters == 1 && newMasters > 0) {
                // update once masters have joined
                validateClusterFormed();
                updateMinMasterNodes(currentMasters + newMasters);
            }
        }
    }

    private synchronized void stopNodesAndClient(NodeAndClient nodeAndClient) throws IOException {
        stopNodesAndClients(Collections.singleton(nodeAndClient));
    }

    private synchronized void stopNodesAndClients(Collection<NodeAndClient> nodeAndClients) throws IOException {
        if (autoManageMinMasterNodes && nodeAndClients.size() > 0) {
            int masters = (int)nodeAndClients.stream().filter(NodeAndClient::isMasterEligible).count();
            if (masters > 0) {
                updateMinMasterNodes(getMasterNodesCount() - masters);
            }
        }
        for (NodeAndClient nodeAndClient: nodeAndClients) {
            removeDisruptionSchemeFromNode(nodeAndClient);
            NodeAndClient previous = nodes.remove(nodeAndClient.name);
            assert previous == nodeAndClient;
            nodeAndClient.close();
        }
    }

    /**
     * Restarts a random node in the cluster
     */
    public void restartRandomNode() throws Exception {
        restartRandomNode(EMPTY_CALLBACK);
    }

    /**
     * Restarts a random node in the cluster and calls the callback during restart.
     */
    public void restartRandomNode(RestartCallback callback) throws Exception {
        restartRandomNode(nc -> true, callback);
    }

    /**
     * Restarts a random data node in the cluster
     */
    public void restartRandomDataNode() throws Exception {
        restartRandomDataNode(EMPTY_CALLBACK);
    }

    /**
     * Restarts a random data node in the cluster and calls the callback during restart.
     */
    public void restartRandomDataNode(RestartCallback callback) throws Exception {
        restartRandomNode(new DataNodePredicate(), callback);
    }

    /**
     * Restarts a random node in the cluster and calls the callback during restart.
     */
    private synchronized void restartRandomNode(Predicate<NodeAndClient> predicate, RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(predicate);
        if (nodeAndClient != null) {
            restartNode(nodeAndClient, callback);
        }
    }

    /**
     * Restarts a node and calls the callback during restart.
     */
    public synchronized void restartNode(String nodeName, RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            restartNode(nodeAndClient, callback);
        }
    }

    public static final RestartCallback EMPTY_CALLBACK = new RestartCallback() {
        @Override
        public Settings onNodeStopped(String node) {
            return null;
        }
    };

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public void fullRestart() throws Exception {
        fullRestart(EMPTY_CALLBACK);
    }

    /**
     * Restarts all nodes in a rolling restart fashion ie. only restarts on node a time.
     */
    public void rollingRestart() throws Exception {
        rollingRestart(EMPTY_CALLBACK);
    }

    /**
     * Restarts all nodes in a rolling restart fashion ie. only restarts on node a time.
     */
    public synchronized void rollingRestart(RestartCallback callback) throws Exception {
        int numNodesRestarted = 0;
        for (NodeAndClient nodeAndClient : nodes.values()) {
            callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
            restartNode(nodeAndClient, callback);
        }
    }

    private void restartNode(NodeAndClient nodeAndClient, RestartCallback callback) throws Exception {
        logger.info("Restarting node [{}] ", nodeAndClient.name);
        if (activeDisruptionScheme != null) {
            activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
        }
        final int masterNodesCount = getMasterNodesCount();
        // special case to allow stopping one node in a two node cluster and keep it functional
        final boolean updateMinMaster = nodeAndClient.isMasterEligible() && masterNodesCount == 2 && autoManageMinMasterNodes;
        if (updateMinMaster) {
            updateMinMasterNodes(masterNodesCount - 1);
        }
        nodeAndClient.restart(callback, true, autoManageMinMasterNodes ? getMinMasterNodes(masterNodesCount) : -1);
        if (activeDisruptionScheme != null) {
            activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
        }
        if (callback.validateClusterForming() || updateMinMaster) {
            // we have to validate cluster size if updateMinMaster == true, because we need the
            // second node to join in order to increment min_master_nodes back to 2.
            // we also have to do via the node that was just restarted as it may be that the master didn't yet process
            // the fact it left
            validateClusterFormed(nodeAndClient.name);
        }
        if (updateMinMaster) {
            updateMinMasterNodes(masterNodesCount);
        }
    }

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public synchronized void fullRestart(RestartCallback callback) throws Exception {
        int numNodesRestarted = 0;
        Map<Set<Role>, List<NodeAndClient>> nodesByRoles = new HashMap<>();
        Set[] rolesOrderedByOriginalStartupOrder =  new Set[nextNodeId.get()];
        for (NodeAndClient nodeAndClient : nodes.values()) {
            callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
            logger.info("Stopping node [{}] ", nodeAndClient.name);
            if (activeDisruptionScheme != null) {
                activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
            }
            nodeAndClient.closeNode();
            // delete data folders now, before we start other nodes that may claim it
            nodeAndClient.clearDataIfNeeded(callback);
            DiscoveryNode discoveryNode = getInstanceFromNode(ClusterService.class, nodeAndClient.node()).localNode();
            rolesOrderedByOriginalStartupOrder[nodeAndClient.nodeAndClientId] = discoveryNode.getRoles();
            nodesByRoles.computeIfAbsent(discoveryNode.getRoles(), k -> new ArrayList<>()).add(nodeAndClient);
        }

        assert nodesByRoles.values().stream().collect(Collectors.summingInt(List::size)) == nodes.size();

        // randomize start up order, but making sure that:
        // 1) A data folder that was assigned to a data node will stay so
        // 2) Data nodes will get the same node lock ordinal range, so custom index paths (where the ordinal is used)
        //    will still belong to data nodes
        for (List<NodeAndClient> sameRoleNodes : nodesByRoles.values()) {
            Collections.shuffle(sameRoleNodes, random);
        }
        List<NodeAndClient> startUpOrder = new ArrayList<>();
        for (Set roles : rolesOrderedByOriginalStartupOrder) {
            if (roles == null) {
                // if some nodes were stopped, we want have a role for that ordinal
                continue;
            }
            final List<NodeAndClient> nodesByRole = nodesByRoles.get(roles);
            startUpOrder.add(nodesByRole.remove(0));
        }
        assert nodesByRoles.values().stream().collect(Collectors.summingInt(List::size)) == 0;

        // do two rounds to minimize pinging (mock zen pings pings with no delay and can create a lot of logs)
        for (NodeAndClient nodeAndClient : startUpOrder) {
            logger.info("resetting node [{}] ", nodeAndClient.name);
            // we already cleared data folders, before starting nodes up
            nodeAndClient.recreateNodeOnRestart(callback, false, autoManageMinMasterNodes ? getMinMasterNodes(getMasterNodesCount()) : -1);
        }

        startAndPublishNodesAndClients(startUpOrder);

        if (callback.validateClusterForming()) {
            validateClusterFormed();
        }
    }


    /**
     * Returns the name of the current master node in the cluster.
     */
    public String getMasterName() {
        return getMasterName(null);
    }

    /**
     * Returns the name of the current master node in the cluster and executes the request via the node specified
     * in the viaNode parameter. If viaNode isn't specified a random node will be picked to the send the request to.
     */
    public String getMasterName(@Nullable String viaNode) {
        try {
            Client client = viaNode != null ? client(viaNode) : client();
            ClusterState state = client.admin().cluster().prepareState().execute().actionGet().getState();
            return state.nodes().getMasterNode().getName();
        } catch (Exception e) {
            logger.warn("Can't fetch cluster state", e);
            throw new RuntimeException("Can't get master node " + e.getMessage(), e);
        }
    }

    synchronized Set<String> allDataNodesButN(int numNodes) {
        return nRandomDataNodes(numDataNodes() - numNodes);
    }

    private synchronized Set<String> nRandomDataNodes(int numNodes) {
        assert size() >= numNodes;
        Map<String, NodeAndClient> dataNodes =
            nodes
                .entrySet()
                .stream()
                .filter(new EntryNodePredicate(new DataNodePredicate()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final HashSet<String> set = new HashSet<>();
        final Iterator<String> iterator = dataNodes.keySet().iterator();
        for (int i = 0; i < numNodes; i++) {
            assert iterator.hasNext();
            set.add(iterator.next());
        }
        return set;
    }

    /**
     * Returns a set of nodes that have at least one shard of the given index.
     */
    public synchronized Set<String> nodesInclude(String index) {
        if (clusterService().state().routingTable().hasIndex(index)) {
            List<ShardRouting> allShards = clusterService().state().routingTable().allShards(index);
            DiscoveryNodes discoveryNodes = clusterService().state().getNodes();
            Set<String> nodes = new HashSet<>();
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

    /**
     * Starts a node with default settings and returns it's name.
     */
    public synchronized String startNode() {
        return startNode(Settings.EMPTY);
    }

    /**
     * Starts a node with the given settings builder and returns it's name.
     */
    public synchronized String startNode(Settings.Builder settings) {
        return startNode(settings.build());
    }

    /**
     * Starts a node with the given settings and returns it's name.
     */
    public synchronized String startNode(Settings settings) {
        final int defaultMinMasterNodes = getMinMasterNodes(getMasterNodesCount() + (Node.NODE_MASTER_SETTING.get(settings) ? 1 : 0));
        NodeAndClient buildNode = buildNode(settings, defaultMinMasterNodes);
        startAndPublishNodesAndClients(Collections.singletonList(buildNode));
        return buildNode.name;
    }

    /**
     * Starts multiple nodes with default settings and returns their names
     */
    public synchronized List<String> startNodes(int numOfNodes) {
        return startNodes(numOfNodes, Settings.EMPTY);
    }

    /**
     * Starts multiple nodes with the given settings and returns their names
     */
    public synchronized List<String> startNodes(int numOfNodes, Settings settings) {
        return startNodes(Collections.nCopies(numOfNodes, settings).stream().toArray(Settings[]::new));
    }

    /**
     * Starts multiple nodes with the given settings and returns their names
     */
    public synchronized List<String> startNodes(Settings... settings) {
        final int defaultMinMasterNodes;
        if (autoManageMinMasterNodes) {
            int mastersDelta = (int) Stream.of(settings).filter(Node.NODE_MASTER_SETTING::get).count();
            defaultMinMasterNodes = getMinMasterNodes(getMasterNodesCount() + mastersDelta);
        } else {
            defaultMinMasterNodes = -1;
        }
        List<NodeAndClient> nodes = new ArrayList<>();
        for (Settings nodeSettings: settings) {
            nodes.add(buildNode(nodeSettings, defaultMinMasterNodes));
        }
        startAndPublishNodesAndClients(nodes);
        if (autoManageMinMasterNodes) {
            validateClusterFormed();
        }

        return nodes.stream().map(NodeAndClient::getName).collect(Collectors.toList());
    }

    public synchronized List<String> startMasterOnlyNodes(int numNodes) {
        return startMasterOnlyNodes(numNodes, Settings.EMPTY);
    }

    public synchronized List<String> startMasterOnlyNodes(int numNodes, Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), true).put(Node.NODE_DATA_SETTING.getKey(), false).build();
        return startNodes(numNodes, settings1);
    }

    public synchronized List<String> startDataOnlyNodes(int numNodes) {
        return startDataOnlyNodes(numNodes, Settings.EMPTY);
    }

    public synchronized List<String> startDataOnlyNodes(int numNodes, Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), true).build();
        return startNodes(numNodes, settings1);
    }

    /**
     * updates the min master nodes setting in the current running cluster.
     *
     * @param eligibleMasterNodeCount the number of master eligible nodes to use as basis for the min master node setting
     */
    private int updateMinMasterNodes(int eligibleMasterNodeCount) {
        assert autoManageMinMasterNodes;
        final int minMasterNodes = getMinMasterNodes(eligibleMasterNodeCount);
        if (getMasterNodesCount() > 0) {
            // there should be at least one master to update
            logger.debug("updating min_master_nodes to [{}]", minMasterNodes);
            try {
                assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                    Settings.builder().put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minMasterNodes)
                ));
            } catch (Exception e) {
                throw new ElasticsearchException("failed to update minimum master node to [{}] (current masters [{}])", e,
                    minMasterNodes, getMasterNodesCount());
            }
        }
        return minMasterNodes;
    }

    /** calculates a min master nodes value based on the given number of master nodes */
    private int getMinMasterNodes(int eligibleMasterNodes) {
        return eligibleMasterNodes / 2 + 1;
    }

    private int getMasterNodesCount() {
        return (int)nodes.values().stream().filter(n -> Node.NODE_MASTER_SETTING.get(n.node().settings())).count();
    }

    public synchronized String startMasterOnlyNode() {
        return startMasterOnlyNode(Settings.EMPTY);
    }

    public synchronized String startMasterOnlyNode(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), true).put(Node.NODE_DATA_SETTING.getKey(), false).build();
        return startNode(settings1);
    }

    public synchronized String startDataOnlyNode() {
        return startDataOnlyNode(Settings.EMPTY);
    }
    public synchronized String startDataOnlyNode(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), true).build();
        return startNode(settings1);
    }

    private synchronized void publishNode(NodeAndClient nodeAndClient) {
        assert !nodeAndClient.node().isClosed();
        nodes.put(nodeAndClient.name, nodeAndClient);
        applyDisruptionSchemeToNode(nodeAndClient);
    }

    public void closeNonSharedNodes(boolean wipeData) throws IOException {
        reset(wipeData);
    }

    @Override
    public int numDataNodes() {
        return dataNodeAndClients().size();
    }

    @Override
    public int numDataAndMasterNodes() {
        return dataAndMasterNodes().size();
    }

    public synchronized int numMasterNodes() {
      return filterNodes(nodes, NodeAndClient::isMasterEligible).size();
    }


    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        assert activeDisruptionScheme == null :
            "there is already and active disruption [" + activeDisruptionScheme + "]. call clearDisruptionScheme first";
        scheme.applyToCluster(this);
        activeDisruptionScheme = scheme;
    }

    public void clearDisruptionScheme() {
        clearDisruptionScheme(true);
    }

    public void clearDisruptionScheme(boolean ensureHealthyCluster) {
        if (activeDisruptionScheme != null) {
            TimeValue expectedHealingTime = activeDisruptionScheme.expectedTimeToHeal();
            logger.info("Clearing active scheme {}, expected healing time {}", activeDisruptionScheme, expectedHealingTime);
            if (ensureHealthyCluster) {
                activeDisruptionScheme.removeAndEnsureHealthy(this);
            } else {
                activeDisruptionScheme.removeFromCluster(this);
            }
        }
        activeDisruptionScheme = null;
    }

    private void applyDisruptionSchemeToNode(NodeAndClient nodeAndClient) {
        if (activeDisruptionScheme != null) {
            assert nodes.containsKey(nodeAndClient.name);
            activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
        }
    }

    private void removeDisruptionSchemeFromNode(NodeAndClient nodeAndClient) {
        if (activeDisruptionScheme != null) {
            assert nodes.containsKey(nodeAndClient.name);
            activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
        }
    }

    private synchronized Collection<NodeAndClient> dataNodeAndClients() {
        return filterNodes(nodes, new DataNodePredicate());
    }

    private synchronized Collection<NodeAndClient> dataAndMasterNodes() {
        return filterNodes(nodes, new DataOrMasterNodePredicate());
    }

    private synchronized Collection<NodeAndClient> filterNodes(Map<String, InternalTestCluster.NodeAndClient> map, Predicate<NodeAndClient> predicate) {
        return map
            .values()
            .stream()
            .filter(predicate)
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private static final class DataNodePredicate implements Predicate<NodeAndClient> {
        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return DiscoveryNode.isDataNode(nodeAndClient.node.settings());
        }
    }

    private static final class DataOrMasterNodePredicate implements Predicate<NodeAndClient> {
        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return DiscoveryNode.isDataNode(nodeAndClient.node.settings()) ||
                DiscoveryNode.isMasterNode(nodeAndClient.node.settings());
        }
    }

    private static final class MasterNodePredicate implements Predicate<NodeAndClient> {
        private final String masterNodeName;

        MasterNodePredicate(String masterNodeName) {
            this.masterNodeName = masterNodeName;
        }

        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return masterNodeName.equals(nodeAndClient.name);
        }
    }

    private static final class NoDataNoMasterNodePredicate implements Predicate<NodeAndClient> {
        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return DiscoveryNode.isMasterNode(nodeAndClient.node.settings()) == false &&
                DiscoveryNode.isDataNode(nodeAndClient.node.settings()) == false;
        }
    }

    private static final class EntryNodePredicate implements Predicate<Map.Entry<String, NodeAndClient>> {
        private final Predicate<NodeAndClient> delegateNodePredicate;

        EntryNodePredicate(Predicate<NodeAndClient> delegateNodePredicate) {
            this.delegateNodePredicate = delegateNodePredicate;
        }

        @Override
        public boolean test(Map.Entry<String, NodeAndClient> entry) {
            return delegateNodePredicate.test(entry.getValue());
        }
    }

    synchronized String routingKeyForShard(Index index, int shard, Random random) {
        assertThat(shard, greaterThanOrEqualTo(0));
        assertThat(shard, greaterThanOrEqualTo(0));
        for (NodeAndClient n : nodes.values()) {
            Node node = n.node;
            IndicesService indicesService = getInstanceFromNode(IndicesService.class, node);
            ClusterService clusterService = getInstanceFromNode(ClusterService.class, node);
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                assertThat(indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1), greaterThan(shard));
                OperationRouting operationRouting = clusterService.operationRouting();
                while (true) {
                    String routing = RandomStrings.randomAsciiOfLength(random, 10);
                    final int targetShard = operationRouting.indexShards(clusterService.state(), index.getName(), null, routing).shardId().getId();
                    if (shard == targetShard) {
                        return routing;
                    }
                }
            }
        }
        fail("Could not find a node that holds " + index);
        return null;
    }

    public synchronized Iterable<Client> getClients() {
        ensureOpen();
        return () -> {
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
        };
    }

    @Override
    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return getInstance(NamedWriteableRegistry.class);
    }

    /**
     * Returns a predicate that only accepts settings of nodes with one of the given names.
     */
    public static Predicate<Settings> nameFilter(String... nodeName) {
        return new NodeNamePredicate(new HashSet<>(Arrays.asList(nodeName)));
    }

    private static final class NodeNamePredicate implements Predicate<Settings> {
        private final HashSet<String> nodeNames;

        NodeNamePredicate(HashSet<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        @Override
        public boolean test(Settings settings) {
            return nodeNames.contains(settings.get("node.name"));

        }
    }


    /**
     * An abstract class that is called during {@link #rollingRestart(InternalTestCluster.RestartCallback)}
     * and / or {@link #fullRestart(InternalTestCluster.RestartCallback)} to execute actions at certain
     * stages of the restart.
     */
    public static class RestartCallback {

        /**
         * Executed once the give node name has been stopped.
         */
        public Settings onNodeStopped(String nodeName) throws Exception {
            return Settings.EMPTY;
        }

        /**
         * Executed for each node before the <tt>n+1</tt> node is restarted. The given client is
         * an active client to the node that will be restarted next.
         */
        public void doAfterNodes(int n, Client client) throws Exception {
        }

        /**
         * If this returns <code>true</code> all data for the node with the given node name will be cleared including
         * gateways and all index data. Returns <code>false</code> by default.
         */
        public boolean clearData(String nodeName) {
            return false;
        }

        /** returns true if the restart should also validate the cluster has reformed */
        public boolean validateClusterForming() { return true; }
    }

    public Settings getDefaultSettings() {
        return defaultSettings;
    }

    @Override
    public void ensureEstimatedStats() {
        if (size() > 0) {
            // Checks that the breakers have been reset without incurring a
            // network request, because a network request can increment one
            // of the breakers
            for (NodeAndClient nodeAndClient : nodes.values()) {
                final IndicesFieldDataCache fdCache = getInstanceFromNode(IndicesService.class, nodeAndClient.node).getIndicesFieldDataCache();
                // Clean up the cache, ensuring that entries' listeners have been called
                fdCache.getCache().refresh();

                final String name = nodeAndClient.name;
                final CircuitBreakerService breakerService = getInstanceFromNode(CircuitBreakerService.class, nodeAndClient.node);
                CircuitBreaker fdBreaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                assertThat("Fielddata breaker not reset to 0 on node: " + name, fdBreaker.getUsed(), equalTo(0L));
                // Anything that uses transport or HTTP can increase the
                // request breaker (because they use bigarrays), because of
                // that the breaker can sometimes be incremented from ping
                // requests from other clusters because Jenkins is running
                // multiple ES testing jobs in parallel on the same machine.
                // To combat this we check whether the breaker has reached 0
                // in an assertBusy loop, so it will try for 10 seconds and
                // fail if it never reached 0
                try {
                    assertBusy(() -> {
                        CircuitBreaker reqBreaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
                        assertThat("Request breaker not reset to 0 on node: " + name, reqBreaker.getUsed(), equalTo(0L));
                    });
                } catch (Exception e) {
                    fail("Exception during check for request breaker reset to 0: " + e);
                }

                NodeService nodeService = getInstanceFromNode(NodeService.class, nodeAndClient.node);
                CommonStatsFlags flags = new CommonStatsFlags(Flag.FieldData, Flag.QueryCache, Flag.Segments);
                NodeStats stats = nodeService.stats(flags,
                        false, false, false, false, false, false, false, false, false, false, false, false);
                assertThat("Fielddata size must be 0 on node: " + stats.getNode(), stats.getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0L));
                assertThat("Query cache size must be 0 on node: " + stats.getNode(), stats.getIndices().getQueryCache().getMemorySizeInBytes(), equalTo(0L));
                assertThat("FixedBitSet cache size must be 0 on node: " + stats.getNode(), stats.getIndices().getSegments().getBitsetMemoryInBytes(), equalTo(0L));
            }
        }
    }

    @Override
    public void assertAfterTest() throws IOException {
        super.assertAfterTest();
        assertRequestsFinished();
        for (NodeAndClient nodeAndClient : nodes.values()) {
            NodeEnvironment env = nodeAndClient.node().getNodeEnvironment();
            Set<ShardId> shardIds = env.lockedShards();
            for (ShardId id : shardIds) {
                try {
                    env.shardLock(id, TimeUnit.SECONDS.toMillis(5)).close();
                } catch (ShardLockObtainFailedException ex) {
                    fail("Shard " + id + " is still locked after 5 sec waiting");
                }
            }
        }
    }

    private void assertRequestsFinished() {
        if (size() > 0) {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                CircuitBreaker inFlightRequestsBreaker = getInstance(CircuitBreakerService.class, nodeAndClient.name)
                    .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
                try {
                    // see #ensureEstimatedStats()
                    assertBusy(() -> {
                        // ensure that our size accounting on transport level is reset properly
                        long bytesUsed = inFlightRequestsBreaker.getUsed();
                        assertThat("All incoming requests on node [" + nodeAndClient.name + "] should have finished. Expected 0 but got " +
                            bytesUsed, bytesUsed, equalTo(0L));
                    });
                } catch (Exception e) {
                    logger.error("Could not assert finished requests within timeout", e);
                    fail("Could not assert finished requests within timeout on node [" + nodeAndClient.name + "]");
                }
            }
        }
    }
}

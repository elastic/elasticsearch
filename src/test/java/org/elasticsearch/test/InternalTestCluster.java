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
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecyclerModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArraysModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.filter.FilterCacheModule;
import org.elasticsearch.index.cache.filter.FilterCacheModule.FilterCacheSettings;
import org.elasticsearch.index.cache.filter.none.NoneFilterCache;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.index.shard.IndexShardModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchServiceModule;
import org.elasticsearch.test.cache.recycler.MockBigArraysModule;
import org.elasticsearch.test.cache.recycler.MockPageCacheRecyclerModule;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.engine.MockEngineFactory;
import org.elasticsearch.test.search.MockSearchServiceModule;
import org.elasticsearch.test.store.MockFSIndexStoreModule;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.fail;
import static org.apache.lucene.util.LuceneTestCase.TEST_NIGHTLY;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.apache.lucene.util.LuceneTestCase.rarely;
import static org.apache.lucene.util.LuceneTestCase.usually;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.test.ElasticsearchTestCase.assertBusy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * InternalTestCluster manages a set of JVM private nodes and allows convenient access to them.
 * The cluster supports randomized configuration such that nodes started in the cluster will
 * automatically load asserting services tracking resources like file handles or open searchers.
 * <p>
 * The Cluster is bound to a test lifecycle where tests must call {@link #beforeTest(java.util.Random, double)} and
 * {@link #afterTest()} to initialize and reset the cluster in order to be more reproducible. The term "more" relates
 * to the async nature of Elasticsearch in combination with randomized testing. Once Threads and asynchronous calls
 * are involved reproducibility is very limited. This class should only be used through {@link ElasticsearchIntegrationTest}.
 * </p>
 */
public final class InternalTestCluster extends TestCluster {

    private final ESLogger logger = Loggers.getLogger(getClass());

    static SettingsSource DEFAULT_SETTINGS_SOURCE = SettingsSource.EMPTY;

    /**
     * A boolean value to enable or disable mock modules. This is useful to test the
     * system without asserting modules that to make sure they don't hide any bugs in
     * production.
     *
     * @see ElasticsearchIntegrationTest
     */
    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";

    /**
     * A node level setting that holds a per node random seed that is consistent across node restarts
     */
    public static final String SETTING_CLUSTER_NODE_SEED = "test.cluster.node.seed";

    private static final int JVM_ORDINAL = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));
    public static final int BASE_PORT = 9300 + 100 * (JVM_ORDINAL + 1);

    private static final boolean ENABLE_MOCK_MODULES = RandomizedTest.systemPropertyAsBoolean(TESTS_ENABLE_MOCK_MODULES, true);

    static final int DEFAULT_MIN_NUM_DATA_NODES = 2;
    static final int DEFAULT_MAX_NUM_DATA_NODES = 6;

    static final int DEFAULT_NUM_CLIENT_NODES = -1;
    static final int DEFAULT_MIN_NUM_CLIENT_NODES = 0;
    static final int DEFAULT_MAX_NUM_CLIENT_NODES = 1;

    static final boolean DEFAULT_ENABLE_HTTP_PIPELINING = true;

    public static final String NODE_MODE = nodeMode();

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

    private final int numSharedDataNodes;

    private final int numSharedClientNodes;

    private final SettingsSource settingsSource;

    private final ExecutorService executor;

    /**
     * All nodes started by the cluster will have their name set to nodePrefix followed by a positive number
     */
    private final String nodePrefix;
    private final Path baseDir;

    private ServiceDisruptionScheme activeDisruptionScheme;

    public InternalTestCluster(long clusterSeed, Path baseDir, int minNumDataNodes, int maxNumDataNodes, String clusterName, int numClientNodes,
                               boolean enableHttpPipelining, String nodePrefix) {
        this(clusterSeed, baseDir, minNumDataNodes, maxNumDataNodes, clusterName, DEFAULT_SETTINGS_SOURCE, numClientNodes, enableHttpPipelining, nodePrefix);
    }

    public InternalTestCluster(long clusterSeed, Path baseDir,
                               int minNumDataNodes, int maxNumDataNodes, String clusterName, SettingsSource settingsSource, int numClientNodes,
                                boolean enableHttpPipelining, String nodePrefix) {
        super(clusterSeed);
        this.baseDir = baseDir;
        this.clusterName = clusterName;

        if (minNumDataNodes < 0 || maxNumDataNodes < 0) {
            throw new IllegalArgumentException("minimum and maximum number of data nodes must be >= 0");
        }

        if (maxNumDataNodes < minNumDataNodes) {
            throw new IllegalArgumentException("maximum number of data nodes must be >= minimum number of  data nodes");
        }

        Random random = new Random(clusterSeed);

        this.numSharedDataNodes = RandomInts.randomIntBetween(random, minNumDataNodes, maxNumDataNodes);
        assert this.numSharedDataNodes >= 0;

        //for now all shared data nodes are also master eligible
        if (numSharedDataNodes == 0) {
            this.numSharedClientNodes = 0;
        } else {
            if (numClientNodes < 0) {
                this.numSharedClientNodes = RandomInts.randomIntBetween(random, DEFAULT_MIN_NUM_CLIENT_NODES, DEFAULT_MAX_NUM_CLIENT_NODES);
            } else {
                this.numSharedClientNodes = numClientNodes;
            }
        }
        assert this.numSharedClientNodes >= 0;

        this.nodePrefix = nodePrefix;

        assert nodePrefix != null;

        /*
         *  TODO
         *  - we might want start some master only nodes?
         *  - we could add a flag that returns a client to the master all the time?
         *  - we could add a flag that never returns a client to the master
         *  - along those lines use a dedicated node that is master eligible and let all other nodes be only data nodes
         */
        sharedNodesSeeds = new long[numSharedDataNodes + numSharedClientNodes];
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            sharedNodesSeeds[i] = random.nextLong();
        }

        logger.info("Setup InternalTestCluster [{}] with seed [{}] using [{}] data nodes and [{}] client nodes", clusterName, SeedUtils.formatSeed(clusterSeed), numSharedDataNodes, numSharedClientNodes);
        this.settingsSource = settingsSource;
        Builder builder = ImmutableSettings.settingsBuilder();
        if (random.nextInt(5) == 0) { // sometimes set this
            // randomize (multi/single) data path, special case for 0, don't set it at all...
            final int numOfDataPaths = random.nextInt(5);
            if (numOfDataPaths > 0) {
                StringBuilder dataPath = new StringBuilder();
                for (int i = 0; i < numOfDataPaths; i++) {
                    dataPath.append(baseDir.resolve("d" + i).toAbsolutePath()).append(',');
                }
                builder.put("path.data", dataPath.toString());
            }
        }
        builder.put("path.home", baseDir);
        builder.put("transport.tcp.port", BASE_PORT + "-" + (BASE_PORT+100));
        builder.put("http.port", BASE_PORT+101 + "-" + (BASE_PORT+200));
        builder.put("config.ignore_system_properties", true);
        builder.put("node.mode", NODE_MODE);
        builder.put("http.pipelining", enableHttpPipelining);
        builder.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false);
        builder.put(NodeEnvironment.SETTING_CUSTOM_DATA_PATH_ENABLED, true);
        if (Strings.hasLength(System.getProperty("es.logger.level"))) {
            builder.put("logger.level", System.getProperty("es.logger.level"));
        }
        if (Strings.hasLength(System.getProperty("es.logger.prefix"))) {
            builder.put("logger.prefix", System.getProperty("es.logger.prefix"));
        }
        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "1b");
        builder.put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "1b");
        if (TEST_NIGHTLY) {
            builder.put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS, RandomInts.randomIntBetween(random, 10, 15));
            builder.put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, RandomInts.randomIntBetween(random, 10, 15));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES, RandomInts.randomIntBetween(random, 5, 10));
        } else if (random.nextInt(100) <= 90) {
            builder.put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_STREAMS, RandomInts.randomIntBetween(random, 3, 6));
            builder.put(RecoverySettings.INDICES_RECOVERY_CONCURRENT_SMALL_FILE_STREAMS, RandomInts.randomIntBetween(random, 3, 6));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES, RandomInts.randomIntBetween(random, 2, 5));
        }
        // always reduce this - it can make tests really slow
        builder.put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC, TimeValue.timeValueMillis(RandomInts.randomIntBetween(random, 20, 50)));
        defaultSettings = builder.build();
        executor = EsExecutors.newCached(0, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory("test_" + clusterName));
    }

    public static String nodeMode() {
        Builder builder = ImmutableSettings.builder();
        if (Strings.isEmpty(System.getProperty("es.node.mode")) && Strings.isEmpty(System.getProperty("es.node.local"))) {
            return "local"; // default if nothing is specified
        }
        if (Strings.hasLength(System.getProperty("es.node.mode"))) {
            builder.put("node.mode", System.getProperty("es.node.mode"));
        }
        if (Strings.hasLength(System.getProperty("es.node.local"))) {
            builder.put("node.local", System.getProperty("es.node.local"));
        }
        if (DiscoveryNode.localNode(builder.build())) {
            return "local";
        } else {
            return "network";
        }
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    public String[] getNodeNames() {
        return nodes.keySet().toArray(Strings.EMPTY_ARRAY);
    }

    private static boolean isLocalTransportConfigured() {
        if ("local".equals(System.getProperty("es.node.mode", "network"))) {
            return true;
        }
        return Boolean.parseBoolean(System.getProperty("es.node.local", "false"));
    }

    private Settings getSettings(int nodeOrdinal, long nodeSeed, Settings others) {
        Builder builder = ImmutableSettings.settingsBuilder().put(defaultSettings)
                .put(getRandomNodeSettings(nodeSeed));
        Settings settings = settingsSource.node(nodeOrdinal);
        if (settings != null) {
            if (settings.get(ClusterName.SETTING) != null) {
                throw new ElasticsearchIllegalStateException("Tests must not set a '" + ClusterName.SETTING + "' as a node setting set '" + ClusterName.SETTING + "': [" + settings.get(ClusterName.SETTING) + "]");
            }
            builder.put(settings);
        }
        if (others != null) {
            builder.put(others);
        }
        builder.put(ClusterName.SETTING, clusterName);
        return builder.build();
    }

    private static Settings getRandomNodeSettings(long seed) {
        Random random = new Random(seed);
        Builder builder = ImmutableSettings.settingsBuilder()
                // decrease the routing schedule so new nodes will be added quickly - some random value between 30 and 80 ms
                .put("cluster.routing.schedule", (30 + random.nextInt(50)) + "ms")
                .put(SETTING_CLUSTER_NODE_SEED, seed);
        if (ENABLE_MOCK_MODULES && usually(random)) {
            builder.put(IndexStoreModule.STORE_TYPE, MockFSIndexStoreModule.class.getName()); // no RAM dir for now!
            builder.put(IndexShardModule.ENGINE_FACTORY, MockEngineFactory.class);
            builder.put(PageCacheRecyclerModule.CACHE_IMPL, MockPageCacheRecyclerModule.class.getName());
            builder.put(BigArraysModule.IMPL, MockBigArraysModule.class.getName());
            builder.put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName());
            builder.put(SearchServiceModule.IMPL, MockSearchServiceModule.class.getName());
        }
        if (isLocalTransportConfigured()) {
            builder.put(TransportModule.TRANSPORT_TYPE_KEY, AssertingLocalTransport.class.getName());
        } else {
            builder.put(Transport.TransportSettings.TRANSPORT_TCP_COMPRESS, rarely(random));
        }
        if (random.nextBoolean()) {
            builder.put("cache.recycler.page.type", RandomPicks.randomFrom(random, PageCacheRecycler.Type.values()));
        }
        if (random.nextInt(10) == 0) { // 10% of the nodes have a very frequent check interval
            builder.put(SearchService.KEEPALIVE_INTERVAL_KEY, TimeValue.timeValueMillis(10 + random.nextInt(2000)));
        } else if (random.nextInt(10) != 0) { // 90% of the time - 10% of the time we don't set anything
            builder.put(SearchService.KEEPALIVE_INTERVAL_KEY, TimeValue.timeValueSeconds(10 + random.nextInt(5 * 60)));
        }
        if (random.nextBoolean()) { // sometimes set a
            builder.put(SearchService.DEFAULT_KEEPALIVE_KEY, TimeValue.timeValueSeconds(100 + random.nextInt(5 * 60)));
        }
        if (random.nextBoolean()) {
            // change threadpool types to make sure we don't have components that rely on the type of thread pools
            for (String name : Arrays.asList(ThreadPool.Names.BULK, ThreadPool.Names.FLUSH, ThreadPool.Names.GET,
                    ThreadPool.Names.INDEX, ThreadPool.Names.MANAGEMENT, ThreadPool.Names.OPTIMIZE,
                    ThreadPool.Names.PERCOLATE, ThreadPool.Names.REFRESH, ThreadPool.Names.SEARCH, ThreadPool.Names.SNAPSHOT,
                    ThreadPool.Names.SUGGEST, ThreadPool.Names.WARMER)) {
                if (random.nextBoolean()) {
                    final String type = RandomPicks.randomFrom(random, Arrays.asList("fixed", "cached", "scaling"));
                    builder.put(ThreadPool.THREADPOOL_GROUP + name + ".type", type);
                }
            }
        }

        if (random.nextInt(10) == 0) {
            // node gets an extra cpu this time
            builder.put(EsExecutors.PROCESSORS, 1 + EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY));
        }

        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                builder.put("indices.fielddata.cache.size", 1 + random.nextInt(1000), ByteSizeUnit.MB);
            }
            if (random.nextBoolean()) {
                builder.put("indices.fielddata.cache.expire", TimeValue.timeValueMillis(1 + random.nextInt(10000)));
            }
        }

        // randomize netty settings
        if (random.nextBoolean()) {
            builder.put(NettyTransport.WORKER_COUNT, random.nextInt(3) + 1);
            builder.put(NettyTransport.CONNECTIONS_PER_NODE_RECOVERY, random.nextInt(2) + 1);
            builder.put(NettyTransport.CONNECTIONS_PER_NODE_BULK, random.nextInt(3) + 1);
            builder.put(NettyTransport.CONNECTIONS_PER_NODE_REG, random.nextInt(6) + 1);
        }

        if (random.nextBoolean()) {
            builder.put(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT, new TimeValue(RandomInts.randomIntBetween(random, 10, 30), TimeUnit.SECONDS));
        }

        if (random.nextInt(10) == 0) {
            builder.put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING, "noop");
            builder.put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING, "noop");
        }

        if (random.nextBoolean()) {
            builder.put(FilterCacheModule.FilterCacheSettings.FILTER_CACHE_TYPE, random.nextBoolean() ? WeightedFilterCache.class : NoneFilterCache.class);
        }

        if (random.nextBoolean()) {
            builder.put(FilterCacheSettings.FILTER_CACHE_EVERYTHING, random.nextBoolean());
        }

        return builder.build();
    }

    public static String clusterName(String prefix, long clusterSeed) {
        StringBuilder builder = new StringBuilder(prefix);
        final int childVM = RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0);
        builder.append('-').append(NetworkUtils.getLocalHostName("__default_host__"));
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

    /**
     * Ensures that at least <code>n</code> data nodes are present in the cluster.
     * if more nodes than <code>n</code> are present this method will not
     * stop any of the running nodes.
     */
    public void ensureAtLeastNumDataNodes(int n) {
        List<ListenableFuture<String>> futures = Lists.newArrayList();
        synchronized (this) {
            int size = numDataNodes();
            for (int i = size; i < n; i++) {
                logger.info("increasing cluster size from {} to {}", size, n);
                futures.add(startNodeAsync());
            }
        }
        try {
            Futures.allAsList(futures).get();
        } catch (Exception e) {
            throw new ElasticsearchException("failed to start nodes", e);
        }
        if (!futures.isEmpty()) {
            synchronized (this) {
                assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodes.size())).get());
            }
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
        final Iterator<NodeAndClient> values = n == 0 ? nodes.values().iterator() : Iterators.filter(nodes.values().iterator(),
                Predicates.and(new DataNodePredicate(), Predicates.not(new MasterNodePredicate(getMasterName()))));

        final Iterator<NodeAndClient> limit = Iterators.limit(values, size - n);
        logger.info("changing cluster size from {} to {}, {} data nodes", size(), n + numSharedClientNodes, n);
        Set<NodeAndClient> nodesToRemove = new HashSet<>();
        while (limit.hasNext()) {
            NodeAndClient next = limit.next();
            nodesToRemove.add(next);
            removeDisruptionSchemeFromNode(next);
            next.close();
        }
        for (NodeAndClient toRemove : nodesToRemove) {
            nodes.remove(toRemove.name);
        }
        if (!nodesToRemove.isEmpty() && size() > 0) {
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodes.size())).get());
        }
    }

    private NodeAndClient buildNode(Settings settings, Version version) {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), settings, version);
    }

    private NodeAndClient buildNode() {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), null, Version.CURRENT);
    }

    private NodeAndClient buildNode(int nodeId, long seed, Settings settings, Version version) {
        assert Thread.holdsLock(this);
        ensureOpen();
        settings = getSettings(nodeId, seed, settings);
        String name = buildNodeName(nodeId);
        assert !nodes.containsKey(name);
        Settings finalSettings = settingsBuilder()
                .put("path.home", baseDir) // allow overriding path.home
                .put(settings)
                .put("name", name)
                .put("discovery.id.seed", seed)
                .put("tests.mock.version", version)
                .build();
        Node node = nodeBuilder().settings(finalSettings).build();
        return new NodeAndClient(name, node);
    }

    private String buildNodeName(int id) {
        return nodePrefix + id;
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
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(Predicates.not(new MasterNodePredicate(getMasterName())));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient(); // ensure node client non-master is requested
        }
        Assert.fail("No non-master client found");
        return null; // can't happen
    }

    /**
     * Returns a client to a node started with "node.client: true"
     */
    public synchronized Client clientNodeClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new ClientNodePredicate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        int nodeId = nextNodeId.getAndIncrement();
        Settings settings = getSettings(nodeId, random.nextLong(), ImmutableSettings.EMPTY);
        startNodeClient(settings);
        return getRandomNodeAndClient(new ClientNodePredicate()).client(random);
    }

    public synchronized Client startNodeClient(Settings settings) {
        ensureOpen(); // currently unused
        Builder builder = settingsBuilder().put(settings).put("node.client", true);
        if (size() == 0) {
            // if we are the first node - don't wait for a state
            builder.put("discovery.initial_state_timeout", 0);
        }
        String name = startNode(builder);
        return nodes.get(name).nodeClient();
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

    @Override
    public void close() {
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
        private Node node;
        private Client nodeClient;
        private Client transportClient;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final String name;

        NodeAndClient(String name, Node node) {
            this.node = node;
            this.name = name;
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
            double nextDouble = random.nextDouble();
            if (nextDouble < transportClientRatio) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Using transport client for node [{}] sniff: [{}]", node.settings().get("name"), false);
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
            if (nodeClient != null) {
                return nodeClient;
            }
            return nodeClient = node.client();
        }

        private Client getOrBuildTransportClient() {
            if (transportClient != null) {
                return transportClient;
            }
            /* no sniff client for now - doesn't work will all tests since it might throw NoNodeAvailableException if nodes are shut down.
             * we first need support of transportClientRatio as annotations or so
             */
            return transportClient = TransportClientFactory.noSniff(settingsSource.transportClient()).client(node, clusterName);
        }

        void resetClient() throws IOException {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            Releasables.close(nodeClient, transportClient);
            nodeClient = null;
            transportClient = null;
        }

        void closeNode() {
            registerDataPath();
            node.close();
        }

        void restart(RestartCallback callback) throws Exception {
            assert callback != null;
            resetClient();
            if (!node.isClosed()) {
                closeNode();
            }
            Settings newSettings = callback.onNodeStopped(name);
            if (newSettings == null) {
                newSettings = ImmutableSettings.EMPTY;
            }
            if (callback.clearData(name)) {
                NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, node);
                if (nodeEnv.hasNodeFile()) {
                    IOUtils.rm(nodeEnv.nodeDataPaths());
                }
            }
            node = nodeBuilder().settings(node.settings()).settings(newSettings).node();
        }

        void registerDataPath() {
            NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, node);
            if (nodeEnv.hasNodeFile()) {
                dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataPaths()));
            }
        }


        @Override
        public void close() throws IOException {
            resetClient();
            closed.set(true);
            closeNode();
        }
    }

    public static final String TRANSPORT_CLIENT_PREFIX = "transport_client_";
    static class TransportClientFactory {
        private static TransportClientFactory NO_SNIFF_CLIENT_FACTORY = new TransportClientFactory(false, ImmutableSettings.EMPTY);
        private static TransportClientFactory SNIFF_CLIENT_FACTORY = new TransportClientFactory(true, ImmutableSettings.EMPTY);

        private final boolean sniff;
        private final Settings settings;

        public static TransportClientFactory noSniff(Settings settings) {
            if (settings == null || settings.names().isEmpty()) {
                return NO_SNIFF_CLIENT_FACTORY;
            }
            return new TransportClientFactory(false, settings);
        }

        public static TransportClientFactory sniff(Settings settings) {
            if (settings == null || settings.names().isEmpty()) {
                return SNIFF_CLIENT_FACTORY;
            }
            return new TransportClientFactory(true, settings);
        }

        TransportClientFactory(boolean sniff, Settings settings) {
            this.sniff = sniff;
            this.settings = settings != null ? settings : ImmutableSettings.EMPTY;
        }

        public Client client(Node node, String clusterName) {
            TransportAddress addr = node.injector().getInstance(TransportService.class).boundAddress().publishAddress();
            Settings nodeSettings = node.settings();
            Builder builder = settingsBuilder()
                    .put("client.transport.nodes_sampler_interval", "1s")
                    .put("name", TRANSPORT_CLIENT_PREFIX + node.settings().get("name"))
                    .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                    .put(ClusterName.SETTING, clusterName).put("client.transport.sniff", sniff)
                    .put("node.mode", nodeSettings.get("node.mode", NODE_MODE))
                    .put("node.local", nodeSettings.get("node.local", ""))
                    .put("logger.prefix", nodeSettings.get("logger.prefix", ""))
                    .put("logger.level", nodeSettings.get("logger.level", "INFO"))
                    .put("config.ignore_system_properties", true)
                    .put(settings);

            TransportClient client = new TransportClient(builder.build());
            client.addTransportAddress(addr);
            return client;
        }
    }

    @Override
    public synchronized void beforeTest(Random random, double transportClientRatio) throws IOException {
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
        if (wipeData) {
            wipeDataDirectories();
        }
        if (nextNodeId.get() == sharedNodesSeeds.length && nodes.size() == sharedNodesSeeds.length) {
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);


        Set<NodeAndClient> sharedNodes = new HashSet<>();
        assert sharedNodesSeeds.length == numSharedDataNodes + numSharedClientNodes;
        boolean changed = false;
        for (int i = 0; i < numSharedDataNodes; i++) {
            String buildNodeName = buildNodeName(i);
            NodeAndClient nodeAndClient = nodes.get(buildNodeName);
            if (nodeAndClient == null) {
                changed = true;
                nodeAndClient = buildNode(i, sharedNodesSeeds[i], null, Version.CURRENT);
                nodeAndClient.node.start();
                logger.info("Start Shared Node [{}] not shared", nodeAndClient.name);
            }
            sharedNodes.add(nodeAndClient);
        }
        for (int i = numSharedDataNodes; i < numSharedDataNodes + numSharedClientNodes; i++) {
            String buildNodeName = buildNodeName(i);
            NodeAndClient nodeAndClient = nodes.get(buildNodeName);
            if (nodeAndClient == null) {
                changed = true;
                Builder clientSettingsBuilder = ImmutableSettings.builder().put("node.client", true);
                nodeAndClient = buildNode(i, sharedNodesSeeds[i], clientSettingsBuilder.build(), Version.CURRENT);
                nodeAndClient.node.start();
                logger.info("Start Shared Node [{}] not shared", nodeAndClient.name);
            }
            sharedNodes.add(nodeAndClient);
        }
        if (!changed && sharedNodes.size() == nodes.size()) {
            logger.debug("Cluster is consistent - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
            if (size() > 0) {
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
        assert size() == sharedNodesSeeds.length;
        if (size() > 0) {
            client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(sharedNodesSeeds.length)).get();
        }
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), sharedNodesSeeds.length);
    }

    @Override
    public synchronized void afterTest() throws IOException {
        wipeDataDirectories();
        randomlyResetClients(); /* reset all clients - each test gets its own client based on the Random instance created above. */
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

    private void wipeDataDirectories() {
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

    private synchronized <T> Iterable<T> getInstances(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        Iterable<NodeAndClient> filteredNodes = Iterables.filter(nodes.values(), predicate);
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
        final Predicate<InternalTestCluster.NodeAndClient> predicate;
        if (node != null) {
            predicate = new Predicate<InternalTestCluster.NodeAndClient>() {
                @Override
                public boolean apply(NodeAndClient nodeAndClient) {
                    return node.equals(nodeAndClient.name);
                }
            };
        } else {
            predicate = Predicates.alwaysTrue();
        }
        return getInstance(clazz, predicate);
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
        return getInstance(clazz, Predicates.<NodeAndClient>alwaysTrue());
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
        List<InetSocketAddress> addresses = Lists.newArrayList();
        for (HttpServerTransport httpServerTransport : getInstances(HttpServerTransport.class)) {
            addresses.add(((InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress()).address());
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
            removeDisruptionSchemeFromNode(nodeAndClient);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
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
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new Predicate<InternalTestCluster.NodeAndClient>() {
            @Override
            public boolean apply(NodeAndClient nodeAndClient) {
                return filter.apply(nodeAndClient.node.settings());
            }
        });
        if (nodeAndClient != null) {
            logger.info("Closing filtered random node [{}] ", nodeAndClient.name);
            removeDisruptionSchemeFromNode(nodeAndClient);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
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
        removeDisruptionSchemeFromNode(nodes.get(masterNodeName));
        NodeAndClient remove = nodes.remove(masterNodeName);
        remove.close();
    }

    /**
     * Stops the any of the current nodes but not the master node.
     */
    public void stopRandomNonMasterNode() throws IOException {
        NodeAndClient nodeAndClient = getRandomNodeAndClient(Predicates.not(new MasterNodePredicate(getMasterName())));
        if (nodeAndClient != null) {
            logger.info("Closing random non master node [{}] current master [{}] ", nodeAndClient.name, getMasterName());
            removeDisruptionSchemeFromNode(nodeAndClient);
            nodes.remove(nodeAndClient.name);
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
        restartRandomNode(Predicates.<NodeAndClient>alwaysTrue(), callback);
    }

    /**
     * Restarts a random data node in the cluster
     */
    public void restartRandomDataNode() throws Exception {
        restartRandomNode(EMPTY_CALLBACK);
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
    private void restartRandomNode(Predicate<NodeAndClient> predicate, RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(predicate);
        if (nodeAndClient != null) {
            logger.info("Restarting random node [{}] ", nodeAndClient.name);
            nodeAndClient.restart(callback);
        }
    }

    private void restartAllNodes(boolean rollingRestart, RestartCallback callback) throws Exception {
        ensureOpen();
        List<NodeAndClient> toRemove = new ArrayList<>();
        try {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                if (!callback.doRestart(nodeAndClient.name)) {
                    logger.info("Closing node [{}] during restart", nodeAndClient.name);
                    toRemove.add(nodeAndClient);
                    if (activeDisruptionScheme != null) {
                        activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                    }
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
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                }
                nodeAndClient.restart(callback);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
                }
            }
        } else {
            int numNodesRestarted = 0;
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Stopping node [{}] ", nodeAndClient.name);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                }
                nodeAndClient.closeNode();
            }
            for (NodeAndClient nodeAndClient : nodes.values()) {
                logger.info("Starting node [{}] ", nodeAndClient.name);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                }
                nodeAndClient.restart(callback);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
                }
            }
        }
    }


    private static final RestartCallback EMPTY_CALLBACK = new RestartCallback() {
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
    public void rollingRestart(RestartCallback function) throws Exception {
        restartAllNodes(true, function);
    }

    /**
     * Restarts all nodes in the cluster. It first stops all nodes and then restarts all the nodes again.
     */
    public void fullRestart(RestartCallback function) throws Exception {
        restartAllNodes(false, function);
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
            return state.nodes().masterNode().name();
        } catch (Throwable e) {
            logger.warn("Can't fetch cluster state", e);
            throw new RuntimeException("Can't get master node " + e.getMessage(), e);
        }
    }

    synchronized Set<String> allDataNodesButN(int numNodes) {
        return nRandomDataNodes(numDataNodes() - numNodes);
    }

    private synchronized Set<String> nRandomDataNodes(int numNodes) {
        assert size() >= numNodes;
        NavigableMap<String, NodeAndClient> dataNodes = Maps.filterEntries(nodes, new EntryNodePredicate(new DataNodePredicate()));
        return Sets.newHashSet(Iterators.limit(dataNodes.keySet().iterator(), numNodes));
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
        return startNode(ImmutableSettings.EMPTY, Version.CURRENT);
    }

    /**
     * Starts a node with default settings ad the specified version and returns it's name.
     */
    public synchronized String startNode(Version version) {
        return startNode(ImmutableSettings.EMPTY, version);
    }

    /**
     * Starts a node with the given settings builder and returns it's name.
     */
    public synchronized String startNode(Settings.Builder settings) {
        return startNode(settings.build(), Version.CURRENT);
    }

    /**
     * Starts a node with the given settings and returns it's name.
     */
    public synchronized String startNode(Settings settings) {
        return startNode(settings, Version.CURRENT);
    }

    /**
     * Starts a node with the given settings and version and returns it's name.
     */
    public synchronized String startNode(Settings settings, Version version) {
        NodeAndClient buildNode = buildNode(settings, version);
        buildNode.node().start();
        publishNode(buildNode);
        return buildNode.name;
    }

    /**
     * Starts a node in an async manner with the given settings and returns future with its name.
     */
    public synchronized ListenableFuture<String> startNodeAsync() {
        return startNodeAsync(ImmutableSettings.EMPTY, Version.CURRENT);
    }

    /**
     * Starts a node in an async manner with the given settings and returns future with its name.
     */
    public synchronized ListenableFuture<String> startNodeAsync(final Settings settings) {
        return startNodeAsync(settings, Version.CURRENT);
    }

    /**
     * Starts a node in an async manner with the given settings and version and returns future with its name.
     */
    public synchronized ListenableFuture<String> startNodeAsync(final Settings settings, final Version version) {
        final SettableFuture<String> future = SettableFuture.create();
        final NodeAndClient buildNode = buildNode(settings, version);
        Runnable startNode = new Runnable() {
            @Override
            public void run() {
                try {
                    buildNode.node().start();
                    publishNode(buildNode);
                    future.set(buildNode.name);
                } catch (Throwable t) {
                    future.setException(t);
                }
            }
        };
        executor.execute(startNode);
        return future;
    }

    /**
     * Starts multiple nodes in an async manner and returns future with its name.
     */
    public synchronized ListenableFuture<List<String>> startNodesAsync(final int numNodes) {
        return startNodesAsync(numNodes, ImmutableSettings.EMPTY, Version.CURRENT);
    }

    /**
     * Starts multiple nodes in an async manner with the given settings and returns future with its name.
     */
    public synchronized ListenableFuture<List<String>> startNodesAsync(final int numNodes, final Settings settings) {
        return startNodesAsync(numNodes, settings, Version.CURRENT);
    }

    /**
     * Starts multiple nodes in an async manner with the given settings and version and returns future with its name.
     */
    public synchronized ListenableFuture<List<String>> startNodesAsync(final int numNodes, final Settings settings, final Version version) {
        List<ListenableFuture<String>> futures = Lists.newArrayList();
        for (int i = 0; i < numNodes; i++) {
            futures.add(startNodeAsync(settings, version));
        }
        return Futures.allAsList(futures);
    }

    /**
     * Starts multiple nodes (based on the number of settings provided) in an async manner, with explicit settings for each node.
     * The order of the node names returned matches the order of the settings provided.
     */
    public synchronized ListenableFuture<List<String>> startNodesAsync(final Settings... settings) {
        List<ListenableFuture<String>> futures = Lists.newArrayList();
        for (Settings setting : settings) {
            futures.add(startNodeAsync(setting, Version.CURRENT));
        }
        return Futures.allAsList(futures);
    }

    private synchronized void publishNode(NodeAndClient nodeAndClient) {
        assert !nodeAndClient.node().isClosed();
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class, nodeAndClient.node);
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataPaths()));
        }
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

    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        clearDisruptionScheme();
        scheme.applyToCluster(this);
        activeDisruptionScheme = scheme;
    }

    public void clearDisruptionScheme() {
        if (activeDisruptionScheme != null) {
            TimeValue expectedHealingTime = activeDisruptionScheme.expectedTimeToHeal();
            logger.info("Clearing active scheme {}, expected healing time {}", activeDisruptionScheme, expectedHealingTime);
            activeDisruptionScheme.removeFromCluster(this);
            // We don't what scheme is picked, certain schemes don't partition the cluster, but process slow, so we need
            // to to sleep, cluster health alone doesn't verify if these schemes have been cleared.
            if (expectedHealingTime != null && expectedHealingTime.millis() > 0) {
                try {
                    Thread.sleep(expectedHealingTime.millis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            assertFalse("cluster failed to form after disruption was healed", client().admin().cluster().prepareHealth()
                    .setWaitForNodes("" + nodes.size())
                    .setWaitForRelocatingShards(0)
                    .get().isTimedOut());
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
        return Collections2.filter(nodes.values(), new DataNodePredicate());
    }

    private synchronized Collection<NodeAndClient> dataAndMasterNodes() {
        return Collections2.filter(nodes.values(), new DataOrMasterNodePredicate());
    }

    private static final class DataNodePredicate implements Predicate<NodeAndClient> {
        @Override
        public boolean apply(NodeAndClient nodeAndClient) {
            return DiscoveryNode.dataNode(nodeAndClient.node.settings());
        }
    }

    private static final class DataOrMasterNodePredicate implements Predicate<NodeAndClient> {
        @Override
        public boolean apply(NodeAndClient nodeAndClient) {
            return DiscoveryNode.dataNode(nodeAndClient.node.settings()) ||
                    DiscoveryNode.masterNode(nodeAndClient.node.settings());
        }
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
            return DiscoveryNode.clientNode(nodeAndClient.node.settings());
        }
    }

    private static final class EntryNodePredicate implements Predicate<Map.Entry<String, NodeAndClient>> {
        private final Predicate<NodeAndClient> delegateNodePredicate;

        EntryNodePredicate(Predicate<NodeAndClient> delegateNodePredicate) {
            this.delegateNodePredicate = delegateNodePredicate;
        }

        @Override
        public boolean apply(Map.Entry<String, NodeAndClient> entry) {
            return delegateNodePredicate.apply(entry.getValue());
        }
    }

    synchronized String routingKeyForShard(String index, String type, int shard, Random random) {
        assertThat(shard, greaterThanOrEqualTo(0));
        assertThat(shard, greaterThanOrEqualTo(0));
        for (NodeAndClient n : nodes.values()) {
            Node node = n.node;
            IndicesService indicesService = getInstanceFromNode(IndicesService.class, node);
            ClusterService clusterService = getInstanceFromNode(ClusterService.class, node);
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                assertThat(indexService.settingsService().getSettings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1), greaterThan(shard));
                OperationRouting operationRouting = indexService.injector().getInstance(OperationRouting.class);
                while (true) {
                    String routing = RandomStrings.randomAsciiOfLength(random, 10);
                    final int targetShard = operationRouting.indexShards(clusterService.state(), index, type, null, routing).shardId().getId();
                    if (shard == targetShard) {
                        return routing;
                    }
                }
            }
        }
        fail("Could not find a node that holds " + index);
        return null;
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

    /**
     * Returns a predicate that only accepts settings of nodes with one of the given names.
     */
    public static Predicate<Settings> nameFilter(String... nodeName) {
        return new NodeNamePredicate(new HashSet<>(Arrays.asList(nodeName)));
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


    /**
     * An abstract class that is called during {@link #rollingRestart(InternalTestCluster.RestartCallback)}
     * and / or {@link #fullRestart(InternalTestCluster.RestartCallback)} to execute actions at certain
     * stages of the restart.
     */
    public static abstract class RestartCallback {

        /**
         * Executed once the give node name has been stopped.
         */
        public Settings onNodeStopped(String nodeName) throws Exception {
            return ImmutableSettings.EMPTY;
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


        /**
         * If this returns <code>false</code> the node with the given node name will not be restarted. It will be
         * closed and removed from the cluster. Returns <code>true</code> by default.
         */
        public boolean doRestart(String nodeName) {
            return true;
        }
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
                final IndicesFieldDataCache fdCache = getInstanceFromNode(IndicesFieldDataCache.class, nodeAndClient.node);
                // Clean up the cache, ensuring that entries' listeners have been called
                fdCache.getCache().cleanUp();

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
                    assertBusy(new Runnable() {
                        @Override
                        public void run() {
                            CircuitBreaker reqBreaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
                            assertThat("Request breaker not reset to 0 on node: " + name, reqBreaker.getUsed(), equalTo(0L));
                        }
                    });
                } catch (Exception e) {
                    fail("Exception during check for request breaker reset to 0: " + e);
                }

                NodeService nodeService = getInstanceFromNode(NodeService.class, nodeAndClient.node);
                NodeStats stats = nodeService.stats(CommonStatsFlags.ALL, false, false, false, false, false, false, false, false, false);
                assertThat("Fielddata size must be 0 on node: " + stats.getNode(), stats.getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0l));
                assertThat("Filter cache size must be 0 on node: " + stats.getNode(), stats.getIndices().getFilterCache().getMemorySizeInBytes(), equalTo(0l));
                assertThat("FixedBitSet cache size must be 0 on node: " + stats.getNode(), stats.getIndices().getSegments().getBitsetMemoryInBytes(), equalTo(0l));
            }
        }
    }

    @Override
    public void assertAfterTest() throws IOException {
        super.assertAfterTest();
        for (NodeEnvironment env : this.getInstances(NodeEnvironment.class)) {
            Set<ShardId> shardIds = env.lockedShards();
            for (ShardId id : shardIds) {
                try {
                    env.shardLock(id, TimeUnit.SECONDS.toMillis(5)).close();
                } catch (IOException ex) {
                    fail("Shard " + id + " is still locked after 5 sec waiting");
                }
            }
        }
    }

}

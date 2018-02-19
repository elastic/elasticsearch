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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
public abstract class ESSingleNodeTestCase extends ESTestCase {

    private static Node NODE = null;

    protected void startNode(long seed) throws Exception {
        assert NODE == null;
        NODE = RandomizedContext.current().runWithPrivateRandomness(seed, this::newNode);
        // we must wait for the node to actually be up and running. otherwise the node might have started,
        // elected itself master but might not yet have removed the
        // SERVICE_UNAVAILABLE/1/state not recovered / initialized block
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertFalse(clusterHealthResponse.isTimedOut());
        client().admin().indices()
            .preparePutTemplate("one_shard_index_template")
            .setPatterns(Collections.singletonList("*"))
            .setOrder(0)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
    }

    private static void stopNode() throws IOException {
        Node node = NODE;
        NODE = null;
        IOUtils.close(node);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //the seed has to be created regardless of whether it will be used or not, for repeatability
        long seed = random().nextLong();
        // Create the node lazily, on the first test. This is ok because we do not randomize any settings,
        // only the cluster name. This allows us to have overridden properties for plugins and the version to use.
        if (NODE == null) {
            startNode(seed);
        }
    }

    @Override
    public void tearDown() throws Exception {
        logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        super.tearDown();
        assertAcked(client().admin().indices().prepareDelete("*").get());
        MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().keySet(),
                metaData.persistentSettings().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().keySet(),
                metaData.transientSettings().size(), equalTo(0));
        if (resetNodeAfterTest()) {
            assert NODE != null;
            stopNode();
            //the seed can be created within this if as it will either be executed before every test method or will never be.
            startNode(random().nextLong());
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        stopNode();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        stopNode();
    }

    /**
     * This method returns <code>true</code> if the node that is used in the background should be reset
     * after each test. This is useful if the test changes the cluster state metadata etc. The default is
     * <code>false</code>.
     */
    protected boolean resetNodeAfterTest() {
        return false;
    }

    /** The plugin classes that should be added to the node. */
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    /** Helper method to create list of plugins without specifying generic types. */
    @SafeVarargs
    @SuppressWarnings("varargs") // due to type erasure, the varargs type is non-reifiable, which causes this warning
    protected final Collection<Class<? extends Plugin>> pluginList(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }

    /** Additional settings to add when creating the node. Also allows overriding the default settings. */
    protected Settings nodeSettings() {
        return Settings.EMPTY;
    }

    private Node newNode() {
        final Path tempDir = createTempDir();
        Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", random().nextLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            // TODO: use a consistent data path for custom paths
            // This needs to tie into the ESIntegTestCase#indexSettings() method
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent())
            .put("node.name", "node_s_0")
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "1000/1m")
            .put(EsExecutors.PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
            .put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .put("transport.type", getTestTransportType())
            .put(Node.NODE_DATA_SETTING.getKey(), true)
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            .put(nodeSettings()) // allow test cases to provide their own settings or override these
            .build();
        Collection<Class<? extends Plugin>> plugins = getPlugins();
        if (plugins.contains(getTestTransportPlugin()) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(getTestTransportPlugin());
        }
        if (plugins.contains(TestZenDiscovery.TestPlugin.class) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(TestZenDiscovery.TestPlugin.class);
        }
        Node build = new MockNode(settings, plugins);
        try {
            build.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        return build;
    }

    /**
     * Returns a client to the single-node cluster.
     */
    public Client client() {
        return NODE.client();
    }

    /**
     * Return a reference to the singleton node.
     */
    protected Node node() {
        return NODE;
    }

    /**
     * Get an instance for a particular class using the injector of the singleton node.
     */
    protected <T> T getInstanceFromNode(Class<T> clazz) {
        return NODE.injector().getInstance(clazz);
    }

    /**
     * Create a new index on the singleton node with empty index settings.
     */
    protected IndexService createIndex(String index) {
        return createIndex(index, Settings.EMPTY);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings, String type, Object... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = client().admin().cluster()
                .health(Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(resolveIndex(index));
    }

    public Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    /**
     * Create a new search context.
     */
    protected SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getBigArrays();
        ThreadPool threadPool = indexService.getThreadPool();
        return new TestSearchContext(threadPool, bigArrays, indexService);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }


    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     *
     * @param timeout time out value to set on {@link org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest}
     */
    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices).timeout(timeout).waitForGreenStatus().waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return getInstanceFromNode(NamedXContentRegistry.class);
    }
}

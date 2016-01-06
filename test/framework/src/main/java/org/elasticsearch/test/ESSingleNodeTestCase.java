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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
public abstract class ESSingleNodeTestCase extends ESTestCase {

    private static Node NODE = null;

    private void reset() {
        assert NODE != null;
        stopNode();
        startNode();
    }

    private void startNode() {
        assert NODE == null;
        NODE = newNode();
        // we must wait for the node to actually be up and running. otherwise the node might have started, elected itself master but might not yet have removed the
        // SERVICE_UNAVAILABLE/1/state not recovered / initialized block
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertFalse(clusterHealthResponse.isTimedOut());
    }

    private static void stopNode() {
        Node node = NODE;
        NODE = null;
        Releasables.close(node);
    }

    private void cleanup(boolean resetNode) {
        assertAcked(client().admin().indices().prepareDelete("*").get());
        if (resetNode) {
            reset();
        }
        MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(),
                metaData.persistentSettings().getAsMap().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(),
                metaData.transientSettings().getAsMap().size(), equalTo(0));
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Create the node lazily, on the first test. This is ok because we do not randomize any settings,
        // only the cluster name. This allows us to have overriden properties for plugins and the version to use.
        if (NODE == null) {
            startNode();
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        super.tearDown();
        cleanup(resetNodeAfterTest());
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        stopNode();
    }

    @AfterClass
    public static void tearDownClass() {
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

    /** The version of elasticsearch the node should act like. */
    protected Version getVersion() {
        return Version.CURRENT;
    }

    /** The plugin classes that should be added to the node. */
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    /** Helper method to create list of plugins without specifying generic types. */
    @SafeVarargs
    @SuppressWarnings("varargs") // due to type erasure, the varargs type is non-reifiable, which casues this warning
    protected final Collection<Class<? extends Plugin>> pluginList(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }

    private Node newNode() {
        Settings settings = Settings.builder()
            .put(ClusterName.SETTING, InternalTestCluster.clusterName("single-node-cluster", randomLong()))
            .put("path.home", createTempDir())
            // TODO: use a consistent data path for custom paths
            // This needs to tie into the ESIntegTestCase#indexSettings() method
            .put("path.shared_data", createTempDir().getParent())
            .put("node.name", nodeName())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("script.inline", "on")
            .put("script.indexed", "on")
            .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
            .put("http.enabled", false)
            .put("node.local", true)
            .put("node.data", true)
            .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true) // make sure we get what we set :)
            .build();
        Node build = new MockNode(settings, getVersion(), getPlugins());
        build.start();
        assertThat(DiscoveryNode.localNode(build.settings()), is(true));
        return build;
    }

    /**
     * Returns a client to the single-node cluster.
     */
    public static Client client() {
        return NODE.client();
    }

    /**
     * Returns the single test nodes name.
     */
    public static String nodeName() {
        return "node_s_0";
    }

    /**
     * Return a reference to the singleton node.
     */
    protected static Node node() {
        return NODE;
    }

    /**
     * Get an instance for a particular class using the injector of the singleton node.
     */
    protected static <T> T getInstanceFromNode(Class<T> clazz) {
        return NODE.injector().getInstance(clazz);
    }

    /**
     * Create a new index on the singleton node with empty index settings.
     */
    protected static IndexService createIndex(String index) {
        return createIndex(index, Settings.EMPTY);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected static IndexService createIndex(String index, Settings settings, String type, Object... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected static IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = client().admin().cluster()
                .health(Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(index);
    }

    /**
     * Create a new search context.
     */
    protected static SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getIndexServices().getBigArrays();
        ThreadPool threadPool = indexService.getIndexServices().getThreadPool();
        PageCacheRecycler pageCacheRecycler = node().injector().getInstance(PageCacheRecycler.class);
        ScriptService scriptService = node().injector().getInstance(ScriptService.class);
        return new TestSearchContext(threadPool, pageCacheRecycler, bigArrays, scriptService, indexService);
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
                .health(Requests.clusterHealthRequest(indices).timeout(timeout).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }


}

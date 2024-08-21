/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptService;
import org.elasticsearch.search.ConcurrentSearchTestPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.search.SearchTransportService.FREE_CONTEXT_ACTION_NAME;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.test.NodeRoles.dataNode;
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
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth().setWaitForGreenStatus().get();
        assertFalse(clusterHealthResponse.isTimedOut());
        indicesAdmin().preparePutTemplate("one_shard_index_template")
            .setPatterns(Collections.singletonList("*"))
            .setOrder(0)
            .setSettings(indexSettings(1, 0))
            .get();
        indicesAdmin().preparePutTemplate("random-soft-deletes-template")
            .setPatterns(Collections.singletonList("*"))
            .setOrder(0)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000)))
            .get();
    }

    private static void stopNode() throws IOException, InterruptedException {
        Node node = NODE;
        NODE = null;
        IOUtils.close(node);
        if (node != null && node.awaitClose(10, TimeUnit.SECONDS) == false) {
            throw new AssertionError("Node couldn't close within 10 seconds.");
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // the seed has to be created regardless of whether it will be used or not, for repeatability
        long seed = random().nextLong();
        // Create the node lazily, on the first test. This is ok because we do not randomize any settings,
        // only the cluster name. This allows us to have overridden properties for plugins and the version to use.
        if (NODE == null) {
            startNode(seed);
        }
    }

    @Override
    public void tearDown() throws Exception {
        logger.trace("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        awaitIndexShardCloseAsyncTasks();
        ensureNoInitializingShards();
        ensureAllFreeContextActionsAreConsumed();

        ensureAllContextsReleased(getInstanceFromNode(SearchService.class));
        super.tearDown();
        var deleteDataStreamsRequest = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, "*");
        deleteDataStreamsRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
        try {
            assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamsRequest));
        } catch (IllegalStateException e) {
            // Ignore if action isn't registered, because data streams is a module and
            // if the delete action isn't registered then there no data streams to delete.
            if (e.getMessage().startsWith("failed to find action") == false) {
                throw e;
            }
        }
        var deleteComposableIndexTemplateRequest = new TransportDeleteComposableIndexTemplateAction.Request("*");
        assertAcked(client().execute(TransportDeleteComposableIndexTemplateAction.TYPE, deleteComposableIndexTemplateRequest).actionGet());
        var deleteComponentTemplateRequest = new TransportDeleteComponentTemplateAction.Request("*");
        assertAcked(client().execute(TransportDeleteComponentTemplateAction.TYPE, deleteComponentTemplateRequest).actionGet());
        assertAcked(indicesAdmin().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN).get());
        Metadata metadata = clusterAdmin().prepareState().get().getState().getMetadata();
        assertThat(
            "test leaves persistent cluster metadata behind: " + metadata.persistentSettings().keySet(),
            metadata.persistentSettings().size(),
            equalTo(0)
        );
        assertThat(
            "test leaves transient cluster metadata behind: " + metadata.transientSettings().keySet(),
            metadata.transientSettings().size(),
            equalTo(0)
        );
        GetIndexResponse indices = indicesAdmin().prepareGetIndex()
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .addIndices("*")
            .get();
        assertThat(
            "test leaves indices that were not deleted: " + Strings.arrayToCommaDelimitedString(indices.indices()),
            indices.indices(),
            equalTo(Strings.EMPTY_ARRAY)
        );
        if (resetNodeAfterTest()) {
            assert NODE != null;
            stopNode();
            // the seed can be created within this if as it will either be executed before every test method or will never be.
            startNode(random().nextLong());
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        stopNode();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        stopNode();
        ESIntegTestCase.awaitGlobalNettyThreadsFinish();
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

    /** True if a dummy http transport should be used, or false if the real http transport should be used. */
    protected boolean addMockHttpTransport() {
        return true;
    }

    @Override
    protected List<String> filteredWarnings() {
        return Stream.concat(
            super.filteredWarnings().stream(),
            Stream.of("[index.data_path] setting was deprecated in Elasticsearch and will be removed in a future release.")
        ).collect(Collectors.toList());
    }

    private Node newNode() {
        final Path tempDir = createTempDir();
        final String nodeName = nodeSettings().get(Node.NODE_NAME_SETTING.getKey(), "node_s_0");
        Settings.Builder settingBuilder = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", random().nextLong()))
            .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            // TODO: use a consistent data path for custom paths
            // This needs to tie into the ESIntegTestCase#indexSettings() method
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent())
            .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
            .put("transport.type", getTestTransportType())
            .put(TransportSettings.PORT.getKey(), ESTestCase.getPortRange())
            .put(dataNode())
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // turning on the real memory circuit breaker leads to spurious test failures. As have no full control over heap usage, we
            // turn it off for these tests.
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(INITIAL_MASTER_NODES_SETTING.getKey(), nodeName)
            .put(nodeSettings());// allow test cases to provide their own settings or override these

        boolean enableConcurrentSearch = enableConcurrentSearch();
        if (enableConcurrentSearch) {
            settingBuilder.put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), 1);
        } else {
            settingBuilder.put(SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey(), false);
        }
        Settings settings = settingBuilder.build();

        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(getPlugins());
        if (plugins.contains(getTestTransportPlugin()) == false) {
            plugins.add(getTestTransportPlugin());
        }
        if (addMockHttpTransport()) {
            plugins.add(MockHttpTransport.TestPlugin.class);
        }
        if (enableConcurrentSearch) {
            plugins.add(ConcurrentSearchTestPlugin.class);
        }
        plugins.add(MockScriptService.TestPlugin.class);
        Node node = new MockNode(settings, plugins, forbidPrivateIndexSettings());
        try {
            node.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        return node;
    }

    /**
     * Returns a client to the single-node cluster.
     */
    public Client client() {
        return wrapClient(NODE.client());
    }

    /**
     * Execute the given {@link ActionRequest} using the given {@link ActionType} and the default node client, wait for it to complete with
     * a timeout of {@link #SAFE_AWAIT_TIMEOUT}, and then return the result. An exceptional response, timeout or interrupt triggers a test
     * failure.
     */
    public <T extends ActionResponse> T safeExecute(ActionType<T> action, ActionRequest request) {
        return safeExecute(client(), action, request);
    }

    /**
     * Returns an admin client.
     */
    protected AdminClient admin() {
        return client().admin();
    }

    /**
     * Returns an indices admin client.
     */
    protected IndicesAdminClient indicesAdmin() {
        return admin().indices();
    }

    /**
     * Returns a cluster admin client.
     */
    protected ClusterAdminClient clusterAdmin() {
        return admin().cluster();
    }

    public Client wrapClient(final Client client) {
        return client;
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
        return createIndex(index, settings, null);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdmin().prepareCreate(index).setSettings(settings);
        if (mappings != null) {
            createIndexRequestBuilder.setMapping(mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings, String type, String... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdmin().prepareCreate(index).setSettings(settings);
        if (type != null) {
            createIndexRequestBuilder.setMapping(mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = clusterAdmin().health(
            new ClusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true)
        ).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(resolveIndex(index));
    }

    public Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = indicesAdmin().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected IndexRequestBuilder prepareIndex(String index) {
        return client().prepareIndex(index);
    }

    /**
     * Create a new search context.
     */
    protected SearchContext createSearchContext(IndexService indexService) {
        return new TestSearchContext(indexService);
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
        ClusterHealthResponse actionGet = clusterAdmin().health(
            new ClusterHealthRequest(indices).masterNodeTimeout(timeout)
                .timeout(timeout)
                .waitForGreenStatus()
                .waitForEvents(Priority.LANGUID)
                .waitForNoRelocatingShards(true)
        ).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "ensureGreen timed out, cluster state:\n{}\n{}",
                clusterAdmin().prepareState().get().getState(),
                ESIntegTestCase.getClusterPendingTasks(client())
            );
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

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

    /**
     * waits until all shard initialization is completed.
     *
     * inspired by {@link ESRestTestCase}
     */
    protected void ensureNoInitializingShards() {
        ClusterHealthResponse actionGet = clusterAdmin().health(new ClusterHealthRequest("_all").waitForNoInitializingShards(true))
            .actionGet();

        assertFalse("timed out waiting for shards to initialize", actionGet.isTimedOut());
    }

    /**
     * waits until all free_context actions have been handled by the generic thread pool
     */
    protected void ensureAllFreeContextActionsAreConsumed() throws Exception {
        logger.info("--> waiting for all free_context tasks to complete within a reasonable time");
        safeGet(clusterAdmin().prepareListTasks().setActions(FREE_CONTEXT_ACTION_NAME + "*").setWaitForCompletion(true).execute());
    }

    /**
     * Whether we'd like to enable inter-segment search concurrency and increase the likelihood of leveraging it, by creating multiple
     * slices with a low amount of documents in them, which would not be allowed in production.
     * Default is true, can be disabled if it causes problems in specific tests.
     */
    protected boolean enableConcurrentSearch() {
        return true;
    }

    protected void awaitIndexShardCloseAsyncTasks() {
        final var latch = new CountDownLatch(1);
        getInstanceFromNode(IndicesClusterStateService.class).onClusterStateShardsClosed(latch::countDown);
        safeAwait(latch);
    }
}

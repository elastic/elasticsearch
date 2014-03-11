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

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.google.common.base.Joiner;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper.Loading;
import org.elasticsearch.index.merge.policy.*;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.client.RandomizingClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.test.TestCluster.clusterName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link ElasticsearchIntegrationTest} is an abstract base class to run integration
 * tests against a JVM private Elasticsearch Cluster. The test class supports 3 different
 * cluster scopes.
 * <ul>
 * <li>{@link Scope#GLOBAL} - uses a cluster shared across test suites. This cluster doesn't allow any modifications to
 * the cluster settings and will fail if any persistent cluster settings are applied during tear down.</li>
 * <li>{@link Scope#TEST} - uses a new cluster for each individual test method.</li>
 * <li>{@link Scope#SUITE} - uses a cluster shared across all test method in the same suite</li>
 * </ul>
 * <p/>
 * The most common test scope it {@link Scope#GLOBAL} which shares a cluster per JVM. This cluster is only set-up once
 * and can be used as long as the tests work on a per index basis without changing any cluster wide settings or require
 * any specific node configuration. This is the best performing option since it sets up the cluster only once.
 * <p/>
 * If the tests need specific node settings or change persistent and/or transient cluster settings either {@link Scope#TEST}
 * or {@link Scope#SUITE} should be used. To configure a scope for the test cluster the {@link ClusterScope} annotation
 * should be used, here is an example:
 * <pre>
 *
 * @ClusterScope(scope=Scope.TEST) public class SomeIntegrationTest extends ElasticsearchIntegrationTest {
 * @Test
 * public void testMethod() {}
 * }
 * </pre>
 * <p/>
 * If no {@link ClusterScope} annotation is present on an integration test the default scope it {@link Scope#GLOBAL}
 * <p/>
 * A test cluster creates a set of nodes in the background before the test starts. The number of nodes in the cluster is
 * determined at random and can change across tests. The minimum number of nodes in the shared global cluster is <code>2</code>.
 * For other scopes the {@link ClusterScope} allows configuring the initial number of nodes that are created before
 * the tests start.
 * <p/>
 *  <pre>
 * @ClusterScope(scope=Scope.SUITE, numNodes=3)
 * public class SomeIntegrationTest extends ElasticsearchIntegrationTest {
 * @Test
 * public void testMethod() {}
 * }
 * </pre>
 * <p/>
 * Note, the {@link ElasticsearchIntegrationTest} uses randomized settings on a cluster and index level. For instance
 * each test might use different directory implementation for each test or will return a random client to one of the
 * nodes in the cluster for each call to {@link #client()}. Test failures might only be reproducible if the correct
 * system properties are passed to the test execution environment.
 * <p/>
 * <p>
 * This class supports the following system properties (passed with -Dkey=value to the application)
 * <ul>
 * <li>-D{@value #TESTS_CLIENT_RATIO} - a double value in the interval [0..1] which defines the ration between node and transport clients used</li>
 * <li>-D{@value TestCluster#TESTS_ENABLE_MOCK_MODULES} - a boolean value to enable or disable mock modules. This is
 * useful to test the system without asserting modules that to make sure they don't hide any bugs in production.</li>
 * <li>-D{@value #INDEX_SEED_SETTING} - a random seed used to initialize the index random context.
 * </ul>
 * </p>
 */
@Ignore
@AbstractRandomizedTest.IntegrationTests
public abstract class ElasticsearchIntegrationTest extends ElasticsearchTestCase {
    private static TestCluster GLOBAL_CLUSTER;

    /**
     * Key used to set the transport client ratio via the commandline -D{@value #TESTS_CLIENT_RATIO}
     */
    public static final String TESTS_CLIENT_RATIO = "tests.client.ratio";

    /**
     * Key used to retrieve the index random seed from the index settings on a running node.
     * The value of this seed can be used to initialize a random context for a specific index.
     * It's set once per test via a generic index template.
     */
    public static final String INDEX_SEED_SETTING = "index.tests.seed";

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the global cluster carried
     * on across test suites.
     */
    private static TestCluster currentCluster;

    private static final double TRANSPORT_CLIENT_RATIO = transportClientRatio();

    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<Class<?>, TestCluster>();

    @BeforeClass
    public final static void beforeClass() throws Exception {
        // Initialize lazily. No need for volatiles/ CASs since each JVM runs at most one test
        // suite at any given moment.
        if (GLOBAL_CLUSTER == null) {
            long masterSeed = SeedUtils.parseSeed(RandomizedContext.current().getRunnerSeedAsString());
            GLOBAL_CLUSTER = new TestCluster(masterSeed, clusterName("shared", ElasticsearchTestCase.CHILD_VM_ID, masterSeed));
        }
    }
    
    @Before
    public final void before() throws IOException {
        assert Thread.getDefaultUncaughtExceptionHandler() instanceof ElasticsearchUncaughtExceptionHandler;
        try {
            final Scope currentClusterScope = getCurrentClusterScope();
            switch (currentClusterScope) {
                case GLOBAL:
                    clearClusters();
                    currentCluster = GLOBAL_CLUSTER;
                    break;
                case SUITE:
                    currentCluster = buildAndPutCluster(currentClusterScope, false);
                    break;
                case TEST:
                    currentCluster = buildAndPutCluster(currentClusterScope, true);
                    break;
                default:
                    fail("Unknown Scope: [" + currentClusterScope + "]");
            }
            currentCluster.beforeTest(getRandom(), getPerTestTransportClientRatio());
            wipeIndices("_all");
            wipeTemplates();
            randomIndexTemplate();
            wipeRepositories();
            logger.info("[{}#{}]: before test", getTestClass().getSimpleName(), getTestName());
        } catch (OutOfMemoryError e) {
            if (e.getMessage().contains("unable to create new native thread")) {
                ElasticsearchTestCase.printStackDump(logger);
            }
            throw e;
        }
    }

    public TestCluster buildAndPutCluster(Scope currentClusterScope, boolean createIfExists) throws IOException {
        TestCluster testCluster = clusters.get(this.getClass());
        if (createIfExists || testCluster == null) {
            testCluster = buildTestCluster(currentClusterScope);
        } else {
            clusters.remove(this.getClass());
        }
        clearClusters();
        clusters.put(this.getClass(), testCluster);
        return testCluster;
    }

    private void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            for (TestCluster cluster : clusters.values()) {
                cluster.close();
            }
            clusters.clear();
        }
    }

    @After
    public final void after() throws IOException {
        try {
            logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
            Scope currentClusterScope = getCurrentClusterScope();
            if (currentClusterScope == Scope.TEST) {
                clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
            } else {
                MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
                assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                        .persistentSettings().getAsMap().size(), equalTo(0));
                assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                        .transientSettings().getAsMap().size(), equalTo(0));

            }
            wipeIndices("_all"); // wipe after to make sure we fail in the test that didn't ack the delete
            wipeTemplates();
            wipeRepositories();
            ensureAllSearchersClosed();
            ensureAllFilesClosed();
            ensureEstimatedStats();
            logger.info("[{}#{}]: cleaned up after test", getTestClass().getSimpleName(), getTestName());
        } catch (OutOfMemoryError e) {
            if (e.getMessage().contains("unable to create new native thread")) {
                ElasticsearchTestCase.printStackDump(logger);
            }
            throw e;
        } finally {
            currentCluster.afterTest();
            currentCluster = null;
        }
    }

    public static TestCluster cluster() {
        return currentCluster;
    }

    public ClusterService clusterService() {
        return cluster().clusterService();
    }

    public static Client client() {
        Client client = cluster().client();
        if (frequently()) {
            client = new RandomizingClient((InternalClient) client, getRandom());
        }
        return client;
    }

    /**
     * Creates a randomized index template. This template is used to pass in randomized settings on a
     * per index basis.
     */
    private static void randomIndexTemplate() {
        // TODO move settings for random directory etc here into the index based randomized settings.
        if (cluster().size() > 0) {
            client().admin().indices().preparePutTemplate("random_index_template")
                    .setTemplate("*")
                    .setOrder(0)
                    .setSettings(setRandomNormsLoading(setRandomMerge(getRandom(), ImmutableSettings.builder())
                            .put(INDEX_SEED_SETTING, randomLong()))
                            .put(SETTING_NUMBER_OF_SHARDS, between(DEFAULT_MIN_NUM_SHARDS, DEFAULT_MAX_NUM_SHARDS))
                            .put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)))
                    .execute().actionGet();
        }
    }

    private static ImmutableSettings.Builder setRandomNormsLoading(ImmutableSettings.Builder builder) {
        if (randomBoolean()) {
            builder.put(SearchService.NORMS_LOADING_KEY, randomFrom(Arrays.asList(Loading.EAGER, Loading.LAZY)));
        }
        return builder;
    }

    private static ImmutableSettings.Builder setRandomMerge(Random random, ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                    random.nextBoolean() ? random.nextDouble() : random.nextBoolean());
        }
        Class<? extends MergePolicyProvider<?>> mergePolicy = TieredMergePolicyProvider.class;
        switch (random.nextInt(5)) {
            case 4:
                mergePolicy = LogByteSizeMergePolicyProvider.class;
                break;
            case 3:
                mergePolicy = LogDocMergePolicyProvider.class;
                break;
            case 0:
                mergePolicy = null;
        }
        if (mergePolicy != null) {
            builder.put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, mergePolicy.getName());
        }

        if (random.nextBoolean()) {
            builder.put(MergeSchedulerProvider.FORCE_ASYNC_MERGE, random.nextBoolean());
        }
        switch (random.nextInt(5)) {
            case 4:
                builder.put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, SerialMergeSchedulerProvider.class.getName());
                break;
            case 3:
                builder.put(MergeSchedulerModule.MERGE_SCHEDULER_TYPE_KEY, ConcurrentMergeSchedulerProvider.class.getName());
                break;
        }

        return builder;
    }

    public static Iterable<Client> clients() {
        return cluster();
    }

    protected static final int DEFAULT_MIN_NUM_SHARDS = 1;
    protected static final int DEFAULT_MAX_NUM_SHARDS = 10;

    protected int minimumNumberOfShards() {
        return DEFAULT_MIN_NUM_SHARDS;
    }

    protected int maximumNumberOfShards() {
        return DEFAULT_MAX_NUM_SHARDS;
    }

    protected int numberOfShards() {
        return between(minimumNumberOfShards(), maximumNumberOfShards());
    }

    protected int minimumNumberOfReplicas() {
        return 0;
    }

    protected int maximumNumberOfReplicas() {
        return cluster().dataNodes() - 1;
    }

    protected int numberOfReplicas() {
        return between(minimumNumberOfReplicas(), maximumNumberOfReplicas());
    }

    /**
     * Returns a settings object used in {@link #createIndex(String...)} and {@link #prepareCreate(String)} and friends.
     * This method can be overwritten by subclasses to set defaults for the indices that are created by the test.
     * By default it returns a settings object that sets a random number of shards. Number of shards and replicas
     * can be controlled through specific methods.
     */
    public Settings indexSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        int numberOfShards = numberOfShards();
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        }
        int numberOfReplicas = numberOfReplicas();
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        }
        return builder.build();
    }

    /**
     * Ensures that the breaker statistics are reset to 0 since we wiped all indices and that
     * means all stats should be set to 0 otherwise something is wrong with the field data
     * calculation.
     */
    private static void ensureEstimatedStats() {
        if (cluster().size() > 0) {
            NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats()
                    .clear().setBreaker(true).execute().actionGet();
            for (NodeStats stats : nodeStats.getNodes()) {
                assertThat("Breaker not reset to 0 on node: " + stats.getNode(),
                        stats.getBreaker().getEstimated(), equalTo(0L));
            }
        }
    }

    /**
     * Deletes the given indices from the tests cluster. If no index name is passed to this method
     * all indices are removed.
     */
    public static void wipeIndices(String... indices) {
        assert indices != null && indices.length > 0;
        if (cluster().size() > 0) {
            try {
                assertAcked(client().admin().indices().prepareDelete(indices));
            } catch (IndexMissingException e) {
                // ignore
            } catch (ElasticsearchIllegalArgumentException e) {
                // Happens if `action.destructive_requires_name` is set to true
                // which is the case in the CloseIndexDisableCloseAllTests
                if ("_all".equals(indices[0])) {
                    ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
                    ObjectArrayList<String> concreteIndices = new ObjectArrayList<String>();
                    for (IndexMetaData indexMetaData : clusterStateResponse.getState().metaData()) {
                        concreteIndices.add(indexMetaData.getIndex());
                    }
                    if (!concreteIndices.isEmpty()) {
                        assertAcked(client().admin().indices().prepareDelete(concreteIndices.toArray(String.class)));
                    }
                }
            }
        }
    }

    /**
     * Deletes index templates, support wildcard notation.
     * If no template name is passed to this method all templates are removed.
     */
    public static void wipeTemplates(String... templates) {
        if (cluster().size() > 0) {
            // if nothing is provided, delete all
            if (templates.length == 0) {
                templates = new String[]{"*"};
            }
            for (String template : templates) {
                try {
                    client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Deletes repositories, supports wildcard notation.
     */
    public static void wipeRepositories(String... repositories) {
        if (cluster().size() > 0) {
            // if nothing is provided, delete all
            if (repositories.length == 0) {
                repositories = new String[]{"*"};
            }
            for (String repository : repositories) {
                try {
                    client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
                } catch (RepositoryMissingException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged. If one of the indices
     * already exists this method will fail and wipe all the indices created so far.
     */
    public final void createIndex(String... names) {

        List<String> created = new ArrayList<String>();
        for (String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success && !created.isEmpty()) {
                    wipeIndices(created.toArray(new String[created.size()]));
                }
            }
        }
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public final CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index).setSettings(indexSettings());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation
     * rules based on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public final CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation
     * rules based on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, ImmutableSettings.Builder settingsBuilder) {
        cluster().ensureAtLeastNumNodes(numNodes);

        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(indexSettings()).put(settingsBuilder.build());

        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private ImmutableSettings.Builder getExcludeSettings(String index, int num, ImmutableSettings.Builder builder) {
        String exclude = Joiner.on(',').join(cluster().allButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    /**
     * Restricts the given index to be allocated on <code>n</code> nodes using the allocation deciders.
     * Yet if the shards can't be allocated on any other node shards for this index will remain allocated on
     * more than <code>n</code> nodes.
     */
    public void allowNodes(String index, int n) {
        assert index != null;
        cluster().ensureAtLeastNumNodes(n);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (n > 0) {
            getExcludeSettings(index, n, builder);
        }
        Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(String... indices) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

    /**
     * Waits for all relocating shards to become active using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    /**
     * Waits for all relocating shards to become active and the cluster has reached the given health status
     * using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForRelocatingShards(0);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("waitForRelocation timed out (status={}), cluster state:\n{}\n{}", status, client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API.
     */
    public ClusterHealthStatus ensureYellow(String... indices) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices).waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }


    /**
     * Ensures the cluster is in a searchable state for the given indices. This means a searchable copy of each
     * shard is available on the cluster.
     */
    protected ClusterHealthStatus ensureSearchable(String... indices) {
        // this is just a temporary thing but it's easier to change if it is encapsulated.
        return ensureGreen(indices);
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareGet(index, type, id).execute().actionGet();
     * </pre>
     */
    protected final GetResponse get(String index, String type, String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    /**
     * Waits for relocations and refreshes all indices in the cluster.
     *
     * @see #waitForRelocation()
     */
    protected final RefreshResponse refresh() {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Flushes and refreshes all indices in the cluster
     */
    protected final void flushAndRefresh() {
        flush(true);
        refresh();
    }

    /**
     * Flushes all indices in the cluster
     */
    protected final FlushResponse flush() {
        return flush(true);
    }

    private FlushResponse flush(boolean ignoreNotAllowed) {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush().execute().actionGet();
        if (ignoreNotAllowed) {
            for (ShardOperationFailedException failure : actionGet.getShardFailures()) {
                assertThat("unexpected flush failure " + failure.reason(), failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            }
        } else {
            assertNoFailures(actionGet);
        }
        return actionGet;
    }

    /**
     * Waits for all relocations and optimized all indices in the cluster to 1 segment.
     */
    protected OptimizeResponse optimize() {
        waitForRelocation();
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Returns <code>true</code> iff the given index exists otherwise <code>false</code>
     */
    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    /**
     * Returns a random admin client. This client can either be a node or a transport client pointing to any of
     * the nodes in the cluster.
     */
    protected AdminClient admin() {
        return client().admin();
    }

    /**
     * Convenience method that forwards to {@link #indexRandom(boolean, List)}.
     */
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, Arrays.asList(builders));
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes they in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     */
    public void indexRandom(boolean forceRefresh, List<IndexRequestBuilder> builders) throws InterruptedException, ExecutionException {
        if (builders.size() == 0) {
            return;
        }

        Random random = getRandom();
        Set<String> indicesSet = new HashSet<String>();
        for (IndexRequestBuilder builder : builders) {
            indicesSet.add(builder.request().index());
        }
        final String[] indices = indicesSet.toArray(new String[indicesSet.size()]);
        Collections.shuffle(builders, random);
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>> errors = new CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>>();
        List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
        if (frequently()) {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), true, false);
            final CountDownLatch latch = new CountDownLatch(builders.size());
            latches.add(latch);
            for (IndexRequestBuilder indexRequestBuilder : builders) {
                indexRequestBuilder.execute(new PayloadLatchedActionListener<IndexResponse, IndexRequestBuilder>(indexRequestBuilder, latch, errors));
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenient()).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenient()).execute(new LatchedActionListener<FlushResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIndicesOptions(IndicesOptions.lenient()).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches)));
                    }
                }
            }

        } else if (randomBoolean()) {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, false);
            for (IndexRequestBuilder indexRequestBuilder : builders) {
                indexRequestBuilder.execute().actionGet();
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenient()).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenient()).execute(new LatchedActionListener<FlushResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIndicesOptions(IndicesOptions.lenient()).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches)));
                    }
                }
            }
        } else {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, true);
            BulkRequestBuilder bulkBuilder = client().prepareBulk();
            for (IndexRequestBuilder indexRequestBuilder : builders) {
                bulkBuilder.add(indexRequestBuilder);
            }
            BulkResponse actionGet = bulkBuilder.execute().actionGet();
            assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
        }
        for (CountDownLatch countDownLatch : latches) {
            countDownLatch.await();
        }
        final List<Throwable> actualErrors = new ArrayList<Throwable>();
        for (Tuple<IndexRequestBuilder, Throwable> tuple : errors) {
            if (ExceptionsHelper.unwrapCause(tuple.v2()) instanceof EsRejectedExecutionException) {
                tuple.v1().execute().actionGet(); // re-index if rejected
            } else {
                actualErrors.add(tuple.v2());
            }
        }
        assertThat(actualErrors, emptyIterable());
        if (forceRefresh) {
            assertNoFailures(client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenient()).execute().get());
        }
    }

    private static CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    private class LatchedActionListener<Response> implements ActionListener<Response> {
        private final CountDownLatch latch;

        public LatchedActionListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public final void onResponse(Response response) {
            latch.countDown();
        }

        @Override
        public final void onFailure(Throwable t) {
            try {
                logger.info("Action Failed", t);
                addError(t);
            } finally {
                latch.countDown();
            }
        }

        protected void addError(Throwable t) {
        }

    }

    private class PayloadLatchedActionListener<Response, T> extends LatchedActionListener<Response> {
        private final CopyOnWriteArrayList<Tuple<T, Throwable>> errors;
        private final T builder;

        public PayloadLatchedActionListener(T builder, CountDownLatch latch, CopyOnWriteArrayList<Tuple<T, Throwable>> errors) {
            super(latch);
            this.errors = errors;
            this.builder = builder;
        }

        protected void addError(Throwable t) {
            errors.add(new Tuple<T, Throwable>(builder, t));
        }

    }

    /**
     * Clears the given scroll Ids
     */
    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }


    /**
     * The scope of a test cluster used together with
     * {@link ClusterScope} annotations on {@link ElasticsearchIntegrationTest} subclasses.
     */
    public static enum Scope {
        /**
         * A globally shared cluster. This cluster doesn't allow modification of transient or persistent
         * cluster settings.
         */
        GLOBAL,
        /**
         * A cluster shared across all method in a single test suite
         */
        SUITE,
        /**
         * A test exclusive test cluster
         */
        TEST
    }

    private ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == ElasticsearchIntegrationTest.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    private Scope getCurrentClusterScope() {
        ClusterScope annotation = getAnnotation(this.getClass());
        // if we are not annotated assume global!
        return annotation == null ? Scope.GLOBAL : annotation.scope();
    }

    private int getNumNodes() {
        ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? -1 : annotation.numNodes();
    }

    private int getMinNumNodes() {
        ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? TestCluster.DEFAULT_MIN_NUM_NODES : annotation.minNumNodes();
    }

    private int getMaxNumNodes() {
        ClusterScope annotation = getAnnotation(this.getClass());
        return annotation == null ? TestCluster.DEFAULT_MAX_NUM_NODES : annotation.maxNumNodes();
    }

    /**
     * This method is used to obtain settings for the <tt>Nth</tt> node in the cluster.
     * Nodes in this cluster are associated with an ordinal number such that nodes can
     * be started with specific configurations. This method might be called multiple
     * times with the same ordinal and is expected to return the same value for each invocation.
     * In other words subclasses must ensure this method is idempotent.
     */
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.EMPTY;
    }

    private TestCluster buildTestCluster(Scope scope) {
        long currentClusterSeed = randomLong();
        int numNodes = getNumNodes();
        NodeSettingsSource nodeSettingsSource;
        if (numNodes > 0) {
            NodeSettingsSource.Immutable.Builder nodesSettings = NodeSettingsSource.Immutable.builder();
            for (int i = 0; i < numNodes; i++) {
                nodesSettings.set(i, nodeSettings(i));
            }
            nodeSettingsSource = nodesSettings.build();
        } else {
            nodeSettingsSource = new NodeSettingsSource() {
                @Override
                public Settings settings(int nodeOrdinal) {
                    return nodeSettings(nodeOrdinal);
                }
            };
        }

        int minNumNodes, maxNumNodes;
        if (numNodes >= 0) {
            minNumNodes = maxNumNodes = numNodes;
        } else {
            minNumNodes = getMinNumNodes();
            maxNumNodes = getMaxNumNodes();
        }

        return new TestCluster(currentClusterSeed, minNumNodes, maxNumNodes, clusterName(scope.name(), ElasticsearchTestCase.CHILD_VM_ID, currentClusterSeed), nodeSettingsSource);
    }

    /**
     * Defines a cluster scope for a {@link ElasticsearchIntegrationTest} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link Scope#GLOBAL} is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link Scope#GLOBAL} is default.
         */
        Scope scope() default Scope.GLOBAL;

        /**
         * Returns the number of nodes in the cluster. Default is <tt>-1</tt> which means
         * a random number of nodes is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numNodes() default -1;

        /**
         * Returns the minimum number of nodes in the cluster. Default is {@link TestCluster#DEFAULT_MIN_NUM_NODES}.
         * Ignored when {@link ClusterScope#numNodes()} is set.
         */
        int minNumNodes() default TestCluster.DEFAULT_MIN_NUM_NODES;

        /**
         * Returns the maximum number of nodes in the cluster.  Default is {@link TestCluster#DEFAULT_MAX_NUM_NODES}.
         * Ignored when {@link ClusterScope#numNodes()} is set.
         */
        int maxNumNodes() default TestCluster.DEFAULT_MAX_NUM_NODES;

        /**
         * Returns the transport client ratio. By default this returns <code>-1</code> which means a random
         * ratio in the interval <code>[0..1]</code> is used.
         */
        double transportClientRatio() default -1;
    }

    /**
     * Returns the client ratio configured via
     */
    private static double transportClientRatio() {
        String property = System.getProperty(TESTS_CLIENT_RATIO);
        if (property == null || property.isEmpty()) {
            return Double.NaN;
        }
        return Double.parseDouble(property);
    }

    /**
     * Returns the transport client ratio from the class level annotation or via
     * {@link System#getProperty(String)} if available. If both are not available this will
     * return a random ratio in the interval <tt>[0..1]</tt>
     */
    protected double getPerTestTransportClientRatio() {
        final ClusterScope annotation = getAnnotation(this.getClass());
        double perTestRatio = -1;
        if (annotation != null) {
            perTestRatio = annotation.transportClientRatio();
        }
        if (perTestRatio == -1) {
            return Double.isNaN(TRANSPORT_CLIENT_RATIO) ? randomDouble() : TRANSPORT_CLIENT_RATIO;
        }
        assert perTestRatio >= 0.0 && perTestRatio <= 1.0;
        return perTestRatio;
    }

    /**
     * Returns a random numeric field data format from the choices of "array",
     * "compressed", or "doc_values".
     */
    public static String randomNumericFieldDataFormat() {
        return randomFrom(Arrays.asList("array", "compressed", "doc_values"));
    }

    /**
     * Returns a random bytes field data format from the choices of
     * "paged_bytes", "fst", or "doc_values".
     */
    public static String randomBytesFieldDataFormat() {
        return randomFrom(Arrays.asList("paged_bytes", "fst", "doc_values"));
    }

    protected NumShards getNumShards(String index) {
        MetaData metaData = client().admin().cluster().prepareState().get().getState().metaData();
        assertThat(metaData.hasIndex(index), equalTo(true));
        int numShards = Integer.valueOf(metaData.index(index).settings().get(SETTING_NUMBER_OF_SHARDS));
        int numReplicas = Integer.valueOf(metaData.index(index).settings().get(SETTING_NUMBER_OF_REPLICAS));
        return new NumShards(numShards, numReplicas);
    }

    protected static class NumShards {
        public final int numPrimaries;
        public final int numReplicas;
        public final int totalNumShards;
        public final int dataCopies;

        private NumShards(int numPrimaries, int numReplicas) {
            this.numPrimaries = numPrimaries;
            this.numReplicas = numReplicas;
            this.dataCopies = numReplicas + 1;
            this.totalNumShards = numPrimaries * dataCopies;
        }
    }
}

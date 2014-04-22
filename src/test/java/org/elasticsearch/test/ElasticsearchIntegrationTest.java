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
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.client.RandomizingClient;
import org.junit.*;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
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
 * @Test public void testMethod() {}
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
 * @Test public void testMethod() {}
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
 * <li>-D{@value org.elasticsearch.test.TestCluster#SETTING_INDEX_SEED} - a random seed used to initialize the index random context.
 * </ul>
 * </p>
 */
@Ignore
@AbstractRandomizedTest.IntegrationTests
public abstract class ElasticsearchIntegrationTest extends ElasticsearchTestCase {
    private static ImmutableTestCluster GLOBAL_CLUSTER;

    /**
     * Key used to set the transport client ratio via the commandline -D{@value #TESTS_CLIENT_RATIO}
     */
    public static final String TESTS_CLIENT_RATIO = "tests.client.ratio";

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_CLUSTER = "tests.cluster";

    /**
     * Property that allows to adapt the tests behaviour to older features/bugs based on the input version
     */
    public static final String TESTS_COMPATIBILITY = "tests.compatibility";

    protected static final TestVersion CURRENT_VERSION = TestVersion.RANDOM_NUM_SHARDS_REPLICAS;

    private static final TestVersion COMPATIBILITY_VERSION = TestVersion.parse(System.getProperty(TESTS_COMPATIBILITY), CURRENT_VERSION);

    /**
     * Threshold at which indexing switches from frequently async to frequently bulk.
     */
    private static final int FREQUENT_BULK_THRESHOLD = 300;

    /**
     * Threshold at which bulk indexing will always be used.
     */
    private static final int ALWAYS_BULK_THRESHOLD = 3000;

    /**
     * Maximum number of async operations that indexRandom will kick off at one time.
     */
    private static final int MAX_IN_FLIGHT_ASYNC_INDEXES = 150;
    /**
     * Maximum number of documents in a single bulk index request.
     */
    private static final int MAX_BULK_INDEX_REQUEST_SIZE = 1000;

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the global cluster carried
     * on across test suites.
     */
    private static ImmutableTestCluster currentCluster;

    private static final double TRANSPORT_CLIENT_RATIO = transportClientRatio();

    private static final Map<Class<?>, ImmutableTestCluster> clusters = new IdentityHashMap<>();

    private static ElasticsearchIntegrationTest INSTANCE = null; // see @SuiteScope

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeGlobalCluster();
        initializeSuiteScope();
    }

    private static void initializeGlobalCluster() {
        // Initialize lazily. No need for volatiles/ CASs since each JVM runs at most one test
        // suite at any given moment.
        if (GLOBAL_CLUSTER == null) {
            String cluster = System.getProperty(TESTS_CLUSTER);
            if (Strings.hasLength(cluster)) {
                String[] stringAddresses = cluster.split(",");
                TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
                int i = 0;
                for (String stringAddress : stringAddresses) {
                    String[] split = stringAddress.split(":");
                    if (split.length < 2) {
                        throw new IllegalArgumentException("address [" + cluster + "] not valid");
                    }
                    try {
                        transportAddresses[i++] = new InetSocketTransportAddress(split[0], Integer.valueOf(split[1]));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("port is not valid, expected number but was [" + split[1] + "]");
                    }
                }
                GLOBAL_CLUSTER = new ExternalTestCluster(transportAddresses);
            } else {
                long masterSeed = SeedUtils.parseSeed(RandomizedContext.current().getRunnerSeedAsString());
                GLOBAL_CLUSTER = new TestCluster(masterSeed, clusterName("shared", ElasticsearchTestCase.CHILD_VM_ID, masterSeed));
            }
        }
    }

    protected final void beforeInternal() throws IOException {
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
            immutableCluster().beforeTest(getRandom(), getPerTestTransportClientRatio(), COMPATIBILITY_VERSION);
            immutableCluster().wipe();
            immutableCluster().randomIndexTemplate();
            logger.info("[{}#{}]: before test", getTestClass().getSimpleName(), getTestName());
        } catch (OutOfMemoryError e) {
            if (e.getMessage().contains("unable to create new native thread")) {
                ElasticsearchTestCase.printStackDump(logger);
            }
            throw e;
        }
    }

    public ImmutableTestCluster buildAndPutCluster(Scope currentClusterScope, boolean createIfExists) throws IOException {
        ImmutableTestCluster testCluster = clusters.get(this.getClass());
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
            for (ImmutableTestCluster cluster : clusters.values()) {
                cluster.close();
            }
            clusters.clear();
        }
    }

    protected final void afterInternal() throws IOException {
        boolean success = false;
        try {
            logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
            final Scope currentClusterScope = getCurrentClusterScope();
            try {
                if (currentClusterScope != Scope.TEST) {
                    MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
                    assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().getAsMap(), metaData
                            .persistentSettings().getAsMap().size(), equalTo(0));
                    assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().getAsMap(), metaData
                            .transientSettings().getAsMap().size(), equalTo(0));
                }
                immutableCluster().wipe(); // wipe after to make sure we fail in the test that didn't ack the delete
                immutableCluster().assertAfterTest();
            } finally {
                if (currentClusterScope == Scope.TEST) {
                    clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
                }
            }
            logger.info("[{}#{}]: cleaned up after test", getTestClass().getSimpleName(), getTestName());
            success = true;
        } catch (OutOfMemoryError e) {
            if (e.getMessage().contains("unable to create new native thread")) {
                ElasticsearchTestCase.printStackDump(logger);
            }
            throw e;
        } finally {
            if (!success) {
                // if we failed that means that something broke horribly so we should
                // clear all clusters and if the current cluster is the global we shut that one
                // down as well to prevent subsequent tests from failing due to the same problem.
                clearClusters();
                if (currentCluster == GLOBAL_CLUSTER) {
                    GLOBAL_CLUSTER.close();
                    GLOBAL_CLUSTER = null;
                    initializeGlobalCluster(); // re-init that cluster
                }
            }
            currentCluster.afterTest();
            currentCluster = null;
        }
    }

    public static ImmutableTestCluster immutableCluster() {
        return currentCluster;
    }

    public static TestCluster cluster() {
        if (!(currentCluster instanceof TestCluster)) {
            throw new UnsupportedOperationException("current test cluster is immutable");
        }
        return (TestCluster) currentCluster;
    }

    public ClusterService clusterService() {
        return cluster().clusterService();
    }

    public static Client client() {
        Client client = immutableCluster().client();
        if (frequently()) {
            client = new RandomizingClient((InternalClient) client, getRandom());
        }
        return client;
    }


    public static Iterable<Client> clients() {
        return immutableCluster();
    }

    protected int minimumNumberOfShards() {
        return ImmutableTestCluster.DEFAULT_MIN_NUM_SHARDS;
    }

    protected int maximumNumberOfShards() {
        return ImmutableTestCluster.DEFAULT_MAX_NUM_SHARDS;
    }

    protected int numberOfShards() {
        return between(minimumNumberOfShards(), maximumNumberOfShards());
    }

    protected int minimumNumberOfReplicas() {
        return 0;
    }

    protected int maximumNumberOfReplicas() {
        return immutableCluster().dataNodes() - 1;
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
        if (COMPATIBILITY_VERSION.onOrAfter(TestVersion.RANDOM_NUM_SHARDS_REPLICAS)) {
            int numberOfShards = numberOfShards();
            if (numberOfShards > 0) {
                builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            }
            int numberOfReplicas = numberOfReplicas();
            if (numberOfReplicas >= 0) {
                builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            }
        }
        return builder.build();
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged. If one of the indices
     * already exists this method will fail and wipe all the indices created so far.
     */
    public final void createIndex(String... names) {

        List<String> created = new ArrayList<>();
        for (String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success && !created.isEmpty()) {
                    immutableCluster().wipeIndices(created.toArray(new String[created.size()]));
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
            logger.debug("allowNodes: updating [{}]'s setting to [{}]", index, build.toDelimitedString(';'));
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
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs number of documents to wait for.
     * @return the actual number of docs seen.
     * @throws InterruptedException
     */
    public long waitForDocs(final long numDocs) throws InterruptedException {
        return waitForDocs(numDocs, null);
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs number of documents to wait for
     * @param indexer a {@link org.elasticsearch.test.BackgroundIndexer}. If supplied it will be first checked for documents indexed.
     *                This saves on unneeded searches.
     * @return the actual number of docs seen.
     * @throws InterruptedException
     */
    public long waitForDocs(final long numDocs, final @Nullable BackgroundIndexer indexer) throws InterruptedException {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        return waitForDocs(numDocs, 90, TimeUnit.SECONDS, indexer);
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs         number of documents to wait for
     * @param maxWaitTime     if not progress have been made during this time, fail the test
     * @param maxWaitTimeUnit the unit in which maxWaitTime is specified
     * @param indexer         a {@link org.elasticsearch.test.BackgroundIndexer}. If supplied it will be first checked for documents indexed.
     *                        This saves on unneeded searches.
     * @return the actual number of docs seen.
     * @throws InterruptedException
     */
    public long waitForDocs(final long numDocs, int maxWaitTime, TimeUnit maxWaitTimeUnit, final @Nullable BackgroundIndexer indexer)
            throws InterruptedException {
        final long[] lastKnownCount = {-1};
        long lastStartCount = -1;
        Predicate<Object> testDocs = new Predicate<Object>() {
            public boolean apply(Object o) {
                lastKnownCount[0] = indexer.totalIndexedDocs();
                if (lastKnownCount[0] >= numDocs) {
                    long count = client().prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
                    if (count == lastKnownCount[0]) {
                        // no progress - try to refresh for the next time
                        client().admin().indices().prepareRefresh().get();
                    }
                    lastKnownCount[0] = count;
                    logger.debug("[{}] docs visible for search. waiting for [{}]", lastKnownCount[0], numDocs);
                } else {
                    logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount[0], numDocs);
                }
                return lastKnownCount[0] >= numDocs;
            }
        };

        while (!awaitBusy(testDocs, maxWaitTime, maxWaitTimeUnit)) {
            if (lastStartCount == lastKnownCount[0]) {
                // we didn't make any progress
                fail("failed to reach " + numDocs + "docs");
            }
            lastStartCount = lastKnownCount[0];
        }
        return lastKnownCount[0];
    }


    /**
     * Sets the cluster's minimum master node and make sure the response is acknowledge.
     * Note: this doesn't guaranty the new settings is in effect, just that it has been received bu all nodes.
     */
    public void setMinimumMasterNodes(int n) {
        assertTrue(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                settingsBuilder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, n))
                .get().isAcknowledged());
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
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().setForce(randomBoolean()).execute().actionGet();
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
        Random random = getRandom();
        Set<String> indicesSet = new HashSet<>();
        for (IndexRequestBuilder builder : builders) {
            indicesSet.add(builder.request().index());
        }
        final String[] indices = indicesSet.toArray(new String[indicesSet.size()]);
        Collections.shuffle(builders, random);
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>> errors = new CopyOnWriteArrayList<>();
        List<CountDownLatch> inFlightAsyncOperations = new ArrayList<>();
        // If you are indexing just a few documents then frequently do it one at a time.  If many then frequently in bulk.
        if (builders.size() < FREQUENT_BULK_THRESHOLD ? frequently() : builders.size() < ALWAYS_BULK_THRESHOLD ? rarely() : false) {
            if (frequently()) {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), true, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute(new PayloadLatchedActionListener<IndexResponse, IndexRequestBuilder>(indexRequestBuilder, newLatch(inFlightAsyncOperations), errors));
                    postIndexAsyncActions(indices, inFlightAsyncOperations);
                }
            } else {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute().actionGet();
                    postIndexAsyncActions(indices, inFlightAsyncOperations);
                }
            }
        } else {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, true);
            for (List<IndexRequestBuilder> segmented : Lists.partition(builders, between(MAX_BULK_INDEX_REQUEST_SIZE / 2, MAX_BULK_INDEX_REQUEST_SIZE))) {
                BulkRequestBuilder bulkBuilder = client().prepareBulk();
                for (IndexRequestBuilder indexRequestBuilder : segmented) {
                    bulkBuilder.add(indexRequestBuilder);
                }
                BulkResponse actionGet = bulkBuilder.execute().actionGet();
                assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
            }
        }
        for (CountDownLatch operation : inFlightAsyncOperations) {
            operation.await();
        }
        final List<Throwable> actualErrors = new ArrayList<>();
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

    /**
     * Maybe refresh, optimize, or flush then always make sure there aren't too many in flight async operations.
     */
    private void postIndexAsyncActions(String[] indices, List<CountDownLatch> inFlightAsyncOperations) throws InterruptedException {
        if (rarely()) {
            if (rarely()) {
                client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenient()).execute(
                        new LatchedActionListener<RefreshResponse>(newLatch(inFlightAsyncOperations)));
            } else if (rarely()) {
                client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenient()).execute(
                        new LatchedActionListener<FlushResponse>(newLatch(inFlightAsyncOperations)));
            } else if (rarely()) {
                client().admin().indices().prepareOptimize(indices).setIndicesOptions(IndicesOptions.lenient()).setMaxNumSegments(between(1, 10)).setFlush(randomBoolean()).execute(
                        new LatchedActionListener<OptimizeResponse>(newLatch(inFlightAsyncOperations)));
            }
        }
        while (inFlightAsyncOperations.size() > MAX_IN_FLIGHT_ASYNC_INDEXES) {
            int waitFor = between(0, inFlightAsyncOperations.size() - 1);
            inFlightAsyncOperations.remove(waitFor).await();
        }
    }

    /**
     * The scope of a test cluster used together with
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} annotations on {@link org.elasticsearch.test.ElasticsearchIntegrationTest} subclasses.
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

    /**
     * Defines a cluster scope for a {@link org.elasticsearch.test.ElasticsearchIntegrationTest} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#GLOBAL} is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#GLOBAL} is default.
         */
        Scope scope() default Scope.GLOBAL;

        /**
         * Returns the number of nodes in the cluster. Default is <tt>-1</tt> which means
         * a random number of nodes is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numNodes() default -1;

        /**
         * Returns the minimum number of nodes in the cluster. Default is {@link org.elasticsearch.test.TestCluster#DEFAULT_MIN_NUM_NODES}.
         * Ignored when {@link ClusterScope#numNodes()} is set.
         */
        int minNumNodes() default TestCluster.DEFAULT_MIN_NUM_NODES;

        /**
         * Returns the maximum number of nodes in the cluster.  Default is {@link org.elasticsearch.test.TestCluster#DEFAULT_MAX_NUM_NODES}.
         * Ignored when {@link ClusterScope#numNodes()} is set.
         */
        int maxNumNodes() default TestCluster.DEFAULT_MAX_NUM_NODES;

        /**
         * Returns the transport client ratio. By default this returns <code>-1</code> which means a random
         * ratio in the interval <code>[0..1]</code> is used.
         */
        double transportClientRatio() default -1;
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
            errors.add(new Tuple<>(builder, t));
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

    private static boolean runTestScopeLifecycle() {
        return INSTANCE == null;
    }


    @Before
    public final void before() throws IOException {
        if (runTestScopeLifecycle()) {
            beforeInternal();
        }
    }


    @After
    public final void after() throws IOException {
        if (runTestScopeLifecycle()) {
            afterInternal();
        }
    }

    @AfterClass
    public static void afterClass() throws IOException {
        if (!runTestScopeLifecycle()) {
            try {
                INSTANCE.afterInternal();
            } finally {
                INSTANCE = null;
            }
        }

    }

    private static void initializeSuiteScope() throws Exception {
        Class<?> targetClass = getContext().getTargetClass();
        assert INSTANCE == null;
        if (isSuiteScope(targetClass)) {
            // note we need to do this this way to make sure this is reproducible
            INSTANCE = (ElasticsearchIntegrationTest) targetClass.newInstance();
            boolean success = false;
            try {
                INSTANCE.beforeInternal();
                INSTANCE.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (!success) {
                    afterClass();
                }
            }
        } else {
            INSTANCE = null;
        }
    }

    /**
     * This method is executed iff the test is annotated with {@link SuiteScopeTest}
     * before the first test of this class is executed.
     *
     * @see SuiteScopeTest
     */
    protected void setupSuiteScopeCluster() throws Exception {
    }

    private static boolean isSuiteScope(Class<?> clazz) {
        if (clazz == Object.class || clazz == ElasticsearchIntegrationTest.class) {
            return false;
        }
        SuiteScopeTest annotation = clazz.getAnnotation(SuiteScopeTest.class);
        if (annotation != null) {
            return true;
        }
        return isSuiteScope(clazz.getSuperclass());
    }

    /**
     * If a test is annotated with {@link org.elasticsearch.test.ElasticsearchIntegrationTest.SuiteScopeTest}
     * the checks and modifications that are applied to the used test cluster are only done after all tests
     * of this class are executed. This also has the side-effect of a suite level setup method {@link #setupSuiteScopeCluster()}
     * that is executed in a separate test instance. Variables that need to be accessible across test instances must be static.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    @Ignore
    public @interface SuiteScopeTest {
    }


}

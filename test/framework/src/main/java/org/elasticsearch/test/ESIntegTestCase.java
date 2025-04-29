/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.MockFieldFilterPlugin;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.node.NodeMocksPlugin;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.script.MockScriptService;
import org.elasticsearch.search.ConcurrentSearchTestPlugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.client.RandomizingClient;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.util.CollectionUtils.eagerPartition;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * {@link ESIntegTestCase} is an abstract base class to run integration
 * tests against a JVM private Elasticsearch Cluster. The test class supports 2 different
 * cluster scopes.
 * <ul>
 * <li>{@link Scope#TEST} - uses a new cluster for each individual test method.</li>
 * <li>{@link Scope#SUITE} - uses a cluster shared across all test methods in the same suite</li>
 * </ul>
 * <p>
 * The most common test scope is {@link Scope#SUITE} which shares a cluster per test suite.
 * <p>
 * If the test methods need specific node settings or change persistent and/or transient cluster settings {@link Scope#TEST}
 * should be used. To configure a scope for the test cluster the {@link ClusterScope} annotation
 * should be used, here is an example:
 * <pre>
 *
 * {@literal @}NodeScope(scope=Scope.TEST) public class SomeIT extends ESIntegTestCase {
 * public void testMethod() {}
 * }
 * </pre>
 * <p>
 * If no {@link ClusterScope} annotation is present on an integration test the default scope is {@link Scope#SUITE}
 * <p>
 * A test cluster creates a set of nodes in the background before the test starts. The number of nodes in the cluster is
 * determined at random and can change across tests. The {@link ClusterScope} allows configuring the initial number of nodes
 * that are created before the tests start.
 *  <pre>
 * {@literal @}NodeScope(scope=Scope.SUITE, numDataNodes=3)
 * public class SomeIT extends ESIntegTestCase {
 * public void testMethod() {}
 * }
 * </pre>
 * <p>
 * Note, the {@link ESIntegTestCase} uses randomized settings on a cluster and index level. For instance
 * each test might use different directory implementation for each test or will return a random client to one of the
 * nodes in the cluster for each call to {@link #client()}. Test failures might only be reproducible if the correct
 * system properties are passed to the test execution environment.
 * <p>
 * This class supports the following system properties (passed with -Dkey=value to the application)
 * <ul>
 * <li>-D{@value #TESTS_ENABLE_MOCK_MODULES} - a boolean value to enable or disable mock modules. This is
 * useful to test the system without asserting modules that to make sure they don't hide any bugs in production.</li>
 * <li> - a random seed used to initialize the index random context.
 * </ul>
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // doesn't work with potential multi data path from test cluster yet
public abstract class ESIntegTestCase extends ESTestCase {

    /** node names of the corresponding clusters will start with these prefixes */
    public static final String SUITE_CLUSTER_NODE_PREFIX = "node_s";
    public static final String TEST_CLUSTER_NODE_PREFIX = "node_t";

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_CLUSTER = "tests.cluster";

    /**
     * Key used to eventually switch to using an external cluster and provide the cluster name
     */
    public static final String TESTS_CLUSTER_NAME = "tests.clustername";

    /**
     * Key used to retrieve the index random seed from the index settings on a running node.
     * The value of this seed can be used to initialize a random context for a specific index.
     * It's set once per test via a generic index template.
     */
    public static final Setting<Long> INDEX_TEST_SEED_SETTING = Setting.longSetting(
        "index.tests.seed",
        0,
        Long.MIN_VALUE,
        Property.IndexScope
    );

    /**
     * A boolean value to enable or disable mock modules. This is useful to test the
     * system without asserting modules that to make sure they don't hide any bugs in
     * production.
     *
     * @see ESIntegTestCase
     */
    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";

    private static final boolean MOCK_MODULES_ENABLED = "true".equals(System.getProperty(TESTS_ENABLE_MOCK_MODULES, "true"));
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
     * Default minimum number of shards for an index
     */
    protected static final int DEFAULT_MIN_NUM_SHARDS = 1;

    /**
     * Default maximum number of shards for an index
     */
    protected static final int DEFAULT_MAX_NUM_SHARDS = 10;

    /**
     * The current cluster depending on the configured {@link Scope}.
     * By default if no {@link ClusterScope} is configured this will hold a reference to the suite cluster.
     */
    private static TestCluster currentCluster;
    private static RestClient restClient = null;

    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<>();

    private static ESIntegTestCase INSTANCE = null; // see @SuiteScope
    private static Long SUITE_SEED = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SUITE_SEED = randomLong();
        initializeSuiteScope();
    }

    @Override
    protected final boolean enableWarningsCheck() {
        // In an integ test it doesn't make sense to keep track of warnings: if the cluster is external the warnings are in another jvm,
        // if the cluster is internal the deprecation logger is shared across all nodes
        return false;
    }

    protected final void beforeInternal() throws Exception {
        final Scope currentClusterScope = getCurrentClusterScope();
        Callable<Void> setup = () -> {
            cluster().beforeTest(random());
            cluster().wipe(excludeTemplates());
            randomIndexTemplate();
            return null;
        };
        switch (currentClusterScope) {
            case SUITE -> {
                assert SUITE_SEED != null : "Suite seed was not initialized";
                currentCluster = buildAndPutCluster(currentClusterScope, SUITE_SEED);
                RandomizedContext.current().runWithPrivateRandomness(SUITE_SEED, setup);
            }
            case TEST -> {
                currentCluster = buildAndPutCluster(currentClusterScope, randomLong());
                setup.call();
            }
        }

    }

    private void printTestMessage(String message) {
        if (isSuiteScopedTest(getClass()) && (getTestName().equals("<unknown>"))) {
            logger.info("[{}]: {} suite", getTestClass().getSimpleName(), message);
        } else {
            logger.info("[{}#{}]: {} test", getTestClass().getSimpleName(), getTestName(), message);
        }
    }

    /**
     * Creates a randomized index template. This template is used to pass in randomized settings on a
     * per index basis. Allows to enable/disable the randomization for number of shards and replicas
     */
    private void randomIndexTemplate() {

        // TODO move settings for random directory etc here into the index based randomized settings.
        if (cluster().size() > 0) {
            Settings.Builder randomSettingsBuilder = setRandomIndexSettings(random(), Settings.builder());
            if (isInternalCluster()) {
                // this is only used by mock plugins and if the cluster is not internal we just can't set it
                randomSettingsBuilder.put(INDEX_TEST_SEED_SETTING.getKey(), random().nextLong());
            }

            randomSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards()).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas());

            // if the test class is annotated with SuppressCodecs("*"), it means don't use lucene's codec randomization
            // otherwise, use it, it has assertions and so on that can find bugs.
            SuppressCodecs annotation = getClass().getAnnotation(SuppressCodecs.class);
            if (annotation != null && annotation.value().length == 1 && "*".equals(annotation.value()[0])) {
                randomSettingsBuilder.put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC));
            } else {
                randomSettingsBuilder.put("index.codec", CodecService.LUCENE_DEFAULT_CODEC);
            }

            for (String setting : randomSettingsBuilder.keys()) {
                assertThat("non index. prefix setting set on index template, its a node setting...", setting, startsWith("index."));
            }
            // always default delayed allocation to 0 to make sure we have tests are not delayed
            randomSettingsBuilder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
            if (randomBoolean()) {
                randomSettingsBuilder.put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), randomBoolean());
            }
            PutIndexTemplateRequestBuilder putTemplate = indicesAdmin().preparePutTemplate("random_index_template")
                .setPatterns(Collections.singletonList("*"))
                .setOrder(0)
                .setSettings(randomSettingsBuilder);
            assertAcked(putTemplate.get());
        }
    }

    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        setRandomIndexMergeSettings(random, builder);
        setRandomIndexTranslogSettings(random, builder);

        if (random.nextBoolean()) {
            builder.put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), false);
        }

        if (random.nextBoolean()) {
            builder.put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), random.nextBoolean());
        }

        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom(random, "false", "checksum", "true"));
        }

        if (random.nextBoolean()) {
            // keep this low so we don't stall tests
            builder.put(
                UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(),
                RandomNumbers.randomIntBetween(random, 1, 15) + "ms"
            );
        }
        if (randomBoolean()) {
            builder.put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), randomBoolean());
        }
        return builder;
    }

    private static Settings.Builder setRandomIndexMergeSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(
                MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(),
                (random.nextBoolean() ? random.nextDouble() : random.nextBoolean()).toString()
            );
        }
        switch (random.nextInt(4)) {
            case 3 -> {
                final int maxThreadCount = RandomNumbers.randomIntBetween(random, 1, 4);
                final int maxMergeCount = RandomNumbers.randomIntBetween(random, maxThreadCount, maxThreadCount + 4);
                builder.put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), maxMergeCount);
                builder.put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), maxThreadCount);
            }
        }

        return builder;
    }

    private static Settings.Builder setRandomIndexTranslogSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(
                IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                new ByteSizeValue(RandomNumbers.randomIntBetween(random, 1, 300), ByteSizeUnit.MB)
            );
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB)); // just
                                                                                                                                    // don't
                                                                                                                                    // flush
        }
        if (random.nextBoolean()) {
            builder.put(
                IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(),
                RandomPicks.randomFrom(random, Translog.Durability.values())
            );
        }

        if (random.nextBoolean()) {
            builder.put(
                IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(),
                RandomNumbers.randomIntBetween(random, 100, 5000),
                TimeUnit.MILLISECONDS
            );
        }

        return builder;
    }

    private TestCluster buildWithPrivateContext(final Scope scope, final long seed) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed, () -> buildTestCluster(scope, seed));
    }

    private TestCluster buildAndPutCluster(Scope currentClusterScope, long seed) throws Exception {
        final Class<?> clazz = this.getClass();
        TestCluster testCluster = clusters.remove(clazz); // remove this cluster first
        clearClusters(); // all leftovers are gone by now... this is really just a double safety if we miss something somewhere
        switch (currentClusterScope) {
            case SUITE -> {
                if (testCluster == null) { // only build if it's not there yet
                    testCluster = buildWithPrivateContext(currentClusterScope, seed);
                }
            }
            case TEST -> {
                // close the previous one and create a new one
                if (testCluster != null) {
                    IOUtils.closeWhileHandlingException(testCluster::close);
                }
                testCluster = buildTestCluster(currentClusterScope, seed);
            }
        }
        clusters.put(clazz, testCluster);
        return testCluster;
    }

    private static void clearClusters() throws Exception {
        if (clusters.isEmpty() == false) {
            IOUtils.close(CloseableTestClusterWrapper.wrap(clusters.values()));
            clusters.clear();
        }
        if (restClient != null) {
            restClient.close();
            restClient = null;
        }
        assertBusy(() -> {
            int numChannels = RestCancellableNodeClient.getNumChannels();
            assertEquals(
                numChannels
                    + " channels still being tracked in "
                    + RestCancellableNodeClient.class.getSimpleName()
                    + " while there should be none",
                0,
                numChannels
            );
        });
    }

    private void afterInternal(boolean afterClass) throws Exception {
        boolean success = false;
        try {
            final Scope currentClusterScope = getCurrentClusterScope();
            if (isInternalCluster()) {
                internalCluster().clearDisruptionScheme();
            }
            try {
                if (cluster() != null) {
                    if (currentClusterScope != Scope.TEST) {
                        Metadata metadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().getMetadata();

                        final Set<String> persistentKeys = new HashSet<>(metadata.persistentSettings().keySet());
                        assertThat("test leaves persistent cluster metadata behind", persistentKeys, empty());

                        final Set<String> transientKeys = new HashSet<>(metadata.transientSettings().keySet());
                        assertThat("test leaves transient cluster metadata behind", transientKeys, empty());
                    }
                    ensureClusterSizeConsistency();
                    ensureClusterStateConsistency();
                    ensureClusterStateCanBeReadByNodeTool();
                    ensureClusterInfoServiceRunning();
                    beforeIndexDeletion();
                    cluster().wipe(excludeTemplates()); // wipe after to make sure we fail in the test that didn't ack the delete
                    cluster().assertAfterTest();
                    if (afterClass || currentClusterScope == Scope.TEST) {
                        cluster().close();
                    }
                }
            } finally {
                if (currentClusterScope == Scope.TEST) {
                    clearClusters(); // it is ok to leave persistent / transient cluster state behind if scope is TEST
                }
            }
            success = true;
        } finally {
            if (success == false) {
                // if we failed here that means that something broke horribly so we should clear all clusters
                // TODO: just let the exception happen, WTF is all this horseshit
                // afterTestRule.forceFailure();
            }
        }
    }

    /**
     * @return An exclude set of index templates that will not be removed in between tests.
     */
    protected Set<String> excludeTemplates() {
        return Collections.emptySet();
    }

    protected void beforeIndexDeletion() throws Exception {
        cluster().beforeIndexDeletion();
    }

    public static TestCluster cluster() {
        return currentCluster;
    }

    public static boolean isInternalCluster() {
        return (currentCluster instanceof InternalTestCluster);
    }

    public static InternalTestCluster internalCluster() {
        if (isInternalCluster() == false) {
            throw new UnsupportedOperationException("current test cluster is immutable");
        }
        return (InternalTestCluster) currentCluster;
    }

    public ClusterService clusterService() {
        return internalCluster().clusterService();
    }

    public static Client client() {
        return client(null);
    }

    public static Client client(@Nullable String node) {
        if (node != null) {
            return internalCluster().client(node);
        }
        Client client = cluster().client();
        if (frequently()) {
            client = new RandomizingClient(client, random());
        }
        return client;
    }

    public static Client dataNodeClient() {
        Client client = internalCluster().dataNodeClient();
        if (frequently()) {
            client = new RandomizingClient(client, random());
        }
        return client;
    }

    /**
     * Execute the given {@link ActionRequest} using the given {@link ActionType} and a default node client, wait for it to complete with
     * a timeout of {@link #SAFE_AWAIT_TIMEOUT}, and then return the result. An exceptional response, timeout or interrupt triggers a test
     * failure.
     */
    public static <T extends ActionResponse> T safeExecute(ActionType<T> action, ActionRequest request) {
        return safeExecute(client(), action, request);
    }

    public static Iterable<Client> clients() {
        return cluster().getClients();
    }

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
        // use either 0 or 1 replica, yet a higher amount when possible, but only rarely
        int maxNumReplicas = Math.max(0, cluster().numDataNodes() - 1);
        return frequently() ? Math.min(1, maxNumReplicas) : maxNumReplicas;
    }

    protected int numberOfReplicas() {
        return between(minimumNumberOfReplicas(), maximumNumberOfReplicas());
    }

    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        internalCluster().setDisruptionScheme(scheme);
    }

    /**
     * Creates a disruption that isolates the current master node from all other nodes in the cluster.
     *
     * @param disruptionType type of disruption to create
     * @return disruption
     */
    protected static NetworkDisruption isolateMasterDisruption(NetworkDisruption.NetworkLinkDisruptionType disruptionType) {
        final String masterNode = internalCluster().getMasterName();
        return new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(
                Collections.singleton(masterNode),
                Arrays.stream(internalCluster().getNodeNames()).filter(name -> name.equals(masterNode) == false).collect(Collectors.toSet())
            ),
            disruptionType
        );
    }

    /**
     * Returns a settings object used in {@link #createIndex(String...)} and {@link #prepareCreate(String)} and friends.
     * This method can be overwritten by subclasses to set defaults for the indices that are created by the test.
     * By default it returns a settings object that sets a random number of shards. Number of shards and replicas
     * can be controlled through specific methods.
     */
    public Settings indexSettings() {
        Settings.Builder builder = Settings.builder();
        int numberOfShards = numberOfShards();
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards);
        }
        int numberOfReplicas = numberOfReplicas();
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas);
        }
        // 30% of the time
        if (randomInt(9) < 3) {
            final String dataPath = randomAlphaOfLength(10);
            logger.info("using custom data_path for index: [{}]", dataPath);
            builder.put(IndexMetadata.SETTING_DATA_PATH, dataPath);
        }
        // always default delayed allocation to 0 to make sure we have tests are not delayed
        builder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
        if (randomBoolean()) {
            builder.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000));
        }
        if (randomBoolean()) {
            builder.put(
                INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(),
                timeValueMillis(
                    randomLongBetween(
                        0,
                        randomBoolean() ? 1000 : INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(Settings.EMPTY).millis()
                    )
                ).getStringRep()
            );
        }
        return builder.build();
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged.
     */
    public final void createIndex(String... names) {
        assertAcked(Arrays.stream(names).map(this::prepareCreate).toArray(CreateIndexRequestBuilder[]::new));
    }

    /**
     * creates an index with the given setting
     */
    public final void createIndex(String name, Settings indexSettings) {
        assertAcked(prepareCreate(name).setSettings(indexSettings));
    }

    /**
     * creates an index with the given shard and replica counts
     */
    public final void createIndex(String name, int shards, int replicas) {
        createIndex(name, indexSettings(shards, replicas).build());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public final CreateIndexRequestBuilder prepareCreate(String index) {
        return prepareCreate(index, -1);
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
        return prepareCreate(index, numNodes, Settings.builder());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}, augmented
     * by the given builder
     */
    public CreateIndexRequestBuilder prepareCreate(String index, Settings.Builder settingsBuilder) {
        return prepareCreate(index, -1, settingsBuilder);
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
    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, Settings.Builder settingsBuilder) {
        Settings.Builder builder = Settings.builder().put(indexSettings()).put(settingsBuilder.build());

        if (numNodes > 0) {
            internalCluster().ensureAtLeastNumDataNodes(numNodes);
            getExcludeSettings(numNodes, builder);
        }
        return indicesAdmin().prepareCreate(index).setSettings(builder.build());
    }

    /**
     * updates the settings for an index
     */
    public static void updateIndexSettings(Settings.Builder settingsBuilder, String... index) {
        UpdateSettingsRequestBuilder settingsRequest = indicesAdmin().prepareUpdateSettings(index);
        settingsRequest.setSettings(settingsBuilder);
        assertAcked(settingsRequest.get());
    }

    public static void setReplicaCount(int replicas, String index) {
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas), index);
    }

    private static Settings.Builder getExcludeSettings(int num, Settings.Builder builder) {
        String exclude = String.join(",", internalCluster().allDataNodesButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    /**
     * Waits for the specified data stream to have the expected number of backing indices.
     */
    public static List<String> waitForDataStreamBackingIndices(String dataStreamName, int expectedSize) {
        return waitForDataStreamIndices(dataStreamName, expectedSize, false);
    }

    /**
     * Waits for the specified data stream to have the expected number of backing or failure indices.
     */
    public static List<String> waitForDataStreamIndices(String dataStreamName, int expectedSize, boolean failureStore) {
        // We listen to the cluster state on the master node to ensure all other nodes have already acked the new cluster state.
        // This avoids inconsistencies in subsequent API calls which might hit a non-master node.
        final var listener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            final var dataStream = clusterState.metadata().dataStreams().get(dataStreamName);
            if (dataStream == null) {
                return false;
            }
            return dataStream.getDataStreamIndices(failureStore).getIndices().size() == expectedSize;
        });
        safeAwait(listener);
        final var backingIndexNames = getDataStreamBackingIndexNames(dataStreamName, failureStore);
        assertEquals(
            Strings.format(
                "Retrieved number of data stream indices doesn't match expectation for data stream [%s]. Expected %d but got %s",
                dataStreamName,
                expectedSize,
                backingIndexNames
            ),
            expectedSize,
            backingIndexNames.size()
        );
        return backingIndexNames;
    }

    /**
     * Returns a list of the data stream's backing index names.
     */
    public static List<String> getDataStreamBackingIndexNames(String dataStreamName) {
        return getDataStreamBackingIndexNames(dataStreamName, false);
    }

    /**
     * Returns a list of the data stream's backing or failure index names.
     */
    public static List<String> getDataStreamBackingIndexNames(String dataStreamName, boolean failureStore) {
        GetDataStreamAction.Response response = safeGet(
            client().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(SAFE_AWAIT_TIMEOUT, new String[] { dataStreamName })
            )
        );
        assertThat(response.getDataStreams().size(), equalTo(1));
        DataStream dataStream = response.getDataStreams().get(0).getDataStream();
        assertThat(dataStream.getName(), equalTo(dataStreamName));
        return dataStream.getDataStreamIndices(failureStore).getIndices().stream().map(Index::getName).toList();
    }

    /**
     * Waits until all nodes have no pending tasks.
     */
    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get());
        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
                PendingClusterTasksResponse pendingTasks = client.execute(
                    TransportPendingClusterTasksAction.TYPE,
                    new PendingClusterTasksRequest(TEST_REQUEST_TIMEOUT).local(true)
                ).get();
                assertThat(
                    "client " + client + " still has pending tasks " + pendingTasks,
                    pendingTasks.pendingTasks(),
                    Matchers.emptyIterable()
                );
                clusterHealth = client.admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
            }
        });
        assertNoTimeout(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get());
    }

    /** Ensures the result counts are as expected, and logs the results if different */
    public void assertResultsAndLogOnFailure(long expectedResults, SearchResponse searchResponse) {
        final TotalHits totalHits = searchResponse.getHits().getTotalHits();
        if (totalHits.value != expectedResults || totalHits.relation != TotalHits.Relation.EQUAL_TO) {
            StringBuilder sb = new StringBuilder("search result contains [");
            String value = Long.toString(totalHits.value) + (totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO ? "+" : "");
            sb.append(value).append("] results. expected [").append(expectedResults).append("]");
            String failMsg = sb.toString();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                sb.append("\n-> _index: [").append(hit.getIndex()).append("] id [").append(hit.getId()).append("]");
            }
            logger.warn("{}", sb);
            fail(failMsg);
        }
    }

    /**
     * Restricts the given index to be allocated on <code>n</code> nodes using the allocation deciders.
     * Yet if the shards can't be allocated on any other node shards for this index will remain allocated on
     * more than <code>n</code> nodes.
     */
    public void allowNodes(String index, int n) {
        assert index != null;
        internalCluster().ensureAtLeastNumDataNodes(n);
        Settings.Builder builder = Settings.builder();
        if (n > 0) {
            getExcludeSettings(n, builder);
        }
        Settings build = builder.build();
        if (build.isEmpty() == false) {
            logger.debug("allowNodes: updating [{}]'s setting to [{}]", index, build.toDelimitedString(';'));
            updateIndexSettings(builder, index);
        }
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
        return ensureColor(ClusterHealthStatus.GREEN, timeout, false, indices);
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API.
     */
    public ClusterHealthStatus ensureYellow(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), false, indices);
    }

    /**
     * Ensures the cluster has a red state via the cluster health API.
     */
    public ClusterHealthStatus ensureRed(String... indices) {
        return ensureColor(ClusterHealthStatus.RED, TimeValue.timeValueSeconds(30), false, indices);
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API and ensures the that cluster has no initializing shards
     * for the given indices
     */
    public ClusterHealthStatus ensureYellowAndNoInitializingShards(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), true, indices);
    }

    private ClusterHealthStatus ensureColor(
        ClusterHealthStatus clusterHealthStatus,
        TimeValue timeout,
        boolean waitForNoInitializingShards,
        String... indices
    ) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);

        ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indices).masterNodeTimeout(timeout)
            .timeout(timeout)
            .waitForStatus(clusterHealthStatus)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(waitForNoInitializingShards)
            // We currently often use ensureGreen or ensureYellow to check whether the cluster is back in a good state after shutting down
            // a node. If the node that is stopped is the master node, another node will become master and publish a cluster state where it
            // is master but where the node that was stopped hasn't been removed yet from the cluster state. It will only subsequently
            // publish a second state where the old master is removed. If the ensureGreen/ensureYellow is timed just right, it will get to
            // execute before the second cluster state update removes the old master and the condition ensureGreen / ensureYellow will
            // trivially hold if it held before the node was shut down. The following "waitForNodes" condition ensures that the node has
            // been removed by the master so that the health check applies to the set of nodes we expect to be part of the cluster.
            .waitForNodes(Integer.toString(cluster().size()));

        final ClusterHealthResponse clusterHealthResponse;
        try {
            clusterHealthResponse = clusterAdmin().health(healthRequest).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("interrupted while waiting for health response", e);
            throw new AssertionError("interrupted while waiting for health response", e);
        } catch (ExecutionException e) {
            logger.error("failed to get health response", e);
            throw new AssertionError("failed to get health response", e);
        }
        if (clusterHealthResponse.isTimedOut()) {
            final var allocationExplainRef = new AtomicReference<ClusterAllocationExplainResponse>();
            final var clusterStateRef = new AtomicReference<ClusterStateResponse>();
            final var pendingTasksRef = new AtomicReference<PendingClusterTasksResponse>();
            final var hotThreadsRef = new AtomicReference<String>();

            final var detailsFuture = new PlainActionFuture<Void>();
            try (var listeners = new RefCountingListener(detailsFuture)) {
                client().execute(
                    TransportClusterAllocationExplainAction.TYPE,
                    new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT),
                    listeners.acquire(allocationExplainRef::set)
                );
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).execute(listeners.acquire(clusterStateRef::set));
                client().execute(
                    TransportPendingClusterTasksAction.TYPE,
                    new PendingClusterTasksRequest(TEST_REQUEST_TIMEOUT),
                    listeners.acquire(pendingTasksRef::set)
                );
                try (var writer = new StringWriter()) {
                    new HotThreads().busiestThreads(9999).ignoreIdleThreads(false).detect(writer);
                    hotThreadsRef.set(writer.toString());
                } catch (Exception e) {
                    logger.error("exception capturing hot threads", e);
                    hotThreadsRef.set("exception capturing hot threads: " + e);
                }
            }

            try {
                detailsFuture.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("failed to get full debug details within 60s timeout", e);
            }

            logger.info(
                "{} timed out\nallocation explain:\n{}\ncluster state:\n{}\npending tasks:\n{}\nhot threads:\n{}\n",
                method,
                safeFormat(allocationExplainRef.get(), r -> Strings.toString(r.getExplanation(), true, true)),
                safeFormat(clusterStateRef.get(), r -> r.getState().toString()),
                safeFormat(pendingTasksRef.get(), r -> Strings.toString(r, true, true)),
                hotThreadsRef.get()
            );
            fail("timed out waiting for " + color + " state");
        }
        assertThat(
            "Expected at least " + clusterHealthStatus + " but got " + clusterHealthResponse.getStatus(),
            clusterHealthResponse.getStatus().value(),
            lessThanOrEqualTo(clusterHealthStatus.value())
        );
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return clusterHealthResponse.getStatus();
    }

    private static <T> String safeFormat(@Nullable T value, Function<T, String> formatter) {
        return value == null ? null : formatter.apply(value);
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
        ClusterHealthRequest request = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, new String[] {}).waitForNoRelocatingShards(true)
            .waitForEvents(Priority.LANGUID);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = clusterAdmin().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "waitForRelocation timed out (status={}), cluster state:\n{}\n{}",
                status,
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState(),
                getClusterPendingTasks()
            );
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    public static PendingClusterTasksResponse getClusterPendingTasks() {
        return getClusterPendingTasks(client());
    }

    public static PendingClusterTasksResponse getClusterPendingTasks(Client client) {
        try {
            return client.execute(TransportPendingClusterTasksAction.TYPE, new PendingClusterTasksRequest(TEST_REQUEST_TIMEOUT))
                .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            return fail(e);
        }
    }

    protected void awaitClusterState(Predicate<ClusterState> statePredicate) throws Exception {
        awaitClusterState(logger, internalCluster().getMasterName(), statePredicate);
    }

    public static void awaitClusterState(Logger logger, Predicate<ClusterState> statePredicate) throws Exception {
        awaitClusterState(logger, internalCluster().getMasterName(), statePredicate);
    }

    public static void awaitClusterState(Logger logger, String viaNode, Predicate<ClusterState> statePredicate) throws Exception {
        ClusterServiceUtils.awaitClusterState(logger, statePredicate, internalCluster().getInstance(ClusterService.class, viaNode));
    }

    public static String getNodeId(String nodeName) {
        return internalCluster().getInstance(ClusterService.class, nodeName).localNode().getId();
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs number of documents to wait for
     * @param indexer a {@link org.elasticsearch.test.BackgroundIndexer}. It will be first checked for documents indexed.
     *                This saves on unneeded searches.
     */
    public void waitForDocs(final long numDocs, final BackgroundIndexer indexer) throws Exception {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        final long maxWaitTimeMs = Math.max(90 * 1000, 200 * numDocs);

        assertBusy(() -> {
            long lastKnownCount = indexer.totalIndexedDocs();

            if (lastKnownCount >= numDocs) {
                try {
                    long count = getTotalHitsAllIndices();
                    if (count < lastKnownCount) {
                        // not caught up - try to refresh
                        indicesAdmin().prepareRefresh().get();
                        count = getTotalHitsAllIndices();
                    }
                    lastKnownCount = count;
                } catch (Exception e) { // count now acts like search and barfs if all shards failed...
                    logger.debug("failed to executed count", e);
                    throw e;
                }
            }

            if (logger.isDebugEnabled()) {
                if (lastKnownCount < numDocs) {
                    logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount, numDocs);
                } else {
                    logger.debug("[{}] docs visible for search (needed [{}])", lastKnownCount, numDocs);
                }
            }

            assertThat(lastKnownCount, greaterThanOrEqualTo(numDocs));
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);
    }

    private static long getTotalHitsAllIndices() {
        return SearchResponseUtils.getTotalHitsValue(prepareSearch().setTrackTotalHits(true).setSize(0).setQuery(matchAllQuery()));
    }

    public static SearchRequestBuilder prepareSearch(String... indices) {
        return client().prepareSearch(indices);
    }

    /**
     * Retrieves the persistent tasks with the requested task name from the given cluster state.
     */
    public static List<PersistentTasksCustomMetadata.PersistentTask<?>> findTasks(ClusterState clusterState, String taskName) {
        return findTasks(clusterState, Set.of(taskName));
    }

    /**
     * Retrieves the persistent tasks with the requested task names from the given cluster state.
     */
    public static List<PersistentTasksCustomMetadata.PersistentTask<?>> findTasks(ClusterState clusterState, Set<String> taskNames) {
        PersistentTasksCustomMetadata tasks = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks == null) {
            return List.of();
        }
        return tasks.tasks().stream().filter(t -> taskNames.contains(t.getTaskName())).toList();
    }

    /**
     * Waits for the health node to be assigned and returns the node
     * that it is assigned to.
     * Returns null if the health node is not assigned in due time.
     */
    @Nullable
    public static DiscoveryNode waitAndGetHealthNode(InternalTestCluster internalCluster) {
        DiscoveryNode[] healthNode = new DiscoveryNode[1];
        waitUntil(() -> {
            ClusterState state = internalCluster.client()
                .admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setMetadata(true)
                .setNodes(true)
                .get()
                .getState();
            healthNode[0] = HealthNode.findHealthNode(state);
            return healthNode[0] != null;
        }, 15, TimeUnit.SECONDS);
        return healthNode[0];
    }

    /**
     * Prints the current cluster state as debug logging.
     */
    public void logClusterState() {
        logger.debug(
            "cluster state:\n{}\n{}",
            clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState(),
            getClusterPendingTasks()
        );
    }

    protected void ensureClusterSizeConsistency() {
        if (cluster() != null && cluster().size() > 0) { // if static init fails the cluster can be null
            logger.trace("Check consistency for [{}] nodes", cluster().size());
            assertNoTimeout(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes(Integer.toString(cluster().size())).get());
        }
    }

    /**
     * Verifies that all nodes that have the same version of the cluster state as master have same cluster state
     */
    protected void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            doEnsureClusterStateConsistency(cluster().getNamedWriteableRegistry());
        }
    }

    protected final void doEnsureClusterStateConsistency(NamedWriteableRegistry namedWriteableRegistry) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        final List<SubscribableListener<ClusterStateResponse>> localStates = new ArrayList<>(cluster().size());
        for (Client client : cluster().getClients()) {
            localStates.add(
                SubscribableListener.newForked(
                    l -> client.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).all().setLocal(true).execute(l)
                )
            );
        }
        try (RefCountingListener refCountingListener = new RefCountingListener(future)) {
            SubscribableListener.<ClusterStateResponse>newForked(
                l -> client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).all().execute(l)
            ).andThenAccept(masterStateResponse -> {
                byte[] masterClusterStateBytes = ClusterState.Builder.toBytes(masterStateResponse.getState());
                // remove local node reference
                final ClusterState masterClusterState = ClusterState.Builder.fromBytes(
                    masterClusterStateBytes,
                    null,
                    namedWriteableRegistry
                );
                Map<String, Object> masterStateMap = convertToMap(masterClusterState);
                String masterId = masterClusterState.nodes().getMasterNodeId();
                for (SubscribableListener<ClusterStateResponse> localStateListener : localStates) {
                    localStateListener.andThenAccept(localClusterStateResponse -> {
                        byte[] localClusterStateBytes = ClusterState.Builder.toBytes(localClusterStateResponse.getState());
                        // remove local node reference
                        final ClusterState localClusterState = ClusterState.Builder.fromBytes(
                            localClusterStateBytes,
                            null,
                            namedWriteableRegistry
                        );
                        final Map<String, Object> localStateMap = convertToMap(localClusterState);
                        // Check that the non-master node has the same version of the cluster state as the master and
                        // that the master node matches the master (otherwise there is no requirement for the cluster state to
                        // match)
                        if (masterClusterState.version() == localClusterState.version()
                            && masterId.equals(localClusterState.nodes().getMasterNodeId())) {
                            try {
                                assertEquals(
                                    "cluster state UUID does not match",
                                    masterClusterState.stateUUID(),
                                    localClusterState.stateUUID()
                                );
                                // Compare JSON serialization
                                assertNull(
                                    "cluster state JSON serialization does not match",
                                    differenceBetweenMapsIgnoringArrayOrder(masterStateMap, localStateMap)
                                );
                            } catch (final AssertionError error) {
                                logger.error(
                                    "Cluster state from master:\n{}\nLocal cluster state:\n{}",
                                    masterClusterState.toString(),
                                    localClusterState.toString()
                                );
                                throw error;
                            }
                        }
                    }).addListener(refCountingListener.acquire());
                }
            }).addListener(refCountingListener.acquire());
        }
        safeGet(future);
    }

    protected void ensureClusterStateCanBeReadByNodeTool() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            final Client masterClient = client();
            Metadata metadata = masterClient.admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState().metadata();
            final Map<String, String> serializationParams = Maps.newMapWithExpectedSize(2);
            serializationParams.put("binary", "true");
            serializationParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
            final ToXContent.Params serializationFormatParams = new ToXContent.MapParams(serializationParams);

            // when comparing XContent output, do not use binary format
            final Map<String, String> compareParams = Maps.newMapWithExpectedSize(2);
            compareParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
            final ToXContent.Params compareFormatParams = new ToXContent.MapParams(compareParams);

            {
                Metadata metadataWithoutIndices = Metadata.builder(metadata).removeAllIndices().build();

                XContentBuilder builder = SmileXContent.contentBuilder();
                builder.startObject();
                ChunkedToXContent.wrapAsToXContent(metadataWithoutIndices).toXContent(builder, serializationFormatParams);
                builder.endObject();
                final BytesReference originalBytes = BytesReference.bytes(builder);

                XContentBuilder compareBuilder = SmileXContent.contentBuilder();
                compareBuilder.startObject();
                ChunkedToXContent.wrapAsToXContent(metadataWithoutIndices).toXContent(compareBuilder, compareFormatParams);
                compareBuilder.endObject();
                final BytesReference compareOriginalBytes = BytesReference.bytes(compareBuilder);

                final Metadata loadedMetadata;
                try (
                    XContentParser parser = createParser(
                        parserConfig().withRegistry(ElasticsearchNodeCommand.namedXContentRegistry),
                        SmileXContent.smileXContent,
                        originalBytes
                    )
                ) {
                    loadedMetadata = Metadata.fromXContent(parser);
                }
                builder = SmileXContent.contentBuilder();
                builder.startObject();
                ChunkedToXContent.wrapAsToXContent(loadedMetadata).toXContent(builder, compareFormatParams);
                builder.endObject();
                final BytesReference parsedBytes = BytesReference.bytes(builder);

                assertNull(
                    "cluster state XContent serialization does not match, expected "
                        + XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE)
                        + " but got "
                        + XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE),
                    differenceBetweenMapsIgnoringArrayOrder(
                        XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE).v2(),
                        XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE).v2()
                    )
                );
            }

            for (IndexMetadata indexMetadata : metadata) {
                XContentBuilder builder = SmileXContent.contentBuilder();
                builder.startObject();
                indexMetadata.toXContent(builder, serializationFormatParams);
                builder.endObject();
                final BytesReference originalBytes = BytesReference.bytes(builder);

                XContentBuilder compareBuilder = SmileXContent.contentBuilder();
                compareBuilder.startObject();
                indexMetadata.toXContent(compareBuilder, compareFormatParams);
                compareBuilder.endObject();
                final BytesReference compareOriginalBytes = BytesReference.bytes(compareBuilder);

                final IndexMetadata loadedIndexMetadata;
                try (
                    XContentParser parser = createParser(
                        parserConfig().withRegistry(ElasticsearchNodeCommand.namedXContentRegistry),
                        SmileXContent.smileXContent,
                        originalBytes
                    )
                ) {
                    loadedIndexMetadata = IndexMetadata.fromXContent(parser);
                }
                builder = SmileXContent.contentBuilder();
                builder.startObject();
                loadedIndexMetadata.toXContent(builder, compareFormatParams);
                builder.endObject();
                final BytesReference parsedBytes = BytesReference.bytes(builder);

                assertNull(
                    "cluster state XContent serialization does not match, expected "
                        + XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE)
                        + " but got "
                        + XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE),
                    differenceBetweenMapsIgnoringArrayOrder(
                        XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE).v2(),
                        XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE).v2()
                    )
                );
            }
        }
    }

    private static void ensureClusterInfoServiceRunning() {
        if (isInternalCluster() && cluster().size() > 0) {
            // ensures that the cluster info service didn't leak its async task, which would prevent future refreshes
            refreshClusterInfo();
        }
    }

    public static void refreshClusterInfo() {
        final ClusterInfoService clusterInfoService = internalCluster().getInstance(
            ClusterInfoService.class,
            internalCluster().getMasterName()
        );
        if (clusterInfoService instanceof InternalClusterInfoService) {
            ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
        }
    }

    /**
     * Ensures the cluster is in a searchable state for the given indices. This means a searchable copy of each
     * shard is available on the cluster.
     */
    protected ClusterHealthStatus ensureSearchable(String... indices) {
        // this is just a temporary thing but it's easier to change if it is encapsulated.
        return ensureGreen(indices);
    }

    protected void ensureStableCluster(int nodeCount) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30));
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue) {
        ensureStableCluster(nodeCount, timeValue, false, null);
    }

    protected void ensureStableCluster(int nodeCount, @Nullable String viaNode) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30), false, viaNode);
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue, boolean local, @Nullable String viaNode) {
        if (viaNode == null) {
            viaNode = randomFrom(internalCluster().getNodeNames());
        }
        logger.debug("ensuring cluster is stable with [{}] nodes. access node: [{}]. timeout: [{}]", nodeCount, viaNode, timeValue);
        ClusterHealthResponse clusterHealthResponse = client(viaNode).admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(nodeCount))
            .setTimeout(timeValue)
            .setLocal(local)
            .setWaitForNoRelocatingShards(true)
            .get();
        if (clusterHealthResponse.isTimedOut()) {
            ClusterStateResponse stateResponse = client(viaNode).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get();
            fail(
                "failed to reach a stable cluster of ["
                    + nodeCount
                    + "] nodes. Tried via ["
                    + viaNode
                    + "]. last cluster state:\n"
                    + stateResponse.getState()
            );
        }
        assertThat(clusterHealthResponse.isTimedOut(), is(false));
        ensureFullyConnectedCluster();
    }

    /**
     * Ensures that all nodes in the cluster are connected to each other.
     *
     * Some network disruptions may leave nodes that are not the master disconnected from each other.
     * {@link org.elasticsearch.cluster.NodeConnectionsService} will eventually reconnect but it's
     * handy to be able to ensure this happens faster
     */
    protected void ensureFullyConnectedCluster() {
        NetworkDisruption.ensureFullyConnectedCluster(internalCluster());
    }

    protected static IndexRequestBuilder prepareIndex(String index) {
        return client().prepareIndex(index);
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index).setSource(source).get();
     * </pre>
     */
    protected final DocWriteResponse index(String index, XContentBuilder source) {
        return prepareIndex(index).setSource(source).get();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index).setSource(source).get();
     * </pre>
     */
    protected final DocWriteResponse index(String index, String id, Map<String, Object> source) {
        return prepareIndex(index).setId(id).setSource(source).get();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index).setId(id).setSource(source).get();
     * </pre>
     */
    protected final DocWriteResponse index(String index, String id, XContentBuilder source) {
        return prepareIndex(index).setId(id).setSource(source).get();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index).setId(id).setSource(source).get();
     * </pre>
     */
    protected final DocWriteResponse indexDoc(String index, String id, Object... source) {
        return prepareIndex(index).setId(id).setSource(source).get();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index).setId(id).setSource(source).get();
     * </pre>
     * <p>
     * where source is a JSON String.
     */
    protected final DocWriteResponse index(String index, String id, String source) {
        return prepareIndex(index).setId(id).setSource(source, XContentType.JSON).get();
    }

    /**
     * Waits for relocations and refreshes all indices in the cluster.
     *
     * @see #waitForRelocation()
     */
    protected final BroadcastResponse refresh(String... indices) {
        waitForRelocation();
        BroadcastResponse actionGet = indicesAdmin().prepareRefresh(indices)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .get();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Flushes and refreshes all indices in the cluster
     */
    protected final void flushAndRefresh(String... indices) {
        flush(indices);
        refresh(indices);
    }

    /**
     * Flush some or all indices in the cluster.
     */
    protected final BroadcastResponse flush(String... indices) {
        waitForRelocation();
        BroadcastResponse actionGet = indicesAdmin().prepareFlush(indices).get();
        for (DefaultShardOperationFailedException failure : actionGet.getShardFailures()) {
            assertThat("unexpected flush failure " + failure.reason(), failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
        return actionGet;
    }

    /**
     * Waits for all relocations and force merge all indices in the cluster to 1 segment.
     */
    protected BroadcastResponse forceMerge() {
        waitForRelocation();
        BroadcastResponse actionGet = indicesAdmin().prepareForceMerge().setMaxNumSegments(1).get();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Returns <code>true</code> iff the given index exists otherwise <code>false</code>
     */
    protected static boolean indexExists(String index) {
        return indexExists(index, client());
    }

    /**
     * Returns <code>true</code> iff the given index exists otherwise <code>false</code>
     */
    public static boolean indexExists(String index, Client client) {
        GetIndexResponse getIndexResponse = client.admin()
            .indices()
            .prepareGetIndex()
            .setIndices(index)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED)
            .get();
        return getIndexResponse.getIndices().length > 0;
    }

    /**
     * Syntactic sugar for enabling allocation for <code>indices</code>
     */
    protected final void enableAllocation(String... indices) {
        updateIndexSettings(
            Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all"),
            indices
        );
    }

    /**
     * Syntactic sugar for disabling allocation for <code>indices</code>
     */
    protected final void disableAllocation(String... indices) {
        updateIndexSettings(
            Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"),
            indices
        );
    }

    /**
     * Returns a random admin client. This client can be pointing to any of the nodes in the cluster.
     */
    protected static AdminClient admin() {
        return client().admin();
    }

    /**
     * Returns a random cluster admin client. This client can be pointing to any of the nodes in the cluster.
     */
    protected static ClusterAdminClient clusterAdmin() {
        return admin().cluster();
    }

    /**
     * Returns a random indices admin client. This client can be pointing to any of the nodes in the cluster.
     */
    protected static IndicesAdminClient indicesAdmin() {
        return admin().indices();
    }

    public void indexRandom(boolean forceRefresh, String index, int numDocs) {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(index).setSource("field", "value");
        }
        indexRandom(forceRefresh, Arrays.asList(builders));
    }

    /**
     * Convenience method that forwards to {@link #indexRandom(boolean, List)}.
     */
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) {
        indexRandom(forceRefresh, Arrays.asList(builders));
    }

    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, IndexRequestBuilder... builders) {
        indexRandom(forceRefresh, dummyDocuments, Arrays.asList(builders));
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh if {@code true} all involved indices are refreshed
     *   once the documents are indexed. Additionally if {@code true} some
     *   empty dummy documents are may be randomly inserted into the document
     *   list and deleted once all documents are indexed. This is useful to
     *   produce deleted documents on the server side.
     * @param builders     the documents to index.
     * @see #indexRandom(boolean, boolean, java.util.List)
     */
    public void indexRandom(boolean forceRefresh, List<IndexRequestBuilder> builders) {
        indexRandom(forceRefresh, forceRefresh, builders);
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh   if {@code true} all involved indices are refreshed once the documents are indexed.
     * @param dummyDocuments if {@code true} some empty dummy documents may be randomly inserted into the document list and deleted once
     *                       all documents are indexed. This is useful to produce deleted documents on the server side.
     * @param builders       the documents to index.
     */
    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, List<IndexRequestBuilder> builders) {
        indexRandom(forceRefresh, dummyDocuments, true, builders);
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh   if {@code true} all involved indices are refreshed once the documents are indexed.
     * @param dummyDocuments if {@code true} some empty dummy documents may be randomly inserted into the document list and deleted once
     *                       all documents are indexed. This is useful to produce deleted documents on the server side.
     * @param maybeFlush     if {@code true} this method may randomly execute full flushes after index operations.
     * @param builders       the documents to index.
     */
    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, boolean maybeFlush, List<IndexRequestBuilder> builders) {
        Random random = random();
        Set<String> indices = new HashSet<>();
        builders = new ArrayList<>(builders);
        for (IndexRequestBuilder builder : builders) {
            indices.add(builder.getIndex());
        }
        Set<List<String>> bogusIds = new HashSet<>(); // (index, type, id)
        if (random.nextBoolean() && builders.isEmpty() == false && dummyDocuments) {
            builders = new ArrayList<>(builders);
            // inject some bogus docs
            final int numBogusDocs = scaledRandomIntBetween(1, builders.size() * 2);
            final int unicodeLen = between(1, 10);
            for (int i = 0; i < numBogusDocs; i++) {
                String id = "bogus_doc_" + randomRealisticUnicodeOfLength(unicodeLen) + dummmyDocIdGenerator.incrementAndGet();
                String index = RandomPicks.randomFrom(random, indices);
                bogusIds.add(Arrays.asList(index, id));
                // We configure a routing key in case the mapping requires it
                builders.add(prepareIndex(index).setId(id).setSource("{}", XContentType.JSON).setRouting(id));
            }
        }
        Collections.shuffle(builders, random());
        List<CountDownLatch> inFlightAsyncOperations = new ArrayList<>();
        // If you are indexing just a few documents then frequently do it one at a time. If many then frequently in bulk.
        final String[] indicesArray = indices.toArray(new String[] {});
        if (builders.size() < FREQUENT_BULK_THRESHOLD ? frequently() : builders.size() < ALWAYS_BULK_THRESHOLD ? rarely() : false) {
            if (frequently()) {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), true, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute(
                        new LatchedActionListener<DocWriteResponse>(ActionListener.noop(), newLatch(inFlightAsyncOperations))
                            .delegateResponse((l, e) -> fail(e))
                    );
                    postIndexAsyncActions(indicesArray, inFlightAsyncOperations, maybeFlush);
                }
            } else {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.get();
                    postIndexAsyncActions(indicesArray, inFlightAsyncOperations, maybeFlush);
                }
            }
        } else {
            List<List<IndexRequestBuilder>> partition = eagerPartition(
                builders,
                Math.min(MAX_BULK_INDEX_REQUEST_SIZE, Math.max(1, (int) (builders.size() * randomDouble())))
            );
            logger.info("Index [{}] docs async: [{}] bulk: [{}] partitions [{}]", builders.size(), false, true, partition.size());
            for (List<IndexRequestBuilder> segmented : partition) {
                BulkResponse actionGet;
                BulkRequestBuilder bulkBuilder = client().prepareBulk();
                for (IndexRequestBuilder indexRequestBuilder : segmented) {
                    bulkBuilder.add(indexRequestBuilder);
                }
                actionGet = bulkBuilder.get();
                assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
            }
        }
        for (CountDownLatch operation : inFlightAsyncOperations) {
            safeAwait(operation, TEST_REQUEST_TIMEOUT);
        }
        if (bogusIds.isEmpty() == false) {
            // delete the bogus types again - it might trigger merges or at least holes in the segments and enforces deleted docs!
            for (List<String> doc : bogusIds) {
                assertEquals(
                    "failed to delete a dummy doc [" + doc.get(0) + "][" + doc.get(1) + "]",
                    DocWriteResponse.Result.DELETED,
                    client().prepareDelete(doc.get(0), doc.get(1)).setRouting(doc.get(1)).get().getResult()
                );
            }
        }
        if (forceRefresh) {
            assertNoFailures(indicesAdmin().prepareRefresh(indicesArray).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get());
        }
    }

    private final AtomicInteger dummmyDocIdGenerator = new AtomicInteger();

    /** Disables an index block for the specified index */
    public static void disableIndexBlock(String index, String block) {
        updateIndexSettings(Settings.builder().put(block, false), index);
    }

    /** Enables an index block for the specified index */
    public static void enableIndexBlock(String index, String block) {
        if (IndexMetadata.APIBlock.fromSetting(block) == IndexMetadata.APIBlock.READ_ONLY_ALLOW_DELETE || randomBoolean()) {
            // the read-only-allow-delete block isn't supported by the add block API so we must use the update settings API here.
            updateIndexSettings(Settings.builder().put(block, true), index);
        } else {
            indicesAdmin().prepareAddBlock(IndexMetadata.APIBlock.fromSetting(block), index).get();
        }
    }

    /** Sets or unsets the cluster read_only mode **/
    public static void setClusterReadOnly(boolean value) {
        updateClusterSettings(
            value
                ? Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), value)
                : Settings.builder().putNull(Metadata.SETTING_READ_ONLY_SETTING.getKey())
        );
    }

    /** Sets cluster persistent settings **/
    public static void updateClusterSettings(Settings.Builder persistentSettings) {
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(persistentSettings).get()
        );
    }

    private static CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    /**
     * Maybe refresh, force merge, or flush then always make sure there aren't too many in flight async operations.
     */
    private void postIndexAsyncActions(String[] indices, List<CountDownLatch> inFlightAsyncOperations, boolean maybeFlush) {
        if (rarely()) {
            if (rarely()) {
                indicesAdmin().prepareRefresh(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .execute(new LatchedActionListener<>(ActionListener.noop(), newLatch(inFlightAsyncOperations)));
            } else if (maybeFlush && rarely()) {
                indicesAdmin().prepareFlush(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .execute(new LatchedActionListener<>(ActionListener.noop(), newLatch(inFlightAsyncOperations)));
            } else if (rarely()) {
                indicesAdmin().prepareForceMerge(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setMaxNumSegments(between(1, 10))
                    .setFlush(maybeFlush && randomBoolean())
                    .execute(new LatchedActionListener<>(ActionListener.noop(), newLatch(inFlightAsyncOperations)));
            }
        }
        while (inFlightAsyncOperations.size() > MAX_IN_FLIGHT_ASYNC_INDEXES) {
            // longer-than-usual timeout, see #112908
            safeAwait(inFlightAsyncOperations.remove(between(0, inFlightAsyncOperations.size() - 1)), timeValueSeconds(60));
        }
    }

    /**
     * The scope of a test cluster used together with
     * {@link ESIntegTestCase.ClusterScope} annotations on {@link ESIntegTestCase} subclasses.
     */
    public enum Scope {
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
     * Defines a cluster scope for a {@link ESIntegTestCase} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link ESIntegTestCase.Scope#SUITE} is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE })
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link ESIntegTestCase.Scope#SUITE} is default.
         */
        Scope scope() default Scope.SUITE;

        /**
         * Returns the number of nodes in the cluster. Default is {@code -1} which means
         * a random number of nodes is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numDataNodes() default -1;

        /**
         * Returns the minimum number of data nodes in the cluster. Default is {@code -1}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int minNumDataNodes() default -1;

        /**
         * Returns the maximum number of data nodes in the cluster.  Default is {@code -1}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int maxNumDataNodes() default -1;

        /**
         * Indicates whether the cluster can have dedicated master nodes. If {@code false} means data nodes will serve as master nodes
         * and there will be no dedicated master (and data) nodes. Default is {@code false} which means
         * dedicated master nodes will be randomly used.
         */
        boolean supportsDedicatedMasters() default true;

        /**
         * Indicates whether the cluster automatically manages cluster bootstrapping and the removal of any master-eligible nodes. If
         * set to {@code false} then the tests must manage these processes explicitly.
         */
        boolean autoManageMasterNodes() default true;

        /**
         * Returns the number of client nodes in the cluster. Default is {@link InternalTestCluster#DEFAULT_NUM_CLIENT_NODES}, a
         * negative value means that the number of client nodes will be randomized.
         */
        int numClientNodes() default InternalTestCluster.DEFAULT_NUM_CLIENT_NODES;
    }

    /**
     * Clears the given scroll Ids
     */
    public static void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll().setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }

    private static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        if (clazz == Object.class || clazz == ESIntegTestCase.class) {
            return null;
        }
        A annotation = clazz.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass(), annotationClass);
    }

    private Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz, ClusterScope.class);
        // if we are not annotated assume suite!
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private boolean getSupportsDedicatedMasters() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.supportsDedicatedMasters();
    }

    private boolean getAutoManageMasterNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.autoManageMasterNodes();
    }

    private int getNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.numDataNodes();
    }

    private int getMinNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.minNumDataNodes() == -1
            ? InternalTestCluster.DEFAULT_MIN_NUM_DATA_NODES
            : annotation.minNumDataNodes();
    }

    private int getMaxNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.maxNumDataNodes() == -1
            ? InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES
            : annotation.maxNumDataNodes();
    }

    private int getNumClientNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? InternalTestCluster.DEFAULT_NUM_CLIENT_NODES : annotation.numClientNodes();
    }

    /**
     * This method is used to obtain settings for the {@code N}th node in the cluster.
     * Nodes in this cluster are associated with an ordinal number such that nodes can
     * be started with specific configurations. This method might be called multiple
     * times with the same ordinal and is expected to return the same value for each invocation.
     * In other words subclasses must ensure this method is idempotent.
     */
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder()
            .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false)
            // Default the watermarks to absurdly low to prevent the tests
            // from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // by default we never cache below 10k docs in a segment,
            // bypass this limit so that caching gets some testing in
            // integration tests that usually create few documents
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), nodeOrdinal % 2 == 0)
            // wait short time for other active shards before actually deleting, default 30s not needed in tests
            .put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS))
            // randomly enable low-level search cancellation to make sure it does not alter results
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), randomBoolean())
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file")
            .put(
                TransportSearchAction.DEFAULT_PRE_FILTER_SHARD_SIZE.getKey(),
                randomFrom(1, 2, SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE)
            );
        if (randomBoolean()) {
            builder.put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), randomFrom("256B", "512B"));
            builder.put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), "1KB");
            builder.put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), randomFrom("1KB", "16KB", "64KB"));
            builder.put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE.getKey(), "256B");
        }
        return builder.build();
    }

    protected Path nodeConfigPath(int nodeOrdinal) {
        return null;
    }

    /**
     * Returns a collection of plugins that should be loaded on each node.
     */
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        final String nodePrefix = switch (scope) {
            case TEST -> TEST_CLUSTER_NODE_PREFIX;
            case SUITE -> SUITE_CLUSTER_NODE_PREFIX;
        };

        boolean supportsDedicatedMasters = getSupportsDedicatedMasters();
        int numDataNodes = getNumDataNodes();
        int minNumDataNodes;
        int maxNumDataNodes;
        if (numDataNodes >= 0) {
            minNumDataNodes = maxNumDataNodes = numDataNodes;
        } else {
            minNumDataNodes = getMinNumDataNodes();
            maxNumDataNodes = getMaxNumDataNodes();
        }
        Collection<Class<? extends Plugin>> mockPlugins = getMockPlugins();
        final NodeConfigurationSource nodeConfigurationSource = getNodeConfigSource();
        if (addMockTransportService()) {
            ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>(mockPlugins);
            // add both mock plugins - local and tcp if they are not there
            // we do this in case somebody overrides getMockPlugins and misses to call super
            if (mockPlugins.contains(getTestTransportPlugin()) == false) {
                mocks.add(getTestTransportPlugin());
            }
            mockPlugins = mocks;
        }
        return new InternalTestCluster(
            seed,
            createTempDir(),
            supportsDedicatedMasters,
            getAutoManageMasterNodes(),
            minNumDataNodes,
            maxNumDataNodes,
            InternalTestCluster.clusterName(scope.name(), seed) + "-cluster",
            nodeConfigurationSource,
            getNumClientNodes(),
            nodePrefix,
            mockPlugins,
            getClientWrapper(),
            forbidPrivateIndexSettings(),
            forceSingleDataPath(),
            autoManageVotingExclusions()
        );
    }

    private NodeConfigurationSource getNodeConfigSource() {
        Settings.Builder initialNodeSettings = Settings.builder();
        if (addMockTransportService()) {
            initialNodeSettings.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        }
        boolean enableConcurrentSearch = enableConcurrentSearch();
        if (enableConcurrentSearch) {
            initialNodeSettings.put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), 1);
        } else {
            initialNodeSettings.put(SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey(), false);
        }
        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                return Settings.builder()
                    .put(initialNodeSettings.build())
                    .put(ESIntegTestCase.this.nodeSettings(nodeOrdinal, otherSettings))
                    .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return ESIntegTestCase.this.nodeConfigPath(nodeOrdinal);
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                if (enableConcurrentSearch) {
                    List<Class<? extends Plugin>> plugins = new ArrayList<>(ESIntegTestCase.this.nodePlugins());
                    plugins.add(ConcurrentSearchTestPlugin.class);
                    return plugins;
                }
                return ESIntegTestCase.this.nodePlugins();
            }
        };
    }

    /**
     * Iff this returns true mock transport implementations are used for the test runs. Otherwise not mock transport impls are used.
     * The default is {@code true}.
     */
    protected boolean addMockTransportService() {
        return true;
    }

    /**
     * Whether we'd like to enable inter-segment search concurrency and increase the likelihood of leveraging it, by creating multiple
     * slices with a low amount of documents in them, which would not be allowed in production.
     * Default is true, can be disabled if it causes problems in specific tests.
     */
    protected boolean enableConcurrentSearch() {
        return true;
    }

    /** Returns {@code true} iff this test cluster should use a dummy http transport */
    protected boolean addMockHttpTransport() {
        return true;
    }

    /**
     * Returns {@code true} if this test cluster can use a mock internal engine. Defaults to true.
     */
    protected boolean addMockInternalEngine() {
        return true;
    }

    /**
     * Returns {@code true} if this test cluster can use the {@link MockFSIndexStore} test plugin. Defaults to true.
     */
    protected boolean addMockFSIndexStore() {
        return true;
    }

    /**
     * Returns a function that allows to wrap / filter all clients that are exposed by the test cluster. This is useful
     * for debugging or request / response pre and post processing. It also allows to intercept all calls done by the test
     * framework. By default this method returns an identity function {@link Function#identity()}.
     */
    protected Function<Client, Client> getClientWrapper() {
        return Function.identity();
    }

    /** Return the mock plugins the cluster should use */
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (MOCK_MODULES_ENABLED && randomBoolean()) { // sometimes run without those completely
            if (randomBoolean() && addMockTransportService()) {
                mocks.add(MockTransportService.TestPlugin.class);
            }
            if (addMockFSIndexStore() && randomBoolean()) {
                mocks.add(MockFSIndexStore.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(NodeMocksPlugin.class);
            }
            if (addMockInternalEngine() && randomBoolean()) {
                mocks.add(MockEngineFactoryPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockSearchService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }
        if (addMockTransportService()) {
            mocks.add(getTestTransportPlugin());
        }
        if (addMockHttpTransport()) {
            mocks.add(MockHttpTransport.TestPlugin.class);
        }
        mocks.add(TestSeedPlugin.class);
        mocks.add(AssertActionNamePlugin.class);
        mocks.add(MockScriptService.TestPlugin.class);
        return Collections.unmodifiableList(mocks);
    }

    public static final class TestSeedPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(INDEX_TEST_SEED_SETTING);
        }
    }

    public static final class AssertActionNamePlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return Arrays.asList(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    Executor executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (TransportService.isValidActionName(action) == false) {
                        throw new IllegalArgumentException(
                            "invalid action name [" + action + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES
                        );
                    }
                    return actualHandler;
                }
            });
        }
    }

    /**
     * Returns path to a random directory that can be used to create a temporary file system repo
     */
    public static Path randomRepoPath() {
        if (currentCluster instanceof InternalTestCluster) {
            return randomRepoPath(((InternalTestCluster) currentCluster).getDefaultSettings());
        }
        throw new UnsupportedOperationException("unsupported cluster type");
    }

    /**
     * Returns path to a random directory that can be used to create a temporary file system repo
     */
    public static Path randomRepoPath(Settings settings) {
        Environment environment = TestEnvironment.newEnvironment(settings);
        Path[] repoFiles = environment.repoDirs();
        assert repoFiles.length > 0;
        Path path;
        do {
            path = repoFiles[0].resolve(randomAlphaOfLength(10));
        } while (Files.exists(path));
        return path;
    }

    protected NumShards getNumShards(String index) {
        Metadata metadata = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata();
        assertThat(metadata.hasIndex(index), equalTo(true));
        int numShards = Integer.valueOf(metadata.index(index).getSettings().get(SETTING_NUMBER_OF_SHARDS));
        int numReplicas = Integer.valueOf(metadata.index(index).getSettings().get(SETTING_NUMBER_OF_REPLICAS));
        return new NumShards(numShards, numReplicas);
    }

    /**
     * Asserts that all shards are allocated on nodes matching the given node pattern.
     */
    public Set<String> assertAllShardsOnNodes(String index, String... pattern) {
        Set<String> nodes = new HashSet<>();
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final IndexShardRoutingTable indexShard = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < indexShard.size(); copy++) {
                    ShardRouting shardRouting = indexShard.shard(copy);
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndexName())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
                        nodes.add(name);
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
        return nodes;
    }

    /**
     * Asserts that all segments are sorted with the provided {@link Sort}.
     */
    public void assertSortedSegments(String indexName, Sort expectedIndexSort) {
        IndicesSegmentResponse segmentResponse = indicesAdmin().prepareSegments(indexName).get();
        IndexSegments indexSegments = segmentResponse.getIndices().get(indexName);
        for (IndexShardSegments indexShardSegments : indexSegments.getShards().values()) {
            for (ShardSegments shardSegments : indexShardSegments.shards()) {
                for (Segment segment : shardSegments) {
                    assertThat(expectedIndexSort, equalTo(segment.getSegmentSort()));
                }
            }
        }
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
    public final void setupTestCluster() throws Exception {
        if (runTestScopeLifecycle()) {
            printTestMessage("setting up");
            beforeInternal();
            printTestMessage("all set up");
        }
    }

    @After
    public final void cleanUpCluster() throws Exception {
        // Deleting indices is going to clear search contexts implicitly so we
        // need to check that there are no more in-flight search contexts before
        // we remove indices
        if (isInternalCluster()) {
            internalCluster().setBootstrapMasterNodeIndex(InternalTestCluster.BOOTSTRAP_MASTER_NODE_INDEX_AUTO);
        }
        super.ensureAllSearchContextsReleased();
        if (runTestScopeLifecycle()) {
            printTestMessage("cleaning up after");
            afterInternal(false);
            printTestMessage("cleaned up after");
        }
    }

    @Override
    protected boolean enableBigArraysReleasedCheck() {
        // checking that all big arrays have been released makes little sense for a still-running cluster, see comments in
        // #ensureAllArraysAreReleased for details
        return isSuiteScopedTest(getTestClass()) == false;
    }

    @AfterClass
    public static void afterClass() throws Exception {
        try {
            if (runTestScopeLifecycle()) {
                clearClusters();
            } else {
                INSTANCE.printTestMessage("cleaning up after");
                INSTANCE.afterInternal(true);
                MockBigArrays.ensureAllArraysAreReleased();
                checkStaticState();
            }
        } finally {
            SUITE_SEED = null;
            currentCluster = null;
            INSTANCE = null;
        }
        awaitGlobalNettyThreadsFinish();
    }

    /**
     *  After the cluster is stopped, there are a few netty threads that can linger, so we make sure we don't leak any tasks on them.
     */
    static void awaitGlobalNettyThreadsFinish() throws Exception {
        // Don't use GlobalEventExecutor#awaitInactivity. It will waste up to 1s for every call and we expect no tasks queued for it
        // except for the odd scheduled shutdown task.
        assertBusy(() -> assertEquals(0, GlobalEventExecutor.INSTANCE.pendingTasks()));
        try {
            ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void initializeSuiteScope() throws Exception {
        Class<?> targetClass = getTestClass();
        /**
         * Note we create these test class instance via reflection
         * since JUnit creates a new instance per test and that is also
         * the reason why INSTANCE is static since this entire method
         * must be executed in a static context.
         */
        assert INSTANCE == null;
        if (isSuiteScopedTest(targetClass)) {
            // note we need to do this this way to make sure this is reproducible
            INSTANCE = (ESIntegTestCase) targetClass.getConstructor().newInstance();
            boolean success = false;
            try {
                INSTANCE.printTestMessage("setup");
                INSTANCE.beforeInternal();
                INSTANCE.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (success == false) {
                    afterClass();
                }
            }
        } else {
            INSTANCE = null;
        }
    }

    /**
     * Compute a routing key that will route documents to the <code>shard</code>-th shard
     * of the provided index.
     */
    protected String routingKeyForShard(String index, int shard) {
        return internalCluster().routingKeyForShard(resolveIndex(index), shard, random());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        if (isInternalCluster() && cluster().size() > 0) {
            // If it's internal cluster - using existing registry in case plugin registered custom data
            return internalCluster().getInstance(NamedXContentRegistry.class);
        } else {
            // If it's external cluster - fall back to the standard set
            return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        }
    }

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

    /**
     * Override to return true in tests that cannot handle multiple data paths.
     */
    protected boolean forceSingleDataPath() {
        return false;
    }

    /**
     * Returns an instance of {@link RestClient} pointing to the current test cluster.
     * Creates a new client if the method is invoked for the first time in the context of the current test scope.
     * The returned client gets automatically closed when needed, it shouldn't be closed as part of tests otherwise
     * it cannot be reused by other tests anymore.
     */
    protected static synchronized RestClient getRestClient() {
        if (restClient == null) {
            restClient = createRestClient();
        }
        return restClient;
    }

    protected static RestClient createRestClient() {
        return createRestClient(null, "http");
    }

    protected static RestClient createRestClient(String node) {
        return createRestClient(client(node).admin().cluster().prepareNodesInfo("_local").get().getNodes(), null, "http");
    }

    protected static RestClient createRestClient(RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback, String protocol) {
        NodesInfoResponse nodesInfoResponse = clusterAdmin().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        return createRestClient(nodesInfoResponse.getNodes(), httpClientConfigCallback, protocol);
    }

    protected static RestClient createRestClient(
        final List<NodeInfo> nodes,
        RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback,
        String protocol
    ) {
        List<HttpHost> hosts = new ArrayList<>();
        for (NodeInfo node : nodes) {
            if (node.getInfo(HttpInfo.class) != null) {
                TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                hosts.add(new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), protocol));
            }
        }
        RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()]));
        if (httpClientConfigCallback != null) {
            builder.setHttpClientConfigCallback(httpClientConfigCallback);
        }
        return builder.build();
    }

    /**
     * This method is executed iff the test is annotated with {@link SuiteScopeTestCase}
     * before the first test of this class is executed.
     *
     * @see SuiteScopeTestCase
     */
    protected void setupSuiteScopeCluster() throws Exception {}

    protected boolean autoManageVotingExclusions() {
        // Temporary workaround until #98055 is tackled
        return true;
    }

    private static boolean isSuiteScopedTest(Class<?> clazz) {
        return clazz.getAnnotation(SuiteScopeTestCase.class) != null;
    }

    /**
     * If a test is annotated with {@link SuiteScopeTestCase}
     * the checks and modifications that are applied to the used test cluster are only done after all tests
     * of this class are executed. This also has the side-effect of a suite level setup method {@link #setupSuiteScopeCluster()}
     * that is executed in a separate test instance. Variables that need to be accessible across test instances must be static.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Target(ElementType.TYPE)
    public @interface SuiteScopeTestCase {
    }

    public static Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = indicesAdmin().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    public static String resolveCustomDataPath(String index) {
        GetIndexResponse getIndexResponse = indicesAdmin().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        return getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_DATA_PATH);
    }

    public static boolean inFipsJvm() {
        return Boolean.parseBoolean(System.getProperty(FIPS_SYSPROP));
    }

    protected void restartNodesOnBrokenClusterState(ClusterState.Builder clusterStateBuilder) throws Exception {
        Map<String, PersistedClusterStateService> lucenePersistedStateFactories = Stream.of(internalCluster().getNodeNames())
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    nodeName -> internalCluster().getInstance(PersistedClusterStateService.class, nodeName)
                )
            );
        final ClusterState clusterState = clusterStateBuilder.build();
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                final PersistedClusterStateService lucenePersistedStateFactory = lucenePersistedStateFactories.get(nodeName);
                try (PersistedClusterStateService.Writer writer = lucenePersistedStateFactory.createWriter()) {
                    writer.writeFullStateAndCommit(clusterState.term(), clusterState);
                }
                return super.onNodeStopped(nodeName);
            }
        });
    }

    /**
     * Allocate the entire capacity of a circuit breaker on a specific node
     *
     * @param targetNode The node on which to allocate
     * @param breakerName The circuit breaker to allocate
     * @return A {@link Releasable} which will de-allocate the amount allocated
     */
    protected static Releasable fullyAllocateCircuitBreakerOnNode(String targetNode, String breakerName) {
        final var circuitBreaker = internalCluster().getInstance(CircuitBreakerService.class, targetNode).getBreaker(breakerName);
        final long totalAllocated = fullyAllocate(circuitBreaker);
        return () -> circuitBreaker.addWithoutBreaking(-totalAllocated);
    }

    /**
     * Fully allocate a circuit breaker
     *
     * @param circuitBreaker The circuit breaker to allocate
     * @return the amount of bytes allocated
     */
    private static long fullyAllocate(CircuitBreaker circuitBreaker) {
        long allocationSize = 1;
        long totalAllocated = 0;
        while (true) {
            try {
                circuitBreaker.addEstimateBytesAndMaybeBreak(allocationSize, "test");
                totalAllocated += allocationSize;
            } catch (CircuitBreakingException e) {
                circuitBreaker.addWithoutBreaking(allocationSize);
                totalAllocated += allocationSize;
                break;
            }
            allocationSize <<= 1;
            assert 0 <= allocationSize;
        }
        return totalAllocated;
    }
}

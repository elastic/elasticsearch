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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import org.apache.lucene.util.AbstractRandomizedTest.IntegrationTests;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.merge.policy.*;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;
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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

/**
 * This abstract base testcase reuses a cluster instance internally and might
 * start an abitrary number of nodes in the background. This class might in the
 * future add random configureation options to created indices etc. unless
 * unless they are explicitly defined by the test.
 * <p/>
 * <p>
 * This test wipes all indices before a testcase is executed and uses
 * elasticsearch features like allocation filters to ensure an index is
 * allocated only on a certain number of nodes. The test doesn't expose explicit
 * information about the client or which client is returned, clients might be
 * node clients or transport clients and the returned client might be rotated.
 * </p>
 * <p/>
 * Tests that need more explict control over the cluster or that need to change
 * the cluster state aside of per-index settings should not use this class as a
 * baseclass. If your test modifies the cluster state with persistent or
 * transient settings the baseclass will raise and error.
 */
@Ignore
@IntegrationTests
public abstract class AbstractIntegrationTest extends ElasticsearchTestCase {
    
    public static final String INDEX_SEED_SETTING = "index.tests.seed";
    
    public static final long SHARED_CLUSTER_SEED = clusterSeed();
    
    private static final double TRANSPORT_CLIENT_RATIO = transportClientRatio();

    private static final TestCluster globalCluster = new TestCluster(SHARED_CLUSTER_SEED, TestCluster.clusterName("shared", ElasticsearchTestCase.CHILD_VM_ID, SHARED_CLUSTER_SEED));
    
    private static TestCluster currentCluster;
    
    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<Class<?>, TestCluster>();
    
    @Before
    public final void before() throws IOException {
        final Scope currentClusterScope = getCurrentClusterScope();
        switch (currentClusterScope) {
        case GLOBAL:
            clearClusters();
            currentCluster = globalCluster;
            break;
        case SUITE:
            currentCluster = buildAndPutCluster(currentClusterScope, false);
            break;
        case TEST:
            currentCluster = buildAndPutCluster(currentClusterScope, true);
            break;
        default:
           assert false : "Unknonw Scope: [" + currentClusterScope + "]";
        }
        currentCluster.beforeTest(getRandom(), getPerTestTransportClientRatio());
        wipeIndices();
        wipeTemplates();
        randomIndexTemplate();
        logger.info("[{}#{}]: before test", getTestClass().getSimpleName(), getTestName());
    }

    public TestCluster buildAndPutCluster(Scope currentClusterScope, boolean createIfExists) throws IOException {
        TestCluster testCluster = clusters.get(this.getClass());
        if (createIfExists || testCluster == null) {
            testCluster = buildTestCluster(currentClusterScope);
        } else {
            assert testCluster != null;
            clusters.remove(this.getClass());
        } 
        clearClusters();
        clusters.put(this.getClass(), testCluster);
        return testCluster;
    }
    
    private void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            IOUtils.close(clusters.values());
            clusters.clear();
        }
    }

    @After
    public void after() throws IOException {
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
                        .persistentSettings().getAsMap().size(), equalTo(0));
            
            }
            wipeIndices(); // wipe after to make sure we fail in the test that
                           // didn't ack the delete
            wipeTemplates();
            ensureAllSearchersClosed();
            ensureAllFilesClosed();
            logger.info("[{}#{}]: cleaned up after test", getTestClass().getSimpleName(), getTestName());
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
        return cluster().client();
    }
    
    private static void randomIndexTemplate() {
        if (cluster().numNodes() > 0) {
            client().admin().indices().preparePutTemplate("random_index_template")
            .setTemplate("*")
            .setOrder(0)
            .setSettings(setRandomMergePolicy(getRandom(), ImmutableSettings.builder()
                    .put(INDEX_SEED_SETTING, getRandom().nextLong())))
                    .execute().actionGet();
        }
    }
    
    
    private static ImmutableSettings.Builder setRandomMergePolicy(Random random, ImmutableSettings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                    random.nextBoolean() ? random.nextDouble() : random.nextBoolean());
        }
        Class<? extends MergePolicyProvider<?>> clazz = TieredMergePolicyProvider.class;
        switch(random.nextInt(5)) {
        case 4:
            clazz = LogByteSizeMergePolicyProvider.class;
            break;
        case 3:
            clazz = LogDocMergePolicyProvider.class;
            break;
        case 0:
            return builder; // don't set the setting at all
        }
        assert clazz != null;
        builder.put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, clazz.getName());
        return builder;
    }

    public static Iterable<Client> clients() {
        return cluster();
    }

    public ImmutableSettings.Builder randomSettingsBuilder() {
        // TODO RANDOMIZE
        return ImmutableSettings.builder();
    }
    // TODO Randomize MergePolicyProviderBase.INDEX_COMPOUND_FORMAT [true|false|"true"|"false"|[0..1]| toString([0..1])]

    public Settings getSettings() {
        return randomSettingsBuilder().build();
    }

    public static void wipeIndices(String... names) {
        if (cluster().numNodes() > 0) {
            try {
                assertAcked(client().admin().indices().prepareDelete(names));
            } catch (IndexMissingException e) {
                // ignore
            }
        }
    }

    public static void wipeIndex(String name) {
        wipeIndices(name);
    }

    /**
     * Deletes index templates, support wildcard notation.
     */
    public static void wipeTemplates(String... templates) {
        if (cluster().numNodes() > 0) {
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

    public void createIndex(String... names) {
        for (String name : names) {
            try {
                assertAcked(prepareCreate(name).setSettings(getSettings()));
                continue;
            } catch (IndexAlreadyExistsException ex) {
                wipeIndex(name);
            }
            assertAcked(prepareCreate(name).setSettings(getSettings()));
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, ImmutableSettings.builder());
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, ImmutableSettings.Builder builder) {
        cluster().ensureAtLeastNumNodes(numNodes);
        Settings settings = getSettings();
        builder.put(settings);
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

    public Set<String> getExcludeNodes(String index, int num) {
        Set<String> nodeExclude = cluster().nodeExclude(index);
        Set<String> nodesInclude = cluster().nodesInclude(index);
        if (nodesInclude.size() < num) {
            Iterator<String> limit = Iterators.limit(nodeExclude.iterator(), num - nodesInclude.size());
            while (limit.hasNext()) {
                limit.next();
                limit.remove();
            }
        } else {
            Iterator<String> limit = Iterators.limit(nodesInclude.iterator(), nodesInclude.size() - num);
            while (limit.hasNext()) {
                nodeExclude.add(limit.next());
                limit.remove();
            }
        }
        return nodeExclude;
    }

    public void allowNodes(String index, int numNodes) {
        cluster().ensureAtLeastNumNodes(numNodes);
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (numNodes > 0) {
            getExcludeSettings(index, numNodes, builder);
        }
        Settings build = builder.build();
        if (!build.getAsMap().isEmpty()) {
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    public CreateIndexRequestBuilder prepareCreate(String index) {
        return client().admin().indices().prepareCreate(index).setSettings(getSettings());
    }

    public ClusterHealthStatus ensureGreen() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

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

    public ClusterHealthStatus ensureYellow() {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }

    public static String commaString(Iterable<String> strings) {
        return Joiner.on(',').join(strings);
    }

    // utils
    protected IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }


    protected GetResponse get(String index, String type, String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected RefreshResponse refresh() {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected void flushAndRefresh() {
        flush(true);
        refresh();
    }

    protected FlushResponse flush() {
        return flush(true);
    }

    protected FlushResponse flush(boolean ignoreNotAllowed) {
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

    protected OptimizeResponse optimize() {
        waitForRelocation();
        OptimizeResponse actionGet = client().admin().indices().prepareOptimize().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<String>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator.asUnordered()) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }

    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    protected AdminClient admin() {
        return client().admin();
    }

    protected <Res extends ActionResponse> Res run(ActionRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        return actionGet;
    }

    protected <Res extends BroadcastOperationResponse> Res run(BroadcastOperationRequestBuilder<?, Res, ?> builder) {
        Res actionGet = builder.execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    // TODO move this into a base class for integration tests
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        if (builders.length == 0) {
            return;
        }
        
        Random random = getRandom();
        Set<String> indicesSet = new HashSet<String>();
        for (int i = 0; i < builders.length; i++) {
            indicesSet.add(builders[i].request().index());
        }
        final String[] indices = indicesSet.toArray(new String[0]);
        List<IndexRequestBuilder> list = Arrays.asList(builders);
        Collections.shuffle(list, random);
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>> errors = new CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Throwable>>();
        List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
        if (frequently()) {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", list.size(), true, false);
            final CountDownLatch latch = new CountDownLatch(list.size());
            latches.add(latch);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute(new PayloadLatchedActionListener<IndexResponse, IndexRequestBuilder>(indexRequestBuilder, latch, errors));
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<FlushResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIgnoreIndices(IgnoreIndices.MISSING).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches)));
                    }
                }
            }

        } else if (randomBoolean()) {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", list.size(), false, false);
            for (IndexRequestBuilder indexRequestBuilder : list) {
                indexRequestBuilder.execute().actionGet();
                if (rarely()) {
                    if (rarely()) {
                        client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<RefreshResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareFlush(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute(new LatchedActionListener<FlushResponse>(newLatch(latches)));
                    } else if (rarely()) {
                        client().admin().indices().prepareOptimize(indices).setIgnoreIndices(IgnoreIndices.MISSING).setMaxNumSegments(between(1, 10)).setFlush(random.nextBoolean()).execute(new LatchedActionListener<OptimizeResponse>(newLatch(latches)));
                    }
                }
            }
        } else {
            logger.info("Index [{}] docs async: [{}] bulk: [{}]", list.size(), false, true);
            BulkRequestBuilder bulkBuilder = client().prepareBulk();
            for (IndexRequestBuilder indexRequestBuilder : list) {
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
            assertNoFailures(client().admin().indices().prepareRefresh(indices).setIgnoreIndices(IgnoreIndices.MISSING).execute().get());
        }
    }

    private static final CountDownLatch newLatch(List<CountDownLatch> latches) {
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

    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll()
                .setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }
    
    public static enum Scope {
        GLOBAL, SUITE, TEST;
    }
    
    private ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == AbstractIntegrationTest.class) {
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
    
    
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.EMPTY;
    }
    
    protected TestCluster buildTestCluster(Scope scope) {
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

        return new TestCluster(currentClusterSeed, numNodes, TestCluster.clusterName(scope.name(), ElasticsearchTestCase.CHILD_VM_ID, currentClusterSeed), nodeSettingsSource);
    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface ClusterScope {
        Scope scope() default Scope.GLOBAL; 
        int numNodes() default -1;
        double transportClientRatio() default -1;
    }
    
    private static long clusterSeed() {
        String property = System.getProperty("tests.cluster_seed");
        if (property == null || property.isEmpty()) {
            return System.nanoTime();
        }
        return SeedUtils.parseSeed(property);
    }
    
    private static double transportClientRatio() {
        String property = System.getProperty("tests.client.ratio");
        if (property == null || property.isEmpty()) {
            return Double.NaN;
        }
        return Double.parseDouble(property);
    }

    private double getPerTestTransportClientRatio() {
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

}

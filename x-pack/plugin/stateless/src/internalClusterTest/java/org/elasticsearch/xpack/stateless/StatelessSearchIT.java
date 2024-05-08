/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesRequest;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngineTestUtils;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesRequestCacheUtils;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.Stateless.CLEAR_BLOB_CACHE_ACTION;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isUnsafe;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessSearchIT extends AbstractStatelessIntegTestCase {

    /**
     * A testing stateless plugin that extends the {@link Engine.IndexCommitListener} to count created number of commits.
     */
    public static class TestStateless extends Stateless {

        private final AtomicInteger createdCommits = new AtomicInteger(0);

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected Engine.IndexCommitListener createIndexCommitListener() {
            Engine.IndexCommitListener superListener = super.createIndexCommitListener();
            return new Engine.IndexCommitListener() {

                @Override
                public void onNewCommit(
                    ShardId shardId,
                    Store store,
                    long primaryTerm,
                    Engine.IndexCommitRef indexCommitRef,
                    Set<String> additionalFiles
                ) {
                    createdCommits.incrementAndGet();
                    superListener.onNewCommit(shardId, store, primaryTerm, indexCommitRef, additionalFiles);
                }

                @Override
                public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                    superListener.onIndexCommitDelete(shardId, deletedCommit);
                }
            };
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService
        ) {
            return new TestStatelessCommitService(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService);
        }

        private int getCreatedCommits() {
            return createdCommits.get();
        }
    }

    public static class TestStatelessCommitService extends StatelessCommitService {

        private final Map<ShardId, AtomicInteger> invocationCounters = ConcurrentCollections.newConcurrentMap();
        private final AtomicReference<CyclicBarrier> onCommitCreationBarrier = new AtomicReference<>();

        public TestStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            SharedBlobCacheWarmingService cacheWarmingService
        ) {
            super(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService);
        }

        @Override
        public void onCommitCreation(StatelessCommitRef reference) {
            final CyclicBarrier barrier = onCommitCreationBarrier.get();
            if (barrier != null) {
                safeAwait(barrier);
            }
            super.onCommitCreation(reference);
        }

        @Override
        public void ensureMaxGenerationToUploadForFlush(ShardId shardId, long generation) {
            invocationCounters.computeIfAbsent(shardId, k -> new AtomicInteger(0)).incrementAndGet();
            super.ensureMaxGenerationToUploadForFlush(shardId, generation);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return super.nodePlugins().stream().map(c -> c.equals(Stateless.class) ? TestStateless.class : c).toList();
    }

    private static int getNumberOfCreatedCommits() {
        int numberOfCreatedCommits = 0;
        for (String node : internalCluster().getNodeNames()) {
            var plugin = internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestStateless.class).findFirst().get();
            numberOfCreatedCommits += plugin.getCreatedCommits();
        }
        return numberOfCreatedCommits;
    }

    private final int numShards = randomIntBetween(1, 3);
    private final int numReplicas = randomIntBetween(1, 2);

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    public void testSearchShardsStarted() {
        startIndexNodes(numShards);
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, numReplicas).build());
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedAfterIndexShards() {
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);

        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedWithDocs() {
        startIndexNodes(numShards);
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, numReplicas).build());
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedAfterIndexShardsWithDocs() {
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }

        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas), indexName);
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }
        ensureGreen(indexName);
    }

    public void testGenerationalDocValues() throws Exception {
        startIndexNodes(numShards);
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, numReplicas).build());
        ensureGreen(indexName);

        Set<String> docIds = indexDocsWithRefreshAndGetIds(indexName, randomIntBetween(1, 100));
        flush(indexName);
        assertEquals(
            docIds.size(),
            SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()))
        );

        deleteDocsById(indexName, docIds);
        flushAndRefresh(indexName);
        assertEquals(0, SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())));

        Set<String> newDocIds = indexDocsWithRefreshAndGetIds(indexName, randomIntBetween(1, 100));
        flush(indexName);
        var getResponse = client().prepareGet().setIndex(indexName).setId(randomFrom(newDocIds)).setRefresh(true).setRealtime(true).get();
        assertTrue(getResponse.isExists());
    }

    public void testBulkRequestFailureWithWaitUntilRefresh() {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, numReplicas).build());
        ensureGreen(indexName);

        var bulkResponse = client().prepareBulk()
            .add(client().prepareUpdate(indexName, "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field", "2"))
            .setRefreshPolicy(WAIT_UNTIL)
            .get();
        assertThat(bulkResponse.getItems().length, equalTo(1));
        var failure = bulkResponse.getItems()[0].getFailure();
        assertThat("failure is " + failure, failure.getCause(), instanceOf(DocumentMissingException.class));
    }

    public void testSearchShardsNotifiedOnNewCommits() throws Exception {
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);
        startSearchNodes(numReplicas);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas), indexName);
        ensureGreen(indexName);

        final AtomicInteger searchNotifications = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addRequestHandlingBehavior(
                TransportNewCommitNotificationAction.NAME + "[u]",
                (handler, request, channel, task) -> {
                    searchNotifications.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }
            );
        }

        final AtomicInteger numberOfUploadedBCCs = new AtomicInteger(0);
        final Index index = resolveIndex(indexName);
        for (int i = 0; i < numShards; i++) {
            final IndexShard indexShard = findIndexShard(index, i);
            final IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
            indexEngine.getStatelessCommitService()
                .addConsumerForNewUploadedBcc(indexShard.shardId(), ignore -> numberOfUploadedBCCs.incrementAndGet());
        }

        final int beginningNumberOfCreatedCommits = getNumberOfCreatedCommits();

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush().setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh().get();
                case 2 -> client().admin().indices().prepareForceMerge().get();
            }
        }

        assertBusy(() -> {
            final int nonUploadedNotification = STATELESS_UPLOAD_DELAYED
                ? (getNumberOfCreatedCommits() - beginningNumberOfCreatedCommits) * numReplicas
                : 0;
            final int uploadedNotification = numberOfUploadedBCCs.get() * numReplicas;
            assertThat(
                "Search shard notifications should be equal to the number of created commits multiplied by the number of replicas.",
                searchNotifications.get(),
                equalTo(nonUploadedNotification + uploadedNotification)
            );
        });
    }

    // TODO move this test to a separate test class for refresh cost optimization
    public void testDifferentiateForFlushByRefresh() {
        final String indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final var indexShard = indexService.getShard(shardId.id());
        final var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = indexEngine.getStatelessCommitService();

        final long initialGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration));

        // External refresh does not change the max generation to upload
        logger.info("--> external refresh");
        indexDocs(indexName, randomIntBetween(1, 100));
        client().admin().indices().prepareRefresh().get(TimeValue.timeValueSeconds(10));
        final long externalRefreshedGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        assertThat(externalRefreshedGeneration, greaterThan(initialGeneration));
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration));

        // Scheduled refresh does not change the max generation to upload
        logger.info("--> external refresh");
        indexDocs(indexName, randomIntBetween(1, 100));
        final PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
        indexEngine.maybeRefresh("test", future);
        future.actionGet(TimeValue.timeValueSeconds(10));
        final long scheduledRefreshedGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        assertThat(scheduledRefreshedGeneration, greaterThan(externalRefreshedGeneration));
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration));

        // Refresh by RTG does not change max generation to upload
        logger.info("--> refresh by RTG");
        indexDocs(indexName, randomIntBetween(1, 100));
        assertTrue(isUnsafe(indexEngine.getLiveVersionMap()));
        client().prepareGet(indexName, "does-not-exist").setRealtime(true).get(TimeValue.timeValueSeconds(10));
        final long rtgRefreshedGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        assertThat(rtgRefreshedGeneration, greaterThan(scheduledRefreshedGeneration));
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration));

        // Flush updates the max generation
        logger.info("--> flush");
        indexDocs(indexName, randomIntBetween(1, 100));
        client().admin().indices().prepareFlush().setForce(randomBoolean()).get(TimeValue.timeValueSeconds(10));

        final long flushGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        assertThat(flushGeneration, greaterThan(rtgRefreshedGeneration));
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(flushGeneration));
    }

    // TODO move this test to a separate test class for refresh cost optimization
    public void testRefreshWillSetMaxUploadGenForFlushThatDoesNotWait() throws Exception {
        final String indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final var indexShard = indexService.getShard(shardId.id());
        final var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();
        final long initialGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();

        indexDocs(indexName, randomIntBetween(1, 100));

        final CyclicBarrier barrier = new CyclicBarrier(2);
        statelessCommitService.onCommitCreationBarrier.set(barrier);

        // A thread simulates scheduled refresh. It will be blocked inside the flush lock
        final Thread refreshThread = new Thread(() -> { indexEngine.maybeRefresh("test", ActionListener.noop()); });
        refreshThread.start();
        assertBusy(() -> assertThat(barrier.getNumberWaiting(), equalTo(1))); // ensure it is blocked

        // A flush that does not force nor wait, it will return immediately.
        client().admin().indices().prepareFlush().setForce(false).setWaitIfOngoing(false).get(TimeValue.timeValueSeconds(10));
        // The flush sets a flag to notify the thread that holds flush lock to set max upload gen. But does not do it on its own.
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration));

        // Unblock the refresh thread which should see the upload flag and set max upload gen accordingly
        barrier.await();
        refreshThread.join();
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration + 1));
        assertThat(statelessCommitService.getCurrentVirtualBcc(shardId), nullValue());
    }

    // TODO move this test to a separate test class for refresh cost optimization
    public void testConcurrentFlushAndMultipleRefreshesWillSetMaxUploadGenOnlyOnce() throws Exception {
        final String indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final var indexShard = indexService.getShard(shardId.id());
        final var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();

        final long initialGeneration = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(initialGeneration));

        // A flusher
        final Runnable flusher = () -> {
            safeSleep(randomLongBetween(0, 50));
            final boolean force = randomBoolean();
            client().admin()
                .indices()
                .prepareFlush()
                .setForce(force)
                .setWaitIfOngoing(force || randomBoolean())
                .get(TimeValue.timeValueSeconds(10));
        };

        // Simulate scheduled refreshes
        final Runnable scheduledRefresher = () -> {
            safeSleep(randomLongBetween(0, 50));
            indexEngine.maybeRefresh("test", ActionListener.noop());
        };

        final Runnable externalRefresher = () -> {
            safeSleep(randomLongBetween(0, 50));
            indexEngine.externalRefresh("test", ActionListener.noop());
        };

        final Runnable rtgRefresher = () -> {
            safeSleep(randomLongBetween(0, 50));
            client().prepareGet(indexName, "does-not-exist").setRealtime(true).get(TimeValue.timeValueSeconds(10));
        };

        long previousGeneration = initialGeneration;
        final int numberOfRuns = between(1, 5);
        for (int i = 0; i < numberOfRuns; i++) {
            final int count = statelessCommitService.invocationCounters.get(shardId).get();
            final List<Thread> threads = List.of(
                new Thread(flusher),
                new Thread(scheduledRefresher),
                new Thread(randomFrom(scheduledRefresher, externalRefresher, rtgRefresher))
            );
            indexDocs(indexName, randomIntBetween(1, 100));
            threads.forEach(Thread::start);
            for (Thread thread : threads) {
                thread.join();
            }
            final long generation = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
            assertThat(generation, greaterThan(previousGeneration));
            // Exactly one generation is marked for upload
            final long maxGenerationToUploadDueToFlush = statelessCommitService.getMaxGenerationToUploadForFlush(shardId);
            assertThat(maxGenerationToUploadDueToFlush, allOf(greaterThan(previousGeneration), lessThanOrEqualTo(generation)));
            assertThat(statelessCommitService.invocationCounters.get(shardId).get(), equalTo(count + 1));
            previousGeneration = generation;
        }
        // The last commit may be a refresh and not uploaded when BCC can contain more than 1 CC
        if (STATELESS_UPLOAD_DELAYED && STATELESS_UPLOAD_MAX_COMMITS > 1) {
            flushNoForceNoWait(indexName);
            assertThat(indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration(), equalTo(previousGeneration));
        }
        // All commits are uploaded
        assertThat(statelessCommitService.getCurrentVirtualBcc(shardId), nullValue());

    }

    // TODO move this test to a separate test class for refresh cost optimization
    public void testConcurrentAppendAndFreezeForVirtualBcc() throws Exception {
        final String indexNode = startIndexNode();
        startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final var indexShard = indexService.getShard(shardId.id());
        final var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();
        final ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        final BlobContainer blobContainer = objectStoreService.getBlobContainer(shardId, indexShard.getOperationPrimaryTerm());

        final AtomicLong currentGeneration = new AtomicLong(indexEngine.getLastCommittedSegmentInfos().getGeneration());
        final AtomicBoolean shouldStop = new AtomicBoolean(false);

        // Run a separate thread that races for freeze and upload
        final Runnable freezeRunner = () -> {
            while (shouldStop.get() == false) {
                final var virtualBcc = statelessCommitService.getCurrentVirtualBcc(shardId);
                if (virtualBcc == null) {
                    statelessCommitService.ensureMaxGenerationToUploadForFlush(shardId, currentGeneration.get());
                } else {
                    statelessCommitService.ensureMaxGenerationToUploadForFlush(
                        shardId,
                        randomLongBetween(virtualBcc.getPrimaryTermAndGeneration().generation(), virtualBcc.getMaxGeneration())
                    );
                }
            }
        };
        final Thread thread = new Thread(freezeRunner);
        thread.start();

        final int numberOfRuns = between(3, 8);
        for (int i = 0; i < numberOfRuns; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
            currentGeneration.set(indexEngine.getLastCommittedSegmentInfos().getGeneration());
            final String compoundCommitFileName = StatelessCompoundCommit.blobNameFromGeneration(currentGeneration.get());
            assertThat(
                compoundCommitFileName + " not found",
                blobContainer.blobExists(operationPurpose, compoundCommitFileName),
                equalTo(true)
            );
        }
        // All commits are uploaded
        assertThat(statelessCommitService.getCurrentVirtualBcc(shardId), nullValue());
        // The concurrent freeze thread ensures every generation for upload
        assertBusy(() -> assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(currentGeneration.get())));
        shouldStop.set(true);
        thread.join();
    }

    public void testRefreshNoFastRefresh() throws Exception {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);

        testRefresh(false);
    }

    public void testRefreshFastRefresh() throws Exception {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);

        final AtomicInteger unpromotableRefreshActions = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.startsWith(TransportUnpromotableShardRefreshAction.NAME)) {
                    unpromotableRefreshActions.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        testRefresh(true);

        assertThat(unpromotableRefreshActions.get(), equalTo(0));
    }

    private void testRefresh(boolean fastRefresh) throws InterruptedException, ExecutionException {
        assert cluster().numDataNodes() > 0 : "Should have already started nodes";
        final String indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(
            indexSettings(numShards, numReplicas).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), fastRefresh).build()
        );
        ensureGreen(indexName);

        List<WriteRequest.RefreshPolicy> refreshPolicies = shuffledList(List.of(NONE, WAIT_UNTIL, IMMEDIATE));
        int totalDocs = 0;
        for (WriteRequest.RefreshPolicy refreshPolicy : refreshPolicies) {
            int docsToIndex = randomIntBetween(1, 100);
            totalDocs += docsToIndex;

            logger.info(
                "Testing refresh policy [{}] expecting [{}] new documents and [{}] total documents",
                refreshPolicy,
                docsToIndex,
                totalDocs
            );

            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < docsToIndex; i++) {
                bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
            }
            bulkRequest.setRefreshPolicy(refreshPolicy);
            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            // When bulk refresh policy is NONE, we test the refresh API instead
            if (refreshPolicy == NONE) {
                assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
            } else {
                for (BulkItemResponse response : bulkResponse.getItems()) {
                    if (response.getResponse() != null) {
                        assertThat(response.getResponse().forcedRefresh(), equalTo(refreshPolicy == IMMEDIATE));
                    }
                }
            }

            final int finalTotalDocs = totalDocs;
            assertNoFailuresAndResponse(
                prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
                searchResponse -> assertEquals(
                    "Failed search hit count refresh test for bulk refresh policy: " + refreshPolicy,
                    finalTotalDocs,
                    searchResponse.getHits().getTotalHits().value
                )
            );
        }
    }

    public void testRefreshOnBulkWithNewShardAllocation() {
        startIndexNodes(1);
        startSearchNodes(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);
        int docsToIndex = randomIntBetween(10, 20);
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < docsToIndex; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }

        bulkRequest.setRefreshPolicy(WAIT_UNTIL);
        var bulkResponse = bulkRequest.get();

        assertNoFailures(bulkResponse);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value)
        );
    }

    public void testForcedRefreshIsVisibleOnNewSearchShard() throws Exception {
        startIndexNodes(1);
        startSearchNodes(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());

        int numDocs = randomIntBetween(1, 100);
        // Either forced refresh via a bulk request or an explicit API call
        indexDocsAndRefresh(indexName, numDocs);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertEquals(numDocs, searchResponse.getHits().getTotalHits().value)
        );
    }

    public void testUnpromotableRefreshFailure() {
        List<String> indexNodes = startIndexNodes(1);
        startSearchNodes(2);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        String beforeShardSearchNode = shardSearchNodeName(indexName);
        String beforeShardAllocationId = shardAllocationId(indexName);

        final MockTransportService transportService = MockTransportService.getInstance(indexNodes.get(0));
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            connection.sendRequest(requestId, action, request, options);
            if (action.equals("indices:admin/refresh/unpromotable[u]")) {
                throw new ConnectTransportException(connection.getNode(), action);
            }
        });

        var bulkRequest = client().prepareBulk();
        int docsToIndex = randomIntBetween(1, 100);
        for (int i = 0; i < docsToIndex; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        bulkRequest.setRefreshPolicy(IMMEDIATE);
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        // Wait until the shard gets re-allocated
        ensureGreen(indexName);
        assertThat(beforeShardAllocationId, not(shardAllocationId(indexName)));
        assertThat(beforeShardSearchNode, equalTo(shardSearchNodeName(indexName)));

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value)
        );
    }

    public void testScrollingSearchNotInterruptedByNewCommit() throws Exception {
        // Use one replica to ensure both searches hit the same shard
        final int numReplicas = 1;
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = indexSettings(numShards, numReplicas);
        createIndex(indexName, indexSettings.build());
        ensureGreen(indexName);

        int bulk1DocsToIndex = randomIntBetween(10, 100);
        Set<String> bulk1DocIds = indexDocsWithRefreshAndGetIds(indexName, bulk1DocsToIndex);
        Set<String> lastBulkIds = bulk1DocIds;
        long docsIndexed = bulk1DocsToIndex;
        int scrollSize = randomIntBetween(10, 100);
        long docsDeleted = 0;
        int scrolls = (int) Math.ceil((float) bulk1DocsToIndex / scrollSize);
        // The scrolling search should only see docs from the first bulk
        Set<String> scrollSearchDocsSeen = new HashSet<>();
        final AtomicReference<String> currentScrollId = new AtomicReference<>();
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setSize(scrollSize).setScroll(TimeValue.timeValueMinutes(2)),
            scrollSearchResponse -> {
                assertThat(scrollSearchResponse.getHits().getTotalHits().value, equalTo((long) bulk1DocsToIndex));
                Arrays.stream(scrollSearchResponse.getHits().getHits()).map(SearchHit::getId).forEach(scrollSearchDocsSeen::add);
                currentScrollId.set(scrollSearchResponse.getScrollId());
            }
        );
        try {
            for (int i = 1; i < scrolls; i++) {
                if (randomBoolean()) {
                    // delete at least one doc
                    int docsToDelete = randomIntBetween(1, lastBulkIds.size());
                    var deletedDocIds = randomSubsetOf(docsToDelete, lastBulkIds);
                    deleteDocsById(indexName, deletedDocIds);
                    docsDeleted += deletedDocIds.size();
                }
                var docsToIndex = randomIntBetween(10, 100);
                lastBulkIds = indexDocsWithRefreshAndGetIds(indexName, docsToIndex);
                docsIndexed += docsToIndex;
                // make sure new docs are visible to new searches
                final long expectedDocs = docsIndexed - docsDeleted;
                assertNoFailuresAndResponse(
                    prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
                    searchResponse -> assertEquals(expectedDocs, searchResponse.getHits().getTotalHits().value)
                );
                // fetch next scroll
                assertNoFailuresAndResponse(
                    client().prepareSearchScroll(currentScrollId.get()).setScroll(TimeValue.timeValueMinutes(2)),
                    scrollSearchResponse -> {
                        assertThat(scrollSearchResponse.getHits().getTotalHits().value, equalTo((long) bulk1DocsToIndex));
                        scrollSearchDocsSeen.addAll(
                            Arrays.stream(scrollSearchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet())
                        );
                        currentScrollId.set(scrollSearchResponse.getScrollId());
                    }
                );
            }
            assertThat(scrollSearchDocsSeen, equalTo(bulk1DocIds));
        } finally {
            clearScroll(currentScrollId.get());
        }
    }

    public void testAcquiredPrimaryTermAndGenerations() {
        startIndexNode();
        startSearchNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        final Supplier<Set<PrimaryTermAndGeneration>> latestPrimaryTermAndGenerationDependencies = () -> {
            var indexShardEngineOrNull = findIndexShard(resolveIndex(indexName), 0).getEngineOrNull();
            assertThat(indexShardEngineOrNull, notNullValue());
            IndexEngine indexEngine = (IndexEngine) indexShardEngineOrNull;
            var currentGeneration = indexEngine.getCurrentGeneration();
            var openReaders = IndexEngineTestUtils.getOpenReaders(indexEngine);
            for (Map.Entry<DirectoryReader, Set<PrimaryTermAndGeneration>> directoryReaderSetEntry : openReaders.entrySet()) {
                var directoryReader = directoryReaderSetEntry.getKey();
                if (IndexEngineTestUtils.getLatestCommittedGeneration(directoryReader) == currentGeneration) {
                    return directoryReaderSetEntry.getValue();
                }
            }
            throw new AssertionError("Expected to find the reader for the current generation " + currentGeneration);
        };

        var searchShardEngineOrNull = findSearchShard(resolveIndex(indexName), 0).getEngineOrNull();
        assertThat(searchShardEngineOrNull, instanceOf(SearchEngine.class));
        var searchEngine = (SearchEngine) searchShardEngineOrNull;
        assertThat(
            latestPrimaryTermAndGenerationDependencies.get(),
            everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations())))
        );

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        final AtomicReference<String> firstScrollId = new AtomicReference<>();
        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), firstScroll -> {
            assertThat(firstScroll.getHits().getTotalHits().value, equalTo(100L));
            firstScrollId.set(firstScroll.getScrollId());
        });

        var firstScrollPrimaryTermAndGenerations = latestPrimaryTermAndGenerationDependencies.get();
        assertThat(firstScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        final AtomicReference<String> secondScrollId = new AtomicReference<>();
        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), secondScroll -> {
            assertThat(secondScroll.getHits().getTotalHits().value, equalTo(200L));
            secondScrollId.set(secondScroll.getScrollId());
        });

        var secondScrollPrimaryTermAndGenerations = latestPrimaryTermAndGenerationDependencies.get();
        assertThat(secondScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

        clearScroll(firstScrollId.get());

        assertThat(secondScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), thirdScroll -> {
            assertThat(thirdScroll.getHits().getTotalHits().value, equalTo(300L));

            var thirdScrollPrimaryTermAndGenerations = latestPrimaryTermAndGenerationDependencies.get();
            assertThat(secondScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));
            assertThat(thirdScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

            clearScroll(thirdScroll.getScrollId());
            indexDocs(indexName, 1);
            flushAndRefresh(indexName);

            assertThat(thirdScrollPrimaryTermAndGenerations, not(equalTo(latestPrimaryTermAndGenerationDependencies.get())));
            assertThat(secondScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));
            assertThat(
                latestPrimaryTermAndGenerationDependencies.get(),
                everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations())))
            );
        });

        clearScroll(secondScrollId.get());

        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), equalTo(latestPrimaryTermAndGenerationDependencies.get()));
    }

    public void testSearchNotInterruptedByNewCommit() throws Exception {
        // Use one replica to ensure both searches hit the same shard
        final int numReplicas = 1;
        // Use at least two shards to ensure there will always be a FETCH phase
        final int numShards = randomIntBetween(2, 3);
        startIndexNodes(numShards);
        String coordinatingSearchNode = startSearchNode();
        startSearchNodes(numReplicas);
        // create index on all nodes except one search node
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = indexSettings(numShards, numReplicas).put("index.routing.allocation.exclude._name", coordinatingSearchNode);
        createIndex(indexName, indexSettings.build());
        ensureGreen(indexName);
        int bulk1DocsToIndex = randomIntBetween(100, 200);
        indexDocsAndRefresh(indexName, bulk1DocsToIndex);
        // Index more docs in between the QUERY and the FETCH phase of the search
        final MockTransportService transportService = MockTransportService.getInstance(coordinatingSearchNode);
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch secondBulkIndexed = new CountDownLatch(1);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(SearchTransportService.FETCH_ID_ACTION_NAME)) {
                try {
                    fetchStarted.countDown();
                    secondBulkIndexed.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        CountDownLatch searchFinished = new CountDownLatch(1);
        client(coordinatingSearchNode).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).execute(new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) bulk1DocsToIndex));
                searchFinished.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });
        fetchStarted.await();
        int bulk2DocsToIndex = randomIntBetween(10, 100);
        indexDocsAndRefresh(indexName, bulk2DocsToIndex);
        // Verify that new docs are visible to new searches
        assertNoFailuresAndResponse(
            client(coordinatingSearchNode).prepareSearch(indexName)
                .setSize(0)  // Avoid a FETCH phase
                .setQuery(QueryBuilders.matchAllQuery()),
            search2Response -> {
                assertEquals(bulk1DocsToIndex + bulk2DocsToIndex, search2Response.getHits().getTotalHits().value);
                secondBulkIndexed.countDown();
            }
        );
        searchFinished.await();
    }

    public void testRequestCache() {
        startMasterOnlyNode();
        int numberOfShards = 1;
        startIndexNodes(numberOfShards);
        startSearchNodes(numberOfShards);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(numberOfShards, numberOfShards).put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(indexName);

        List<Integer> data = randomList(4, 64, ESTestCase::randomInt);
        for (int i = 0; i < data.size(); i++) {
            indexDocWithRange(indexName, String.valueOf(i + 1), data.get(i));
        }
        refresh(indexName);

        // Use a fixed client in order to avoid randomizing timeouts which leads to different cache entries
        var client = client();
        assertRequestCacheStats(client, indexName, equalTo(0L), 0, 0);

        int min = Collections.min(data);
        int max = Collections.max(data);
        var cacheMiss = countDocsInRange(client, indexName, min, max);
        try {
            assertThat(cacheMiss.getHits().getTotalHits().value, equalTo((long) data.size()));
        } finally {
            cacheMiss.decRef();
        }
        assertRequestCacheStats(client, indexName, greaterThan(0L), 0, 1);

        int nbSearchesWithCacheHits = randomIntBetween(1, 10);
        for (int i = 0; i < nbSearchesWithCacheHits; i++) {
            var cacheHit = countDocsInRange(client, indexName, min, max);
            try {
                assertThat(cacheHit.getHits().getTotalHits().value, equalTo((long) data.size()));
                assertRequestCacheStats(client, indexName, greaterThan(0L), i + 1, 1);
            } finally {
                cacheHit.decRef();
            }
        }

        List<Integer> moreData = randomList(4, 64, () -> randomIntBetween(min, max));
        for (int i = 0; i < moreData.size(); i++) {
            indexDocWithRange(indexName, String.valueOf(data.size() + i + 1), moreData.get(i));
        }
        // refresh forces a reopening of the reader on the search shard. Because the reader is part of the request
        // cache key further count requests will account for cache misses
        refresh(indexName);

        var cacheMissDueRefresh = countDocsInRange(client, indexName, min, max);
        try {
            assertThat(cacheMissDueRefresh.getHits().getTotalHits().value, equalTo((long) (data.size() + moreData.size())));
        } finally {
            cacheMissDueRefresh.decRef();
        }
        assertRequestCacheStats(client, indexName, greaterThan(0L), nbSearchesWithCacheHits, 2);

        // Verify that the request cache evicts the closed index
        client().admin().indices().prepareClose(indexName).get();
        ensureGreen(indexName);
        for (var indicesService : internalCluster().getInstances(IndicesService.class)) {
            var indicesRequestCache = IndicesRequestCacheUtils.getRequestCache(indicesService);
            IndicesRequestCacheUtils.cleanCache(indicesRequestCache);
            assertThat(Iterables.size(IndicesRequestCacheUtils.cachedKeys(indicesRequestCache)), equalTo(0L));
        }
    }

    public void testIndexSort() {
        startMasterOnlyNode();
        final int numberOfShards = 1;
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName, indexSettings(numberOfShards, 0).put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "rank"))
                .setMapping("rank", "type=integer")
                .get()
        );
        ensureGreen(indexName);

        index(indexName, "1", Map.of("rank", 4));
        index(indexName, "2", Map.of("rank", 1));
        index(indexName, "3", Map.of("rank", 3));
        index(indexName, "4", Map.of("rank", 2));

        refresh(indexName);

        index(indexName, "5", Map.of("rank", 8));
        index(indexName, "6", Map.of("rank", 6));
        index(indexName, "7", Map.of("rank", 5));
        index(indexName, "8", Map.of("rank", 7));

        refresh(indexName);

        startSearchNodes(1);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        assertResponse(prepareSearch(indexName).setSource(new SearchSourceBuilder().sort("rank")).setSize(1), searchResponse -> {
            assertHitCount(searchResponse, 8L);
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        });

        assertResponse(
            prepareSearch(indexName).setSource(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("rank").from(0)).sort("rank"))
                .setTrackTotalHits(false)
                .setSize(1),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits(), nullValue());
                assertThat(searchResponse.getHits().getHits().length, equalTo(1));
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
            }
        );

        assertNoFailures(client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        refresh(indexName);

        assertResponse(prepareSearch(indexName).setSource(new SearchSourceBuilder().sort("_doc")), searchResponse -> {
            assertHitCount(searchResponse, 8L);
            assertThat(searchResponse.getHits().getHits().length, equalTo(8));
            assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
            assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("4"));
            assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
            assertThat(searchResponse.getHits().getAt(3).getId(), equalTo("1"));
            assertThat(searchResponse.getHits().getAt(4).getId(), equalTo("7"));
            assertThat(searchResponse.getHits().getAt(5).getId(), equalTo("6"));
            assertThat(searchResponse.getHits().getAt(6).getId(), equalTo("8"));
            assertThat(searchResponse.getHits().getAt(7).getId(), equalTo("5"));
        });

        assertResponse(
            prepareSearch(indexName).setSource(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("rank").from(0)).sort("rank"))
                .setTrackTotalHits(false)
                .setSize(3),
            searchResponse -> {
                assertThat(searchResponse.getHits().getTotalHits(), nullValue());
                assertThat(searchResponse.getHits().getHits().length, equalTo(3));
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
                assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("4"));
                assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
            }
        );

        var exception = expectThrows(
            ActionRequestValidationException.class,
            () -> client().prepareSearch(indexName)
                .setSource(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("rank").from(0)).sort("rank"))
                .setTrackTotalHits(false)
                .setScroll(TimeValue.timeValueMinutes(1))
                .setSize(3)
                .get()
                .decRef()
        );
        assertThat(exception.getMessage(), containsString("disabling [track_total_hits] is not allowed in a scroll context"));
    }

    public void testSearchWithWaitForCheckpoint() throws ExecutionException, InterruptedException {
        startMasterOnlyNode();
        var indexNode = startIndexNodes(1).get(0);
        startSearchNodes(1);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        final int docCount = randomIntBetween(1, 100);
        indexDocs(indexName, docCount);
        final Index index = clusterAdmin().prepareState().get().getState().metadata().index(indexName).getIndex();
        var seqNoStats = clusterAdmin().prepareNodesStats(indexNode)
            .setIndices(true)
            .get()
            .getNodes()
            .get(0)
            .getIndices()
            .getShardStats(index)
            .get(0)
            .getShards()[0].getSeqNoStats();
        boolean refreshBefore = randomBoolean();
        if (refreshBefore) {
            refresh(indexName);
            // TODO Revisit the following flush call once ES-8275 is resolved
            flushNoForceNoWait(indexName);
        }
        var searchFuture = client().prepareSearch(indexName)
            .setWaitForCheckpoints(Map.of(indexName, new long[] { seqNoStats.getGlobalCheckpoint() }))
            .execute();
        if (refreshBefore == false) {
            refresh(indexName);
            // TODO Revisit the following flush call once ES-8275 is resolved
            flushNoForceNoWait(indexName);
        }
        assertHitCount(searchFuture, docCount);
    }

    public void testFastRefreshSearch() throws Exception {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);
        final String indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(numShards, numReplicas).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());
        ensureGreen(indexName);
        int docsToIndex = randomIntBetween(1, 100);
        indexDocsAndRefresh(indexName, docsToIndex);

        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.contains(TransportSearchAction.NAME)) {
                    assertThat(connection.getNode().getRoles(), contains(DiscoveryNodeRole.INDEX_ROLE));
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value)
        );
    }

    public void testFastRefreshGetAndMGet() {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);
        final String indexName = SYSTEM_INDEX_NAME;
        createSystemIndex(indexSettings(numShards, numReplicas).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());
        ensureGreen(indexName);

        var bulkRequest = client().prepareBulk();
        int customDocs = randomIntBetween(5, 10);
        for (int i = 0; i < customDocs; i++) {
            var indexRequest = new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25));
            if (randomBoolean()) {
                indexRequest.id(String.valueOf(i));
            }
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        final AtomicInteger fromTranslogActionsSent = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.startsWith(TransportGetAction.TYPE.name()) || action.startsWith(TransportMultiGetAction.NAME)) {
                    assertThat(connection.getNode().getRoles(), contains(DiscoveryNodeRole.INDEX_ROLE));
                } else if (action.startsWith(TransportGetFromTranslogAction.NAME)
                    || action.startsWith(TransportShardMultiGetFomTranslogAction.NAME)) {
                        fromTranslogActionsSent.incrementAndGet();
                    }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        // Test get
        {
            int i = randomInt(customDocs - 1);
            String id = bulkResponse.getItems()[i].getId();
            boolean realtime = randomBoolean();
            final var get = client().prepareGet(indexName, id).setRealtime(realtime);
            if (realtime) {
                assertTrue(get.get().isExists());
                assertThat(get.get().getVersion(), equalTo(bulkResponse.getItems()[i].getVersion()));
            }
            assertThat(get.get().getId(), equalTo(id));
        }

        // Test mget
        {
            boolean realtime = randomBoolean();
            final var mget = client().prepareMultiGet().setRealtime(realtime);
            int idStartInclusive = randomInt(customDocs - 1);
            int idEndExclusive = randomIntBetween(idStartInclusive + 1, customDocs);
            int[] ids = IntStream.range(idStartInclusive, idEndExclusive).toArray();
            String[] stringIds = Arrays.stream(ids).mapToObj(i -> bulkResponse.getItems()[i].getId()).toArray(String[]::new);
            mget.addIds(indexName, stringIds);
            MultiGetResponse response = mget.get();
            Arrays.stream(ids).forEach(i -> {
                int id = i - idStartInclusive;
                if (realtime) {
                    assertTrue(response.getResponses()[id].getResponse().isExists());
                    assertThat(response.getResponses()[id].getResponse().getVersion(), equalTo(bulkResponse.getItems()[id].getVersion()));
                }
                assertThat(response.getResponses()[id].getId(), equalTo(stringIds[id]));
            });
        }

        assertThat(fromTranslogActionsSent.get(), equalTo(0));
    }

    /**
     * Tests that an index shard will not retain commits in the blob store for active readers on search nodes that no longer own the search
     * shard. The index shard only tracks commits in use by search nodes that own a shard, not search nodes that used to own a shard replica
     * and still have active readers depending on old shard commits.
     *
     * This is behavior that ES-6685 will change / fix.
     */
    public void testRetainCommitForReadersAfterShardMovedAway() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        final String searchNodeA = startSearchNode();
        final String searchNodeB = startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1)
                // Start with the shard replica on searchNodeA.
                .put("index.routing.allocation.exclude._name", searchNodeB)
                .build()
        );
        ensureGreen(indexName);

        // Set up some data to be read.
        final int numDocsToIndex = randomIntBetween(5, 100);
        indexDocsAndRefresh(indexName, numDocsToIndex);

        // Start a scroll to pin the reader state on the search node until the scroll is exhausted / released.
        final var scrollSearchResponse = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();
        try {
            assertThat(scrollSearchResponse.getScrollId(), Matchers.is(notNullValue()));

            // Move shard away from searchNodeA while the search is still active and using the latest shard commit.
            logger.info("--> moving shards in index [{}] away from node [{}]", indexName, searchNodeA);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeA))));
            logger.info("--> shards in index [{}] have been moved off of node [{}]", indexName, searchNodeA);

            // Run some indexing and create a new commit. Then force merge down to a single segment (in another new commit). Newer commits
            // can reference information in older commit, rather than copying everything: force merge will ensure older commits are not
            // retained for this reason.
            // The indexNode should then delete the prior commits because searchNodeB is not using them, and searchNodeA is ignored because
            // the routing indicates it has no shard.
            indexDocsAndRefresh(indexName, numDocsToIndex);
            client().admin().indices().forceMerge(new ForceMergeRequest(indexName).maxNumSegments(1)).actionGet();
            refresh(indexName);

            // Evict data from all node blob caches. This should force the scroll on searchNodeA to fetch data from the remote blob store.
            client().execute(CLEAR_BLOB_CACHE_ACTION, new ClearBlobCacheNodesRequest()).get();

            // Try to fetch more data from the scroll, which should throw an error because the remote blob store no longer has the commit
            // that the reader is using.
            assertHitCount(scrollSearchResponse, numDocsToIndex);
            assertThat(scrollSearchResponse.getHits().getHits().length, equalTo(1));
            assertBusy(
                () -> assertThrows(
                    ExecutionException.class,
                    () -> client().searchScroll(new SearchScrollRequest(scrollSearchResponse.getScrollId())).get().decRef()
                )
            );
        } finally {
            // There's an implicit incRef in prepareSearch(), so call decRef() to release the response object back into the resource pool.
            scrollSearchResponse.decRef();
        }
    }

    private static SearchResponse countDocsInRange(Client client, String index, int min, int max) {
        SearchResponse response = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0) // index request cache only supports count requests
            .setQuery(QueryBuilders.rangeQuery("f").gte(min).lte(max))
            .get();
        assertNoFailures(response);
        return response;
    }

    private static void assertRequestCacheStats(
        Client client,
        String index,
        Matcher<Long> memorySize,
        long expectedHits,
        long expectedMisses
    ) {
        var requestCache = client.admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache();
        assertThat(requestCache.getMemorySize().getBytes(), memorySize);
        assertThat(requestCache.getHitCount(), equalTo(expectedHits));
        assertThat(requestCache.getMissCount(), equalTo(expectedMisses));
    }

    private static void indexDocWithRange(String index, String id, int value) {
        assertThat(client().prepareIndex(index).setId(id).setSource("f", value).get().status(), equalTo(RestStatus.CREATED));
    }

    private Set<String> indexDocsWithRefreshAndGetIds(String indexName, int numDocs) throws Exception {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        boolean bulkRefreshes = randomBoolean();
        if (bulkRefreshes) {
            bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        }
        BulkResponse response = bulkRequest.get();
        assertNoFailures(response);
        if (bulkRefreshes == false) {
            assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        }
        return Arrays.stream(response.getItems()).map(BulkItemResponse::getId).collect(Collectors.toSet());
    }

    private void deleteDocsById(String indexName, Collection<String> docIds) {
        var bulkRequest = client().prepareBulk();
        for (String id : docIds) {
            bulkRequest.add(new DeleteRequest(indexName, id));
        }
        assertNoFailures(bulkRequest.get());
    }

    private static ShardRouting searchShard(String indexName) {
        return client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .getRoutingTable()
            .index(indexName)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(sr -> sr.role() == ShardRouting.Role.SEARCH_ONLY)
            .findFirst()
            .orElseThrow();
    }

    private static String shardAllocationId(String indexName) {
        return searchShard(indexName).allocationId().getId();
    }

    private static String shardSearchNodeName(String indexName) {
        var nodeId = searchShard(indexName).currentNodeId();
        return client().admin().cluster().prepareNodesStats(nodeId).get().getNodesMap().get(nodeId).getNode().getName();
    }

    /**
     * Tests the lifecycle of active reader state saved in {@link ClosedShardService}.
     *
     * When an {@link IndexShard} with active readers is closed, the reader state should move to the {@link ClosedShardService} and be
     * retained there until all readers finish, at which point the state should be removed from {@link ClosedShardService} on {@link Store}
     * closure.
     */
    public void testShardClosureMovesActiveReaderCommitTrackingToClosedShardService() throws Exception {
        final String indexNode = startMasterAndIndexNode();
        final String searchNodeA = startSearchNode();
        final String searchNodeB = startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1)
                // Start with the shard replica on searchNodeA.
                .put("index.routing.allocation.exclude._name", searchNodeB)
                .build()
        );
        ensureGreen(indexName);

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var searchNodeAClosedShardService = internalCluster().getInstance(ClosedShardService.class, searchNodeA);
        assertThat(searchNodeAClosedShardService.getPrimaryTermAndGenerations(shardId).size(), equalTo(0));

        // Set up some data to be read.
        final int numDocsToIndex = randomIntBetween(5, 100);
        indexDocsAndRefresh(indexName, numDocsToIndex);

        // Start a scroll to pin the reader state on the search node until the scroll is exhausted.
        final var scrollSearchResponse = client().prepareSearch(indexName)
            .setQuery(matchAllQuery())
            .setSize(1)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();
        try {
            assertThat(scrollSearchResponse.getScrollId(), Matchers.is(CoreMatchers.notNullValue()));

            // Move shards away from searchNodeA while the search is still active and using the latest shard commit.
            // The index should be closed, but the active reader state tracking should move to the ClosedShardService until the search
            // completes and the Store closes.
            assertThat(searchNodeAClosedShardService.getPrimaryTermAndGenerations(shardId).size(), equalTo(0));
            {
                logger.info("--> moving shards in index [{}] away from node [{}]", indexName, searchNodeA);
                updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
                assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeA))));
                logger.info("--> shards in index [{}] have been moved off of node [{}]", indexName, searchNodeA);
            }
            assertBusy(() -> assertThat(searchNodeAClosedShardService.getPrimaryTermAndGenerations(shardId).size(), equalTo(1)));

            // Clear the scroll, allowing the Store to close, and the ClosedShardService should be cleared.
            assertHitCount(scrollSearchResponse, numDocsToIndex);
            assertThat(scrollSearchResponse.getHits().getHits().length, equalTo(1));
            client().prepareClearScroll().addScrollId(scrollSearchResponse.getScrollId()).get();
            assertBusy(() -> assertThat(searchNodeAClosedShardService.getPrimaryTermAndGenerations(shardId).size(), equalTo(0)));
        } finally {
            // There's an implicit incRef in prepareSearch(), so call decRef() to release the response object back into the resource pool.
            scrollSearchResponse.decRef();
        }
    }

}

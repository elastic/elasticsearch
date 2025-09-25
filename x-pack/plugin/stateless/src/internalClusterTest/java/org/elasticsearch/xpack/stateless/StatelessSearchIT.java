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
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesRequest;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngineTestUtils;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.TransportAnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SequenceNumbers;
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
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.Stateless.CLEAR_BLOB_CACHE_ACTION;
import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.blobcache.CachePopulationSource.BlobStore;
import static org.elasticsearch.blobcache.CachePopulationSource.Peer;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isUnsafe;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

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
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
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
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            super(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
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
    protected boolean addMockFsRepository() {
        return false;
    }

    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
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
        startIndexNodes(numShards, disableIndexingDiskAndMemoryControllersNodeSettings());
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
            final int nonUploadedNotification = (getNumberOfCreatedCommits() - beginningNumberOfCreatedCommits) * numReplicas;
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
        final String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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
        final String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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
    public void testConcurrentFlushAndMultipleRefreshesWillSetMaxUploadGen() throws Exception {
        final String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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
        final boolean force = randomBoolean();
        final boolean waitIfOngoing = force || randomBoolean();
        final Runnable flusher = () -> {
            safeSleep(randomLongBetween(0, 50));
            client().admin().indices().prepareFlush().setForce(force).setWaitIfOngoing(waitIfOngoing).get(TimeValue.timeValueSeconds(10));
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
            final long maxGenerationToUploadDueToFlush = statelessCommitService.getMaxGenerationToUploadForFlush(shardId);
            assertThat(maxGenerationToUploadDueToFlush, allOf(greaterThan(previousGeneration), lessThanOrEqualTo(generation)));
            final int newCount = statelessCommitService.invocationCounters.get(shardId).get();
            if (newCount > count + 2) {
                fail("expected 1 or 2 invocations for ensuring flush generation, got " + (newCount - count));
            }
            if (newCount == count + 2) {
                assertThat(force == false && waitIfOngoing == false, equalTo(true));
            }
            previousGeneration = generation;
        }
        // The last commit may be a refresh and not uploaded when BCC can contain more than 1 CC
        if (getUploadMaxCommits() > 1) {
            flushNoForceNoWait(indexName);
            assertThat(indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration(), equalTo(previousGeneration));
        }
        // All commits are uploaded
        assertThat(statelessCommitService.getCurrentVirtualBcc(shardId), nullValue());

    }

    // TODO move this test to a separate test class for refresh cost optimization
    public void testConcurrentAppendAndFreezeForVirtualBcc() throws Exception {
        final String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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
        final ObjectStoreService objectStoreService = getObjectStoreService(indexNode);
        final BlobContainer blobContainer = objectStoreService.getProjectBlobContainer(shardId, indexShard.getOperationPrimaryTerm());

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
            assertBusy(
                () -> assertThat(
                    compoundCommitFileName + " not found",
                    blobContainer.blobExists(operationPurpose, compoundCommitFileName),
                    is(true)
                )
            );
        }
        // All commits are uploaded
        assertThat(statelessCommitService.getCurrentVirtualBcc(shardId), nullValue());
        // The concurrent freeze thread ensures every generation for upload
        assertBusy(() -> assertThat(statelessCommitService.getMaxGenerationToUploadForFlush(shardId), equalTo(currentGeneration.get())));
        shouldStop.set(true);
        thread.join();
    }

    public void testRefreshAndGet() throws Exception {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);

        assert cluster().numDataNodes() > 0 : "Should have already started nodes";
        final String indexName = SYSTEM_INDEX_NAME;
        if (randomBoolean()) {
            createSystemIndex(
                indexSettings(numShards, numReplicas).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), randomBoolean()).build()
            );
        } else {
            createIndex(indexName, indexSettings(numShards, numReplicas).build());
        }
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
                    searchResponse.getHits().getTotalHits().value()
                )
            );
        }

        // Preparation to test get and mget
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

        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.startsWith(TransportGetFromTranslogAction.NAME)
                    || action.startsWith(TransportShardMultiGetFomTranslogAction.NAME)) {
                    assertThat(connection.getNode().getRoles(), contains(DiscoveryNodeRole.INDEX_ROLE));
                } else if (action.startsWith(TransportGetAction.TYPE.name()) || action.startsWith(TransportMultiGetAction.NAME)) {
                    assertThat(connection.getNode().getRoles(), contains(DiscoveryNodeRole.SEARCH_ROLE));
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
            searchResponse -> assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value())
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
            searchResponse -> assertEquals(numDocs, searchResponse.getHits().getTotalHits().value())
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
            searchResponse -> assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value())
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
                assertThat(scrollSearchResponse.getHits().getTotalHits().value(), equalTo((long) bulk1DocsToIndex));
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
                    searchResponse -> assertEquals(expectedDocs, searchResponse.getHits().getTotalHits().value())
                );
                // fetch next scroll
                assertNoFailuresAndResponse(
                    client().prepareSearchScroll(currentScrollId.get()).setScroll(TimeValue.timeValueMinutes(2)),
                    scrollSearchResponse -> {
                        assertThat(scrollSearchResponse.getHits().getTotalHits().value(), equalTo((long) bulk1DocsToIndex));
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
        startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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
            assertThat(firstScroll.getHits().getTotalHits().value(), equalTo(100L));
            firstScrollId.set(firstScroll.getScrollId());
        });

        var firstScrollPrimaryTermAndGenerations = latestPrimaryTermAndGenerationDependencies.get();
        assertThat(firstScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        final AtomicReference<String> secondScrollId = new AtomicReference<>();
        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), secondScroll -> {
            assertThat(secondScroll.getHits().getTotalHits().value(), equalTo(200L));
            secondScrollId.set(secondScroll.getScrollId());
        });

        var secondScrollPrimaryTermAndGenerations = latestPrimaryTermAndGenerationDependencies.get();
        assertThat(secondScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

        clearScroll(firstScrollId.get());

        assertThat(secondScrollPrimaryTermAndGenerations, everyItem(is(in(searchEngine.getAcquiredPrimaryTermAndGenerations()))));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), thirdScroll -> {
            assertThat(thirdScroll.getHits().getTotalHits().value(), equalTo(300L));

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
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) bulk1DocsToIndex));
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
                assertEquals(bulk1DocsToIndex + bulk2DocsToIndex, search2Response.getHits().getTotalHits().value());
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
            assertThat(cacheMiss.getHits().getTotalHits().value(), equalTo((long) data.size()));
        } finally {
            cacheMiss.decRef();
        }
        assertRequestCacheStats(client, indexName, greaterThan(0L), 0, 1);

        int nbSearchesWithCacheHits = randomIntBetween(1, 10);
        for (int i = 0; i < nbSearchesWithCacheHits; i++) {
            var cacheHit = countDocsInRange(client, indexName, min, max);
            try {
                assertThat(cacheHit.getHits().getTotalHits().value(), equalTo((long) data.size()));
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
            assertThat(cacheMissDueRefresh.getHits().getTotalHits().value(), equalTo((long) (data.size() + moreData.size())));
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

    public void testSearchWithWaitForUnissuedCheckpoint() {
        var nodeSettings = disableIndexingDiskAndMemoryControllersNodeSettings();
        startMasterOnlyNode(nodeSettings);
        startIndexNode(nodeSettings);
        startSearchNode(nodeSettings);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            refresh(indexName);
        }

        var exception = expectThrows(Exception.class, () -> {
            var request = new SearchRequest(indexName);
            request.setWaitForCheckpoints(Map.of(indexName, new long[] { Long.MAX_VALUE }));
            request.setWaitForCheckpointsTimeout(TimeValue.timeValueMinutes(2));
            client().search(request).actionGet();
        });
        assertThat(exception.getCause().getMessage(), containsString("Cannot wait for unissued seqNo checkpoint"));
    }

    public void testSearchWithWaitForCheckpoint() {
        startMasterOnlyNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        var indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        final int docCount = randomIntBetween(1, 100);
        indexDocs(indexName, docCount);
        final Index index = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(indexName)
            .getIndex();
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
        }
        var searchFuture = client().prepareSearch(indexName)
            .setWaitForCheckpoints(Map.of(indexName, new long[] { seqNoStats.getGlobalCheckpoint() }))
            .execute();
        if (refreshBefore == false) {
            refresh(indexName);
        }
        assertHitCount(searchFuture, docCount);
    }

    public void testSearchWithWaitForCheckpointWithTranslogDelay() throws Exception {
        // To test that non-uploaded commit notifications wait for the GCP, we ensure VBCC is not immediately frozen.
        var nodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(1))
            .build();
        startMasterOnlyNode(nodeSettings);
        final var indexNode = startIndexNode(nodeSettings);
        startSearchNode(nodeSettings);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        IndexShard indexShard = findIndexShard(indexName);

        // Repeatedly fail translog uploads
        ObjectStoreService objectStoreService = getObjectStoreService(indexNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(Long.MAX_VALUE);
        repository.setRandomIOExceptionPattern(".*translog.*");

        var bulkRequest = client().prepareBulk();
        final long docCount = randomLongBetween(1, 100);
        for (var i = 0; i < docCount; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        ActionFuture<BulkResponse> bulkFuture = bulkRequest.execute();

        // Wait until the documents are processed
        IndexEngine engine = (IndexEngine) indexShard.getEngineOrNull();
        assertBusy(() -> assertThat(engine.getProcessedLocalCheckpoint(), greaterThanOrEqualTo(0L)));
        assertThat(engine.getPersistedLocalCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));

        // Assert that any new commit notification should be sent after the translog is persisted.
        // Also, make a latch to control whether to delay when the new commit notification is sent.
        boolean sendNewCommitNotificationAfterSearch = randomBoolean();
        CountDownLatch newCommitNotificationLatch = new CountDownLatch(sendNewCommitNotificationAfterSearch ? 1 : 0);
        var mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNode);
        mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                assertThat(engine.getPersistedLocalCheckpoint(), greaterThanOrEqualTo(0L));
                mockTransportService.getThreadPool().generic().execute(() -> {
                    try {
                        safeAwait(newCommitNotificationLatch);
                        connection.sendRequest(requestId, action, request, options);
                    } catch (Exception e) {
                        assert false : e;
                        throw new RuntimeException(e);
                    }
                });
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });

        // Fire a refresh, which triggers a new commit notification
        var refreshRequest = indicesAdmin().prepareRefresh(indexName).execute();

        // Let translog pass through and wait for global checkpoint to increase
        repository.setRandomControlIOExceptionRate(0.0);
        repository.setRandomDataFileIOExceptionRate(0.0);
        AtomicLong globalCheckpoint = new AtomicLong(-1);
        assertBusy(() -> {
            globalCheckpoint.set(engine.getLastSyncedGlobalCheckpoint());
            assertThat(globalCheckpoint.get(), greaterThanOrEqualTo(docCount - 1));
        });

        // Issue a search waiting for the global checkpoint
        var searchFuture = client().prepareSearch(indexName)
            .setWaitForCheckpoints(Map.of(indexName, new long[] { globalCheckpoint.get() }))
            .execute();
        newCommitNotificationLatch.countDown();
        assertHitCount(searchFuture, docCount);

        assertNoFailures(bulkFuture.get());
        assertNoFailures(refreshRequest.get());
    }

    public void testSearchWithWaitForCheckpointOnNewSearchShard() throws Exception {
        startMasterOnlyNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        final var indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final int docCount = randomIntBetween(100, 100);
        if (docCount > 0) {
            indexDocs(indexName, docCount);
            if (randomBoolean()) {
                flush(indexName);
            } else if (randomBoolean()) {
                refresh(indexName);
            } else {
                // In this case, we expect the scheduled refresh to trigger the wait-for-checkpoint search
            }
        }

        setReplicaCount(1, indexName);
        ensureGreen(indexName);

        final Index index = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(indexName)
            .getIndex();
        var seqNoStats = clusterAdmin().prepareNodesStats(indexNode)
            .setIndices(true)
            .get()
            .getNodes()
            .get(0)
            .getIndices()
            .getShardStats(index)
            .get(0)
            .getShards()[0].getSeqNoStats();

        // Issue a search waiting for the global checkpoint
        var searchFuture = client().prepareSearch(indexName)
            .setWaitForCheckpoints(Map.of(indexName, new long[] { seqNoStats.getGlobalCheckpoint() }))
            .execute();
        assertHitCount(searchFuture, docCount);
    }

    public void testConcurrentIndexingAndSearches() throws Exception {

        int maxNonUploadedCommits = randomIntBetween(1, 20);
        startIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxNonUploadedCommits)
                .build()
        );
        startSearchNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        // abort all indexing and searching threads if any of them encounter error
        var erroneousStop = new AtomicBoolean(false);
        var threads = new ArrayList<Thread>();

        int indexingThreadsNumber = randomIntBetween(1, 5);
        var runningIndexingThreadsCount = new CountDownLatch(indexingThreadsNumber);

        final Runnable indexer = () -> {
            try {
                var bulkCount = randomIntBetween(1, 10);
                for (int bulk = 0; bulk < bulkCount && erroneousStop.get() == false; bulk++) {
                    indexQueryableDocs(indexName, scaledRandomIntBetween(10, 50));
                    if (rarely()) {
                        flush(indexName);
                    }
                    safeSleep(randomLongBetween(100, 200));
                }
            } catch (Exception e) {
                erroneousStop.set(true);
                throw new AssertionError(e);
            } finally {
                runningIndexingThreadsCount.countDown();
            }
        };
        for (int i = 0; i < indexingThreadsNumber; i++) {
            threads.add(new Thread(indexer));
        }

        int searchThreadsNumber = randomIntBetween(1, 5);
        var runningSearchingThreadsCount = new CountDownLatch(searchThreadsNumber);

        final Runnable searcher = () -> {
            try {
                while (runningIndexingThreadsCount.getCount() > 0 && erroneousStop.get() == false) {
                    if (rarely()) {
                        if (randomBoolean()) {
                            // fan out to all nodes
                            client().execute(CLEAR_BLOB_CACHE_ACTION, new ClearBlobCacheNodesRequest()).get();
                        } else {
                            // clear cache on just searching node
                            evictSearchShardCache(indexName);
                        }
                    }

                    var searchType = randomFrom(TestSearchType.values());
                    var searchRequest = prepareSearch(indexName, searchType).setTimeout(TimeValue.timeValueMinutes(60));
                    if (searchType != TestSearchType.SCROLL) {
                        assertNoFailures(searchRequest);
                    } else {
                        assertScrollResponses(searchRequest);
                    }
                    safeSleep(randomLongBetween(100, 200));
                }
            } catch (Exception e) {
                erroneousStop.set(true);
                throw new AssertionError(e);
            } finally {
                runningSearchingThreadsCount.countDown();
            }
        };
        for (int i = 0; i < searchThreadsNumber; i++) {
            threads.add(new Thread(searcher));
        }

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            thread.join(10_000);
        }

        safeAwait(runningIndexingThreadsCount);
        safeAwait(runningSearchingThreadsCount);
    }

    public void testConcurrentReadAfterWrite() {
        int maxNonUploadedCommits = randomIntBetween(1, 5);
        startIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxNonUploadedCommits)
                .build()
        );
        startSearchNode();

        var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        var threads = new ArrayList<Thread>();
        var numReadWriteTaskPairs = randomIntBetween(1, 5);
        var threadCounter = new CountDownLatch(2 * numReadWriteTaskPairs);

        for (int i = 0; i < numReadWriteTaskPairs; i++) {

            final int customValue = randomInt();
            final int docsNum = randomIntBetween(1, 10);
            final int commitsNum = randomIntBetween(1, 2 * maxNonUploadedCommits);
            final CountDownLatch latch = new CountDownLatch(1);

            threads.add(new Thread(() -> {
                for (int c = 0; c < commitsNum; c++) {
                    // every indexing-search task works on its own set of documents to be able to guarantee doc count checks
                    indexDocsWithCustomValue(indexName, docsNum, customValue);
                    refresh(indexName);
                }
                latch.countDown();
                threadCounter.countDown();
            }));

            threads.add(new Thread(() -> {

                safeAwait(latch);

                if (rarely()) {
                    evictSearchShardCache(indexName);
                }
                long expectedDocsNum = (long) docsNum * commitsNum;
                var search = prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", customValue));
                if (randomBoolean()) {
                    // Set a large size to retrieve all documents to force reading all relevant search data
                    assertHitCount(search.setSize(10_000), expectedDocsNum);
                } else {
                    assertScrollResponsesAndHitCount(
                        client(),
                        TimeValue.timeValueSeconds(60),
                        search.setSize(randomIntBetween(1, (int) expectedDocsNum)),
                        (int) expectedDocsNum,
                        (respNum, response) -> assertNoFailures(response)
                    );
                }
                threadCounter.countDown();
            }));
        }

        Collections.shuffle(threads, random());

        threads.forEach(Thread::start);

        safeAwait(threadCounter);
    }

    public void testSearchTriggeredDownloadsTelemetry() {
        startMasterAndIndexNode();
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String searchNode = startSearchNode();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        for (int i = 0; i < 10; i++) {
            indexDocs(indexName, 1_000);
            refresh(indexName);
        }
        flush(indexName);

        evictSearchShardCache(indexName);
        SearchResponse searchResponse = null;
        try {
            searchResponse = prepareSearch(indexName).setQuery(matchAllQuery()).get();
        } finally {
            if (searchResponse != null) {
                searchResponse.decRef();
            }
        }

        TestTelemetryPlugin telemetryPlugin = getTelemetryPlugin(searchNode);
        List<Measurement> searchOriginMeasurement = telemetryPlugin.getLongHistogramMeasurement(
            BlobCacheMetrics.SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME
        );
        assertThat(searchOriginMeasurement.size(), greaterThan(0));
        Measurement measurement = searchOriginMeasurement.stream().findFirst().get();
        assertThat(measurement.getLong(), greaterThanOrEqualTo(0L));
        assertThat(
            measurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY),
            is(oneOf(BlobStore.name(), Peer.name()))
        );
    }

    private void assertScrollResponses(SearchRequestBuilder searchRequestBuilder) {
        var responses = new ArrayList<SearchResponse>();
        var scrollResponse = searchRequestBuilder.get();
        assertNoFailures(scrollResponse);
        responses.add(scrollResponse);
        try {
            while (scrollResponse.getHits().getHits().length > 0) {
                scrollResponse = client().prepareSearchScroll(scrollResponse.getScrollId()).setScroll(TimeValue.timeValueSeconds(60)).get();
                assertNoFailures(scrollResponse);
                responses.add(scrollResponse);
            }
        } finally {
            ClearScrollResponse clearResponse = client().prepareClearScroll()
                .setScrollIds(Arrays.asList(scrollResponse.getScrollId()))
                .get();
            responses.forEach(SearchResponse::decRef);
            assertThat(clearResponse.isSucceeded(), Matchers.equalTo(true));
        }
    }

    private void evictSearchShardCache(String indexName) {
        IndexShard shard = findSearchShard(indexName);
        Directory directory = shard.store().directory();
        SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(directory);
        getCacheService(searchDirectory).forceEvict((key) -> true);
    }

    private void indexQueryableDocs(String indexName, int numDocs) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("field", randomUnicodeOfCodepointLengthBetween(1, 25));
            if (randomBoolean()) {
                doc.put("number", randomInt());
            }
            if (randomBoolean()) {
                doc.put("custom", "value");
            }
            bulkRequest.add(new IndexRequest(indexName).source(doc));
        }
        var refreshPolicy = rarely() ? WAIT_UNTIL : randomFrom(NONE, IMMEDIATE);
        logger.info("--> indexing [{}] docs with refresh_policy [{}]", numDocs, refreshPolicy);
        bulkRequest.setRefreshPolicy(refreshPolicy).setTimeout(TimeValue.timeValueSeconds(60));
        assertNoFailures(bulkRequest.get());
    }

    private enum TestSearchType {
        MATCH_ALL,
        MATCH_CUSTOM,
        SUM,
        SCROLL
    }

    static SearchRequestBuilder prepareSearch(String indexName, TestSearchType testSearchType) {
        // Set a large size to retrieve all documents to force reading all relevant search data
        return switch (testSearchType) {
            case MATCH_ALL -> prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(10_000);
            case MATCH_CUSTOM -> prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")).setSize(10_000);
            case SUM -> prepareSearch(indexName).addAggregation(sum("sum").field("number")).setSize(10_000);
            case SCROLL -> prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())
                .setSize(randomIntBetween(10, 1000))
                .setScroll(TimeValue.timeValueSeconds(60));
        };
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

    private void indexDocsWithCustomValue(String indexName, int numDocs, int customValue) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("custom", customValue));
        }
        assertNoFailures(bulkRequest.get());
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
            .prepareState(TEST_REQUEST_TIMEOUT)
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
        startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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

    public void testAnalyzeDiskUsage() {
        startIndexNodes(numShards);
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, numReplicas).build());
        ensureGreen(indexName);
        var request = new AnalyzeIndexDiskUsageRequest(
            new String[] { indexName },
            AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS,
            false
        );
        var resp = client().execute(TransportAnalyzeIndexDiskUsageAction.TYPE, request).actionGet();
        assertThat(resp.getTotalShards(), equalTo(numShards));
        assertThat(resp.getFailedShards(), equalTo(0));
        assertThat(resp.getSuccessfulShards(), equalTo(numShards));
    }
}

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
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
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
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesRequestCacheUtils;
import org.elasticsearch.indices.IndicesService;
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
import org.hamcrest.Matcher;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
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

        private int getCreatedCommits() {
            return createdCommits.get();
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
            assertThat(
                "Search shard notifications should be equal to the number of created commits multiplied by the number of replicas.",
                searchNotifications.get(),
                equalTo((getNumberOfCreatedCommits() - beginningNumberOfCreatedCommits) * numReplicas)
            );
        });
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

        final Supplier<PrimaryTermAndGeneration> latestPrimaryTermAndGeneration = () -> {
            var indexShardEngineOrNull = findIndexShard(resolveIndex(indexName), 0).getEngineOrNull();
            assertThat(indexShardEngineOrNull, notNullValue());
            return new PrimaryTermAndGeneration(
                indexShardEngineOrNull.config().getPrimaryTermSupplier().getAsLong(),
                ((IndexEngine) indexShardEngineOrNull).getCurrentGeneration()
            );
        };

        var searchShardEngineOrNull = findSearchShard(resolveIndex(indexName), 0).getEngineOrNull();
        assertThat(searchShardEngineOrNull, instanceOf(SearchEngine.class));
        var searchEngine = (SearchEngine) searchShardEngineOrNull;
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(latestPrimaryTermAndGeneration.get()));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        final AtomicReference<String> firstScrollId = new AtomicReference<>();
        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), firstScroll -> {
            assertThat(firstScroll.getHits().getTotalHits().value, equalTo(100L));
            firstScrollId.set(firstScroll.getScrollId());
        });

        var firstScrollPrimaryTermAndGeneration = latestPrimaryTermAndGeneration.get();
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(firstScrollPrimaryTermAndGeneration));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        final AtomicReference<String> secondScrollId = new AtomicReference<>();
        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), secondScroll -> {
            assertThat(secondScroll.getHits().getTotalHits().value, equalTo(200L));
            secondScrollId.set(secondScroll.getScrollId());
        });

        var secondScrollPrimaryTermAndGeneration = latestPrimaryTermAndGeneration.get();
        assertThat(
            searchEngine.getAcquiredPrimaryTermAndGenerations(),
            containsInAnyOrder(firstScrollPrimaryTermAndGeneration, secondScrollPrimaryTermAndGeneration)
        );

        clearScroll(firstScrollId.get());

        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(secondScrollPrimaryTermAndGeneration));

        indexDocs(indexName, 100);
        flushAndRefresh(indexName);

        assertNoFailuresAndResponse(prepareSearch().setScroll(TimeValue.timeValueHours(1L)), thirdScroll -> {
            assertThat(thirdScroll.getHits().getTotalHits().value, equalTo(300L));

            var thirdScrollPrimaryTermAndGeneration = latestPrimaryTermAndGeneration.get();
            assertThat(
                searchEngine.getAcquiredPrimaryTermAndGenerations(),
                containsInAnyOrder(secondScrollPrimaryTermAndGeneration, thirdScrollPrimaryTermAndGeneration)
            );

            clearScroll(thirdScroll.getScrollId());
            indexDocs(indexName, 1);
            flushAndRefresh(indexName);

            assertThat(thirdScrollPrimaryTermAndGeneration, not(equalTo(latestPrimaryTermAndGeneration.get())));
            assertThat(
                searchEngine.getAcquiredPrimaryTermAndGenerations(),
                containsInAnyOrder(secondScrollPrimaryTermAndGeneration, latestPrimaryTermAndGeneration.get())
            );
        });

        clearScroll(secondScrollId.get());

        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), contains(latestPrimaryTermAndGeneration.get()));
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
        }
        var searchFuture = client().prepareSearch(indexName)
            .setWaitForCheckpoints(Map.of(indexName, new long[] { seqNoStats.getGlobalCheckpoint() }))
            .execute();
        if (refreshBefore == false) {
            refresh(indexName);
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
}

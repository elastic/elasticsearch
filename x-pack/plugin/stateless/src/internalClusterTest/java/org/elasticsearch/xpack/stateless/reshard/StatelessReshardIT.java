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

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.explain.TransportExplainAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportGetFromTranslogAction;
import org.elasticsearch.action.get.TransportShardMultiGetAction;
import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockMasterServiceOnMaster;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.resizeRequest;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.common.blobstore.OperationPurpose.INDICES;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.index.IndexSettings.MODE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xpack.stateless.reshard.ReshardingTestHelpers.indexMetadata;
import static org.elasticsearch.xpack.stateless.reshard.ReshardingTestHelpers.makeIdThatRoutesToShard;
import static org.elasticsearch.xpack.stateless.reshard.ReshardingTestHelpers.postSplitRouting;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;

public class StatelessReshardIT extends AbstractStatelessPluginIntegTestCase {

    // Given an index with a certain number of shards, it can be resharded only
    // using a multiple of 2. Here we test some valid and invalid combinations.
    public void testReshardTargetNumShardsIsValid() {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        int numShards = randomIntBetween(1, 5);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, numShards);

        for (int i = 0; i < 5; i++) {
            logger.info("attempting reshard from {} to {} shards should succeed", numShards, numShards * 2);
            client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(SAFE_AWAIT_TIMEOUT);
            waitForReshardCompletion(indexName);
            numShards *= 2;
            checkNumberOfShardsSetting(indexNode, indexName, numShards);
        }
    }

    public void testRoutingAfterSplittingMultipleShards() {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(2, 1).build());
        ensureGreen(indexName);

        final Index index = resolveIndex(indexName);

        // Create an IndexRouting object for 4 shards to find document IDs that would route to each shard
        final var indexRoutingWith4Shards = postSplitRouting(clusterService().state(), index, 4);

        // Find 4 document IDs that would route to each of the 4 shards if the index had 4 shards
        final var doc0Id = makeIdThatRoutesToShard(indexRoutingWith4Shards, 0);
        final var doc1Id = makeIdThatRoutesToShard(indexRoutingWith4Shards, 1);
        final var doc2Id = makeIdThatRoutesToShard(indexRoutingWith4Shards, 2);
        final var doc3Id = makeIdThatRoutesToShard(indexRoutingWith4Shards, 3);

        // Index the four documents with initial values
        indexDoc(indexName, doc0Id, "field", "value0");
        indexDoc(indexName, doc1Id, "field", "value1");
        indexDoc(indexName, doc2Id, "field", "value2");
        indexDoc(indexName, doc3Id, "field", "value3");

        // Verify documents were indexed
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), 4);

        // Execute reshard from 2 shards to 4 shards
        final var reshardRequest = new ReshardIndexRequest(indexName, 4);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet();
        waitForReshardCompletion(indexName);

        // Update each document with new values
        indexDoc(indexName, doc0Id, "field", "updated0");
        indexDoc(indexName, doc1Id, "field", "updated1");
        indexDoc(indexName, doc2Id, "field", "updated2");
        indexDoc(indexName, doc3Id, "field", "updated3");

        // Verify each document was updated by getting them individually
        // Version should be 2 (initial index + update) proving the document was updated, not recreated
        var response0 = client().prepareGet(indexName, doc0Id).execute().actionGet();
        assertThat(response0.isExists(), is(true));
        assertThat(response0.getSource().get("field"), equalTo("updated0"));
        assertThat(response0.getVersion(), equalTo(2L));

        var response1 = client().prepareGet(indexName, doc1Id).execute().actionGet();
        assertThat(response1.isExists(), is(true));
        assertThat(response1.getSource().get("field"), equalTo("updated1"));
        assertThat(response1.getVersion(), equalTo(2L));

        var response2 = client().prepareGet(indexName, doc2Id).execute().actionGet();
        assertThat(response2.isExists(), is(true));
        assertThat(response2.getSource().get("field"), equalTo("updated2"));
        assertThat(response2.getVersion(), equalTo(2L));

        var response3 = client().prepareGet(indexName, doc3Id).execute().actionGet();
        assertThat(response3.isExists(), is(true));
        assertThat(response3.getSource().get("field"), equalTo("updated3"));
        assertThat(response3.getVersion(), equalTo(2L));

        // Verify all documents are still present via search
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), 4);
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService:DEBUG", reason = "debugging")
    public void testReshardWillCopyDataAndRouteDocumentsToNewShard() {
        String indexNode = startMasterAndIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        startSearchNode();

        ensureStableCluster(2);

        final int multiple = 2;
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, 1);

        final Index index = resolveIndex(indexName);
        final var sourceShard = new ShardId(index, 0);

        int[] shardDocs = new int[multiple];
        Consumer<Integer> indexShardDocs = (numDocs) -> {
            for (var item : indexDocs(indexName, numDocs).getItems()) {
                var docShard = item.getResponse().getShardId().getId();
                shardDocs[docShard]++;
            }
        };

        final int numDocs = randomIntBetween(10, 100);
        indexShardDocs.accept(numDocs);
        int totalNumberOfDocumentsInIndex = numDocs;
        assertShardDocCountEquals(sourceShard, totalNumberOfDocumentsInIndex);

        // flushing here ensures that the initial copy phase does some work, rather than relying entirely on catching new commits after
        // resharding has begun.
        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        final var initialIndexMetadata = indexMetadata(clusterService().state(), index);
        // before resharding there should be no resharding metadata
        assertNull(initialIndexMetadata.getReshardingMetadata());

        // block handoff until we've indexed some more documents after resharding has started
        CountDownLatch preHandoffLatch = new CountDownLatch(1);
        // block ongoing copy until we've added some new commits that the first copy phase hasn't seen
        CountDownLatch postCopyLatch = new CountDownLatch(1);
        setNodeRepositoryStrategy(indexNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCopyBlob(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                BlobContainer sourceBlobContainer,
                String sourceBlobName,
                String blobName,
                long blobSize
            ) throws IOException {
                super.blobContainerCopyBlob(originalRunnable, purpose, sourceBlobContainer, sourceBlobName, blobName, blobSize);
                // Signal that we've started the initial copy. It won't see any new commits generated after this, so we know
                // that the new ones come from ongoing copying.
                postCopyLatch.countDown();
            }
        });

        // used to wait for new documents to be indexed after we've started copy but before we enter handoff
        // this introduces a special test-only sync point in SplitSourceService which is a little ugly. I suppose
        // alternatively we could just have a thread running and doing indexing throughout the process, and probabilistically it
        // will generate some in this window without explicit synchronization.
        var splitSourceService = internalCluster().getInstance(SplitSourceService.class, indexNode);
        splitSourceService.setPreHandoffHook(() -> {
            try {
                logger.info("waiting for prehandoff latch");
                preHandoffLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
                logger.info("prehandoff latch released");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // there should be split metadata at some point during resharding
        var splitState = waitForClusterState((state) -> indexMetadata(state, index).getReshardingMetadata() != null);

        logger.info("starting reshard");
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        logger.info("getting reshard metadata");
        var reshardingMetadata = indexMetadata(splitState.actionGet(SAFE_AWAIT_TIMEOUT), index).getReshardingMetadata();
        assertNotNull(reshardingMetadata.getSplit());
        assert reshardingMetadata.shardCountBefore() == 1;
        assert reshardingMetadata.shardCountAfter() == multiple;

        // wait for initial copy to start
        try {
            postCopyLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("interrupted");
            throw new RuntimeException(e);
        }

        // index some more documents and flush to generate new commits
        final int postCopyDocs = randomIntBetween(10, 20);
        indexShardDocs.accept(postCopyDocs);
        totalNumberOfDocumentsInIndex += postCopyDocs;
        logger.info("allowing handoff after post-copy index");
        preHandoffLatch.countDown();

        waitForReshardCompletion(indexName);

        // resharding metadata should eventually be removed after split executes
        var indexMetadata = indexMetadata(
            waitForClusterState((state) -> indexMetadata(state, index).getReshardingMetadata() == null).actionGet(SAFE_AWAIT_TIMEOUT),
            index
        );

        // index documents until all the new shards have received at least one document
        int docsPerRequest = randomIntBetween(10, 100);
        do {
            indexShardDocs.accept(docsPerRequest);
            totalNumberOfDocumentsInIndex += docsPerRequest;
        } while (Arrays.stream(shardDocs).filter(shard -> shard > 0).count() < multiple);

        // Verify that each shard id contains the expected number of documents indexed into it.
        // Note that stats won't include data copied from the source shard since they didn't go through the "normal" indexing logic.
        final var postReshardStatsResponse = client().admin().indices().prepareStats(indexName).execute().actionGet();

        IntStream.range(0, multiple)
            .forEach(shardId -> assertShardDocCountEquals(postReshardStatsResponse, new ShardId(index, shardId), shardDocs[shardId]));

        // index more documents to verify that a search query returns all indexed documents thus far
        final int numDocsRound3 = randomIntBetween(10, 100);
        indexDocs(indexName, numDocsRound3);
        totalNumberOfDocumentsInIndex += numDocsRound3;

        refresh(indexName);

        // verify that the index metadata returned matches the expected multiple of shards
        checkNumberOfShardsSetting(indexNode, index.getName(), multiple);

        var search = prepareSearchAll(indexName).get();
        assertHitCount(search, totalNumberOfDocumentsInIndex);

        // all documents should be on their owning shards
        var indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        assertTrue(
            StreamSupport.stream(search.getHits().spliterator(), false)
                .allMatch(hit -> hit.getShard().getShardId().getId() == indexRouting.indexShard(new IndexRequest().id(hit.getId())))
        );

        search.decRef();
    }

    public void testSearchDuringReshard() throws Exception {
        runSearchTest(SearchTestType.SEARCH_API);
    }

    public void testEsqlSearchDuringReshard() throws Exception {
        runSearchTest(SearchTestType.ESQL);
    }

    public void testEsqlWithLookupJoinSearchDuringReshard() throws Exception {
        runSearchTest(SearchTestType.ESQL_WITH_LOOKUP_JOIN);
    }

    private enum SearchTestType {
        SEARCH_API,
        ESQL,
        ESQL_WITH_LOOKUP_JOIN
    }

    private static final String ESQL_JOIN_INDEX = "join_index";
    private static final String ESQL_JOIN_FIELD = "join_field";
    private static final Integer ESQL_JOIN_FIELD_VALUE = 42;

    private void runSearchTest(SearchTestType searchTestType) throws Exception {
        var masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchCoordinator = startSearchNode();
        ensureStableCluster(4);

        final int multiple = 2;
        final String indexName = randomIndexName();
        // Disable periodic refresh because we specifically test refresh here and want to explicitly control it.
        createIndex(indexName, indexSettings(1, 1).put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        if (searchTestType == SearchTestType.ESQL_WITH_LOOKUP_JOIN) {
            createIndex(ESQL_JOIN_INDEX, indexSettings(1, 1).put(MODE.getKey(), IndexMode.LOOKUP.getName()).build());

            prepareIndex(ESQL_JOIN_INDEX).setSource(Map.of(ESQL_JOIN_FIELD, ESQL_JOIN_FIELD_VALUE)).get();
            assertNoFailures(refresh(ESQL_JOIN_INDEX));
        }

        var index = resolveIndex(indexName);
        var indexMetadata = indexMetadata(internalCluster().clusterService(masterNode).state(), index);

        // We re-create the metadata directly in test in order to have access to after-reshard routing.
        var wouldBeMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(multiple).build();
        var wouldBeAfterSplitRouting = IndexRouting.fromIndexMetadata(wouldBeMetadata);

        var allIndexedDocuments = Collections.synchronizedMap(new HashMap<String, Integer>());
        int documentsPerRound = randomIntBetween(10, 50);

        var documentCounter = new AtomicInteger(0);
        Supplier<String> idSupplier = () -> "id" + documentCounter.getAndIncrement();

        var initialIndexedDocuments = indexDocuments(searchTestType, indexName, documentsPerRound, idSupplier, wouldBeAfterSplitRouting);
        allIndexedDocuments.putAll(initialIndexedDocuments);

        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        // Search works before resharding.
        refresh(indexName);
        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(1), equalTo(allIndexedDocuments.keySet()));

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client(masterNode).execute(TransportReshardAction.TYPE, reshardRequest).actionGet();

        CountDownLatch handOffStarted = new CountDownLatch(multiple - 1); // (multiple - 1) target shards
        CyclicBarrier stateTransitionBlock = new CyclicBarrier(multiple); // (multiple - 1) target shards + the test itself

        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    if (handOffStarted.getCount() > 0) {
                        handOffStarted.countDown();
                    }
                    stateTransitionBlock.await();
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        });

        awaitClusterState(searchCoordinator, clusterState -> indexMetadata(clusterState, index).getReshardingMetadata() != null);

        // wait for all target shards to arrive at handoff point
        handOffStarted.await();

        // All target shards are in CLONE state.
        // At this point the handoff is in progress, all target shards received handoff requests
        // but didn't respond to them due to the `stateTransitionBlock`.
        // Since handoff is in progress, source acquired all primary operation permits
        // meaning that refresh below will not complete until the handoff completes
        // (which in this test means never since we blocked handoff).
        try {
            client(searchCoordinator).admin().indices().prepareRefresh(indexName).get(TimeValue.timeValueMillis(500));
            fail("It is possible to perform refresh while handoff is in progress");
        } catch (Exception e) {
            assertTrue(e instanceof ElasticsearchTimeoutException);
        }

        // We can still perform search on the source shard and see all previous writes due to the pre-reshard refresh.
        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(1), equalTo(allIndexedDocuments.keySet()));

        // At this point handoff is in progress and the source shard is holding all primary operation permits.
        // So writes will only succeed after HANDOFF transition is unblocked below, but they should succeed.
        var duringHandoffIndexedDocuments = new AtomicReference<Map<String, Integer>>();
        var duringHandoffIndexingThread = new Thread(() -> {
            var docs = indexDocuments(searchTestType, indexName, documentsPerRound, idSupplier, wouldBeAfterSplitRouting);
            allIndexedDocuments.putAll(docs);
            duringHandoffIndexedDocuments.set(docs);
        });
        duringHandoffIndexingThread.start();

        // Refresh should be blocked until the target shard is in SPLIT state. Start a request now and verify
        // that it doesn't complete until then.
        final var refresh = client(searchCoordinator).admin().indices().prepareRefresh(indexName).execute();
        final var refreshThread = new Thread(() -> {
            var refreshResponse = refresh.actionGet();
            assertEquals(Arrays.toString(refreshResponse.getShardFailures()), multiple, refreshResponse.getTotalShards());
            assertEquals(Arrays.toString(refreshResponse.getShardFailures()), multiple, refreshResponse.getSuccessfulShards());
            // Note that the coordinator does not need to observe SPLIT since refresh requests are resplit.
            var split = getSplit(indexNode, index);
            assertTrue(
                "Unexpected split state " + split,
                split.targetStates().allMatch(IndexReshardingState.Split.TargetShardState.SPLIT::equals)
            );
        });
        refreshThread.start();

        // unblock HANDOFF transition
        stateTransitionBlock.await();

        duringHandoffIndexingThread.join(SAFE_AWAIT_TIMEOUT.millis());

        awaitClusterState(
            searchCoordinator,
            clusterState -> indexMetadata(clusterState, index).getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.HANDOFF)
        );

        // Once the handoff happens the source shard will release primary operation permits and execute indexing
        // and refresh that was blocked.
        // But refresh is still be blocked on the target shard.
        // So we may see some newly indexed documents belonging to the source shard here.
        // We don't know what exact documents these are since refresh happens concurrently with indexing.
        var duringHandoffDocumentsBelongingToTheSourceShard = duringHandoffIndexedDocuments.get()
            .entrySet()
            .stream()
            .filter(e -> e.getValue() == 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        // This is a complicated way to say what is described above:
        // 1. Everything in `initialIndexedDocuments` should be returned
        // 2. Anything beyond that is just the source portion of `duringHandoffIndexedDocuments`
        var indexedDuringHandoffMatcher = everyItem(
            is(either(in(initialIndexedDocuments.keySet())).or(in(duringHandoffDocumentsBelongingToTheSourceShard)))
        );
        assertSearchResults(
            searchCoordinator,
            indexName,
            searchTestType,
            equalTo(1),
            both(hasItems(initialIndexedDocuments.keySet().toArray(String[]::new))).and(indexedDuringHandoffMatcher)
        );

        // We can still index between handoff and split.
        var afterHandOffIndexedDocuments = indexDocuments(
            searchTestType,
            indexName,
            documentsPerRound,
            idSupplier,
            wouldBeAfterSplitRouting
        );
        allIndexedDocuments.putAll(afterHandOffIndexedDocuments);

        // The refresh is still blocked on the target shard.
        // The source shard refresh would complete but we don't get a clear signal when that happens
        // and so can't really assert much here.

        // unblock SPLIT transition
        stateTransitionBlock.await();

        awaitClusterState(
            searchCoordinator,
            clusterState -> clusterState.getMetadata()
                .indexMetadata(index)
                .getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.SPLIT)
        );

        // Transition of target shards to DONE state is blocked, all targets are in SPLIT state.

        // Refresh that was blocked earlier should now complete.
        refreshThread.join(SAFE_AWAIT_TIMEOUT.millis());

        // As mentioned the source shard was refreshed shortly after the handoff and so we won't necessarily see all
        // documents indexed on the source shard here.
        var splitAllDocsOnTheTargetShard = allIndexedDocuments.entrySet()
            .stream()
            .filter(e -> e.getValue() == 1)
            .map(Map.Entry::getKey)
            .toArray(String[]::new);
        // Make sure we see every document that is on the target shard.
        var splitSearchMatcher = both(hasItems(splitAllDocsOnTheTargetShard)).and(everyItem(is(in(allIndexedDocuments.keySet()))));
        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(multiple), splitSearchMatcher);

        // If we issue a new refresh it shouldn't be blocked and search should return all documents.
        // Note that at this point `deleteUnownedDocuments()` is already done on target shards
        // because it is done before transition to DONE and `startStateTransitionBlock.await()` above
        // guarantees that all target shards sent the request to transition to DONE.
        // So this test does not directly exercise search filters, those are covered in other tests.
        var splitRefresh = client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        assertEquals(Arrays.toString(splitRefresh.getShardFailures()), multiple, splitRefresh.getTotalShards());
        assertEquals(Arrays.toString(splitRefresh.getShardFailures()), multiple, splitRefresh.getSuccessfulShards());

        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(multiple), equalTo(allIndexedDocuments.keySet()));

        // Indexing also works as expected.
        var splitIndexedDocuments = indexDocuments(searchTestType, indexName, documentsPerRound, idSupplier, wouldBeAfterSplitRouting);
        allIndexedDocuments.putAll(splitIndexedDocuments);

        // Sanity check that the indexing above worked.
        client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(multiple), equalTo(allIndexedDocuments.keySet()));

        // unblock DONE transition
        stateTransitionBlock.await();

        awaitClusterState(searchCoordinator, clusterState -> {
            var metadata = indexMetadata(clusterState, index);
            if (metadata.getReshardingMetadata() == null) {
                // all shards are DONE so resharding metadata is removed by master
                return true;
            }

            boolean targetsDone = metadata.getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.DONE);
            boolean sourceDone = metadata.getReshardingMetadata()
                .getSplit()
                .sourceStates()
                .allMatch(s -> s == IndexReshardingState.Split.SourceShardState.DONE);
            return targetsDone && sourceDone;
        });

        // All target shards and the source shard are DONE.
        var doneRefresh = client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        assertEquals(multiple, doneRefresh.getTotalShards());
        assertEquals(multiple, doneRefresh.getSuccessfulShards());

        // All unowned documents should be deleted now and we still see all expected documents.
        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(multiple), equalTo(allIndexedDocuments.keySet()));

        // Indexing still works as expected.
        var doneIndexedDocuments = indexDocuments(searchTestType, indexName, documentsPerRound, idSupplier, wouldBeAfterSplitRouting);
        allIndexedDocuments.putAll(doneIndexedDocuments);

        // Sanity check that the indexing above worked.
        client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        assertSearchResults(searchCoordinator, indexName, searchTestType, equalTo(multiple), equalTo(allIndexedDocuments.keySet()));

        waitForReshardCompletion(indexName);
    }

    private Map<String, Integer> indexDocuments(
        SearchTestType searchTestType,
        String indexName,
        int count,
        Supplier<String> idSupplier,
        IndexRouting finalRouting
    ) {
        var docsPerShard = new HashMap<String, Integer>();

        Supplier<Map<String, ?>> sourceSupplier = null;
        if (searchTestType == SearchTestType.ESQL_WITH_LOOKUP_JOIN) {
            sourceSupplier = () -> Map.of("field", randomUnicodeOfCodepointLengthBetween(1, 25), ESQL_JOIN_FIELD, ESQL_JOIN_FIELD_VALUE);
        }
        var response = indexDocs(indexName, count, UnaryOperator.identity(), idSupplier, sourceSupplier);
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        for (var item : response.getItems()) {
            int shard = finalRouting.getShard(item.getId(), null);
            docsPerShard.put(item.getId(), shard);
        }

        return docsPerShard;
    }

    private void assertSearchResults(
        String searchNode,
        String indexName,
        SearchTestType searchTestType,
        Matcher<Integer> shardCountMatcher,
        Matcher<? super Iterable<String>> documentIdsMatcher
    ) {
        switch (searchTestType) {
            case ESQL -> {
                final var query = "FROM $index METADATA _id | KEEP _id".replace("$index", indexName);
                final var request = new EsqlQueryRequest().allowPartialResults(false).query(query);

                try (var response = client(searchNode).execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
                    final var shardCount = response.getExecutionInfo().getCluster("").getTotalShards();
                    assertThat("unexpected shard count in ESQL response", shardCount, shardCountMatcher);

                    var idColumn = response.column(0);
                    var ids = new HashSet<String>();
                    while (idColumn.hasNext()) {
                        ids.add((String) idColumn.next());
                    }
                    assertThat("unexpected documents in ESQL response", ids, documentIdsMatcher);
                }
            }
            case SEARCH_API -> {
                assertResponse(prepareSearchAll(searchNode, indexName), response -> {
                    final var shardCount = response.getTotalShards();
                    assertThat("unexpected shard count in search response", shardCount, shardCountMatcher);

                    var ids = new HashSet<String>();
                    for (var hit : response.getHits().getHits()) {
                        ids.add(hit.getId());
                    }
                    assertThat("unexpected documents in search response", ids, documentIdsMatcher);
                });
            }
            case ESQL_WITH_LOOKUP_JOIN -> {
                final var query = "FROM $index METADATA _id | LOOKUP JOIN $join_index ON $join_field | KEEP _id".replace(
                    "$index",
                    indexName
                ).replace("$join_index", ESQL_JOIN_INDEX).replace("$join_field", ESQL_JOIN_FIELD);
                final var request = new EsqlQueryRequest().allowPartialResults(false).query(query);

                try (var response = client(searchNode).execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
                    final var shardCount = response.getExecutionInfo().getCluster("").getTotalShards();
                    assertThat("unexpected shard count in ESQL response", shardCount, shardCountMatcher);

                    var idColumn = response.column(0);
                    var ids = new HashSet<String>();
                    while (idColumn.hasNext()) {
                        ids.add((String) idColumn.next());
                    }
                    assertThat("unexpected documents in ESQL response", ids, documentIdsMatcher);
                }
            }
        }
    }

    public void testSearchWithCustomRoutingDuringReshard() throws Exception {
        String masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final int multiple = 2;
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        var index = resolveIndex(indexName);
        var indexMetadata = indexMetadata(internalCluster().clusterService(masterNode).state(), index);

        // We re-create the metadata directly in test in order to have access to after-reshard routing.
        var wouldBeMetadata = IndexMetadata.builder(indexMetadata)
            .reshardAddShards(multiple)
            .reshardingMetadata(
                IndexReshardingMetadata.newSplitByMultiple(1, multiple)
                    .transitionSplitTargetToNewState(new ShardId(index, 1), IndexReshardingState.Split.TargetShardState.HANDOFF)
            )
            .build();
        var wouldBeAfterSplitRouting = IndexRouting.fromIndexMetadata(wouldBeMetadata);

        int ingestedDocumentsPerShard = randomIntBetween(10, 20);

        var routingValuePerShard = new HashMap<Integer, String>() {
            {
                put(0, makeRoutingValueForShard(wouldBeAfterSplitRouting, 0));
                put(1, makeRoutingValueForShard(wouldBeAfterSplitRouting, 1));
            }
        };

        int id = 0;
        for (var shardAndRouting : routingValuePerShard.entrySet()) {
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < ingestedDocumentsPerShard; i++) {
                var indexRequest = client().prepareIndex(indexName).setId(String.valueOf(id++)).setRouting(shardAndRouting.getValue());

                var source = Map.of("field", randomUnicodeOfCodepointLengthBetween(1, 25));
                bulkRequest.add(indexRequest.setSource(source));
            }
            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);
        }

        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        refresh(indexName);

        var preSplitSearch = prepareSearchAll(indexName).setRouting(routingValuePerShard.get(1));

        // There is only one shard so any routing value will get routed to shard 0.
        assertResponse(preSplitSearch, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard * multiple, r.getHits().getTotalHits().value());
        });

        // Start split and block transition to HANDOFF.
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        CountDownLatch handOffStarted = new CountDownLatch(multiple - 1); // (multiple - 1) target shards
        CyclicBarrier stateTransitionBlock = new CyclicBarrier(multiple); // (multiple - 1) target shards + the test itself

        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    if (handOffStarted.getCount() > 0) {
                        handOffStarted.countDown();
                    }
                    stateTransitionBlock.await();
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        });

        awaitClusterState(searchNode, clusterState -> indexMetadata(clusterState, index).getReshardingMetadata() != null);

        // wait for all target shards to arrive at handoff point
        handOffStarted.await();

        // Don't refresh since it is blocked anyway, covered in other tests.

        // Transition to HANDOFF is blocked, all target shards are in CLONE state.
        var cloneSearch = prepareSearchAll(searchNode, indexName).setRouting(routingValuePerShard.get(1));

        // Since all target shards are in CLONE state they are not considered in routing logic at this point.
        // So even though the routing value used will eventually route to shard 1, right now this search
        // is still routed to shard 0 and all documents are returned (consistent with the search before split started).
        assertResponse(cloneSearch, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard * multiple, r.getHits().getTotalHits().value());
        });

        // unblock HANDOFF transition
        stateTransitionBlock.await();

        // Wait for recovery of the target shard to complete because a flush is performed during recovery
        // and we don't want to block it.
        awaitClusterState(masterNode, clusterState -> clusterState.routingTable().index(index).shard(1).primaryShard().started());

        awaitClusterState(
            searchNode,
            clusterState -> indexMetadata(clusterState, index).getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.HANDOFF)
        );

        // Transition to SPLIT is blocked, all target shards are in HANDOFF state.
        var handoffSearch = prepareSearchAll(searchNode, indexName).setRouting(routingValuePerShard.get(2));

        // Since all target shards are in HANDOFF state they are not considered in routing logic at this point.
        // So even though the routing value used will eventually route to shard 2, right now this search
        // is still routed to shard 0 and all documents are returned (consistent with the search before split started).
        assertResponse(handoffSearch, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard * multiple, r.getHits().getTotalHits().value());
        });

        var commitUploadLatch = new CountDownLatch(multiple - 1); // number of target shards
        // Block commit uploads in order to block the flush performed after deletion of unowned documents.
        // This allows to test shards in SPLIT state before they execute `deleteUnownedDocuments()` and test search filters.
        setNodeRepositoryStrategy(indexNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                try {
                    if (commitUploadLatch.getCount() > 0) {
                        commitUploadLatch.await();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        // unblock SPLIT transition
        stateTransitionBlock.await();

        awaitClusterState(
            searchNode,
            clusterState -> indexMetadata(clusterState, index).getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.SPLIT)
        );

        try {
            var splitSearchShard0 = prepareSearchAll(searchNode, indexName).setRouting(routingValuePerShard.get(0));

            // Now that target shards are in SPLIT state they should be considered in routing calculation.

            // This search uses routing value that routes to shard 0 so search is send to shard 0.
            assertResponse(splitSearchShard0, r -> {
                assertEquals(1, r.getTotalShards());
                assertEquals(ingestedDocumentsPerShard, r.getHits().getTotalHits().value());
            });

            for (int targetShardId = 1; targetShardId < multiple; targetShardId++) {
                var splitSearchTargetShard = prepareSearchAll(searchNode, indexName).setRouting(routingValuePerShard.get(targetShardId));

                // Search is correctly routed to the target shard based on the routing parameter in the search request.
                // Search filter is correctly applied and only owned documents are returned.
                int finalShardId = targetShardId;
                assertResponse(splitSearchTargetShard, r -> {
                    assertEquals(1, r.getTotalShards());
                    assertEquals(ingestedDocumentsPerShard, r.getHits().getTotalHits().value());
                    assertEquals(ingestedDocumentsPerShard, r.getHits().getHits().length);
                    for (var hit : r.getHits().getHits()) {
                        assertEquals(finalShardId, hit.getShard().getShardId().getId());
                        assertEquals(routingValuePerShard.get(finalShardId), hit.getMetadataFields().get("_routing").getValue());
                    }
                });
            }
        } finally {
            // Unblock commit uploads on all target shards.
            for (int i = 0; i < multiple - 1; i++) {
                commitUploadLatch.countDown();
                commitUploadLatch.countDown();
            }
        }

        // unblock DONE transition
        stateTransitionBlock.await();

        awaitClusterState(searchNode, clusterState -> {
            var metadata = indexMetadata(clusterState, index);
            if (metadata.getReshardingMetadata() == null) {
                // all shards are DONE so resharding metadata is removed by master
                return true;
            }

            boolean targetsDone = metadata.getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.DONE);
            boolean sourceDone = metadata.getReshardingMetadata()
                .getSplit()
                .sourceStates()
                .allMatch(s -> s == IndexReshardingState.Split.SourceShardState.DONE);
            return targetsDone && sourceDone;
        });

        for (int shardId = 0; shardId < multiple; shardId++) {
            var splitSearchTargetShard = prepareSearchAll(searchNode, indexName).setRouting(routingValuePerShard.get(shardId));

            // Search is correctly routed to the shard based on the routing parameter in the search request.
            // All unowned documents are deleted and are not returned.
            int finalShardId = shardId;
            assertResponse(splitSearchTargetShard, r -> {
                assertEquals(1, r.getTotalShards());
                assertEquals(ingestedDocumentsPerShard, r.getHits().getTotalHits().value());
                assertEquals(ingestedDocumentsPerShard, r.getHits().getHits().length);
                for (var hit : r.getHits().getHits()) {
                    assertEquals(finalShardId, hit.getShard().getShardId().getId());
                    assertEquals(routingValuePerShard.get(finalShardId), hit.getMetadataFields().get("_routing").getValue());
                }
            });
        }

        waitForReshardCompletion(indexName);
    }

    /*
     * Test that search at DONE returns results consistent with delete unowned having been applied (#5404)
     */
    public void testSearchAtDone() throws Exception {
        // separate from the master node to create interposable TransportUpdateSplitTargetShardStateAction
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final int numDocs = randomIntBetween(20, 50);
        indexDocs(indexName, numDocs);

        // block receiving commit notification requests on the search node after split, then attempt to wait for DONE
        // briefly (should time out because DONE should wait for notification to be acknowledged). Perform search,
        // check that it doesn't have too many documents (it's still filtering unowned). Release commit block.

        final var deferredNotifications = new LinkedBlockingQueue<CheckedRunnable<Exception>>();
        final var blockNotification = new AtomicBoolean(false);
        final var notificationBlocked = new CountDownLatch(1);
        final var notificationsProcessed = new AtomicBoolean(false);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                if (blockNotification.get()) {
                    logger.info("deferring new commit notification {}", request);
                    deferredNotifications.add(() -> {
                        logger.info("processing deferred notification {}", request);
                        handler.messageReceived(request, channel, task);
                    });
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                logger.info("received update split target shard state request {}", actualRequest);

                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                        logger.info("signalling to block commit notification");
                        blockNotification.set(true);
                        notificationBlocked.countDown();
                    }
                    assert splitStateRequest.getNewTargetShardState() != IndexReshardingState.Split.TargetShardState.DONE
                        || notificationsProcessed.get() : "all commit notifications should have been processed first";
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // once we've started blocking commit notifications, wait a bit before releasing them so that if resharding were going
        // to move to DONE without waiting for the notifications, we'd (probably) catch it here. Maybe there's a better way?
        // This works reliably on my laptop to catch the failure if the refresh is disabled.
        notificationBlocked.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        // do it in the background so we can start waiting for DONE concurrently and run search immediately afterwards.
        final var unblockThread = new Thread(() -> {
            try {
                Thread.sleep(100); // allow reshard to reach the refresh-wait in deleteUnownedDocuments
                blockNotification.set(false);
                while (deferredNotifications.isEmpty() == false) {
                    deferredNotifications.take().run();
                }
                notificationsProcessed.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        unblockThread.start();

        waitForReshardCompletion(indexName);
        assertHitCount(prepareSearchAll(indexName), numDocs);
        unblockThread.join();
    }

    /*
     * The coordinator may see the target move to SPLIT before the source shard does.
     * This test checks that filtering is performed in that case, as a regression because there was a bug where
     * previously it was not, resulting in duplicate documents in search results.
     */
    public void testSearchWhenCoordinatorSeesSplitFirst() {
        // install a block on the search node to prevent it from seeing the split transition, then kick off a reshard.
        // When the coordinator sees the split, issue a search and verify the expected number of documents are returned.
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        final var index = resolveIndex(indexName);

        final int numDocs = randomIntBetween(20, 50);
        indexDocs(indexName, numDocs);

        // block split application on stale search node
        final CountDownLatch completedSearch = new CountDownLatch(1);
        final var searchClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
        final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    try {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                            // wait for search shard cluster block to be in place before releasing SPLIT
                            searchClusterService.getClusterApplierService().runOnApplierThread("get", Priority.IMMEDIATE, state -> {
                                try {
                                    completedSearch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }, ActionListener.noop());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // reshard up to split attempt
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        awaitClusterState(
            indexNode,
            clusterState -> clusterState.getMetadata()
                .indexMetadata(index)
                .getReshardingMetadata()
                .getSplit()
                .getTargetShardState(1) == IndexReshardingState.Split.TargetShardState.SPLIT
        );

        // issue search from the unblocked indexNode as coordinator
        logger.info("issuing search before stale node sees split");
        final var searchResponse = prepareSearchAll(indexNode, indexName).get();
        logger.info("search before stale node sees split complete");
        completedSearch.countDown();

        waitForReshardCompletion(indexName);

        try {
            assertHitCount(searchResponse, numDocs);
        } finally {
            searchResponse.decRef();
        }
    }

    /*
        This test exercises a known edge case of the search filters implementation related to "ghost terms".
        These are the terms from the unowned documents that are produced in this specific term aggregation case.
        By default, the check that term is a ghost is not performed in the aggregation and needs to be explicitly enabled.
     */
    public void testTermAggregationDuringReshard() throws BrokenBarrierException, InterruptedException {
        String masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        // Mapping needed to be able to do terms aggregation
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1).build()).setMapping("""
            {"properties":{"field":{"type": "keyword"}}}"""));
        ensureGreen(indexName);

        var index = resolveIndex(indexName);
        var indexMetadata = internalCluster().clusterService(masterNode).state().getMetadata().indexMetadata(index);

        // We re-create the metadata directly in test in order to have access to after-reshard routing.
        var wouldBeMetadata = IndexMetadata.builder(indexMetadata)
            .reshardAddShards(2)
            .reshardingMetadata(
                IndexReshardingMetadata.newSplitByMultiple(1, 2)
                    .transitionSplitTargetToNewState(new ShardId(index, 1), IndexReshardingState.Split.TargetShardState.HANDOFF)
            )
            .build();
        var wouldBeAfterSplitRouting = IndexRouting.fromIndexMetadata(wouldBeMetadata);

        int ingestedDocumentsPerShard = randomIntBetween(10, 20);

        Function<Integer, String> findRoutingValue = shard -> {
            while (true) {
                String routingValue = randomAlphaOfLength(5);
                int routedShard = wouldBeAfterSplitRouting.indexShard(new IndexRequest().id("dummy").routing(routingValue));
                if (routedShard == shard) {
                    return routingValue;
                }
            }
        };

        var routingValuePerShard = new HashMap<Integer, String>() {
            {
                put(0, findRoutingValue.apply(0));
                put(1, findRoutingValue.apply(1));
            }
        };

        int id = 0;
        for (var shardAndRouting : routingValuePerShard.entrySet()) {
            var bulkRequest = client().prepareBulk();
            for (int i = 0; i < ingestedDocumentsPerShard; i++) {
                var indexRequest = client().prepareIndex(indexName).setId(String.valueOf(id++)).setRouting(shardAndRouting.getValue());

                var source = Map.of("field", shardAndRouting.getValue());
                bulkRequest.add(indexRequest.setSource(source));
            }
            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);
        }

        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        refresh(indexName);

        // Start split and block transition to HANDOFF.
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        CyclicBarrier stateTransitionBlock = new CyclicBarrier(2); // target shard + the test itself

        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    stateTransitionBlock.await();
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        });

        awaitClusterState(searchNode, clusterState -> clusterState.getMetadata().indexMetadata(index).getReshardingMetadata() != null);

        // transition to HANDOFF
        stateTransitionBlock.await();

        // Wait for recovery of the target shard to complete because a flush is performed during recovery
        // and we don't want to block it.
        awaitClusterState(masterNode, clusterState -> clusterState.routingTable().index(index).shard(1).primaryShard().started());

        var commitUploadLatch = new CountDownLatch(1); // number of target shards

        // Block commit uploads in order to block the flush performed after deletion of unowned documents.
        // This allows to test shards in SPLIT state before they execute `deleteUnownedDocuments()` and test search filters.
        setNodeRepositoryStrategy(indexNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                try {
                    if (commitUploadLatch.getCount() > 0) {
                        commitUploadLatch.await();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });
        try {
            // transition to SPLIT
            stateTransitionBlock.await();

            awaitClusterState(
                searchNode,
                clusterState -> clusterState.getMetadata()
                    .indexMetadata(index)
                    .getReshardingMetadata()
                    .getSplit()
                    .targetStates()
                    .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.SPLIT)
            );

            var sourceShardTermsAggregation = prepareSearchAll(searchNode, indexName).setSize(0)
                // note the minDocCount(0) - this is the specific edge case we are testing
                .addAggregation(AggregationBuilders.terms("term").field("field").minDocCount(0))
                .setRouting(routingValuePerShard.get(0));

            assertResponse(sourceShardTermsAggregation, r -> {
                InternalMultiBucketAggregation<?, ?> terms = r.getAggregations().get("term");
                // TODO this is a current gap, we should only see one bucket here
                // since all documents routed to the target shard have the same value for "field"
                // Instead we see ghost term here.
                assertEquals(2, terms.getBuckets().size());
                assertEquals(ingestedDocumentsPerShard, terms.getBuckets().get(0).getDocCount());
                assertEquals(0, terms.getBuckets().get(1).getDocCount());
            });

            var targetShardTermsAggregation = prepareSearchAll(searchNode, indexName).setSize(0)
                // note the minDocCount(0) - this is the specific edge case we are testing
                .addAggregation(AggregationBuilders.terms("term").field("field").minDocCount(0))
                .setRouting(routingValuePerShard.get(1));

            assertResponse(targetShardTermsAggregation, r -> {
                InternalMultiBucketAggregation<?, ?> terms = r.getAggregations().get("term");
                // TODO this is a current gap, we should only see one bucket here
                // since all documents routed to the target shard have the same value for "field"
                // Instead we see ghost term here.
                assertEquals(2, terms.getBuckets().size());
                var bucket = terms.getBuckets().get(0);
                var secondBucket = terms.getBuckets().get(1);
                if (bucket.getDocCount() == 0) {
                    assertEquals(routingValuePerShard.get(0), bucket.getKey());

                    assertEquals(routingValuePerShard.get(1), secondBucket.getKey());
                    assertEquals(ingestedDocumentsPerShard, secondBucket.getDocCount());
                } else {
                    assertEquals(routingValuePerShard.get(0), secondBucket.getKey());
                    assertEquals(0, secondBucket.getDocCount());

                    assertEquals(routingValuePerShard.get(1), bucket.getKey());
                    assertEquals(ingestedDocumentsPerShard, bucket.getDocCount());
                }
            });
        } finally {
            commitUploadLatch.countDown();
        }

        // unblock transition to DONE
        stateTransitionBlock.await();

        waitForReshardCompletion(indexName);
    }

    public void testReshardEmptyIndex() {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        final int multiple = 2;

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        waitForReshardCompletion(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, multiple);

        assertHitCount(prepareSearchAll(indexName), 0);

        // All shards should be usable
        var shards = IntStream.range(0, multiple).boxed().collect(Collectors.toSet());
        int docsPerRequest = randomIntBetween(10, 100);
        int indexedDocs = 0;
        do {
            for (var item : indexDocs(indexName, docsPerRequest).getItems()) {
                indexedDocs += 1;
                shards.remove(item.getResponse().getShardId().getId());
            }
        } while (shards.isEmpty() == false);

        refresh(indexName);

        assertHitCount(prepareSearchAll(indexName), indexedDocs);
    }

    public void testReshardSearchShardWillNotBeAllocatedUntilIndexingShard() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();

        ensureStableCluster(3);

        /* This allocation rule is used to prevent the new reshard index shard from getting allocated.
         * This will also prevent the search shard from getting allocated, hence we have two search nodes above.
         */
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        assertBusy(() -> {
            GetSettingsResponse postReshardSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                .get();
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
                equalTo(2)
            );
        });

        // Briefly pause to give time for allocation to have theoretically occurred
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

        final var indexingShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(true);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, indexingShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();

        waitForReshardCompletion(indexName);
    }

    // test GET during resharding
    // Two gets are issued before resharding starts, so they see only the original shard.
    // They are blocked until resharding completes. Each routes to a different shard. Both should succeed,
    // but the one that routes to the target must retry after the coordinator sees the split.
    public void testGet() {
        startMasterOnlyNode();
        startSearchNode();
        final var indexNode = startIndexNode();

        ensureStableCluster(3);

        final String indexName = "test-get";
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var indexRoutingPostSplit = postSplitRouting(clusterService().state(), index, 2);

        // We'll set up two gets before resharding, so that they route as if there is only 1 shard.
        // get should find this document after resharding, on the original shard
        final var shard0docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 0);
        // get should find this document after resharding, on the target after an internal retry.
        final var shard1docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 1);
        indexDoc(indexName, shard0docId, "field", "shard0");
        indexDoc(indexName, shard1docId, "field", "shard1");

        var response = client().prepareGet(indexName, shard0docId).execute().actionGet();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().get("field"), equalTo("shard0"));
        response = client().prepareGet(indexName, shard1docId).execute().actionGet();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().get("field"), equalTo("shard1"));

        // install a block on sending get to the shard, then issue the get request.
        // Unblock the get after resharding completes so it may be rerouted
        // also block transition to DONE on target shard after split completes, so that the source
        // shard doesn't start delete-unowned before processing the get.
        final var SHARD_GET_ACTION = TransportGetAction.TYPE.name() + "[s]";
        final var getPrepared = new CountDownLatch(2);
        final var reshardDone = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(indexNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            // block GET once it is prepared until resharding completes
            if ((SHARD_GET_ACTION).equals(action)) {
                // signal that get has been prepared so resharding can start
                getPrepared.countDown();
                safeAwait(reshardDone);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var getShard0Response = new AtomicReference<GetResponse>();
        final var getShard0Thread = new Thread(
            () -> getShard0Response.set(
                client(indexNode).prepareGet(indexName, shard0docId).setRealtime(false).execute().actionGet(SAFE_AWAIT_TIMEOUT)
            )
        );
        getShard0Thread.start();

        final var getShard1Response = new AtomicReference<GetResponse>();
        final var getShard1Thread = new Thread(
            () -> getShard1Response.set(
                client(indexNode).prepareGet(indexName, shard1docId).setRealtime(false).execute().actionGet(SAFE_AWAIT_TIMEOUT)
            )
        );
        getShard1Thread.start();

        // don't start resharding until get is waiting on search shard
        safeAwait(getPrepared);
        final var reshardRequest = new ReshardIndexRequest(indexName, 2);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet(SAFE_AWAIT_TIMEOUT);
        waitForReshardCompletion(indexName);
        reshardDone.countDown();

        safeJoin(getShard0Thread);
        safeJoin(getShard1Thread);

        assertThat("Document should exist on source shard", getShard0Response.get().isExists(), is(true));
        assertThat(getShard0Response.get().getSource().get("field"), equalTo("shard0"));

        assertThat("Document should be found on target shard after retry", getShard1Response.get().isExists(), is(true));
        assertThat(getShard1Response.get().getSource().get("field"), equalTo("shard1"));
    }

    // test MultiGet during resharding - same pattern as testGet but with a single multiget request
    public void testMultiGet() {
        String masterOnlyNode = startMasterOnlyNode();
        startSearchNode();
        final var indexNode = startIndexNode();

        ensureStableCluster(3);

        final String indexName = "test-multiget";
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var indexRoutingPostSplit = postSplitRouting(clusterService().state(), index, 2);

        // this document should be found by multiget after resharding, on the original shard
        final var shard0docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 0);
        // this document should fail in multiget after resharding because the request is stale
        final var shard1docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 1);
        indexDoc(indexName, shard0docId, "field", "shard0");
        indexDoc(indexName, shard1docId, "field", "shard1");

        var response = client().prepareMultiGet().add(indexName, shard0docId).add(indexName, shard1docId).execute().actionGet();
        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field"), equalTo("shard0"));
        assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[1].getResponse().getSource().get("field"), equalTo("shard1"));

        // install a block on sending multiget to the shard, then issue the multiget request.
        // Unblock the multiget after resharding completes
        final var SHARD_MGET_ACTION = TransportShardMultiGetAction.TYPE.name() + "[s]";
        final var mgetPrepared = new CountDownLatch(1);
        final var reshardDone = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(masterOnlyNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            // block MultiGet once it is prepared until resharding completes
            if ((SHARD_MGET_ACTION).equals(action)) {
                // signal that multiget has been prepared so resharding can start
                mgetPrepared.countDown();
                safeAwait(reshardDone);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var mgetResponse = new AtomicReference<MultiGetResponse>();
        final var mgetThread = new Thread(() -> {
            try {
                // execute will block until reshardDone counts down
                mgetResponse.set(
                    client(masterOnlyNode).prepareMultiGet()
                        .add(indexName, shard0docId)
                        .add(indexName, shard1docId)
                        .setRealtime(false)
                        .execute()
                        .actionGet(SAFE_AWAIT_TIMEOUT)
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        mgetThread.start();

        // don't start resharding until multiget is waiting to be sent to shard
        safeAwait(mgetPrepared);
        final var reshardRequest = new ReshardIndexRequest(indexName, 2);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet(SAFE_AWAIT_TIMEOUT);
        waitForReshardCompletion(indexName);
        reshardDone.countDown();

        safeJoin(mgetThread);

        response = mgetResponse.get();
        assertThat(response.getResponses().length, equalTo(2));

        // Although the request is initially too stale to serve, it should retry internally and succeed.
        for (int i = 0; i < response.getResponses().length; i++) {
            final var item = response.getResponses()[i];
            assertThat(item.getFailure(), is(nullValue()));
            assertThat(item.getResponse().isExists(), is(true));
            assertThat(item.getResponse().getSource().get("field"), equalTo("shard" + i));
        }
    }

    // A successful realtime get should return the latest value of a doc regardless of refresh.
    // It may fail if it has been routed to a stale shard due to concurrent resharding.
    public void testRealtimeGet() throws InterruptedException {
        startMasterOnlyNode();
        final var searchNode = startSearchNode();
        final var indexNode = startIndexNode();

        ensureStableCluster(3);

        final String indexName = "test-realtime-get";
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var indexRoutingPostSplit = postSplitRouting(clusterService().state(), index, 2);

        // prep realtime get but block until after resharding reaches split so that
        // it routes to the old shard
        // update doc and verify get returns the latest version
        final var shard1docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 1);
        indexDoc(indexName, shard1docId, "field", "shard1_v0");

        var response = client().prepareGet(indexName, shard1docId).setRealtime(true).execute().actionGet();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().get("field"), equalTo("shard1_v0"));

        final var getPrepared = new CountDownLatch(1);
        final var atSplit = new CountDownLatch(1);
        final var docUpdated = new CountDownLatch(1);

        final var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        final var searchNodeTransportService = MockTransportService.getInstance(searchNode);

        searchNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            // block get_from_translog until resharding reaches SPLIT so that the arriving request will be stale
            if ((TransportGetFromTranslogAction.NAME).equals(action)) {
                // signal that get has been prepared so resharding can start
                getPrepared.countDown();
                try {
                    docUpdated.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
                    logger.info("sending blocked get from translog");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        indexNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                        // block SPLIT transition until get has been prepared
                        atSplit.countDown();
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var getShard1Response = new AtomicReference<GetResponse>();
        final var getShard1Thread = new Thread(
            () -> getShard1Response.set(
                client(searchNode).prepareGet(indexName, shard1docId).setRealtime(true).execute().actionGet(SAFE_AWAIT_TIMEOUT)
            )
        );
        getShard1Thread.start();

        // don't start resharding until get is waiting on search shard
        getPrepared.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        final var reshardRequest = new ReshardIndexRequest(indexName, 2);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet(SAFE_AWAIT_TIMEOUT);
        atSplit.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        indexDoc(indexName, shard1docId, "field", "shard1_v1");
        docUpdated.countDown();
        getShard1Thread.join(SAFE_AWAIT_TIMEOUT.millis());

        assertThat(getShard1Response.get().isExists(), is(true));
        assertThat(getShard1Response.get().getSource().get("field"), equalTo("shard1_v1"));

        waitForReshardCompletion(indexName);
    }

    // A successful realtime multiget should return the latest values of docs regardless of refresh.
    public void testRealtimeMultiGet() {
        startMasterOnlyNode();
        final var searchNode = startSearchNode();
        final var indexNode = startIndexNode();

        ensureStableCluster(3);

        final String indexName = "test-realtime-multiget";
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var indexRoutingPostSplit = postSplitRouting(clusterService().state(), index, 2);

        // prep realtime multiget but block until after resharding reaches split so that
        // it routes to the old shard
        final var shard0docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 0);
        final var shard1docId = makeIdThatRoutesToShard(indexRoutingPostSplit, 1);
        indexDoc(indexName, shard0docId, "field", "shard0_v0");
        indexDoc(indexName, shard1docId, "field", "shard1_v0");

        var response = client().prepareMultiGet()
            .add(indexName, shard0docId)
            .add(indexName, shard1docId)
            .setRealtime(true)
            .execute()
            .actionGet();
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field"), equalTo("shard0_v0"));
        assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[1].getResponse().getSource().get("field"), equalTo("shard1_v0"));

        final var mgetPrepared = new CountDownLatch(1);
        final var atSplit = new CountDownLatch(1);

        final var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        final var searchNodeTransportService = MockTransportService.getInstance(searchNode);

        searchNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            // block ShardMultiGetFromTranslog until resharding reaches SPLIT
            if ((TransportShardMultiGetFomTranslogAction.NAME).equals(action)) {
                // signal that multiget has been prepared so resharding can start
                mgetPrepared.countDown();
                safeAwait(atSplit);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        indexNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                        atSplit.countDown();
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // expect item-level failures for docs that route to different shards after HANDOFF
        final var mgetThread = new Thread(() -> {
            MultiGetResponse mgetResponse = client(searchNode).prepareMultiGet()
                .add(indexName, shard0docId)
                .add(indexName, shard1docId)
                .setRealtime(true)
                .execute()
                .actionGet(SAFE_AWAIT_TIMEOUT);

            // Check that we have results for both documents
            assertThat(mgetResponse.getResponses().length, equalTo(2));

            MultiGetItemResponse item0 = mgetResponse.getResponses()[0];
            assertThat("Document on source shard should succeed", item0.isFailed(), is(false));
            assertThat(item0.getResponse().isExists(), is(true));

            MultiGetItemResponse item1 = mgetResponse.getResponses()[1];
            assertThat("Document moved to target shard should succeed after retry", item1.isFailed(), is(false));
            assertThat(item1.getResponse().isExists(), is(true));

        });
        mgetThread.start();

        // don't start resharding until multiget is waiting on search shard
        safeAwait(mgetPrepared);
        final var reshardRequest = new ReshardIndexRequest(indexName, 2);
        client().execute(TransportReshardAction.TYPE, reshardRequest).actionGet(SAFE_AWAIT_TIMEOUT);
        waitForReshardCompletion(indexName);

        safeJoin(mgetThread);
    }

    public void testReshardMustMatchExpectedNumberOfShards() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String failsIndexName = "test-index-fails";
        createIndex(failsIndexName, indexSettings(1, 0).build());
        ensureGreen(failsIndexName);

        // We are allowed only to double the number of shards
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(failsIndexName, 4)).actionGet()
        );

        assertThat(iae.getMessage(), equalTo("Requested new shard count [4] does not match required new shard count [2]"));
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.commits:debug", reason = "Issue #6241")
    public void testReshardFailsDuringResize() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        String sourceIndex = "source";
        int sourceShards = 2;

        createIndex(sourceIndex, indexSettings(sourceShards, 1).build());
        ensureGreen(sourceIndex);

        int numDocs = randomIntBetween(1, 20);
        indexDocsAndRefresh(sourceIndex, numDocs);
        flush(sourceIndex);

        // Set write block (required for resize)
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);

        // Start a clone with ActiveShardCount.NONE so it returns immediately,
        // and route the target's shards to a non-existent node so they never allocate.
        // This keeps the resizing operation without completing.
        String clonedIndex = "cloned";
        var cloneRequest = resizeRequest(
            ResizeType.CLONE,
            sourceIndex,
            clonedIndex,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.require._name", "non_existent_node")
                .putNull("index.blocks.write")
        );
        cloneRequest.setWaitForActiveShards(ActiveShardCount.NONE);
        client(indexNode).execute(TransportResizeAction.TYPE, cloneRequest).actionGet(SAFE_AWAIT_TIMEOUT);

        // Attempt to reshard the source — should be rejected.
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(sourceIndex)).actionGet(SAFE_AWAIT_TIMEOUT)
        );
        assertThat(e.getMessage(), containsString("cannot be resharded while it is the source of a resize operation"));

        // Let resize complete
        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.require._name"), clonedIndex);
        ensureGreen(clonedIndex);
        refresh(clonedIndex);
        logger.info("Resizing completed");

        // Now we can reshard the source index
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(sourceIndex)).actionGet(SAFE_AWAIT_TIMEOUT);
        waitForReshardCompletion(sourceIndex);
        checkNumberOfShardsSetting(indexNode, sourceIndex, sourceShards * 2);
        refresh(sourceIndex);
        logger.info("Resharding completed");

        // Write new documents to the cloned index
        int numNewDocs = randomIntBetween(0, 50);
        if (numNewDocs > 0) {
            indexDocsAndRefresh(clonedIndex, numNewDocs);
        }
        logger.info("{} new docs to cloned", numNewDocs);

        // Verify all documents are present in the cloned index
        assertHitCount(prepareSearch(clonedIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs + numNewDocs);

        // Verify all documents are present in the resharded source index
        ensureGreen(sourceIndex);
        assertHitCount(prepareSearch(sourceIndex).setSize(0).setQuery(QueryBuilders.matchAllQuery()), numDocs);
    }

    public void testReshardFailsWithNullIndex() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // Test null index
        ReshardIndexRequest request = new ReshardIndexRequest("");
        expectThrows(InvalidIndexNameException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardFailsWithWildcardIndex() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName1 = "test-1";
        final String indexName2 = "test-2";
        createIndex(indexName1, indexSettings(1, 0).build());
        createIndex(indexName2, indexSettings(1, 0).build());
        ensureGreen(indexName1);
        ensureGreen(indexName2);

        // Multiple indices not allowed "test*"
        ReshardIndexRequest request = new ReshardIndexRequest("test*");
        expectThrows(IndexNotFoundException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardWithIndexClose() throws Exception {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo(100L));

        assertBusy(() -> closeIndices(indexName));
        expectThrows(
            IndexClosedException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet()
        );

        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(1)
        );
    }

    public void testDeleteByQueryAfterReshard() throws Exception {
        final int NUM_DOCS = 100;

        String indexNode = startMasterAndIndexNode();
        startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, NUM_DOCS);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo(100L));

        logger.info("starting reshard");
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        waitForReshardCompletion(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 2);
        ensureGreen(indexName);

        // doc count should not have increased since both shards will have deleted unowned documents
        assertResponse(prepareSearchAll(indexName), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) NUM_DOCS));
        });
    }

    public void testReshardWithConcurrentIndexClose() throws Exception {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo(100L));

        // Close an index concurrently with resharding.
        // Either the close or the reshard should fail here since close logic
        // validates that index is not being concurrently resharded.
        AtomicBoolean success = new AtomicBoolean(false);
        runInParallel(2, i -> {
            if (i == 0) {
                Consumer<Exception> assertCloseException = e -> {
                    if (e instanceof IllegalArgumentException ia) {
                        assertTrue(
                            "Unexpected exception from index close operation: " + e,
                            ia.getMessage().contains("Cannot close indices that are being resharded")
                        );
                    } else if (e instanceof IllegalStateException is) {
                        assertTrue(
                            "Unexpected exception from index close operation: " + e,
                            is.getMessage().contains("index is being resharded in the meantime")
                        );
                    } else {
                        fail("Unexpected exception from index close operation: " + e);
                    }
                };

                try {
                    Thread.sleep(randomIntBetween(0, 200));

                    var response = indicesAdmin().prepareClose(indexName).get();
                    assertEquals(1, response.getIndices().size());
                    var indexResponse = response.getIndices().get(0);
                    assertEquals(indexName, indexResponse.getIndex().getName());
                    if (indexResponse.hasFailures()) {
                        assertCloseException.accept(indexResponse.getException());
                    }
                } catch (Exception e) {
                    assertCloseException.accept(e);
                }
            } else {
                try {
                    Thread.sleep(randomIntBetween(0, 200));

                    client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
                    waitForReshardCompletion(indexName);
                    success.set(true);
                } catch (Exception e) {
                    // IndexNotFoundException is possible if closed index is already removed from the node at this time.
                    boolean isExpectedException = e instanceof IndexClosedException || e instanceof IndexNotFoundException;
                    assertTrue("Unexpected error while resharding an index: " + e, isExpectedException);
                }
            }
        });

        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        var expectedNumberOfShards = success.get() ? 2 : 1;
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(expectedNumberOfShards)
        );
    }

    // If two concurrent resharding requests are submitted for the same index with the same target shard count, one of them should fail,
    // either because resharding metadata is already present, or
    // resharding metadata is gone and the reshard shardcount not 2x current shard count .
    public void testConcurrentReshardWithSameTargetShardsFails() throws Exception {
        String masterNode = startMasterOnlyNode();
        var indexNodes = startIndexNodes(2);

        ensureStableCluster(3);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(2, 0).build());
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        String indexNodeA = indexNodes.get(0);
        String indexNodeB = indexNodes.get(1);
        indexDocs(indexName, 100);

        // validates that index is not being concurrently resharded.
        runInParallel(2, i -> {
            Consumer<Exception> assertReshardException = e -> {
                if (e instanceof IllegalArgumentException ia) {
                    assertTrue(
                        "Unexpected exception from index reshard operation: " + e,
                        ia.getMessage().contains("Requested new shard count [4] does not match required new shard count [8]")
                    );
                } else if (e instanceof IllegalStateException is) {
                    assertTrue(
                        "Unexpected exception from index reshard operation: " + e,
                        is.getMessage().contains("an existing resharding operation on " + index + " is unfinished")
                    );
                } else {
                    fail("Unexpected exception from index reshard operation: " + e);
                }
            };
            if (i == 0) {
                try {
                    Thread.sleep(randomIntBetween(0, 1000));

                    client(indexNodeA).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 4)).actionGet();
                    waitForReshardCompletion(indexName);
                } catch (Exception e) {
                    logger.info("Reshard failed on node " + indexNodeA + " with " + e);
                    assertReshardException.accept(e);
                }
            } else {
                try {
                    Thread.sleep(randomIntBetween(0, 1000));

                    client(indexNodeB).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 4)).actionGet();
                    waitForReshardCompletion(indexName);
                } catch (Exception e) {
                    logger.info("Reshard failed on node " + indexNodeB + " with " + e);
                    assertReshardException.accept(e);
                }
            }
        });

        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(4)
        );
        ensureGreen(indexName);
    }

    public void testReshardSystemIndex() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = SYSTEM_INDEX_NAME + "-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        ReshardIndexRequest request = new ReshardIndexRequest(indexName);
        expectThrows(IllegalArgumentException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardDataStream() throws Exception {
        startMasterAndIndexNode();
        ensureStableCluster(1);

        var putComposableIndexTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("my_data_stream_template")
            .indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("my-data-stream*"))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, putComposableIndexTemplateRequest));
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TimeValue.MINUS_ONE, TimeValue.MINUS_ONE, "my-data-stream")
            )
        );

        // Try to reshard the data stream itself, it should fail
        ReshardIndexRequest dataStreamRequest = new ReshardIndexRequest("my-data-stream");
        expectThrows(IndexNotFoundException.class, () -> client().execute(TransportReshardAction.TYPE, dataStreamRequest).actionGet());

        // Try to reshard an index that is part of a data stream, it should fail as well
        GetDataStreamAction.Response getDataStreamResponse = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "my-data-stream" })
        ).actionGet();
        var dataStreamIndex = getDataStreamResponse.getDataStreams().get(0).getDataStream().getWriteIndex();

        ReshardIndexRequest dataStreamIndexRequest = new ReshardIndexRequest(dataStreamIndex.getName());
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(TransportReshardAction.TYPE, dataStreamIndexRequest).actionGet()
        );
    }

    private void assertReshardNonstandardIndexFails(final String indexName, IndexMode indexMode) {
        ReshardIndexRequest request = new ReshardIndexRequest(indexName);
        IllegalArgumentException failure = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(TransportReshardAction.TYPE, request).actionGet(SAFE_AWAIT_TIMEOUT)
        );
        assertEquals("resharding " + indexMode + " indices not supported", failure.getMessage());
    }

    public void testReshardNonstandardIndexFails() {
        startMasterAndIndexNode();
        ensureStableCluster(1);

        // lookup indices are required to have exactly one shard
        final var lookupIndexName = "lookup-index";
        createIndex(lookupIndexName, indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build());
        ensureGreen(lookupIndexName);
        assertReshardNonstandardIndexFails(lookupIndexName, IndexMode.LOOKUP);

        final String logsdbIndexName = "logsdb-index";
        createIndex(
            logsdbIndexName,
            Settings.builder()
                .put(indexSettings(randomIntBetween(1, 5), 0).build())
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName())
                .build()
        );
        ensureGreen(logsdbIndexName);
        assertReshardNonstandardIndexFails(logsdbIndexName, IndexMode.LOGSDB);

        final String timeSeriesIndexName = "time-series-index";
        final Settings settings = Settings.builder()
            .put(indexSettings(randomIntBetween(1, 5), 0).build())
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList("routing_path", List.of("host"))
            .build();
        client().admin()
            .indices()
            .prepareCreate(timeSeriesIndexName)
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "host",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=long,time_series_metric=gauge"
            )
            .get();
        ensureGreen(timeSeriesIndexName);
        assertReshardNonstandardIndexFails(timeSeriesIndexName, IndexMode.TIME_SERIES);
    }

    public void testReshardTargetWillEqualToPrimaryTermOfSource() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);

        IndicesStatsResponse statsResponse = client(indexNode).admin().indices().prepareStats(indexName).execute().actionGet();
        long indexCount = statsResponse.getAt(0).getStats().indexing.getTotal().getIndexCount();
        assertThat(indexCount, equalTo(100L));

        Index index = resolveIndex(indexName);
        long currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);

        int primaryTermIncrements = randomIntBetween(2, 4);
        for (int i = 0; i < primaryTermIncrements; i++) {
            IndexShard indexShard = findIndexShard(index, 0);
            indexShard.failShard("broken", new Exception("boom local"));
            long finalCurrentPrimaryTerm = currentPrimaryTerm;
            assertBusy(() -> assertThat(getCurrentPrimaryTerm(index, 0), greaterThan(finalCurrentPrimaryTerm)));
            ensureGreen(indexName);
            currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);
        }

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        waitForReshardCompletion(indexName);
        ensureGreen(indexName);

        assertThat(getCurrentPrimaryTerm(index, 1), equalTo(currentPrimaryTerm));
    }

    public void testReshardTargetWillNotTransitionToHandoffIfSourcePrimaryTermChanged() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);

        IndicesStatsResponse statsResponse = client(indexNode).admin().indices().prepareStats(indexName).execute().actionGet();
        long indexCount = statsResponse.getAt(0).getStats().indexing.getTotal().getIndexCount();
        assertThat(indexCount, equalTo(100L));

        Index index = resolveIndex(indexName);
        long currentPrimaryTerm = getCurrentPrimaryTerm(index, 0);

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(1);
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait for the first handoff attempt to trigger an UpdateSplitTargetShardStateAction on the target shard
        handoffAttemptedLatch.await();

        IndexShard indexShard = findIndexShard(index, 0);
        indexShard.failShard("broken", new Exception("boom local"));
        final long finalPrimaryTerm = currentPrimaryTerm;
        // Wait for the primary term to advance before releasing the handoff latch on the first handoff request, so that we
        // know that the shard failure has been processed.
        assertBusy(() -> assertThat(getCurrentPrimaryTerm(index, 0), greaterThan(finalPrimaryTerm)));

        handoffLatch.countDown();
        // Although the first attempt at HANDOFF will fail, the subsequent START_SPLIT attempt with the new source shard
        // will succeed.
        waitForReshardCompletion(indexName);
        ensureGreen(indexName);

        // The primary term has synchronized with the source
        assertThat(getCurrentPrimaryTerm(index, 1), equalTo(getCurrentPrimaryTerm(index, 0)));
    }

    public void testDeleteIndexOnceHandoffInitiated() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);
        indexDocs(indexName, 100);

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(1);
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait for the first handoff attempt (UpdateSplitTargetShardStateAction sent to target shard)
        handoffAttemptedLatch.await();

        // Delete the index while handoff is in progress
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet());

        // Release the handoff so the in-flight request proceeds; reshard will see index is gone
        handoffLatch.countDown();

        // Index must be gone from cluster state
        IndexNotFoundException inf = expectThrows(
            IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(indexName).get()
        );

        assertThat(inf.getMessage(), equalTo("no such index [" + indexName + "]"));
        ensureStableCluster(2);
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.reshard.ReshardIndexService:DEBUG", reason = "logging assertions")
    public void testReshardTargetStateWillNotTransitionTargetPrimaryTermChanged() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);

        IndicesStatsResponse statsResponse = client(indexNode).admin().indices().prepareStats(indexName).execute().actionGet();
        long indexCount = statsResponse.getAt(0).getStats().indexing.getTotal().getIndexCount();
        assertThat(indexCount, equalTo(100L));

        Index index = resolveIndex(indexName);

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        IndexReshardingState.Split.TargetShardState targetShardStateToDisrupt = randomFrom(
            IndexReshardingState.Split.TargetShardState.HANDOFF,
            IndexReshardingState.Split.TargetShardState.SPLIT,
            IndexReshardingState.Split.TargetShardState.DONE
        );
        CountDownLatch stateChangeAttemptedLatch = new CountDownLatch(switch (targetShardStateToDisrupt) {
            case HANDOFF -> 1;
            case SPLIT -> 2;
            case DONE -> 3;
            case CLONE -> throw new AssertionError();
        });
        CountDownLatch proceedAfterShardFailure = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action) && stateChangeAttemptedLatch.getCount() != 0) {
                try {
                    stateChangeAttemptedLatch.countDown();
                    if (stateChangeAttemptedLatch.getCount() == 0) {
                        proceedAfterShardFailure.await();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        stateChangeAttemptedLatch.await();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
            final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
            assertThat(projectState.routingTable().index(index).shard(1).primaryShard().allocationId(), notNullValue());
        });

        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
        final ShardRouting shardRouting = projectState.routingTable().index(index).shard(1).primaryShard();
        final long currentPrimaryTerm = getCurrentPrimaryTerm(index, 1);

        ShardStateAction shardStateAction = internalCluster().getInstance(ShardStateAction.class, internalCluster().getRandomNodeName());
        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        shardStateAction.localShardFailed(shardRouting, "broken", new Exception("boom remote"), listener);
        listener.actionGet();

        assertBusy(() -> assertThat(getCurrentPrimaryTerm(index, 1), greaterThan(currentPrimaryTerm)));

        MockLog.assertThatLogger(() -> {
            // When we release the handoff block the recovery will progress. However, it will fail because the target shard primary term
            // has advanced.
            proceedAfterShardFailure.countDown();
            waitForReshardCompletion(indexName);
        },
            ReshardIndexService.class,
            new MockLog.PatternSeenEventExpectation(
                "state transition failed",
                ReshardIndexService.class.getCanonicalName(),
                Level.DEBUG,
                ".*\\["
                    + indexName
                    + "\\]\\[1\\] cannot transition target state \\["
                    + targetShardStateToDisrupt
                    + "\\] because target primary term advanced \\[.*"
            )
        );

        // After the target shard recovery tries again it will synchronize its primary term with the source and come online.
        ensureGreen(indexName);
    }

    public void testSourceWillHandleHandoffFailure() throws Exception {
        String masterNode = startMasterOnlyNode();
        String sourceNode = startIndexNode();
        // We need 2 search nodes after resharding to get a green cluster state. This is because we limit number of shards
        // per node to 1 so that source and target shards go to different nodes.
        startSearchNodes(2);
        ensureStableCluster(4);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        // Prepare routing for the post-split index so we can send a doc to the target shard
        final Index index = resolveIndex(indexName);
        final IndexMetadata currentIndexMetadata = indexMetadata(clusterService().state(), index);
        final IndexMetadata indexMetadataPostSplit = IndexMetadata.builder(currentIndexMetadata).reshardAddShards(2).build();
        final IndexRouting indexRoutingPostSplit = IndexRouting.fromIndexMetadata(indexMetadataPostSplit);
        // Find a doc ID that will route to shard 1 (the target shard) after the split
        final String sourceShardRoutingValue = makeRoutingValueForShard(indexRoutingPostSplit, 0);
        final String targetShardRoutingValue = makeRoutingValueForShard(indexRoutingPostSplit, 1);

        // Index to setup and apply mappings.
        client(sourceNode).prepareIndex(indexName).setRouting(sourceShardRoutingValue).setSource("field", "value_target").get();

        // Start the second index node which will host the target shard
        String targetNode = startIndexNode();
        ensureStableCluster(5);

        MockTransportService sourceTransport = MockTransportService.getInstance(sourceNode);
        MockTransportService targetTransport = MockTransportService.getInstance(targetNode);

        CountDownLatch handoffFailedLatch = new CountDownLatch(1);

        // 1) Block cluster state updates on the source node so it never sees the HANDOFF transition.
        // We set this up before the HANDOFF RPC is sent. We use the pattern from
        // StatelessIntermediateReshardRoutingIT: block when the source is about to send the HANDOFF request.
        AtomicBoolean addOnce = new AtomicBoolean(true);
        sourceTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME.equals(action) && addOnce.compareAndSet(true, false)) {
                // Before the HANDOFF RPC is sent, block cluster state publications on the source node.
                // This ensures the source never observes the target transitioning to HANDOFF.
                sourceTransport.addRequestHandlingBehavior(
                    PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
                    (handler, req, channel, task) -> {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                        channel.sendResponse(new IllegalStateException("cluster state updates blocked on source"));
                    }
                );
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // 2) On the target node, intercept the HANDOFF request handler.
        // Let the handler process the request normally (which will update cluster state to HANDOFF),
        // but wrap the channel so the response sent back to the source is an error.
        targetTransport.addRequestHandlingBehavior(
            TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME,
            (handler, request, channel, task) -> {
                // Wrap the channel: convert success responses to errors
                TransportChannel failingChannel = new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        // The target has processed the handoff and updated cluster state to HANDOFF.
                        // But we simulate a network failure by sending an error back to the source.
                        channel.sendResponse(new ElasticsearchException("simulated handoff ACK failure"));
                        handoffFailedLatch.countDown();
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        assert false : "we did not expect an exception on target";
                        channel.sendResponse(exception);
                        handoffFailedLatch.countDown();
                    }
                };
                // Process the request normally (triggers HANDOFF state update) but with the failing channel
                handler.messageReceived(request, failingChannel, task);
            }
        );

        // Start the reshard
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait for the handoff failure to have occurred
        handoffFailedLatch.await();

        try {
            // At this point:
            // - The target has transitioned to HANDOFF in the cluster state
            // - The source received an error for the HANDOFF RPC and released permits
            // - The source has NOT observed the HANDOFF state (cluster state publications are blocked)
            // - The START_SPLIT RPC failed, so the target's state machine entered FailedInRecovery

            // Now try to index a document that should route to the target shard (shard 1).
            // The source has released permits and doesn't know about HANDOFF, so this document
            // will be indexed into the source shard only (not forwarded to the target).
            // This demonstrates the bug: the source should not release permits until it observes HANDOFF.
            Thread thread = new Thread(
                () -> client(sourceNode).prepareIndex(indexName)
                    .setRouting(targetShardRoutingValue)
                    .setSource("field", "value_target")
                    .get()
            );
            thread.start();

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            // Clean up transport rules to allow recovery to proceed
            sourceTransport.clearAllRules();
            targetTransport.clearAllRules();

            thread.join();

            // Without correct HANDOFF handling on the source shard, the document meant for the target shard also ends up being indexed
            // to the source shard post HANDOFF.
            assertShardDocCountEquals(new ShardId(index, 0), 1);
            assertShardDocCountEquals(new ShardId(index, 1), 1);
        } finally {
            // Perform a trivial cluster state update to ensure that failed updates are suddenly propagated to formerly isolated node.
            internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .submitUnbatchedStateUpdateTask("trivial cluster state update", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail(e);
                    }
                });
        }

        // The system should eventually recover:
        // - The target shard's recovery failed, so it will be reassigned
        // - On next recovery, the target sees HANDOFF state and starts from RecoveringInHandoff
        // - The state machine proceeds: Handoff -> SearchShardsOnline -> Split -> Done
        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
    }

    public void testSplitTargetWillNotStartUntilHandoff() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        startIndexNode();

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(1);
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME.equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        handoffAttemptedLatch.await();

        ensureRed(indexName);

        // Allow handoff to proceed
        handoffLatch.countDown();
        waitForReshardCompletion(indexName);

        ensureGreen(indexName);

    }

    public void testTargetDoesNotTransitionToSplitUntilSearchShardsActive() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startIndexNode();
        startSearchNode();

        ensureStableCluster(3);

        /* This allocation rule is used to prevent the new reshard index shard from getting allocated.
         * This will also prevent the search shard from getting allocated, hence we have two search nodes above.
         */
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        assertBusy(() -> {
            GetSettingsResponse postReshardSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                .get();
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
                equalTo(2)
            );
        });

        Index index = resolveIndex(indexName);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).setMetadata(true).get().getState();
            final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
            final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);
            final IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
            assertNotNull(reshardingMetadata);
            assertThat(reshardingMetadata.getSplit().getTargetShardState(1), equalTo(IndexReshardingState.Split.TargetShardState.HANDOFF));
        });

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // Pause briefly to give chance for a buggy state transition to proceed
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));

        // Assert still handoff and has not transitioned to SPLIT
        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).setMetadata(true).get().getState();
        final ProjectState projectState = state.projectState(state.metadata().projectFor(index).id());
        final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);
        final IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
        assertNotNull(reshardingMetadata);
        assertThat(reshardingMetadata.getSplit().getTargetShardState(1), equalTo(IndexReshardingState.Split.TargetShardState.HANDOFF));

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();

        waitForReshardCompletion(indexName);
    }

    public void testTargetWillTransitionToSplitIfSearchShardsActiveTimesOut() throws Exception {
        Settings settings = Settings.builder().put(SplitTargetService.RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT.getKey(), "200ms").build();
        String indexNode = startMasterAndIndexNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);

        ensureStableCluster(3);

        /* This allocation rule is used to prevent the new reshard index shard from getting allocated.
         * This will also prevent the search shard from getting allocated, hence we have two search nodes above.
         */
        final String indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        assertBusy(() -> {
            GetSettingsResponse postReshardSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                .get();
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
                equalTo(2)
            );
        });

        final var index = resolveIndex(indexName);
        awaitClusterState(
            indexNode,
            clusterState -> clusterState.getMetadata()
                .indexMetadata(index)
                .getReshardingMetadata()
                .getSplit()
                .getTargetShardState(1) == IndexReshardingState.Split.TargetShardState.SPLIT
        );

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);

        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();
    }

    // only one resharding operation on a given index should be allowed to be in flight at a time
    public void testConcurrentReshardFails() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        // create index
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, 1);

        // block allocation so that resharding will stall
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.enable", "none"));

        // start first resharding operation
        var splitState = waitForClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() != null);
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // wait until we know it's in progress
        var ignored = splitState.actionGet(SAFE_AWAIT_TIMEOUT).projectState().metadata().index(indexName).getReshardingMetadata();

        // now start a second reshard, which should fail
        assertThrows(
            IllegalStateException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(SAFE_AWAIT_TIMEOUT)
        );

        // unblock allocation to allow operations to proceed
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.enable"));

        waitForReshardCompletion(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 2);

        // now we should be able to resplit
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        waitForReshardCompletion(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, 4);
    }

    // This test checks that batched cluster state updates performed in scope of resharding are correct.
    public void testConcurrentReshardOfDifferentIndices() {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        int indexCount = randomIntBetween(1, 10);

        final String indexNameTemplate = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);

        for (int i = 0; i < indexCount; i++) {
            final String indexName = indexNameTemplate + i;
            createIndex(indexName, indexSettings(1, 0).build());
            ensureGreen(indexName);
            checkNumberOfShardsSetting(indexNode, indexName, 1);
        }

        // Block master service to force all resharding tasks to be executed in one batch.
        ServiceDisruptionScheme disruption = new BlockMasterServiceOnMaster(random());
        setDisruptionScheme(disruption);
        disruption.startDisrupting();

        var futures = new ArrayList<ActionFuture<ActionResponse.Empty>>();
        for (int i = 0; i < indexCount; i++) {
            final String indexName = indexNameTemplate + i;
            futures.add(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)));
        }

        disruption.stopDisrupting();

        for (int i = 0; i < indexCount; i++) {
            futures.get(i).actionGet();
            waitForReshardCompletion(indexNameTemplate + i);
        }

        for (int i = 0; i < indexCount; i++) {
            final String indexName = indexNameTemplate + i;
            checkNumberOfShardsSetting(indexNode, indexName, 2);
        }
    }

    // Test that documents are always routed to source shard before target shards are in handoff
    public void testWriteRequestsRoutedToSourceBeforeHandoff() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        IndexMetadata sourceMetadata = clusterService().state().projectState().metadata().index(indexName);

        // Build target Index metadata with 2 shards
        IndexMetadata targetMetadata = IndexMetadata.builder(sourceMetadata).reshardAddShards(2).build();

        // Generate a document id that would route to the target shard
        int numTargetDocs = 0;
        String targetDocId = null;
        while (numTargetDocs < 1) {
            String term = randomAlphaOfLength(10);
            final int shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(targetMetadata), term, null);
            if (shard == 1) {
                numTargetDocs++;
                targetDocId = term;
            }
        }

        startIndexNode();

        MockTransportService mockTransportService = MockTransportService.getInstance(indexNode);
        CountDownLatch handoffAttemptedLatch = new CountDownLatch(1);
        CountDownLatch handoffLatch = new CountDownLatch(1);
        mockTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME.equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        handoffAttemptedLatch.await();

        // Now index a document that belongs to the target shard and verify that it goes to the source shard id.
        IndexMetadata indexMetadata = clusterService().state().projectState().metadata().index(indexName);
        int shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(indexMetadata), targetDocId, null);
        assertThat(shard, equalTo(0));

        // Allow handoff to proceed
        handoffLatch.countDown();
        waitForReshardCompletion(indexName);
        indexMetadata = clusterService().state().projectState().metadata().index(indexName);
        // Again index a document that belongs to the target shard and verify that it goes to the target shard id.
        shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(indexMetadata), targetDocId, null);
        assertThat(shard, equalTo(1));

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();
    }

    // Test that documents are always routed to source shard before target shards are in handoff
    // Similar to testWriteRequestsRoutedToSourceBeforeHandoff() except we actually index the document into the
    // source shard.
    public void testIndexRequestBeforeHandoff() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        IndexMetadata sourceMetadata = clusterService().state().projectState().metadata().index(indexName);

        // Build target Index metadata with 2 shards
        IndexMetadata targetMetadata = IndexMetadata.builder(sourceMetadata).reshardAddShards(2).build();

        // Generate a document id that would route to the target shard
        int numTargetDocs = 0;
        String targetDocId = null;
        while (numTargetDocs < 1) {
            String term = randomAlphaOfLength(10);
            final int shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(targetMetadata), term, null);
            if (shard == 1) {
                numTargetDocs++;
                targetDocId = term;
            }
        }

        startIndexNode();

        final var preHandoffEnteredLatch = new CountDownLatch(1);
        final var preHandoffLatch = new CountDownLatch(1);
        var splitSourceService = internalCluster().getInstance(SplitSourceService.class, indexNode);
        splitSourceService.setPreHandoffHook(() -> {
            try {
                logger.info("waiting for prehandoff latch");
                preHandoffEnteredLatch.countDown();
                preHandoffLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
                logger.info("prehandoff latch released");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        preHandoffEnteredLatch.await(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);

        // Now index a document that belongs to the target shard and verify that it goes to the source shard id.
        IndexMetadata indexMetadata = clusterService().state().projectState().metadata().index(indexName);
        int shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(indexMetadata), targetDocId, null);
        assertThat(shard, equalTo(0));
        index(indexName, targetDocId, Map.of("foo", "bar"));
        logger.info("Target shard document indexed into source");

        // Allow handoff to proceed
        preHandoffLatch.countDown();
        waitForReshardCompletion(indexName);

        // verify that the index metadata returned matches the expected multiple of shards
        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(2)
        );

        refresh(indexName);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), 1);
        assertSearchHits(prepareSearch(indexName).setQuery(matchQuery("foo", "bar")), targetDocId);

        // Note that stats won't include data copied from the source shard to target shard,
        // since they didn't go through the "normal" indexing logic.
        final var indexStats = client().admin().indices().prepareStats(indexName).execute().actionGet();
        assertThat(getIndexCount(indexStats, 0), equalTo(1L));
        assertThat(getIndexCount(indexStats, 1), equalTo(0L));

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();
    }

    // Test race where a target shard gets marked as splitting and copies the source directory in the middle of an upload
    public void testUploadCopyRace() throws Exception {
        var reshardStarted = new AtomicBoolean(false);
        var startSplitLatch = new CountDownLatch(1);
        var uploadLatch = new CountDownLatch(1);

        var indexNode = startMasterAndIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build(),
            new StatelessMockRepositoryStrategy() {
                @Override
                public void blobContainerWriteBlobAtomic(
                    CheckedRunnable<IOException> originalRunnable,
                    OperationPurpose purpose,
                    String blobName,
                    InputStream inputStream,
                    long blobSize,
                    boolean failIfAlreadyExists
                ) throws IOException {
                    if (reshardStarted.get()) {
                        startSplitLatch.countDown();
                        try {
                            uploadLatch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    originalRunnable.run();
                }
            }
        );
        startSearchNode();
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, 1);

        indexDocs(indexName, 100);
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), 100);

        var indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportReshardSplitAction.START_SPLIT_ACTION_NAME.equals(action)) {
                try {
                    startSplitLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        var splitSourceService = internalCluster().getInstance(SplitSourceService.class, indexNode);
        splitSourceService.setPreHandoffHook(uploadLatch::countDown);
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        reshardStarted.set(true);

        // upload will unblock split and wait for pre-handoff hook
        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearchAll(indexName), 100);
    }

    public void testSourceRelocationDuringReshard() {
        testRelocationDuringReshard(0);
    }

    public void testTargetRelocationDuringReshard() {
        testRelocationDuringReshard(1);
    }

    private void testRelocationDuringReshard(int shardId) {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 100).build());
        ensureGreen(indexName);
        checkNumberOfShardsSetting(indexNodeA, indexName, 1);

        indexDocs(indexName, 100);
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), 100);

        // spin up new index node
        var indexNodeB = startIndexNode();
        ensureStableCluster(4);
        var clusterState = clusterService().state();

        logger.info("starting reshard");
        client(indexNodeA).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        clusterState = waitForClusterState(state -> {
            var shardRoutingTable = state.routingTable().index(indexName).shard(shardId);
            return shardRoutingTable != null && shardRoutingTable.primaryShard().state() == ShardRoutingState.STARTED;
        }).actionGet();
        var nodeId = clusterState.routingTable().index(indexName).shard(shardId).primaryShard().currentNodeId();
        var fromNode = clusterState.getNodes().get(nodeId).getName();
        var toNode = fromNode.equals(indexNodeA) ? indexNodeB : indexNodeA;
        logger.info("Relocating shard {} from {} to {}", shardId, fromNode, toNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, shardId, fromNode, toNode));

        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearchAll(indexName), 100);
    }

    public void testMismatchedSourceAndTargetPrimaryTerm() {
        String indexNode = startMasterAndIndexNode();
        String indexNodeB = startIndexNode();
        startSearchNodes(2);
        ensureStableCluster(4);

        // Exclude second node from allocation meaning that it won't be possible to allocate the source shard
        // and later the target shard on this node (since we also have shards per node limit).
        // This is needed to delay the recovery of the target shard until source shard primary term advances.
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", indexNodeB));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        Index index = resolveIndex(indexName);
        ensureGreen(indexName);
        // Disable rebalancing so the relocated shard doesn't get moved back
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false)
        );

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        int docs = randomIntBetween(10, 100);
        indexDocs(indexName, docs);
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), docs);

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        awaitClusterState(state -> state.routingTable(state.metadata().projectFor(index).id()).index(index).shard(1) != null);

        // Now fail and recover the source shard
        IndexShard indexShard = findIndexShard(index, 0);
        indexShard.failShard("broken", new Exception("boom local"));
        awaitClusterState(state -> state.metadata().indexMetadata(index).primaryTerm(0) > 1);

        // And unblock recovery of the target shard.
        // Target shard will initiate a start split request with latest source shard primary term but its own primary term is 1.
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", "null"));

        // Split successfully completes.
        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearchAll(indexName), docs);
    }

    public void testRestartTargetWhileSourceIsCopying() throws Exception {
        var copyLatch = new CountDownLatch(1);
        var restartLatch = new CountDownLatch(1);
        var copyBlockStrategy = new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCopyBlob(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                BlobContainer sourceBlobContainer,
                String sourceBlobName,
                String blobName,
                long blobSize
            ) throws IOException {
                restartLatch.countDown();
                try {
                    copyLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                super.blobContainerCopyBlob(originalRunnable, purpose, sourceBlobContainer, sourceBlobName, blobName, blobSize);
            }
        };

        startMasterOnlyNode();
        var indexNodeA = startIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        setNodeRepositoryStrategy(indexNodeA, copyBlockStrategy);
        startSearchNode();
        ensureStableCluster(3);

        // Disable rebalancing
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false)
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put("index.allocation.max_retries", Integer.MAX_VALUE).build());
        ensureGreen(indexName);

        int numDocs = 100;
        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertHitCount(prepareSearch(indexName), 100);

        // Spin up new index node
        var indexNodeB = startIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        setNodeRepositoryStrategy(indexNodeB, copyBlockStrategy);

        logger.info("starting reshard");
        client(indexNodeA).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait until source shard is stuck copying
        restartLatch.await();
        internalCluster().restartNode(indexNodeB);

        ensureStableCluster(4);
        copyLatch.countDown();

        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);
    }

    public void testSourceRelocationAndTargetRestart() throws Exception {
        Set<String> copiesInProgress = ConcurrentHashMap.newKeySet();
        var blobToBlock = new AtomicReference<String>();
        var blockedFirstCopy = new AtomicBoolean(false);
        var copyLatch = new CountDownLatch(1);
        var relocateLatch = new CountDownLatch(1);
        var testFailure = new AtomicReference<Throwable>();
        var copyRaceCheckStrategy = new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCopyBlob(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                BlobContainer sourceBlobContainer,
                String sourceBlobName,
                String blobName,
                long blobSize
            ) throws IOException {
                if (copiesInProgress.add(blobName) == false) {
                    // Copies from the old and new source shard have interfered
                    testFailure.compareAndSet(null, new AssertionError("Copy still in progress: " + blobName));
                }
                try {
                    // Block only the copy of latest blob from the old source shard
                    if (blobToBlock.get().equals(blobName) && blockedFirstCopy.getAndSet(true) == false) {
                        relocateLatch.countDown();
                        copyLatch.await();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                super.blobContainerCopyBlob(originalRunnable, purpose, sourceBlobContainer, sourceBlobName, blobName, blobSize);
                copiesInProgress.remove(blobName);
            }
        };

        var masterNode = startMasterOnlyNode();
        var indexNodes = startIndexNodes(
            3,
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        for (String indexNode : indexNodes) {
            setNodeRepositoryStrategy(indexNode, copyRaceCheckStrategy);
        }
        var searchNode = startSearchNode();
        ensureStableCluster(5);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).put("index.allocation.max_retries", Integer.MAX_VALUE).build());
        ensureGreen(indexName);

        int numDocs = 100;
        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);

        // Disable rebalancing
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false)
        );

        var index = resolveIndex(indexName);
        var sourceShardOldNode = findIndexNode(index, 0).getName();
        flush(indexName);
        var blobContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, sourceShardOldNode, 0);
        var latestBlob = blobContainer.listBlobs(INDICES)
            .values()
            .stream()
            .map(BlobMetadata::name)
            .max(Comparator.comparingLong(StatelessCompoundCommit::parseGenerationFromBlobName))
            .orElseThrow();
        blobToBlock.set(latestBlob);

        logger.info("starting reshard");
        client(sourceShardOldNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait until old source shard is stuck copying
        relocateLatch.await();

        var targetShardNodeId = new AtomicReference<String>();
        awaitClusterState(state -> {
            targetShardNodeId.set(state.routingTable().index(indexName).shard(1).primaryShard().currentNodeId());
            return targetShardNodeId.get() != null;
        });
        var targetShardNode = nodeIdsToNames().get(targetShardNodeId.get());
        assertNotEquals(sourceShardOldNode, targetShardNode);
        var sourceShardNewNode = indexNodes.stream()
            .filter(node -> node.equals(sourceShardOldNode) == false && node.equals(targetShardNode) == false)
            .findFirst()
            .orElseThrow();
        logger.info("Relocating source shard from {} to {}", sourceShardOldNode, sourceShardNewNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceShardOldNode, sourceShardNewNode));

        internalCluster().restartNode(targetShardNode);
        // Check that all split requests are routed to the new source shard instance.
        // Add send behavior to all index nodes since the target shard can get assigned to a different node after the restart
        for (var indexNode : indexNodes) {
            MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportReshardSplitAction.START_SPLIT_ACTION_NAME)) {
                    var splitRequest = (TransportReshardSplitAction.Request) request;
                    if (splitRequest.sourceNode().getName().equals(sourceShardOldNode)) {
                        testFailure.compareAndSet(null, new AssertionError("Target routed split request to old source shard"));
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        copyLatch.countDown();

        // This test simulates a scenario where the source shard gets stuck copying to the target. During this period, the source shard is
        // scheduled for relocation, which gets blocked because copying takes a permit. The target shard node also restarts, and upon
        // recovery it starts sending split requests to the new source shard, which rejects them since it hasn't started.
        //
        // Once the old source shard instance finishes copying, it releases the permit, unblocking relocation. After relocation, the old
        // source instance continues the initial split request and tries to acquire all primary permits in preparation for handoff but fails
        // as it is no longer the primary. The new source shard instance, once started, will proceed with the split request.
        //
        // This test checks that the old and new source shard instances don't copy concurrently. It also checks that the target shard routes
        // split requests to the new source shard instance.
        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);
        if (testFailure.get() != null) {
            fail(testFailure.get());
        }
    }

    public void testSplitStateNeedsToBeAppliedOnAllKnownNodes() throws Exception {
        startMasterOnlyNode(Settings.builder().put("cluster.publish.timeout", TimeValue.timeValueMillis(100)).build());
        String indexNode = startIndexNode();
        startSearchNodes(1);
        String nodeWithBlockedClusterStateProcessing = startSearchNode();
        ensureStableCluster(4);

        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", nodeWithBlockedClusterStateProcessing));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), numDocs);

        var splitAttempted = new CountDownLatch(1);
        var splitBlocked = new CountDownLatch(1);
        var doneAttempted = new CountDownLatch(1);
        var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    try {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                            splitAttempted.countDown();
                            splitBlocked.await();
                        } else if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE) {
                            if (doneAttempted.getCount() > 0) {
                                doneAttempted.countDown();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait for HANDOFF to complete normally and then start delaying cluster state updates on one node.
        splitAttempted.await();

        var clusterStateApplicationBlock = new CountDownLatch(1);
        internalCluster().getInstance(ClusterService.class, nodeWithBlockedClusterStateProcessing).addHighPriorityApplier(event -> {
            try {
                clusterStateApplicationBlock.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            // Unblock application of SPLIT state - it should not succeed because we are waiting for an ack.
            splitBlocked.countDown();

            assertFalse(doneAttempted.await(200, TimeUnit.MILLISECONDS));

            var doneAttemptedWaiter = new Thread(() -> {
                try {
                    doneAttempted.await();
                    assertEquals(0, clusterStateApplicationBlock.getCount());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            doneAttemptedWaiter.start();

            // Unblock stuck node to receive the update.
            clusterStateApplicationBlock.countDown();

            doneAttemptedWaiter.join(SAFE_AWAIT_TIMEOUT.millis());

            waitForReshardCompletion(indexName);
            ensureGreen(indexName);
            assertHitCount(prepareSearchAll(indexName), numDocs);
        } finally {
            clusterStateApplicationBlock.countDown();
        }
    }

    public void testReshardMetrics() throws Exception {
        String indexNode = startMasterAndIndexNode();
        startSearchNodes(1);
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numShards = randomIntBetween(1, 10);
        int numShardsAfter = 2 * numShards;
        createIndex(indexName, indexSettings(numShards, 1).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, numShardsAfter)).actionGet();
        waitForReshardCompletion(indexName);

        var telemetryPlugin = getTelemetryPlugin(indexNode);
        assertThat(getTotalLongCounterValue(ReshardMetrics.RESHARD_COUNT, getTelemetryPlugin(indexNode)), equalTo(1L));

        var reshardTargetShardCountHistogram = telemetryPlugin.getLongHistogramMeasurement(ReshardMetrics.RESHARD_TARGET_SHARD_COUNT);
        assertEquals(List.of((long) numShardsAfter), reshardTargetShardCountHistogram.stream().map(Measurement::getLong).toList());

        var cloneDurationHistogram = telemetryPlugin.getDoubleHistogramMeasurement(ReshardMetrics.RESHARD_TARGET_CLONE_DURATION);
        assertEquals(numShards, cloneDurationHistogram.size());
        var cloneDurationStats = cloneDurationHistogram.stream().mapToDouble(Measurement::getDouble).summaryStatistics();
        assertThat(cloneDurationStats.getMin(), greaterThanOrEqualTo(0.0));
        assertThat(cloneDurationStats.getMax(), lessThanOrEqualTo(30.0)); // timeout

        final List<String> durationHistograms = List.of(
            ReshardMetrics.RESHARD_INDEXING_BLOCKED_DURATION,
            ReshardMetrics.RESHARD_TARGET_HANDOFF_DURATION,
            ReshardMetrics.RESHARD_TARGET_SPLIT_DURATION
        );
        for (String durationHistogram : durationHistograms) {
            var histogram = telemetryPlugin.getLongHistogramMeasurement(durationHistogram);
            assertEquals(numShards, histogram.size());
            var stats = histogram.stream().mapToLong(Measurement::getLong).summaryStatistics();
            assertThat(stats.getMin(), greaterThanOrEqualTo(0L));
            assertThat(stats.getMax(), lessThanOrEqualTo(TimeValue.THIRTY_SECONDS.millis())); // timeout
        }
    }

    public void testSourceShardMonitoringSucceedsWhenTargetsAreAlreadyDone() throws InterruptedException, BrokenBarrierException {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNodes(1);
        ensureStableCluster(3);

        final String indexName = randomIndexName();
        int numShards = randomIntBetween(1, 10);
        createIndex(indexName, indexSettings(numShards, 1).build());
        ensureGreen(indexName);

        var changeSourceShardStateAttempts = new CyclicBarrier(numShards + 1); // source shards and the test itself
        var moveToDoneFailures = new CountDownLatch(numShards);
        var sourceShardMoveToDoneBlocked = new AtomicBoolean(true);

        // Wait for the source shards to observe target shards being DONE but don't allow them to proceed to READY_FOR_CLEANUP.
        var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitSourceShardStateAction.TYPE.name().equals(action)) {
                try {
                    changeSourceShardStateAttempts.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

                if (sourceShardMoveToDoneBlocked.get()) {
                    moveToDoneFailures.countDown();
                    throw new RuntimeException("broken");
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait for all search shards to attempt to transition to READY_FOR_CLEANUP meaning that all target shards (for all sources)
        // are now DONE.
        changeSourceShardStateAttempts.await();
        // Make sure we completed this "round" of source shard logic.
        moveToDoneFailures.await();

        // Now source shards will retry the entire state machine including waiting for target shards to be DONE.
        // That check should fast-succeed even though there are no cluster state changes happening at this time.
        // We specifically check this behavior since it is possible to get this wrong due to a "trap"
        // of using `waitForNextChange` on a newly created `ClusterStateObserver` instead of `ClusterStateObserver#waitForState`.
        // The latter checks the predicate on the current state but the former does not which would make us stall here.

        // So we expect all source shards to arrive at the transition to READY_FOR_CLEANUP state again and this time we'll allow them to
        // proceed.
        sourceShardMoveToDoneBlocked.set(false);
        changeSourceShardStateAttempts.await();
        // After READY_FOR_CLEANUP all source shards will proceed to DONE.
        changeSourceShardStateAttempts.await();

        waitForReshardCompletion(indexName);
    }

    public void testSourceSearchShardNotAwareOfSplit() throws InterruptedException {
        String masterNode = startMasterOnlyNode(Settings.builder().put("cluster.publish.timeout", TimeValue.timeValueMillis(100)).build());
        startIndexNodes(2);
        startSearchNodes(2);
        ensureStableCluster(5);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        checkNumberOfShardsSetting(masterNode, indexName, 1);

        var sourceSearchShardNode = clusterService().state()
            .nodes()
            .getNodes()
            .get(findSearchShard(index, 0).routingEntry().currentNodeId())
            .getName();
        var sourceIndexShardNode = clusterService().state()
            .nodes()
            .getNodes()
            .get(findIndexShard(index, 0).routingEntry().currentNodeId())
            .getName();

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), numDocs);

        var splitAttempted = new CountDownLatch(1);
        var splitBlocked = new CountDownLatch(1);
        // Shard 1 doesn't exist yet so we have to do this based on nodes.
        var targetIndexShardNode = clusterService().state()
            .nodes()
            .getAllNodes()
            .stream()
            .filter(node -> node.getName().equals(sourceIndexShardNode) == false && node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
            .map(DiscoveryNode::getName)
            .findFirst()
            .get();
        var targetIndexShardNodeTransportService = MockTransportService.getInstance(targetIndexShardNode);
        targetIndexShardNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    try {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                            splitAttempted.countDown();
                            splitBlocked.await();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        // Wait for HANDOFF to complete normally and then start delaying cluster state updates on one node.
        splitAttempted.await();

        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(6);

        var clusterStateApplicationBlock = new CountDownLatch(1);
        var sourceSearchShardNodeTransportService = MockTransportService.getInstance(sourceSearchShardNode);
        sourceSearchShardNodeTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, req, channel, task) -> {
                clusterStateApplicationBlock.await();
                handler.messageReceived(req, channel, task);
            }
        );

        try {
            // Unblock application of SPLIT state - it will not succeed because we are waiting for an ack from all nodes.
            // But it will be applied on the coordinator.
            splitBlocked.countDown();

            awaitClusterState(
                coordinator,
                state -> state.metadata()
                    .lookupProject(index)
                    .map(p -> p.index(index))
                    .get()
                    .getReshardingMetadata()
                    .getSplit()
                    .getTargetShardState(1)
                    .equals(IndexReshardingState.Split.TargetShardState.SPLIT)
            );

            // At this point coordinator is aware of SPLIT target state and will produce current shard count summary.
            // However, the source search shard is not aware of SPLIT target state.
            // We should still apply correct search filters here.
            assertResponse(prepareSearchAll(client(coordinator), indexName), r -> {
                var ids = Arrays.stream(r.getHits().getHits()).map(h -> h.getId()).collect(Collectors.toSet());
                assertEquals(r.getHits().getHits().length, ids.size());
            });
        } finally {
            // Unblock stuck node to receive the update.
            clusterStateApplicationBlock.countDown();
        }

        waitForReshardCompletion(indexName);
        ensureGreen(indexName);
    }

    public void testStaleSearchRequestsAreRejected() throws InterruptedException {
        String masterNode = startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(4);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        checkNumberOfShardsSetting(masterNode, indexName, 1);

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);
        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), numDocs);

        // We'll issue a search now but block the shard-level request on the coordinator until the split progresses far enough.
        var searchInitiated = new CountDownLatch(1);
        var searchBlock = new CountDownLatch(1);
        var coordinatorTransportService = MockTransportService.getInstance(coordinator);
        coordinatorTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (SearchTransportService.QUERY_ACTION_NAME.equals(action)) {
                try {
                    searchInitiated.countDown();
                    searchBlock.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try (var searchExecutor = Executors.newSingleThreadExecutor()) {
            var searchFuture = searchExecutor.submit(
                () -> prepareSearchAll(coordinator, indexName).setSearchType(SearchType.QUERY_THEN_FETCH).get()
            );
            searchInitiated.await();

            var sourceIndexShardNode = clusterService().state()
                .nodes()
                .getNodes()
                .get(findIndexShard(index, 0).routingEntry().currentNodeId())
                .getName();

            var sourceShardMoveToDoneAttempted = new CountDownLatch(1);
            var sourceShardMoveToDoneBlocked = new CountDownLatch(1);
            var sourceIndexShardNodeTransportService = MockTransportService.getInstance(sourceIndexShardNode);
            sourceIndexShardNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (TransportUpdateSplitSourceShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    if (actualRequest instanceof TransportUpdateSplitSourceShardStateAction.Request sourceStateRequest) {
                        try {
                            if (sourceStateRequest.getState() == IndexReshardingState.Split.SourceShardState.DONE) {
                                sourceShardMoveToDoneAttempted.countDown();
                                sourceShardMoveToDoneBlocked.await();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });

            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

            // Wait for the source index shard to attempt to move to DONE state.
            // That means that it already applied READY_TO_CLEANUP state and ensured that the search shard
            // is aware of it.
            sourceShardMoveToDoneAttempted.await();

            try {
                searchBlock.countDown();
                // This search should be rejected as stale.
                // Note that in production this will only happen after a grace period
                // but we disable it in tests.
                try {
                    searchFuture.get(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
                    fail("Search should throw stale request exception");
                } catch (ExecutionException e) {
                    var searchPhaseException = (SearchPhaseExecutionException) e.getCause();
                    assertEquals(1, searchPhaseException.shardFailures().length);
                    assertTrue(searchPhaseException.shardFailures()[0].getCause() instanceof StaleRequestException);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                sourceShardMoveToDoneBlocked.countDown();
            }
        }

        waitForReshardCompletion(indexName);
    }

    public void testExplainApi() throws InterruptedException {
        String masterNode = startMasterOnlyNode();
        startIndexNode();
        startSearchNodes(2);
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(5);

        var indexName = randomIndexName();
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        var afterSplitMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 2, 1)).build();
        var afterSplitRouting = IndexRouting.fromIndexMetadata(afterSplitMetadata);

        var shard0RoutingValue = makeRoutingValueForShard(afterSplitRouting, 0);
        var shard1RoutingValue = makeRoutingValueForShard(afterSplitRouting, 1);

        var id1 = client(coordinator).prepareIndex(indexName)
            .setRouting(shard0RoutingValue)
            .setSource("field", "source_value")
            .get()
            .getId();
        var id2 = client(coordinator).prepareIndex(indexName)
            .setRouting(shard1RoutingValue)
            .setSource("field", "target_value")
            .get()
            .getId();
        refresh(indexName);

        var sourceShardReferenceResponse = client(coordinator).prepareExplain(indexName, id1)
            .setRouting(shard0RoutingValue)
            .setQuery(QueryBuilders.matchQuery("field", "source_value"))
            .setFetchSource(true)
            .get();
        assertTrue(sourceShardReferenceResponse.isExists());
        assertTrue(sourceShardReferenceResponse.isMatch());
        var targetShardReferenceResponse = client(coordinator).prepareExplain(indexName, id2)
            .setRouting(shard1RoutingValue)
            .setQuery(QueryBuilders.matchQuery("field", "target_value"))
            .setFetchSource(true)
            .get();
        assertTrue(targetShardReferenceResponse.isExists());
        assertTrue(targetShardReferenceResponse.isMatch());

        var targetShardNode = startIndexNode();
        ensureStableCluster(6);

        var attemptingSplit = new CountDownLatch(1);
        var splitBlocked = new CountDownLatch(1);

        var attemptingDone = new CountDownLatch(1);
        var doneBlocked = new CountDownLatch(1);

        var indexNodeMockTransportService = MockTransportService.getInstance(targetShardNode);
        indexNodeMockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                        attemptingSplit.countDown();
                        try {
                            splitBlocked.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE) {
                        attemptingDone.countDown();
                        try {
                            doneBlocked.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        attemptingSplit.await();

        var sourceShardHandoffResponse = client(coordinator).prepareExplain(indexName, id1)
            .setRouting(shard0RoutingValue)
            .setQuery(QueryBuilders.matchQuery("field", "source_value"))
            .setFetchSource(true)
            .get();
        assertEquals(sourceShardReferenceResponse, sourceShardHandoffResponse);
        var targetShardHandoffResponse = client(coordinator).prepareExplain(indexName, id2)
            .setRouting(shard1RoutingValue)
            .setQuery(QueryBuilders.matchQuery("field", "target_value"))
            .setFetchSource(true)
            .get();
        assertEquals(targetShardReferenceResponse, targetShardHandoffResponse);

        splitBlocked.countDown();
        attemptingDone.await();

        try {
            var sourceShardSplitResponse = client(coordinator).prepareExplain(indexName, id1)
                .setRouting(shard0RoutingValue)
                .setQuery(QueryBuilders.matchQuery("field", "source_value"))
                .setFetchSource(true)
                .get();
            assertEquals(sourceShardReferenceResponse, sourceShardSplitResponse);
            var targetShardSplitResponse = client(coordinator).prepareExplain(indexName, id2)
                .setRouting(shard1RoutingValue)
                .setQuery(QueryBuilders.matchQuery("field", "target_value"))
                .setFetchSource(true)
                .get();
            assertEquals(targetShardReferenceResponse, targetShardSplitResponse);
        } finally {
            doneBlocked.countDown();
        }

        waitForReshardCompletion(indexName);
    }

    public void testExplainApiStaleRequest() throws InterruptedException {
        String masterNode = startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(4);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(masterNode, indexName, 1);

        final Index index = resolveIndex(indexName);
        final var indexRoutingPostSplit = postSplitRouting(clusterService().state(), index, 2);

        // Use the document that will be routed to the new shard after the split
        // to test the retry logic for stale requests.
        var documentId = makeIdThatRoutesToShard(indexRoutingPostSplit, 1);
        var document = Map.of("field", randomAlphaOfLength(10));
        prepareIndex(indexName).setId(documentId).setSource(document).get().getId();

        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), 1);

        // We'll issue an explain now but block the shard-level request on the coordinator until the split progresses far enough.
        var explainInitiated = new CountDownLatch(1);
        var explainBlock = new CountDownLatch(1);
        var coordinatorTransportService = MockTransportService.getInstance(coordinator);
        coordinatorTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportExplainAction.TYPE.name() + "[s]")) {
                try {
                    explainInitiated.countDown();
                    explainBlock.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try (var executor = Executors.newSingleThreadExecutor()) {
            var future = executor.submit(
                () -> client(coordinator).prepareExplain(indexName, documentId)
                    .setFetchSource(true)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .get()
            );
            explainInitiated.await();

            var sourceIndexShardNode = clusterService().state()
                .nodes()
                .getNodes()
                .get(findIndexShard(index, 0).routingEntry().currentNodeId())
                .getName();

            var sourceShardMoveToDoneAttempted = new CountDownLatch(1);
            var sourceShardMoveToDoneBlocked = new CountDownLatch(1);
            var sourceIndexShardNodeTransportService = MockTransportService.getInstance(sourceIndexShardNode);
            sourceIndexShardNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (TransportUpdateSplitSourceShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    if (actualRequest instanceof TransportUpdateSplitSourceShardStateAction.Request sourceStateRequest) {
                        try {
                            if (sourceStateRequest.getState() == IndexReshardingState.Split.SourceShardState.DONE) {
                                sourceShardMoveToDoneAttempted.countDown();
                                sourceShardMoveToDoneBlocked.await();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });

            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

            // Wait for the source index shard to attempt to move to DONE state.
            // That means that it already applied READY_TO_CLEANUP state and ensured that the search shard
            // is aware of it.
            sourceShardMoveToDoneAttempted.await();

            try {
                explainBlock.countDown();
                // This request will be rejected as stale but `TransportSingleShardAction` retries it and it succeeds.
                try {
                    var response = future.get(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
                    assertTrue(response.isExists());
                    assertEquals(document, response.getGetResult().sourceAsMap());
                } catch (TimeoutException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                sourceShardMoveToDoneBlocked.countDown();
            }
        }

        waitForReshardCompletion(indexName);
    }

    public void testTermVectorsApi() throws IOException {
        String masterNode = startMasterOnlyNode();
        startIndexNode();
        startSearchNodes(2);
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(5);

        var indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
            ).setMapping("""
                {"properties":{"field":{"type": "text","term_vector":"yes"}}}
                """)
        );
        ensureGreen(indexName);

        var afterSplitMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 2, 1)).build();
        var afterSplitRouting = IndexRouting.fromIndexMetadata(afterSplitMetadata);

        var id1 = makeIdThatRoutesToShard(afterSplitRouting, 0);
        var id2 = makeIdThatRoutesToShard(afterSplitRouting, 1);

        client(coordinator).prepareIndex(indexName).setId(id1).setSource("field", "source_value").get();
        client(coordinator).prepareIndex(indexName).setId(id2).setSource("field", "target_value").get();
        refresh(indexName);

        TermVectorsResponse sourceShardReferenceResponse = client(coordinator).prepareTermVectors(indexName, id1).setRealtime(false).get();
        assertTrue(sourceShardReferenceResponse.isExists());
        assertEquals(1, sourceShardReferenceResponse.getFields().size());
        assertEquals("field", sourceShardReferenceResponse.getFields().iterator().next());

        TermVectorsResponse targetShardReferenceResponse = client(coordinator).prepareTermVectors(indexName, id2).setRealtime(false).get();
        assertTrue(targetShardReferenceResponse.isExists());
        assertEquals(1, targetShardReferenceResponse.getFields().size());
        assertEquals("field", targetShardReferenceResponse.getFields().iterator().next());

        TermVectorsResponse artificialDocumentReferenceResponse = client(coordinator).prepareTermVectors()
            .setIndex(indexName)
            .setDoc(XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field("field", "artificial_value").endObject())
            .setRealtime(false)
            .get();
        assertTrue(artificialDocumentReferenceResponse.isExists());
        assertTrue(artificialDocumentReferenceResponse.isArtificial());
        assertEquals(1, artificialDocumentReferenceResponse.getFields().size());
        assertEquals("field", artificialDocumentReferenceResponse.getFields().iterator().next());

        var targetShardNode = startIndexNode();
        ensureStableCluster(6);

        var attemptingSplit = new CountDownLatch(1);
        var splitBlocked = new CountDownLatch(1);

        var attemptingDone = new CountDownLatch(1);
        var doneBlocked = new CountDownLatch(1);

        var indexNodeMockTransportService = MockTransportService.getInstance(targetShardNode);
        indexNodeMockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                    if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                        attemptingSplit.countDown();
                        try {
                            splitBlocked.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE) {
                        attemptingDone.countDown();
                        try {
                            doneBlocked.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        safeAwait(attemptingSplit);

        var sourceShardHandoffResponse = client(coordinator).prepareTermVectors(indexName, id1).setRealtime(false).get();
        assertTrue(sourceShardHandoffResponse.isExists());
        assertEquals(1, sourceShardHandoffResponse.getFields().size());
        assertEquals("field", sourceShardHandoffResponse.getFields().iterator().next());

        var targetShardHandoffResponse = client(coordinator).prepareTermVectors(indexName, id2).setRealtime(false).get();
        assertTrue(targetShardHandoffResponse.isExists());
        assertEquals(1, targetShardHandoffResponse.getFields().size());
        assertEquals("field", targetShardHandoffResponse.getFields().iterator().next());

        var artificialDocumentHandoffResponse = client(coordinator).prepareTermVectors()
            .setIndex(indexName)
            .setDoc(XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field("field", "artificial_value").endObject())
            .setRealtime(false)
            .get();
        assertTrue(artificialDocumentHandoffResponse.isExists());
        assertTrue(artificialDocumentHandoffResponse.isArtificial());
        assertEquals(1, artificialDocumentHandoffResponse.getFields().size());
        assertEquals("field", artificialDocumentHandoffResponse.getFields().iterator().next());

        splitBlocked.countDown();
        safeAwait(attemptingDone);

        try {
            var sourceShardSplitResponse = client(coordinator).prepareTermVectors(indexName, id1).setRealtime(false).get();
            assertTrue(sourceShardSplitResponse.isExists());
            assertEquals(1, sourceShardSplitResponse.getFields().size());
            assertEquals("field", sourceShardSplitResponse.getFields().iterator().next());

            var targetShardSplitResponse = client(coordinator).prepareTermVectors(indexName, id2).setRealtime(false).get();
            assertTrue(targetShardSplitResponse.isExists());
            assertEquals(1, targetShardSplitResponse.getFields().size());
            assertEquals("field", targetShardSplitResponse.getFields().iterator().next());

            var artificialDocumentSplitResponse = client(coordinator).prepareTermVectors()
                .setIndex(indexName)
                .setDoc(XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field("field", "artificial_value").endObject())
                .setRealtime(false)
                .get();
            assertTrue(artificialDocumentSplitResponse.isExists());
            assertTrue(artificialDocumentSplitResponse.isArtificial());
            assertEquals(1, artificialDocumentSplitResponse.getFields().size());
            assertEquals("field", artificialDocumentSplitResponse.getFields().iterator().next());
        } finally {
            doneBlocked.countDown();
        }

        waitForReshardCompletion(indexName);
    }

    public void testTermVectorsApiStaleRequest() throws InterruptedException, IOException {
        String masterNode = startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(4);

        final String indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1).build()).setMapping("""
            {"properties":{"field":{"type": "text","term_vector":"yes"}}}
            """));
        ensureGreen(indexName);

        checkNumberOfShardsSetting(masterNode, indexName, 1);

        final Index index = resolveIndex(indexName);
        final IndexMetadata indexMetadata = indexMetadata(clusterService().state(), index);
        final var indexMetadataPostSplit = IndexMetadata.builder(indexMetadata).reshardAddShards(2).build();
        final var indexRoutingPostSplit = IndexRouting.fromIndexMetadata(indexMetadataPostSplit);

        // Use the document that will be routed to the new shard after the split
        // to test the retry logic for stale requests.
        var documentId = makeIdThatRoutesToShard(indexRoutingPostSplit, 1);
        var document = Map.of("field", randomAlphaOfLength(10));
        prepareIndex(indexName).setId(documentId).setSource(document).get().getId();

        refresh(indexName);
        assertHitCount(prepareSearchAll(indexName), 1);

        // We'll issue the request now but block the shard-level request on the coordinator until the split progresses far enough.
        var shardOperationInitiated = new CountDownLatch(1);
        var shardOperationBlocked = new CountDownLatch(1);
        var coordinatorTransportService = MockTransportService.getInstance(coordinator);
        coordinatorTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TermVectorsAction.NAME + "[s]")) {
                try {
                    shardOperationInitiated.countDown();
                    shardOperationBlocked.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try (var executor = Executors.newSingleThreadExecutor()) {
            var future = executor.submit(() -> client(coordinator).prepareTermVectors(indexName, documentId).setRealtime(false).get());
            shardOperationInitiated.await();

            var sourceIndexShardNode = clusterService().state()
                .nodes()
                .getNodes()
                .get(findIndexShard(index, 0).routingEntry().currentNodeId())
                .getName();

            var sourceShardMoveToDoneAttempted = new CountDownLatch(1);
            var sourceShardMoveToDoneBlocked = new CountDownLatch(1);
            var sourceIndexShardNodeTransportService = MockTransportService.getInstance(sourceIndexShardNode);
            sourceIndexShardNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (TransportUpdateSplitSourceShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    if (actualRequest instanceof TransportUpdateSplitSourceShardStateAction.Request sourceStateRequest) {
                        try {
                            if (sourceStateRequest.getState() == IndexReshardingState.Split.SourceShardState.DONE) {
                                sourceShardMoveToDoneAttempted.countDown();
                                sourceShardMoveToDoneBlocked.await();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });

            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

            // Wait for the source index shard to attempt to move to DONE state.
            // That means that it already applied READY_TO_CLEANUP state and ensured that the search shard
            // is aware of it.
            sourceShardMoveToDoneAttempted.await();

            try {
                shardOperationBlocked.countDown();
                // This request will be rejected as stale but `TransportSingleShardAction` retries it and it succeeds.
                try {
                    TermVectorsResponse response = future.get(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
                    assertTrue(response.isExists());
                    assertEquals(1, response.getFields().size());
                    assertEquals("field", response.getFields().iterator().next());
                } catch (TimeoutException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                sourceShardMoveToDoneBlocked.countDown();
            }
        }

        waitForReshardCompletion(indexName);
    }

    @TestIssueLogging(
        value = "org.elasticsearch.xpack.stateless.action.TransportEnsureDocsSearchableAction:TRACE,"
            + "org.elasticsearch.action.termvectors.TransportTermVectorsAction:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch-serverless/issues/6372"
    )
    public void testTermVectorsApiRealtimeGet() throws Exception {
        String masterNode = startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(4);

        var indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1).build()).setMapping("""
            {"properties":{"field":{"type": "text","term_vector":"yes"}}}
            """));
        ensureGreen(indexName);

        var afterSplitMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 2, 1)).build();
        var afterSplitRouting = IndexRouting.fromIndexMetadata(afterSplitMetadata);

        // A document that routes to the target shard post split.
        String document1Id = makeIdThatRoutesToShard(afterSplitRouting, 1, "1");

        client(coordinator).prepareIndex(indexName).setId(document1Id).setSource("field", "value").get();

        // Note that we don't refresh since this is realtime.

        var preSplitResponse = client(coordinator).prepareTermVectors(indexName, document1Id).setRealtime(true).get();
        assertTrue(preSplitResponse.isExists());
        assertEquals(1, preSplitResponse.getFields().size());
        assertEquals("field", preSplitResponse.getFields().iterator().next());

        var handoffStarted = new CountDownLatch(1);
        var handoffBlocked = new CountDownLatch(1);
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                    if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.HANDOFF) {
                            handoffStarted.countDown();
                            handoffBlocked.await();
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        String document2Id = makeIdThatRoutesToShard(afterSplitRouting, 1, "2");

        // Simulate a request arriving at a stale coordinator by blocking shard level requests with current old summary.
        var readInitiated = new CountDownLatch(1);
        var readBlocked = new CountDownLatch(1);
        var termVectorShardRequests = new AtomicInteger(0);
        var coordinatorTransportService = MockTransportService.getInstance(coordinator);
        coordinatorTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TermVectorsAction.NAME + "[s]")) {
                int count = termVectorShardRequests.incrementAndGet();
                logger.info("TermVectors [s] request #{} to [{}]", count, connection.getNode().getName());
                if (count == 1) {
                    try {
                        readInitiated.countDown();
                        readBlocked.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try (var executor = Executors.newFixedThreadPool(2)) {
            var readFuture = executor.submit(() -> client(coordinator).prepareTermVectors(indexName, document2Id).setRealtime(true).get());
            readInitiated.await();

            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

            handoffStarted.await();

            // Now we will index the document and it should be resplit to the target shard.
            var indexFuture = executor.submit(
                () -> client(coordinator).prepareIndex(indexName).setId(document2Id).setSource("field", "value2").get()
            );

            // Once we unblock handoff the write should complete.
            handoffBlocked.countDown();
            indexFuture.get();

            // And now we perform the "stale" read.
            readBlocked.countDown();

            // It should still be successful.
            var response = readFuture.get();
            if (response.isExists() == false) {
                var index = resolveIndex(indexName);
                var reshardMeta = indexMetadata(clusterService().state(), index).getReshardingMetadata();
                fail(
                    "term vectors returned exists=false after "
                        + termVectorShardRequests.get()
                        + " [s] requests. Reshard metadata: "
                        + reshardMeta
                );
            }
            assertEquals(1, response.getFields().size());
            assertEquals("field", response.getFields().iterator().next());

            waitForReshardCompletion(indexName);
        }
    }

    public void testMultiTermVectorsApiRealtimeGet() throws IOException, InterruptedException, ExecutionException {
        String masterNode = startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        var coordinator = startSearchNode();
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", coordinator));
        ensureStableCluster(4);

        var indexName = randomIndexName();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1).build()).setMapping("""
            {"properties":{"field":{"type": "text","term_vector":"yes"}}}
            """));
        ensureGreen(indexName);

        var afterSplitMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 2, 1)).build();
        var afterSplitRouting = IndexRouting.fromIndexMetadata(afterSplitMetadata);

        String document1Id = makeIdThatRoutesToShard(afterSplitRouting, 0, "1");
        String document2Id = makeIdThatRoutesToShard(afterSplitRouting, 1, "2");

        client(coordinator).prepareIndex(indexName).setId(document1Id).setSource("field", "value").get();
        client(coordinator).prepareIndex(indexName).setId(document2Id).setSource("field", "value2").get();

        // Note that we don't refresh since this is realtime.

        var preSplitResponse = client(coordinator).prepareMultiTermVectors()
            .add(new TermVectorsRequest(indexName, document1Id).realtime(true))
            .add(new TermVectorsRequest(indexName, document2Id).realtime(true))
            .get();
        var document1PreSplitResponse = preSplitResponse.getResponses()[0].getResponse();
        assertTrue(document1PreSplitResponse.isExists());
        assertEquals(1, document1PreSplitResponse.getFields().size());
        assertEquals("field", document1PreSplitResponse.getFields().iterator().next());
        var document2PreSplitResponse = preSplitResponse.getResponses()[0].getResponse();
        assertTrue(document2PreSplitResponse.isExists());
        assertEquals(1, document2PreSplitResponse.getFields().size());
        assertEquals("field", document2PreSplitResponse.getFields().iterator().next());

        var handoffStarted = new CountDownLatch(1);
        var handoffBlocked = new CountDownLatch(1);
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);
                    if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.HANDOFF) {
                            handoffStarted.countDown();
                            handoffBlocked.await();
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // Document queued to be indexed on the target shard.
        String document3Id = makeIdThatRoutesToShard(afterSplitRouting, 1, "3");

        // Simulate a request arriving at a stale coordinator by blocking shard level requests with current old summary.
        var readInitiated = new CountDownLatch(1);
        var readBlocked = new CountDownLatch(1);
        var coordinatorTransportService = MockTransportService.getInstance(coordinator);
        coordinatorTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportShardMultiTermsVectorAction.TYPE.name() + "[s]")) {
                try {
                    readInitiated.countDown();
                    readBlocked.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        try (var executor = Executors.newFixedThreadPool(2)) {
            var readFuture = executor.submit(
                () -> client(coordinator).prepareMultiTermVectors()
                    .add(new TermVectorsRequest(indexName, document1Id).realtime(true))
                    .add(new TermVectorsRequest(indexName, document2Id).realtime(true))
                    .add(new TermVectorsRequest(indexName, document3Id).realtime(true))
                    .get()
            );
            readInitiated.await();

            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

            handoffStarted.await();

            // Now we will index the document and it should be resplit to the target shard.
            var indexFuture = executor.submit(
                () -> client(coordinator).prepareIndex(indexName).setId(document3Id).setSource("field", "value3").get()
            );

            // Once we unblock handoff the write should complete.
            handoffBlocked.countDown();
            indexFuture.get();

            // And now we perform the "stale" read.
            readBlocked.countDown();

            var response = readFuture.get();
            // We retry the operation because it is stale and get a legit response.
            var document1Response = response.getResponses()[0].getResponse();
            assertTrue(document1Response.isExists());
            assertEquals(1, document1Response.getFields().size());
            assertEquals("field", document1Response.getFields().iterator().next());
            var document2Response = response.getResponses()[1].getResponse();
            assertTrue(document2Response.isExists());
            assertEquals(1, document2Response.getFields().size());
            assertEquals("field", document2Response.getFields().iterator().next());
            var document3Response = response.getResponses()[2].getResponse();
            assertTrue(document3Response.isExists());
            assertEquals(1, document3Response.getFields().size());
            assertEquals("field", document3Response.getFields().iterator().next());

            waitForReshardCompletion(indexName);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(EsqlPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Test framework randomly sets this to 0, but we rely on retries to handle target shards still being in recovery
            // when we start re-splitting bulk requests.
            .put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s")
            // These tests are carefully set up and do not hit the situations that the delete unowned grace period prevents.
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO);
    }

    @Override
    protected boolean addMockFsRepository() {
        // Use FS repository because it supports blob copy
        return false;
    }

    private static long getCurrentPrimaryTerm(Index index, int shardId) {
        return client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .setMetadata(true)
            .get()
            .getState()
            .getMetadata()
            .findIndex(index)
            .get()
            .primaryTerm(shardId);
    }

    private static long getIndexCount(IndicesStatsResponse statsResponse, int shardId) {
        ShardStats primaryStats = Arrays.stream(statsResponse.getShards())
            .filter(shardStat -> shardStat.getShardRouting().primary() && shardStat.getShardRouting().id() == shardId)
            .findAny()
            .get();
        return primaryStats.getStats().indexing.getTotal().getIndexCount();
    }

    /**
     * Generate a string that if used as a routing value will route to the given shardId of indexRouting.
     */
    private String makeRoutingValueForShard(IndexRouting indexRouting, int shardId) {
        while (true) {
            String routingValue = randomAlphaOfLength(5);
            int routedShard = indexRouting.indexShard(new IndexRequest().id("dummy").routing(routingValue));
            if (routedShard == shardId) {
                return routingValue;
            }
        }
    }

    private static void assertShardDocCountEquals(ShardId shard, long expectedCount) {
        assertShardDocCountEquals(
            client().admin().indices().prepareStats(shard.getIndexName()).execute().actionGet(),
            shard,
            expectedCount
        );
    }

    private static void assertShardDocCountEquals(IndicesStatsResponse statsResponse, ShardId shard, long expectedCount) {
        assertThat(shard + " has unexpected number of documents", getIndexCount(statsResponse, shard.id()), equalTo(expectedCount));
    }

    private static IndexReshardingState.Split getSplit(String nodeName, Index index) {
        final var reshardingMetadata = client(nodeName).admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .getMetadata()
            .indexMetadata(index)
            .getReshardingMetadata();

        return reshardingMetadata != null ? reshardingMetadata.getSplit() : null;
    }

    private static SearchRequestBuilder prepareSearchAll(String indexName) {
        return prepareSearchAll(client(), indexName);
    }

    private static SearchRequestBuilder prepareSearchAll(String searchNode, String indexName) {
        return prepareSearchAll(client(searchNode), indexName);
    }

    private static SearchRequestBuilder prepareSearchAll(Client client, String indexName) {
        return client.prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(10000)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false);
    }

    private static void closeIndices(final String... indices) {
        closeIndices(indicesAdmin().prepareClose(indices));
    }

    private static void closeIndices(final CloseIndexRequestBuilder requestBuilder) {
        final CloseIndexResponse response = requestBuilder.get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isShardsAcknowledged(), is(true));

        final String[] indices = requestBuilder.request().indices();
        if (indices != null) {
            assertThat(response.getIndices().size(), equalTo(indices.length));
            for (String index : indices) {
                CloseIndexResponse.IndexResult indexResult = response.getIndices()
                    .stream()
                    .filter(result -> index.equals(result.getIndex().getName()))
                    .findFirst()
                    .get();
                assertThat(indexResult, notNullValue());
                assertThat(indexResult.hasFailures(), is(false));
                assertThat(indexResult.getException(), nullValue());
                assertThat(indexResult.getShards(), notNullValue());
                Arrays.stream(indexResult.getShards()).forEach(shardResult -> {
                    assertThat(shardResult.hasFailures(), is(false));
                    assertThat(shardResult.getFailures(), notNullValue());
                    assertThat(shardResult.getFailures().length, equalTo(0));
                });
            }
        } else {
            assertThat(response.getIndices().size(), equalTo(0));
        }
    }

    private static void checkNumberOfShardsSetting(String indexNode, String indexName, int expected_shards) {
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client(indexNode).admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(expected_shards)
        );
    }

    public PlainActionFuture<ClusterState> waitForClusterState(Predicate<ClusterState> predicate) {
        return waitForClusterState(logger, clusterService(), predicate);
    }

    /**
     * A future that waits if necessary for cluster state to match a given predicate, and returns that state
     * @param predicate continue waiting for state updates until true
     * @return A future whose get() will resolve to the cluster state that matches the supplied predicate
     */
    public static PlainActionFuture<ClusterState> waitForClusterState(
        Logger logger,
        ClusterService clusterService,
        Predicate<ClusterState> predicate
    ) {
        var future = new PlainActionFuture<ClusterState>();
        var listener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.info("cluster state updated: version {}", state.version());
                future.onResponse(state);
            }

            @Override
            public void onClusterServiceClose() {
                future.onFailure(null);
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                future.onFailure(new TimeoutException(timeout.toString()));
            }
        };

        ClusterStateObserver.waitForState(clusterService, new ThreadContext(Settings.EMPTY), listener, predicate, null, logger);

        return future;
    }

    /**
     * Extract a shardId from a "simple" {@link IndexRouting} using a randomly
     * chosen method. All of the random methods <strong>should</strong> return the
     * same results.
     */
    private int shardIdFromSimple(IndexRouting indexRouting, String id, @Nullable String routing) {
        return switch (between(0, 2)) {
            case 0 -> indexRouting.indexShard(new IndexRequest().id(id).routing(routing));
            case 1 -> indexRouting.updateShard(id, routing);
            case 2 -> indexRouting.deleteShard(id, routing);
            default -> throw new AssertionError("invalid option");
        };
    }

    private void waitForReshardCompletion(String indexName) {
        awaitClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() == null);
    }
}

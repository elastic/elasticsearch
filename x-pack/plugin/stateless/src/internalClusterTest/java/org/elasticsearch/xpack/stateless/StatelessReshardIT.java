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

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.reshard.ReshardIndexRequest;
import co.elastic.elasticsearch.stateless.reshard.ReshardIndexResponse;
import co.elastic.elasticsearch.stateless.reshard.ReshardIndexService;
import co.elastic.elasticsearch.stateless.reshard.SplitSourceService;
import co.elastic.elasticsearch.stateless.reshard.SplitTargetService;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardAction;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardSplitAction;
import co.elastic.elasticsearch.stateless.reshard.TransportUpdateSplitStateAction;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.disruption.BlockMasterServiceOnMaster;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessReshardIT extends AbstractStatelessIntegTestCase {

    // Given an index with a certain number of shards, it can be resharded only to valid
    // number of target shards. Here we test some valid and invalid combinations.
    public void testReshardTargetNumShardsIsValid() {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        final int multiple2 = 2;
        final int multiple3 = 3;
        // Note that we can go from 1 shard to any number of shards (< 1024)
        int startingNumShards = 1;
        int targetNumShards = multiple2 * startingNumShards;
        logger.info("Starting reshard to go from {} to {} shards", startingNumShards, targetNumShards);
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple2)).actionGet();
        waitForReshardCompletion(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, targetNumShards);

        // Now lets try to go from 2 shards to 6 shards (this is not allowed)
        startingNumShards = 2;
        checkNumberOfShardsSetting(indexNode, indexName, startingNumShards);
        targetNumShards = multiple3 * startingNumShards;
        logger.info("Starting reshard to go from {} to {} shards", startingNumShards, targetNumShards);
        assertThrows(
            IllegalStateException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple3))
                .actionGet(SAFE_AWAIT_TIMEOUT)
        );
        checkNumberOfShardsSetting(indexNode, indexName, startingNumShards);

        // Now lets try to go from 2 shards to 4 shards (this is allowed)
        startingNumShards = 2;
        checkNumberOfShardsSetting(indexNode, indexName, startingNumShards);
        targetNumShards = multiple2 * startingNumShards;
        logger.info("Starting reshard to go from {} to {} shards", startingNumShards, targetNumShards);
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple2)).actionGet();
        waitForReshardCompletion(indexName);
        checkNumberOfShardsSetting(indexNode, indexName, targetNumShards);
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService:DEBUG", reason = "debugging")
    public void testReshardWillCopyDataAndRouteDocumentsToNewShard() {
        String indexNode = startMasterAndIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );
        startSearchNode();

        ensureStableCluster(2);

        // Note that we can go from 1 shard to any number of shards (< 1024)
        final int multiple = randomIntBetween(2, 10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

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

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo((long) numDocs));

        // flushing here ensures that the initial copy phase does some work, rather than relying entirely on catching new commits after
        // resharding has begun.
        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        var initialIndexMetadata = clusterService().state().projectState().metadata().index(indexName);
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
        var splitState = waitForClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() != null);

        logger.info("starting reshard");
        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple)).actionGet());

        logger.info("getting reshard metadata");
        var reshardingMetadata = splitState.actionGet(SAFE_AWAIT_TIMEOUT)
            .projectState()
            .metadata()
            .index(indexName)
            .getReshardingMetadata();
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
        var indexMetadata = waitForClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() == null)
            .actionGet(SAFE_AWAIT_TIMEOUT)
            .projectState()
            .metadata()
            .index(indexName);

        // index documents until all the new shards have received at least one document
        int docsPerRequest = randomIntBetween(10, 100);
        do {
            indexShardDocs.accept(docsPerRequest);
            totalNumberOfDocumentsInIndex += docsPerRequest;
        } while (Arrays.stream(shardDocs).filter(shard -> shard > 0).count() < multiple);

        // Verify that each shard id contains the expected number of documents indexed into it.
        // Note that stats won't include data copied from the source shard since they didn't go through the "normal" indexing logic.
        IndicesStatsResponse postReshardStatsResponse = client().admin().indices().prepareStats(indexName).execute().actionGet();

        IntStream.range(0, multiple).forEach(shardId -> {
            assertThat(
                "Shard " + shardId + " has unexpected number of documents",
                getIndexCount(postReshardStatsResponse, shardId),
                equalTo((long) shardDocs[shardId])
            );
        });

        // index more documents to verify that a search query returns all indexed documents thus far
        final int numDocsRound3 = randomIntBetween(10, 100);
        indexDocs(indexName, numDocsRound3);
        totalNumberOfDocumentsInIndex += numDocsRound3;

        refresh(indexName);

        // verify that the index metadata returned matches the expected multiple of shards
        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(multiple)
        );

        var search = prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())
            .setSize(totalNumberOfDocumentsInIndex)
            .setTrackTotalHits(true)
            .get();
        assertHitCount(search, totalNumberOfDocumentsInIndex);

        // all documents should be on their owning shards
        var indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        assertTrue(
            StreamSupport.stream(search.getHits().spliterator(), false)
                .allMatch(hit -> hit.getShard().getShardId().getId() == indexRouting.indexShard(hit.getId(), null, null, null))
        );

        search.decRef();
    }

    public void testSearchDuringReshard() throws Exception {
        runSearchTest(false);
    }

    public void testEsqlSearchDuringReshard() throws Exception {
        runSearchTest(true);
    }

    private void runSearchTest(boolean useEsql) throws Exception {
        var masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchCoordinator = startSearchNode();
        ensureStableCluster(4);

        final int multiple = randomIntBetween(2, 3);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        int initialIndexedDocuments = randomIntBetween(10, 100);
        indexDocs(indexName, initialIndexedDocuments);

        var flushResponse = indicesAdmin().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        assertNoFailures(flushResponse);

        long totalNumberOfDocumentsInIndex = initialIndexedDocuments;

        // works before resharding
        refresh(indexName);

        long initialSearchExpected = totalNumberOfDocumentsInIndex;
        var initialSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertEquals(initialSearchExpected, documentCount);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(1, response.getTotalShards());
                assertEquals(initialSearchExpected, response.getHits().getTotalHits().value());
            }
        };
        assertSearch(searchCoordinator, indexName, initialSearchAssertion, useEsql);

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName, multiple);
        assertAcked(client(masterNode).execute(TransportReshardAction.TYPE, reshardRequest).actionGet());

        CyclicBarrier startStateTransitionBlock = new CyclicBarrier(multiple); // (multiple - 1) target shards + the test itself
        CyclicBarrier stateTransitionBlock = new CyclicBarrier(multiple);

        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitStateAction.TYPE.name().equals(action)) {
                    // Line up all targets here to make sure that we can properly reset `stateTransitionBlock` below.
                    startStateTransitionBlock.await();
                    stateTransitionBlock.await();
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        });

        Index index = resolveIndex(indexName);

        awaitClusterState(
            searchCoordinator,
            clusterState -> clusterState.getMetadata().indexMetadata(index).getReshardingMetadata() != null
        );

        // wait for all target shards to arrive at handoff point
        startStateTransitionBlock.await();

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
        long cloneSearchExpected = totalNumberOfDocumentsInIndex;
        var cloneSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertEquals(cloneSearchExpected, documentCount);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(1, response.getTotalShards());
                assertEquals(cloneSearchExpected, response.getHits().getTotalHits().value());
            }
        };
        assertSearch(searchCoordinator, indexName, cloneSearchAssertion, useEsql);

        // TODO write more data here.
        // Writes to source shard at this point will not be copied to the target
        // because they happen after commit copy logic is already disabled.
        // Eventually we'll forward such writes to the target.
        // But for now we can't write here because eventually we remove some documents written here
        // as unowned but they are also not copied to the target.
        // That of course leads to incorrect search results.

        // unblock HANDOFF transition
        stateTransitionBlock.await();

        awaitClusterState(
            searchCoordinator,
            clusterState -> clusterState.getMetadata()
                .indexMetadata(index)
                .getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.HANDOFF)
        );

        stateTransitionBlock.reset();
        startStateTransitionBlock.await();

        // Transition of target shards to SPLIT state is blocked, all targets are in HANDOFF state.
        // Refresh includes target shards, search only uses the source shard.
        var handoffRefresh = client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        assertEquals(multiple, handoffRefresh.getTotalShards());
        assertEquals(multiple, handoffRefresh.getSuccessfulShards());

        long handoffSearchExpected = totalNumberOfDocumentsInIndex;
        var handoffSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertEquals(handoffSearchExpected, documentCount);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(1, response.getTotalShards());
                assertEquals(handoffSearchExpected, response.getHits().getTotalHits().value());
            }
        };
        assertSearch(searchCoordinator, indexName, handoffSearchAssertion, useEsql);

        // indexing new data and searching for it works
        final int handoffIndexedDocuments = randomIntBetween(10, 20);
        indexDocs(indexName, handoffIndexedDocuments);
        totalNumberOfDocumentsInIndex += handoffIndexedDocuments;

        client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();

        // TODO this is a gap - we can write to target shards and refresh them but we won't see our writes
        // because only the source shard is used for search.
        // Refresh block on target shard will solve this.
        long handoffNewDocsTotalDocuments = totalNumberOfDocumentsInIndex;
        var handoffNewDocsSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertTrue(
                    "Lost documents during reshard",
                    documentCount >= handoffNewDocsTotalDocuments - handoffIndexedDocuments && documentCount <= handoffNewDocsTotalDocuments
                );
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(1, response.getTotalShards());
                assertTrue(
                    "Lost documents during reshard",
                    response.getHits().getTotalHits().value() >= handoffNewDocsTotalDocuments - handoffIndexedDocuments
                        && response.getHits().getTotalHits().value() <= handoffNewDocsTotalDocuments
                );
            }
        };
        assertSearch(searchCoordinator, indexName, handoffNewDocsSearchAssertion, useEsql);

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

        stateTransitionBlock.reset();
        startStateTransitionBlock.await();

        // Transition of target shards to DONE state is blocked, all targets are in SPLIT state.
        // Refresh includes target shards, search includes target shards.
        var splitRefresh = client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        assertEquals(Arrays.toString(splitRefresh.getShardFailures()), multiple, splitRefresh.getTotalShards());
        assertEquals(Arrays.toString(splitRefresh.getShardFailures()), multiple, splitRefresh.getSuccessfulShards());

        // TODO this is a gap - search filters do not exist yet
        // and we'll see previously indexed documents that are moved to the target shard twice.
        long splitTotalDocuments = totalNumberOfDocumentsInIndex;

        var splitActualCount = new AtomicLong();
        var splitSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                splitActualCount.set(documentCount);
                assertTrue("Lost documents during reshard", splitActualCount.get() >= splitTotalDocuments);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                splitActualCount.set(response.getHits().getTotalHits().value());
                assertEquals(multiple, response.getTotalShards());
                assertTrue("Lost documents during reshard", splitActualCount.get() >= splitTotalDocuments);
            }
        };
        assertSearch(searchCoordinator, indexName, splitSearchAssertion, useEsql);

        final int splitIndexedDocuments = randomIntBetween(10, 20);
        indexDocs(indexName, splitIndexedDocuments);
        totalNumberOfDocumentsInIndex += splitIndexedDocuments;

        client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();
        // We still see old documents belonging to the target shard twice.
        // But newly indexed documents are indexed correctly and returned in search results only once.
        long splitNewDocsTotalDocuments = splitActualCount.get() + splitIndexedDocuments;
        var splitNewDocsSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertEquals(splitNewDocsTotalDocuments, documentCount);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(multiple, response.getTotalShards());
                assertEquals(splitNewDocsTotalDocuments, response.getHits().getTotalHits().value());
            }
        };
        assertSearch(searchCoordinator, indexName, splitNewDocsSearchAssertion, useEsql);

        // unblock DONE transition
        stateTransitionBlock.await();

        awaitClusterState(searchCoordinator, clusterState -> {
            var metadata = internalCluster().clusterService(searchCoordinator).state().getMetadata().indexMetadata(index);
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

        // all unowned documents should be deleted now so we should get the exact count
        long doneExpected = totalNumberOfDocumentsInIndex;
        var doneSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertEquals(doneExpected, documentCount);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(multiple, response.getTotalShards());
                assertEquals(doneExpected, response.getHits().getTotalHits().value());
            }
        };
        assertSearch(searchCoordinator, indexName, doneSearchAssertion, useEsql);

        // indexing new data and searching for it works
        final int doneIndexedDocuments = randomIntBetween(10, 20);
        indexDocs(indexName, doneIndexedDocuments);
        totalNumberOfDocumentsInIndex += doneIndexedDocuments;

        client(searchCoordinator).admin().indices().prepareRefresh(indexName).get();

        long doneNewDocsExpected = totalNumberOfDocumentsInIndex;
        var doneNewDocsSearchAssertion = new SearchAssertion() {
            @Override
            public void assertEsql(long documentCount) {
                assertEquals(doneNewDocsExpected, documentCount);
            }

            @Override
            public void assertSearch(SearchResponse response) {
                assertEquals(multiple, response.getTotalShards());
                assertEquals(doneNewDocsExpected, response.getHits().getTotalHits().value());
            }
        };
        assertSearch(searchCoordinator, indexName, doneNewDocsSearchAssertion, useEsql);

        waitForReshardCompletion(indexName);
    }

    private interface SearchAssertion {
        void assertEsql(long documentCount);

        void assertSearch(SearchResponse response);
    }

    private void assertSearch(String searchNode, String indexName, SearchAssertion assertion, boolean useEsql) {
        if (useEsql) {
            assertion.assertEsql(countDocsWithEsql(searchNode, indexName));
        }

        var search = client(searchNode).prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(10000)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false);

        assertResponse(search, assertion::assertSearch);
    }

    private long countDocsWithEsql(String searchNode, String indexName) {
        var query = "FROM $index | STATS COUNT(*)".replace("$index", indexName);
        var request = new EsqlQueryRequest().allowPartialResults(false).query(query);

        try (var response = client(searchNode).execute(EsqlQueryAction.INSTANCE, request).actionGet()) {
            return (long) response.column(0).next();
        }
    }

    public void testSearchWithCustomRoutingDuringReshard() throws Exception {
        String masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final int multiple = 3;
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        var index = resolveIndex(indexName);
        var indexMetadata = internalCluster().clusterService(masterNode).state().getMetadata().indexMetadata(index);

        // We re-create the metadata directly in test in order to have access to after-reshard routing.
        var wouldBeMetadata = IndexMetadata.builder(indexMetadata)
            .reshardAddShards(multiple)
            .reshardingMetadata(
                IndexReshardingMetadata.newSplitByMultiple(1, multiple)
                    .transitionSplitTargetToNewState(new ShardId(index, 1), IndexReshardingState.Split.TargetShardState.HANDOFF)
                    .transitionSplitTargetToNewState(new ShardId(index, 2), IndexReshardingState.Split.TargetShardState.HANDOFF)
            )
            .build();
        var wouldBeAfterSplitRouting = IndexRouting.fromIndexMetadata(wouldBeMetadata);

        int ingestedDocumentsPerShard = randomIntBetween(10, 20);

        Function<Integer, String> findRoutingValue = shard -> {
            while (true) {
                String routingValue = randomAlphaOfLength(5);
                int routedShard = wouldBeAfterSplitRouting.indexShard("dummy", routingValue, null, null);
                if (routedShard == shard) {
                    return routingValue;
                }
            }
        };

        var routingValuePerShard = new HashMap<Integer, String>() {
            {
                put(0, findRoutingValue.apply(0));
                put(1, findRoutingValue.apply(1));
                put(2, findRoutingValue.apply(2));
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

        // Search with routing specified.
        var preSplitSearch = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(ingestedDocumentsPerShard * multiple * 2)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setRouting(routingValuePerShard.get(1));

        // Everything routes to the only shard so no matter what the routing is, we'll get all documents
        assertResponse(preSplitSearch, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard * multiple, r.getHits().getTotalHits().value());
        });

        // Start split and block transition to HANDOFF.
        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple)).actionGet());

        CyclicBarrier stateTransitionBlock = new CyclicBarrier(multiple); // (multiple - 1) target shards + test itself
        CyclicBarrier resetStateTransitionBlock = new CyclicBarrier(multiple);

        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitStateAction.TYPE.name().equals(action)) {
                    // line up all targets and block them from state transition
                    stateTransitionBlock.await();
                }
                connection.sendRequest(requestId, action, request, options);

                if (TransportUpdateSplitStateAction.TYPE.name().equals(action)) {
                    // wait and exit only after `targetsAttemptToChangeState` was properly reset
                    resetStateTransitionBlock.await();
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        });

        assertBusy(() -> {
            var metadata = internalCluster().clusterService(searchNode).state().getMetadata().indexMetadata(index);
            if (metadata.getReshardingMetadata() == null) {
                throw new AssertionError("Waiting for search coordinator to see resharding metadata");
            }
        });

        // Transition to HANDOFF is blocked, all targets are CLONE, search uses source shard only.
        var cloneSearch = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(ingestedDocumentsPerShard * multiple * 2)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setRouting(routingValuePerShard.get(1));

        // Even though the routing value would now route to shard 1, search is still routed to the source shard,
        // and we get all documents.
        assertResponse(cloneSearch, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard * multiple, r.getHits().getTotalHits().value());
        });

        // unblock HANDOFF transition
        stateTransitionBlock.await();
        stateTransitionBlock.reset();
        resetStateTransitionBlock.await();

        assertBusy(() -> {
            var metadata = internalCluster().clusterService(searchNode).state().getMetadata().indexMetadata(index);
            if (metadata.getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.HANDOFF) == false) {
                throw new AssertionError("Waiting for search coordinator to see target shards transition to HANDOFF");
            }
        });

        // transition to SPLIT is blocked, all targets are HANDOFF, search uses source shard, refresh uses target shards
        var handoffSearch = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(ingestedDocumentsPerShard * multiple * 2)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setRouting(routingValuePerShard.get(2));

        // Even though the routing value would now route to shard 2, search is still routed to the source shard,
        // and we get all documents.
        assertResponse(handoffSearch, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard * multiple, r.getHits().getTotalHits().value());
        });

        // unblock SPLIT transition
        stateTransitionBlock.await();
        stateTransitionBlock.reset();
        resetStateTransitionBlock.await();

        assertBusy(() -> {
            var metadata = internalCluster().clusterService(searchNode).state().getMetadata().indexMetadata(index);
            if (metadata.getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.SPLIT) == false) {
                throw new AssertionError("Waiting for search coordinator to see target shards transition to SPLIT");
            }
        });

        // TODO
        // Since search filters do not exist, we'll still see all documents here in all shards until they are deleted.
        // So we advance to DONE and check.

        // unblock DONE transition
        stateTransitionBlock.await();
        stateTransitionBlock.reset();
        resetStateTransitionBlock.await();

        assertBusy(() -> {
            var metadata = internalCluster().clusterService(searchNode).state().getMetadata().indexMetadata(index);
            if (metadata.getReshardingMetadata() == null) {
                // all shards are DONE so resharding metadata is removed by master
                return;
            }

            boolean targetsDone = metadata.getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.DONE);
            boolean sourceDone = metadata.getReshardingMetadata()
                .getSplit()
                .sourceStates()
                .allMatch(s -> s == IndexReshardingState.Split.SourceShardState.DONE);
            if (targetsDone == false || sourceDone == false) {
                throw new AssertionError("Waiting for search coordinator to see all shards transition to DONE");
            }
        });

        var doneSearchShard0 = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(ingestedDocumentsPerShard * multiple * 2)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setRouting(routingValuePerShard.get(0));

        // Routing is correctly applied, one shard is searched and only documents from this shard are returned.
        assertResponse(doneSearchShard0, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard, r.getHits().getTotalHits().value());
        });

        var doneSearchShard1 = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(ingestedDocumentsPerShard * multiple * 2)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setRouting(routingValuePerShard.get(1));

        // Routing is correctly applied, one shard is searched and only documents from this shard are returned.
        assertResponse(doneSearchShard1, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard, r.getHits().getTotalHits().value());
        });

        var doneSearchShard2 = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(ingestedDocumentsPerShard * multiple * 2)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setRouting(routingValuePerShard.get(2));

        // Routing is correctly applied, one shard is searched and only documents from this shard are returned.
        assertResponse(doneSearchShard2, r -> {
            assertEquals(1, r.getTotalShards());
            assertEquals(ingestedDocumentsPerShard, r.getHits().getTotalHits().value());
        });

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

        final int multiple = randomIntBetween(2, 10);

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple)).actionGet());
        waitForReshardCompletion(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, multiple);

        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), 0);

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

        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), indexedDocs);
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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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

    public void testReshardFailsWithNullIndex() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // Test null index
        ReshardIndexRequest request = new ReshardIndexRequest("", 2);
        expectThrows(IndexNotFoundException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
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
        ReshardIndexRequest request = new ReshardIndexRequest("test*", 2);
        expectThrows(IndexNotFoundException.class, () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet());
    }

    public void testReshardFailsWithInvalidMultiple() {
        String indexNode = startMasterAndIndexNode();

        ensureStableCluster(1);

        final String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        // the multiple for resharding must be > 1, verify a few invalid multiples fail validation
        ReshardIndexRequest request = new ReshardIndexRequest("test-index", -1);
        expectThrows(
            ActionRequestValidationException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet()
        );

        // verify that the index metadata still contains only a single shard
        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(1)
        );

        ReshardIndexRequest request2 = new ReshardIndexRequest("test-index", 0);
        expectThrows(
            ActionRequestValidationException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, request2).actionGet()
        );

        GetSettingsResponse postReshardSettingsResponse1 = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse1.getIndexToSettings().get(indexName)),
            equalTo(1)
        );

        ReshardIndexRequest request3 = new ReshardIndexRequest("test-index", 1);
        expectThrows(
            ActionRequestValidationException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, request3).actionGet()
        );

        GetSettingsResponse postReshardSettingsResponse2 = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse2.getIndexToSettings().get(indexName)),
            equalTo(1)
        );
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
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet()
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
        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());
        waitForReshardCompletion(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 2);
        ensureGreen(indexName);

        // doc count should not have increased since both shards will have deleted unowned documents
        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
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

                    assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());
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

    public void testReshardSystemIndex() throws Exception {
        String indexNode = startMasterAndIndexNode();
        ensureStableCluster(1);

        final String indexName = SYSTEM_INDEX_NAME + "-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, 2);
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
        ReshardIndexRequest dataStreamRequest = new ReshardIndexRequest("my-data-stream", 2);
        expectThrows(IndexNotFoundException.class, () -> client().execute(TransportReshardAction.TYPE, dataStreamRequest).actionGet());

        // Try to reshard an index that is part of a data stream, it should fail as well
        GetDataStreamAction.Response getDataStreamResponse = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "my-data-stream" })
        ).actionGet();
        var dataStreamIndex = getDataStreamResponse.getDataStreams().get(0).getDataStream().getWriteIndex();

        ReshardIndexRequest dataStreamIndexRequest = new ReshardIndexRequest(dataStreamIndex.getName(), 2);
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(TransportReshardAction.TYPE, dataStreamIndexRequest).actionGet()
        );
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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());
        waitForReshardCompletion(indexName);
        ensureGreen(indexName);

        assertThat(getCurrentPrimaryTerm(index, 1), equalTo(currentPrimaryTerm));
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.reshard.ReshardIndexService:DEBUG", reason = "logging assertions")
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
            if (TransportUpdateSplitStateAction.TYPE.name().equals(action) && handoffAttemptedLatch.getCount() != 0) {
                try {
                    handoffAttemptedLatch.countDown();
                    handoffLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

        handoffAttemptedLatch.await();

        IndexShard indexShard = findIndexShard(index, 0);
        indexShard.failShard("broken", new Exception("boom local"));
        final long finalPrimaryTerm = currentPrimaryTerm;
        assertBusy(() -> {
            assertThat(getCurrentPrimaryTerm(index, 0), greaterThan(finalPrimaryTerm));
            assertThat(
                client().admin()
                    .cluster()
                    .prepareHealth(TimeValue.timeValueSeconds(30))
                    .setIndices(indexName)
                    .get()
                    .getActivePrimaryShards(),
                equalTo(1)
            );
        });

        MockLog.assertThatLogger(() -> {
            // When we release the handoff block the recovery will progress. However, it will fail because the source shard primary term
            // has advanced.
            handoffLatch.countDown();
            waitForReshardCompletion(indexName);
        },
            ReshardIndexService.class,
            new MockLog.PatternSeenEventExpectation(
                "split handoff failed",
                ReshardIndexService.class.getCanonicalName(),
                Level.DEBUG,
                ".*\\[" + indexName + "\\]\\[1\\] cannot transition target state \\[HANDOFF\\] because source primary term advanced \\[.*"
            )
        );

        // After the target shard recovery tries again it will synchronize its primary term with the source and come online.
        ensureGreen(indexName);

        // The primary term has synchronized with the source
        assertThat(getCurrentPrimaryTerm(index, 1), equalTo(getCurrentPrimaryTerm(index, 0)));
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.reshard.ReshardIndexService:DEBUG", reason = "logging assertions")
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
            if (TransportUpdateSplitStateAction.TYPE.name().equals(action) && stateChangeAttemptedLatch.getCount() != 0) {
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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 1);

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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

        // TODO: At this point we finish the reshard once all the target states are DONE. In the future we maybe need to switch this
        // assertion to reflect the usage of additional states
        waitForReshardCompletion(indexName);

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
        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

        // wait until we know it's in progress
        var ignored = splitState.actionGet(SAFE_AWAIT_TIMEOUT).projectState().metadata().index(indexName).getReshardingMetadata();

        // now start a second reshard, which should fail
        assertThrows(
            IllegalStateException.class,
            () -> client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2))
                .actionGet(SAFE_AWAIT_TIMEOUT)
        );

        // unblock allocation to allow operations to proceed
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.enable"));

        waitForReshardCompletion(indexName);

        checkNumberOfShardsSetting(indexNode, indexName, 2);

        // now we should be able to resplit
        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());
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

        var futures = new ArrayList<ActionFuture<ReshardIndexResponse>>();
        for (int i = 0; i < indexCount; i++) {
            final String indexName = indexNameTemplate + i;
            futures.add(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)));
        }

        disruption.stopDisrupting();

        for (int i = 0; i < indexCount; i++) {
            assertAcked(futures.get(i).actionGet());
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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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

        assertAcked(client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet());

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

        // TODO This should work once we have the logic to copy commits that arrive during handoff, ES-11531 ?
        // assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), 1);
        // assertSearchHits(prepareSearch(indexName).setQuery(matchQuery("foo", "bar")), targetDocId);

        // TODO Modify this once ES-11531 is merged ?
        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo((long) 1));
        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 1), equalTo((long) 0));

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(EsqlPlugin.class);
        return plugins;
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

    /**
     * A future that waits if necessary for cluster state to match a given predicate, and returns that state
     * @param predicate continue waiting for state updates until true
     * @return A future whose get() will resolve to the cluster state that matches the supplied predicate
     */
    private PlainActionFuture<ClusterState> waitForClusterState(Predicate<ClusterState> predicate) {
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

        ClusterStateObserver.waitForState(clusterService(), new ThreadContext(Settings.EMPTY), listener, predicate, null, logger);

        return future;
    }

    /**
     * Extract a shardId from a "simple" {@link IndexRouting} using a randomly
     * chosen method. All of the random methods <strong>should</strong> return the
     * same results.
     */
    private int shardIdFromSimple(IndexRouting indexRouting, String id, @Nullable String routing) {
        return switch (between(0, 2)) {
            case 0 -> indexRouting.indexShard(id, routing, null, null);
            case 1 -> indexRouting.updateShard(id, routing);
            case 2 -> indexRouting.deleteShard(id, routing);
            default -> throw new AssertionError("invalid option");
        };
    }

    private void waitForReshardCompletion(String indexName) {
        awaitClusterState((state) -> state.projectState().metadata().index(indexName).getReshardingMetadata() == null);
    }
}

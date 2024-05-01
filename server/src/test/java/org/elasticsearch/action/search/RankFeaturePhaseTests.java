/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class RankFeaturePhaseTests extends ESTestCase {

    public void testRankFeaturePhaseWith1Shard() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 0;
        final int size = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            int totalHits = randomIntBetween(2, 100);
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                // this would have been populated during the QueryPhase by the appropriate QueryPhaseShardContext
                queryResult.setRankShardResult(
                    new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1), new RankFeatureDoc(2, 9.0F, -1) })
                );
                queryResult.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) }
                        ),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );

                queryResult.size(totalHits);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard1Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit[] searchHits = new SearchHit[2];
                                for (int i = 0; i < searchHits.length; i++) {
                                    searchHits[i] = SearchHit.unpooled(i + 1);
                                    searchHits[i].shard(shard1Target);
                                    searchHits[i].score(10 - i);
                                    searchHits[i].setDocumentField(
                                        field,
                                        new DocumentField(field, Collections.singletonList(searchHits[i].docId()))
                                    );
                                }
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(randomIntBetween(10, 100), TotalHits.Relation.EQUAL_TO),
                                    10F
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard1Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(1, rankPhaseResults.getAtomicArray().length());
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof RankFeatureResult);
                RankFeatureResult rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());

                RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs.length);

                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_1", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[1].doc);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[1].rank);
                assertEquals("ranked_2", rankFeatureShardResult.rankFeatureDocs[1].featureData);
                assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());

                assertEquals(2, finalResults[0].length);
                assertEquals(1, finalResults[0][0].doc);
                assertEquals(110, finalResults[0][0].score, 10E-5);
                assertEquals(1, ((RankFeatureDoc) finalResults[0][0]).rank);
                assertEquals(2, finalResults[0][1].doc);
                assertEquals(109, finalResults[0][1].score, 10E-5);
                assertEquals(2, ((RankFeatureDoc) finalResults[0][1]).rank);
            } finally {
                queryResult.decRef();
            }
        }
    }

    public void testRankFeaturePhaseWithMultipleShardsOneEmpty() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 0;
        final int size = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);
        SearchShardTarget shard3Target = new SearchShardTarget("node2", new ShardId("test", "na", 2), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(3);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2 found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
            final int shard1Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
            final int shard2Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard3 = new ShardSearchContextId(UUIDs.base64UUID(), 789);
            QuerySearchResult queryResultShard3 = new QuerySearchResult(ctxShard3, shard2Target, null);
            queryResultShard3.setShardIndex(shard3Target.getShardId().getId());
            try {
                queryResultShard1.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1) }));
                queryResultShard1.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 10.0F) }),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard1.size(shard1Results);

                queryResultShard2.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(2, 9.0F, -1) }));
                queryResultShard2.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(2, 9.0F) }),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard2.size(shard2Results);

                queryResultShard3.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[0]));
                queryResultShard3.topDocs(new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN), new DocValueFormat[0]);
                queryResultShard3.size(0);

                results.consumeResult(queryResultShard2, () -> {});
                results.consumeResult(queryResultShard3, () -> {});
                results.consumeResult(queryResultShard1, () -> {});

                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard1Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit hit = SearchHit.unpooled(1);
                                hit.shard(shard1Target);
                                hit.score(10f);
                                hit.setDocumentField(field, new DocumentField(field, Collections.singletonList(hit.docId())));

                                SearchHit[] searchHits = new SearchHit[] { hit };
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO),
                                    10F
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard1Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }
                        } else if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 2 })) {
                            // second shard
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard2Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit hit = SearchHit.unpooled(2);
                                hit.shard(shard2Target);
                                hit.score(9F);
                                hit.setDocumentField(field, new DocumentField(field, Collections.singletonList(hit.docId())));
                                SearchHit[] searchHits = new SearchHit[] { hit };
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(shard2Results, TotalHits.Relation.EQUAL_TO),
                                    9f
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard2Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }

                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(3, rankPhaseResults.getAtomicArray().length());
                // one result is null
                assertEquals(2, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof RankFeatureResult);
                RankFeatureResult rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_1", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                shardResult = rankPhaseResults.getAtomicArray().get(1);
                assertTrue(shardResult instanceof RankFeatureResult);
                rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_2", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                shardResult = rankPhaseResults.getAtomicArray().get(2);
                assertNull(shardResult);

                assertEquals(2, finalResults[0].length);
                assertEquals(1, finalResults[0][0].doc);
                assertEquals(110, finalResults[0][0].score, 10E-5);
                assertEquals(1, ((RankFeatureDoc) finalResults[0][0]).rank);
                assertEquals(2, finalResults[0][1].doc);
                assertEquals(109, finalResults[0][1].score, 10E-5);
                assertEquals(2, ((RankFeatureDoc) finalResults[0][1]).rank);
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
                queryResultShard3.decRef();
            }
        }
    }

    public void testRankFeaturePhaseNoNeedForFetchingFieldData() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 0;
        final int size = 10;
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder; using a null rankFeaturePhaseRankShardContext
        // and non-field based rankFeaturePhaseRankCoordinatorContext
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            null,
            negatingScoresRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            int totalHits = randomIntBetween(2, 100);
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                queryResult.setRankShardResult(
                    new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1), new RankFeatureDoc(2, 9.0F, -1) })
                );
                // top results are 42 and 84;
                // the RankCoordinatorContext will negate the scores and revert the final order
                queryResult.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) }
                        ),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );

                queryResult.size(totalHits);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            listener.onFailure(new UnsupportedOperationException("should not have reached here"));
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        // store the results that are passed to fetch phase so that we can verify them later on
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                // in this case there was no additional "RankFeature" results on shards, so we shortcut directly to queryPhaseResults
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.queryPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(1, rankPhaseResults.getAtomicArray().length());
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof QuerySearchResult);
                QuerySearchResult rankResult = (QuerySearchResult) shardResult;
                assertNull(rankResult.rankFeatureResult());
                assertNotNull(rankResult.queryResult());

                assertEquals(2, finalResults[0].length);
                assertEquals(2, finalResults[0][0].doc);
                assertEquals(-9, finalResults[0][0].score, 10E-5);
                assertEquals(1, ((RankFeatureDoc) finalResults[0][0]).rank);
                assertEquals(1, finalResults[0][1].doc);
                assertEquals(-10, finalResults[0][1].score, 10E-5);
                assertEquals(2, ((RankFeatureDoc) finalResults[0][1]).rank);
            } finally {
                queryResult.decRef();
            }
        }
    }

    public void testRankFeaturePhaseNoMatchingDocs() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 0;
        final int size = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 0 results
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            int totalHits = 0;
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                // this would have been populated during the QueryPhase by the appropriate QueryPhaseShardContext
                queryResult.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[0]));
                queryResult.topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), 10.0F),
                    new DocValueFormat[0]
                );

                queryResult.size(totalHits);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123) {
                            listener.onFailure(new UnsupportedOperationException("should not have reached here"));
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                // in this case there was no additional "RankFeature" results on shards, so we shortcut directly to queryPhaseResults
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.queryPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(1, rankPhaseResults.getAtomicArray().length());
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof QuerySearchResult);
                QuerySearchResult rankResult = (QuerySearchResult) shardResult;
                assertNull(rankResult.rankFeatureResult());
                assertNotNull(rankResult.queryResult());

                assertEquals(0, finalResults[0].length);
            } finally {
                queryResult.decRef();
            }
        }
    }

    public void testRankFeaturePhaseEmptyRankCoordinatorContext() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder; no reranking on coordinator though
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            null
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            int totalHits = randomIntBetween(2, 100);
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                // this would have been populated during the QueryPhase by the appropriate QueryPhaseShardContext
                queryResult.setRankShardResult(
                    new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1), new RankFeatureDoc(2, 9.0F, -1) })
                );
                queryResult.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) }
                        ),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );

                queryResult.size(totalHits);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            listener.onFailure(new UnsupportedOperationException("should not have reached here"));
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                // in this case there was no additional "RankFeature" results on shards, so we shortcut directly to queryPhaseResults
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.queryPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(1, rankPhaseResults.getAtomicArray().length());
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof QuerySearchResult);
                QuerySearchResult rankResult = (QuerySearchResult) shardResult;
                assertNull(rankResult.rankFeatureResult());
                assertNotNull(rankResult.queryResult());

                assertEquals(2, finalResults[0].length);
                assertEquals(1, finalResults[0][0].doc);
                assertEquals(10, finalResults[0][0].score, 10E-5);
                assertEquals(-1, ((RankFeatureDoc) finalResults[0][0]).rank);
                assertEquals(2, finalResults[0][1].doc);
                assertEquals(9, finalResults[0][1].score, 10E-5);
                assertEquals(-1, ((RankFeatureDoc) finalResults[0][1]).rank);
            } finally {
                queryResult.decRef();
            }
        }
    }

    public void testRankFeaturePhaseOneShardFails() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 0;
        final int size = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2 found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
            final int shard1Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
            final int shard2Results = randomIntBetween(1, 100);

            try {
                queryResultShard1.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1) }));
                queryResultShard1.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 10.0F) }),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard1.size(shard1Results);

                queryResultShard2.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(2, 9.0F, -1) }));
                queryResultShard2.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(2, 9.0F) }),
                        9F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard2.size(shard2Results);

                results.consumeResult(queryResultShard2, () -> {});
                results.consumeResult(queryResultShard1, () -> {});

                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 2 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard1Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit hit = SearchHit.unpooled(2);
                                hit.shard(shard1Target);
                                hit.score(9);
                                hit.setDocumentField(field, new DocumentField(field, Collections.singletonList(hit.docId())));
                                SearchHit[] searchHits = new SearchHit[] { hit };
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO),
                                    9F
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard1Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }
                        } else if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            // other shard; this one throws an exception
                            listener.onFailure(new IllegalArgumentException("simulated failure"));
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertEquals(1, mockSearchPhaseContext.failures.size());
                assertTrue(mockSearchPhaseContext.failures.get(0).getCause().getMessage().contains("simulated failure"));
                assertEquals(0, phaseDone.getCount());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(2, rankPhaseResults.getAtomicArray().length());
                // one shard failed
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertNull(shardResult);

                shardResult = rankPhaseResults.getAtomicArray().get(1);
                assertTrue(shardResult instanceof RankFeatureResult);
                RankFeatureResult rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_2", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                assertEquals(1, finalResults[0].length);
                assertEquals(2, finalResults[0][0].doc);
                assertEquals(109, finalResults[0][0].score, 10E-5);
                assertEquals(1, ((RankFeatureDoc) finalResults[0][0]).rank);
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
            }
        }
    }

    public void testRankFeaturePhaseExceptionThrownOnPhase() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 0;
        final int size = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            int totalHits = randomIntBetween(2, 100);
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                // this would have been populated during the QueryPhase by the appropriate QueryPhaseShardContext
                queryResult.setRankShardResult(
                    new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1), new RankFeatureDoc(2, 9.0F, -1) })
                );
                queryResult.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) }
                        ),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );

                queryResult.size(totalHits);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard1Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit[] searchHits = new SearchHit[2];
                                for (int i = 0; i < searchHits.length; i++) {
                                    searchHits[i] = SearchHit.unpooled(i + 1);
                                    searchHits[i].shard(shard1Target);
                                    searchHits[i].score(10 - i);
                                    searchHits[i].setDocumentField(
                                        field,
                                        new DocumentField(field, Collections.singletonList(searchHits[i].docId()))
                                    );
                                }
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO),
                                    10F
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard1Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    void innerRun() {
                        throw new IllegalArgumentException("simulated failure");
                    }

                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                assertNotNull(mockSearchPhaseContext.phaseFailure.get());
                assertTrue(mockSearchPhaseContext.phaseFailure.get().getMessage().contains("simulated failure"));
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(1, phaseDone.getCount());
                assertTrue(rankFeaturePhase.rankPhaseResults.getAtomicArray().asList().isEmpty());
                assertNull(finalResults[0][0]);
            } finally {
                queryResult.decRef();
            }
        }
    }

    public void testRankFeatureWithPagination() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 10;
        final int from = 1;
        final int size = 1;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);
        SearchShardTarget shard3Target = new SearchShardTarget("node2", new ShardId("test", "na", 2), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(3);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 3 results, with doc ids 1 and (11, 2) found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
            final int shard1Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
            final int shard2Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard3 = new ShardSearchContextId(UUIDs.base64UUID(), 789);
            QuerySearchResult queryResultShard3 = new QuerySearchResult(ctxShard3, shard2Target, null);
            queryResultShard3.setShardIndex(shard3Target.getShardId().getId());
            try {
                queryResultShard1.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1) }));
                queryResultShard1.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 10.0F) }),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard1.size(shard1Results);

                queryResultShard2.setRankShardResult(
                    new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(11, 100.0F, -1), new RankFeatureDoc(2, 9.0F, -1) })
                );
                queryResultShard2.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(shard2Results, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] { new ScoreDoc(11, 100.0F), new ScoreDoc(2, 9.0F) }
                        ),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard2.size(shard2Results);

                queryResultShard3.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[0]));
                queryResultShard3.topDocs(new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN), new DocValueFormat[0]);
                queryResultShard3.size(0);

                results.consumeResult(queryResultShard2, () -> {});
                results.consumeResult(queryResultShard3, () -> {});
                results.consumeResult(queryResultShard1, () -> {});

                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard1Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit hit = SearchHit.unpooled(1);
                                hit.shard(shard1Target);
                                hit.score(10f);
                                hit.setDocumentField(field, new DocumentField(field, Collections.singletonList(hit.docId())));
                                SearchHit[] searchHits = new SearchHit[] { hit };
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO),
                                    10F
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard1Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }
                        } else if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 11, 2 })) {
                            // second shard
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard2Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit firstHit = SearchHit.unpooled(11);
                                firstHit.shard(shard2Target);
                                firstHit.score(100F);
                                firstHit.setDocumentField(field, new DocumentField(field, Collections.singletonList(firstHit.docId())));
                                SearchHit secondHit = SearchHit.unpooled(2);
                                secondHit.shard(shard2Target);
                                secondHit.score(9F);
                                secondHit.setDocumentField(field, new DocumentField(field, Collections.singletonList(secondHit.docId())));
                                SearchHits hits = SearchHits.unpooled(
                                    new SearchHit[] { firstHit, secondHit },
                                    new TotalHits(shard2Results, TotalHits.Relation.EQUAL_TO),
                                    100f
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard2Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }

                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(3, rankPhaseResults.getAtomicArray().length());
                // one result is null
                assertEquals(2, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof RankFeatureResult);
                RankFeatureResult rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_1", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                shardResult = rankPhaseResults.getAtomicArray().get(1);
                assertTrue(shardResult instanceof RankFeatureResult);
                rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(11, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_11", rankFeatureShardResult.rankFeatureDocs[0].featureData);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[1].doc);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[1].rank);
                assertEquals("ranked_2", rankFeatureShardResult.rankFeatureDocs[1].featureData);

                shardResult = rankPhaseResults.getAtomicArray().get(2);
                assertNull(shardResult);

                assertEquals(1, finalResults[0].length);
                assertEquals(1, finalResults[0][0].doc);
                assertEquals(110, finalResults[0][0].score, 10E-5);
                assertEquals(2, ((RankFeatureDoc) finalResults[0][0]).rank);
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
                queryResultShard3.decRef();
            }
        }
    }

    public void testRankFeatureCollectOnlyRankWindowSizeFeatures() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 2;
        final int from = 0;
        final int size = 10;
        final String field = "some_field";
        List<Query> queries = new ArrayList<>();
        CountDownLatch phaseDone = new CountDownLatch(1);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(queries, rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(field),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);
        SearchShardTarget shard3Target = new SearchShardTarget("node2", new ShardId("test", "na", 2), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(3);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (
            SearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                mockSearchPhaseContext.getRequest(),
                mockSearchPhaseContext.numShards,
                exc -> {}
            )
        ) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 3 results, with doc ids 1, and (11, 2) found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
            final int shard1Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
            final int shard2Results = randomIntBetween(1, 100);

            final ShardSearchContextId ctxShard3 = new ShardSearchContextId(UUIDs.base64UUID(), 789);
            QuerySearchResult queryResultShard3 = new QuerySearchResult(ctxShard3, shard2Target, null);
            queryResultShard3.setShardIndex(shard3Target.getShardId().getId());
            try {
                queryResultShard1.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(1, 10.0F, -1) }));
                queryResultShard1.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 10.0F) }),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard1.size(shard1Results);

                queryResultShard2.setRankShardResult(
                    new RankFeatureShardResult(new RankFeatureDoc[] { new RankFeatureDoc(11, 100.0F, -1), new RankFeatureDoc(2, 9.0F, -1) })
                );
                queryResultShard2.topDocs(
                    new TopDocsAndMaxScore(
                        new TopDocs(
                            new TotalHits(shard2Results, TotalHits.Relation.EQUAL_TO),
                            new ScoreDoc[] { new ScoreDoc(11, 100.0F), new ScoreDoc(2, 9.0F) }
                        ),
                        10.0F
                    ),
                    new DocValueFormat[0]
                );
                queryResultShard2.size(shard2Results);

                queryResultShard3.setRankShardResult(new RankFeatureShardResult(new RankFeatureDoc[0]));
                queryResultShard3.topDocs(new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN), new DocValueFormat[0]);
                queryResultShard3.size(0);

                results.consumeResult(queryResultShard2, () -> {});
                results.consumeResult(queryResultShard3, () -> {});
                results.consumeResult(queryResultShard1, () -> {});

                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final SearchActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard1Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit hit = SearchHit.unpooled(1);
                                hit.shard(shard1Target);
                                hit.score(10f);
                                hit.setDocumentField(field, new DocumentField(field, Collections.singletonList(hit.docId())));
                                SearchHit[] searchHits = new SearchHit[] { hit };
                                SearchHits hits = SearchHits.unpooled(
                                    searchHits,
                                    new TotalHits(shard1Results, TotalHits.Relation.EQUAL_TO),
                                    10F
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard1Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }
                        } else if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 11 })) {
                            // second shard
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            try {
                                rankFeatureResult.setSearchShardTarget(shard2Target);
                                // these are the SearchHits generated by the FetchFieldPhase processor
                                SearchHit hit = SearchHit.unpooled(11);
                                hit.shard(shard2Target);
                                hit.score(100F);
                                hit.setDocumentField(field, new DocumentField(field, Collections.singletonList(hit.docId())));
                                SearchHits hits = SearchHits.unpooled(
                                    new SearchHit[] { hit },
                                    new TotalHits(shard2Results, TotalHits.Relation.EQUAL_TO),
                                    100f
                                );
                                RankBuilder shardRankBuilder = mockSearchPhaseContext.getRequest().source().rankBuilder();
                                // construct the appropriate RankFeatureDoc objects based on the rank builder
                                RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder
                                    .buildRankFeaturePhaseShardContext();
                                RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext
                                    .buildRankFeatureShardResult(hits, shard2Target.getShardId().id());
                                rankFeatureResult.shardResult(rankShardResult);
                                listener.onResponse(rankFeatureResult);
                            } finally {
                                rankFeatureResult.decRef();
                            }

                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
                // override the RankFeaturePhase to skip moving to next phase
                RankFeaturePhase rankFeaturePhase = new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
                    @Override
                    public void moveToNextPhase(
                        SearchPhaseResults<SearchPhaseResult> phaseResults,
                        SearchPhaseController.ReducedQueryPhase reducedQueryPhase
                    ) {
                        // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                        phaseDone.countDown();
                        finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                        logger.debug("Skipping moving to next phase");
                    }
                };
                assertEquals("rank-feature", rankFeaturePhase.getName());
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertEquals(0, phaseDone.getCount());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(3, rankPhaseResults.getAtomicArray().length());
                // one result is null
                assertEquals(2, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shardResult = rankPhaseResults.getAtomicArray().get(0);
                assertTrue(shardResult instanceof RankFeatureResult);
                RankFeatureResult rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(2, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_1", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                shardResult = rankPhaseResults.getAtomicArray().get(1);
                assertTrue(shardResult instanceof RankFeatureResult);
                rankResult = (RankFeatureResult) shardResult;
                assertNotNull(rankResult.rankFeatureResult());
                assertNull(rankResult.queryResult());
                assertNotNull(rankResult.rankFeatureResult().shardResult());
                rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs.length);
                assertEquals(11, rankFeatureShardResult.rankFeatureDocs[0].doc);
                assertEquals(1, rankFeatureShardResult.rankFeatureDocs[0].rank);
                assertEquals("ranked_11", rankFeatureShardResult.rankFeatureDocs[0].featureData);

                shardResult = rankPhaseResults.getAtomicArray().get(2);
                assertNull(shardResult);

                assertEquals(2, finalResults[0].length);
                assertEquals(11, finalResults[0][0].doc);
                assertEquals(200, finalResults[0][0].score, 10E-5);
                assertEquals(1, ((RankFeatureDoc) finalResults[0][0]).rank);
                assertEquals(1, finalResults[0][1].doc);
                assertEquals(110, finalResults[0][1].score, 10E-5);
                assertEquals(2, ((RankFeatureDoc) finalResults[0][1]).rank);
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
                queryResultShard3.decRef();
            }
        }
    }

    private RankFeaturePhaseRankCoordinatorContext defaultRankFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize) {
        return new RankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize) {
            @Override
            public void rankGlobalResults(List<SearchPhaseResult> rankSearchResults, Consumer<ScoreDoc[]> onFinish) {
                List<RankFeatureDoc> features = new ArrayList<>();
                for (SearchPhaseResult phaseResult : rankSearchResults) {
                    assert phaseResult instanceof RankFeatureResult;
                    RankFeatureResult rankFeatureResult = (RankFeatureResult) phaseResult;
                    RankFeatureShardResult shardResult = rankFeatureResult.shardResult();
                    features.addAll(Arrays.stream(shardResult.rankFeatureDocs).toList());
                }
                RankFeatureDoc[] featureDocs = features.toArray(new RankFeatureDoc[0]);
                Arrays.sort(featureDocs, Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
                RankFeatureDoc[] topResults = new RankFeatureDoc[Math.max(0, Math.min(size, featureDocs.length - from))];
                // perform pagination
                for (int rank = 0; rank < topResults.length; ++rank) {
                    topResults[rank] = featureDocs[from + rank];
                    topResults[rank].rank = from + rank + 1;
                }
                onFinish.accept(topResults);
            }
        };
    }

    private RankFeaturePhaseRankCoordinatorContext negatingScoresRankFeaturePhaseRankCoordinatorContext(
        int size,
        int from,
        int rankWindowSize
    ) {
        return new RankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize) {
            @Override
            public void rankGlobalResults(List<SearchPhaseResult> rankSearchResults, Consumer<ScoreDoc[]> onFinish) {
                List<ScoreDoc> docScores = new ArrayList<>();
                for (SearchPhaseResult phaseResults : rankSearchResults) {
                    assert phaseResults instanceof QuerySearchResult;
                    docScores.addAll(Arrays.asList(((QuerySearchResult) phaseResults).topDocs().topDocs.scoreDocs));
                }
                ScoreDoc[] sortedDocs = docScores.toArray(new ScoreDoc[0]);
                // negating scores
                Arrays.stream(sortedDocs).forEach(doc -> doc.score *= -1);

                Arrays.sort(sortedDocs, Comparator.comparing((ScoreDoc doc) -> doc.score).reversed());
                RankFeatureDoc[] topResults = new RankFeatureDoc[Math.max(0, Math.min(size, sortedDocs.length - from))];
                // perform pagination
                for (int rank = 0; rank < topResults.length; ++rank) {
                    ScoreDoc base = sortedDocs[from + rank];
                    topResults[rank] = new RankFeatureDoc(base.doc, base.score, base.shardIndex);
                    topResults[rank].rank = from + rank + 1;
                }
                onFinish.accept(topResults);
            }

            @Override
            public boolean needsFieldData() {
                return false;
            }
        };

    }

    private RankFeaturePhaseRankShardContext defaultRankFeaturePhaseRankShardContext(String field) {
        return new RankFeaturePhaseRankShardContext(field) {
            @Override
            public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                for (int i = 0; i < hits.getHits().length; i++) {
                    SearchHit hit = hits.getHits()[i];
                    rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                    rankFeatureDocs[i].score += 100f;
                    rankFeatureDocs[i].featureData("ranked_" + hit.docId());
                    rankFeatureDocs[i].rank = i + 1;
                }
                return new RankFeatureShardResult(rankFeatureDocs);
            }
        };
    }

    private QueryPhaseRankCoordinatorContext defaultQueryPhaseRankCoordinatorContext(int rankWindowSize) {
        return new QueryPhaseRankCoordinatorContext(rankWindowSize) {
            @Override
            public ScoreDoc[] rankQueryPhaseResults(
                List<QuerySearchResult> querySearchResults,
                SearchPhaseController.TopDocsStats topDocStats
            ) {
                List<RankFeatureDoc> rankDocs = new ArrayList<>();
                for (int i = 0; i < querySearchResults.size(); i++) {
                    QuerySearchResult querySearchResult = querySearchResults.get(i);
                    RankFeatureShardResult shardResult = (RankFeatureShardResult) querySearchResult.getRankShardResult();
                    for (RankFeatureDoc frd : shardResult.rankFeatureDocs) {
                        frd.shardIndex = i;
                        rankDocs.add(frd);
                    }
                }
                rankDocs.sort(Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
                RankFeatureDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankFeatureDoc[]::new);
                topDocStats.fetchHits = topResults.length;
                return topResults;
            }
        };
    }

    private QueryPhaseRankShardContext defaultQueryPhaseRankShardContext(List<Query> queries, int rankWindowSize) {
        return new QueryPhaseRankShardContext(queries, rankWindowSize) {
            @Override
            public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                throw new UnsupportedOperationException(
                    "shard-level QueryPhase context should not be accessed as part of the RankFeature phase"
                );
            }
        };
    }

    private SearchPhaseController searchPhaseController() {
        return new SearchPhaseController((task, request) -> InternalAggregationTestCase.emptyReduceContextBuilder());
    }

    private RankBuilder rankBuilder(
        int rankWindowSize,
        QueryPhaseRankShardContext queryPhaseRankShardContext,
        QueryPhaseRankCoordinatorContext queryPhaseRankCoordinatorContext,
        RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext,
        RankFeaturePhaseRankCoordinatorContext rankFeaturePhaseRankCoordinatorContext
    ) {
        return new RankBuilder(rankWindowSize) {
            @Override
            protected void doWriteTo(StreamOutput out) throws IOException {
                // no-op
            }

            @Override
            protected void doXContent(XContentBuilder builder, Params params) throws IOException {
                // no-op
            }

            @Override
            public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                return queryPhaseRankShardContext;
            }

            @Override
            public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                return queryPhaseRankCoordinatorContext;
            }

            @Override
            public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                return rankFeaturePhaseRankShardContext;
            }

            @Override
            public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
                return rankFeaturePhaseRankCoordinatorContext;
            }

            @Override
            protected boolean doEquals(RankBuilder other) {
                return other != null && other.rankWindowSize() == rankWindowSize;
            }

            @Override
            protected int doHashCode() {
                return 0;
            }

            @Override
            public String getWriteableName() {
                return "test-rank-builder";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersions.V_8_12_0;
            }
        };
    }

    private SearchSourceBuilder searchSourceWithRankBuilder(RankBuilder rankBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(rankBuilder);
        return searchSourceBuilder;
    }

}

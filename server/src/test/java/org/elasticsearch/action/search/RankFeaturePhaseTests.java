/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.search.rank.RankDoc;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class RankFeaturePhaseTests extends ESTestCase {

    private static final int DEFAULT_RANK_WINDOW_SIZE = 10;
    private static final int DEFAULT_FROM = 0;
    private static final int DEFAULT_SIZE = 10;
    private static final String DEFAULT_FIELD = "some_field";

    private final RankBuilder DEFAULT_RANK_BUILDER = rankBuilder(
        DEFAULT_RANK_WINDOW_SIZE,
        defaultQueryPhaseRankShardContext(new ArrayList<>(), DEFAULT_RANK_WINDOW_SIZE),
        defaultQueryPhaseRankCoordinatorContext(DEFAULT_RANK_WINDOW_SIZE),
        defaultRankFeaturePhaseRankShardContext(DEFAULT_FIELD),
        defaultRankFeaturePhaseRankCoordinatorContext(DEFAULT_SIZE, DEFAULT_FROM, DEFAULT_RANK_WINDOW_SIZE)
    );

    private record ExpectedRankFeatureDoc(int doc, int rank, float score, String featureData) {}

    public void testRankFeaturePhaseWith1Shard() {
        // request params used within SearchSourceBuilder and *RankContext classes
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(DEFAULT_RANK_BUILDER);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
                // here we have 2 results, with doc ids 1 and 2
                int totalHits = randomIntBetween(2, 100);
                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) };
                populateQuerySearchResult(queryResult, totalHits, shard1Docs);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final ActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard1Target,
                                totalHits,
                                shard1Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
            } finally {
                queryResult.decRef();
            }

            RankFeaturePhase rankFeaturePhase = rankFeaturePhase(results, mockSearchPhaseContext, finalResults, phaseDone);
            try {
                rankFeaturePhase.run();

                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertTrue(phaseDone.get());
                assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());

                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(1, rankPhaseResults.getAtomicArray().length());
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shard1Result = rankPhaseResults.getAtomicArray().get(0);
                List<ExpectedRankFeatureDoc> expectedShardResults = List.of(
                    new ExpectedRankFeatureDoc(1, 1, 110.0F, "ranked_1"),
                    new ExpectedRankFeatureDoc(2, 2, 109.0F, "ranked_2")
                );
                List<ExpectedRankFeatureDoc> expectedFinalResults = new ArrayList<>(expectedShardResults);
                assertShardResults(shard1Result, expectedShardResults);
                assertFinalResults(finalResults[0], expectedFinalResults);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    public void testRankFeaturePhaseWithMultipleShardsOneEmpty() {
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(DEFAULT_RANK_BUILDER);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);
        SearchShardTarget shard3Target = new SearchShardTarget("node2", new ShardId("test", "na", 2), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(3);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2 found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            final ShardSearchContextId ctxShard3 = new ShardSearchContextId(UUIDs.base64UUID(), 789);

            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            QuerySearchResult queryResultShard3 = new QuerySearchResult(ctxShard3, shard2Target, null);
            try {
                queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
                queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
                queryResultShard3.setShardIndex(shard3Target.getShardId().getId());

                final int shard1Results = randomIntBetween(1, 100);
                final int shard2Results = randomIntBetween(1, 100);
                final int shard3Results = 0;

                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F) };
                populateQuerySearchResult(queryResultShard1, shard1Results, shard1Docs);
                final ScoreDoc[] shard2Docs = new ScoreDoc[] { new ScoreDoc(2, 9.0F) };
                populateQuerySearchResult(queryResultShard2, shard2Results, shard2Docs);
                final ScoreDoc[] shard3Docs = new ScoreDoc[0];
                populateQuerySearchResult(queryResultShard3, shard3Results, shard3Docs);

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
                        final ActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        RankFeatureResult rankFeatureResult = new RankFeatureResult();
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard1Target,
                                shard1Results,
                                shard1Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 2 })) {
                            // second shard
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard2Target,
                                shard2Results,
                                shard2Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else if (request.contextId().getId() == 789) {
                            listener.onResponse(rankFeatureResult);
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
                queryResultShard3.decRef();
            }
            RankFeaturePhase rankFeaturePhase = rankFeaturePhase(results, mockSearchPhaseContext, finalResults, phaseDone);
            try {
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertTrue(phaseDone.get());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(3, rankPhaseResults.getAtomicArray().length());
                // one result is null
                assertEquals(2, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shard1Result = rankPhaseResults.getAtomicArray().get(0);
                List<ExpectedRankFeatureDoc> expectedShard1Results = List.of(new ExpectedRankFeatureDoc(1, 1, 110.0F, "ranked_1"));
                assertShardResults(shard1Result, expectedShard1Results);

                SearchPhaseResult shard2Result = rankPhaseResults.getAtomicArray().get(1);
                List<ExpectedRankFeatureDoc> expectedShard2Results = List.of(new ExpectedRankFeatureDoc(2, 1, 109.0F, "ranked_2"));
                assertShardResults(shard2Result, expectedShard2Results);

                SearchPhaseResult shard3Result = rankPhaseResults.getAtomicArray().get(2);
                assertNull(shard3Result);

                List<ExpectedRankFeatureDoc> expectedFinalResults = List.of(
                    new ExpectedRankFeatureDoc(1, 1, 110.0F, "ranked_1"),
                    new ExpectedRankFeatureDoc(2, 2, 109.0F, "ranked_2")
                );
                assertFinalResults(finalResults[0], expectedFinalResults);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    public void testRankFeaturePhaseNoNeedForFetchingFieldData() {
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder; using a null rankFeaturePhaseRankShardContext
        // and non-field based rankFeaturePhaseRankCoordinatorContext
        RankBuilder rankBuilder = rankBuilder(
            DEFAULT_RANK_WINDOW_SIZE,
            defaultQueryPhaseRankShardContext(Collections.emptyList(), DEFAULT_RANK_WINDOW_SIZE),
            negatingScoresQueryFeaturePhaseRankCoordinatorContext(DEFAULT_SIZE, DEFAULT_FROM, DEFAULT_RANK_WINDOW_SIZE),
            null,
            null
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);

            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                int totalHits = randomIntBetween(2, 100);
                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) };
                populateQuerySearchResult(queryResult, totalHits, shard1Docs);
                results.consumeResult(queryResult, () -> {});
                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final ActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            listener.onFailure(new UnsupportedOperationException("should not have reached here"));
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
            } finally {
                queryResult.decRef();
            }
            // override the RankFeaturePhase to skip moving to next phase
            RankFeaturePhase rankFeaturePhase = rankFeaturePhase(results, mockSearchPhaseContext, finalResults, phaseDone);
            try {
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertTrue(phaseDone.get());

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

                List<ExpectedRankFeatureDoc> expectedFinalResults = List.of(
                    new ExpectedRankFeatureDoc(2, 1, -9.0F, null),
                    new ExpectedRankFeatureDoc(1, 2, -10.0F, null)
                );
                assertFinalResults(finalResults[0], expectedFinalResults);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    public void testRankFeaturePhaseOneShardFails() {
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(DEFAULT_RANK_BUILDER);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2 found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);

            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            try {
                queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
                queryResultShard2.setShardIndex(shard2Target.getShardId().getId());

                final int shard1Results = randomIntBetween(1, 100);
                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F) };
                populateQuerySearchResult(queryResultShard1, shard1Results, shard1Docs);

                final int shard2Results = randomIntBetween(1, 100);
                final ScoreDoc[] shard2Docs = new ScoreDoc[] { new ScoreDoc(2, 9.0F) };
                populateQuerySearchResult(queryResultShard2, shard2Results, shard2Docs);

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
                        final ActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 2 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard2Target,
                                shard2Results,
                                shard2Docs
                            );
                            listener.onResponse(rankFeatureResult);

                        } else if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            // other shard; this one throws an exception
                            listener.onFailure(new IllegalArgumentException("simulated failure"));
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
            }
            RankFeaturePhase rankFeaturePhase = rankFeaturePhase(results, mockSearchPhaseContext, finalResults, phaseDone);
            try {
                rankFeaturePhase.run();

                mockSearchPhaseContext.assertNoFailure();
                assertEquals(1, mockSearchPhaseContext.failures.size());
                assertTrue(mockSearchPhaseContext.failures.get(0).getCause().getMessage().contains("simulated failure"));
                assertTrue(phaseDone.get());

                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(2, rankPhaseResults.getAtomicArray().length());
                // one shard failed
                assertEquals(1, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shard1Result = rankPhaseResults.getAtomicArray().get(0);
                assertNull(shard1Result);

                SearchPhaseResult shard2Result = rankPhaseResults.getAtomicArray().get(1);
                List<ExpectedRankFeatureDoc> expectedShard2Results = List.of(new ExpectedRankFeatureDoc(2, 1, 109.0F, "ranked_2"));
                List<ExpectedRankFeatureDoc> expectedFinalResults = new ArrayList<>(expectedShard2Results);
                assertShardResults(shard2Result, expectedShard2Results);
                assertFinalResults(finalResults[0], expectedFinalResults);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    public void testRankFeaturePhaseExceptionThrownOnPhase() {
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(DEFAULT_RANK_BUILDER);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 2 results, with doc ids 1 and 2
            final ShardSearchContextId ctx = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            QuerySearchResult queryResult = new QuerySearchResult(ctx, shard1Target, null);
            try {
                queryResult.setShardIndex(shard1Target.getShardId().getId());
                int totalHits = randomIntBetween(2, 100);
                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F), new ScoreDoc(2, 9.0F) };
                populateQuerySearchResult(queryResult, totalHits, shard1Docs);
                results.consumeResult(queryResult, () -> {});

                // do not make an actual http request, but rather generate the response
                // as if we would have read it from the RankFeatureShardPhase
                mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null, null) {
                    @Override
                    public void sendExecuteRankFeature(
                        Transport.Connection connection,
                        final RankFeatureShardRequest request,
                        SearchTask task,
                        final ActionListener<RankFeatureResult> listener
                    ) {
                        // make sure to match the context id generated above, otherwise we throw
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1, 2 })) {
                            RankFeatureResult rankFeatureResult = new RankFeatureResult();
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard1Target,
                                totalHits,
                                shard1Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
            } finally {
                queryResult.decRef();
            }
            // override the RankFeaturePhase to raise an exception
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
                    phaseDone.set(true);
                    finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                    logger.debug("Skipping moving to next phase");
                }
            };
            assertEquals("rank-feature", rankFeaturePhase.getName());
            try {
                rankFeaturePhase.run();
                assertNotNull(mockSearchPhaseContext.phaseFailure.get());
                assertTrue(mockSearchPhaseContext.phaseFailure.get().getMessage().contains("simulated failure"));
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertFalse(phaseDone.get());
                assertTrue(rankFeaturePhase.rankPhaseResults.getAtomicArray().asList().isEmpty());
                assertNull(finalResults[0][0]);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    public void testRankFeatureWithPagination() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int from = 1;
        final int size = 1;
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            DEFAULT_RANK_WINDOW_SIZE,
            defaultQueryPhaseRankShardContext(Collections.emptyList(), DEFAULT_RANK_WINDOW_SIZE),
            defaultQueryPhaseRankCoordinatorContext(DEFAULT_RANK_WINDOW_SIZE),
            defaultRankFeaturePhaseRankShardContext(DEFAULT_FIELD),
            defaultRankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);
        SearchShardTarget shard3Target = new SearchShardTarget("node2", new ShardId("test", "na", 2), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(3);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 4 results, with doc ids 1 and (11, 2, 200) found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            final ShardSearchContextId ctxShard3 = new ShardSearchContextId(UUIDs.base64UUID(), 789);

            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            QuerySearchResult queryResultShard3 = new QuerySearchResult(ctxShard3, shard2Target, null);

            try {
                queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
                queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
                queryResultShard3.setShardIndex(shard3Target.getShardId().getId());

                final int shard1Results = randomIntBetween(1, 100);
                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F) };
                populateQuerySearchResult(queryResultShard1, shard1Results, shard1Docs);

                final int shard2Results = randomIntBetween(1, 100);
                final ScoreDoc[] shard2Docs = new ScoreDoc[] {
                    new ScoreDoc(11, 100.0F, -1),
                    new ScoreDoc(2, 9.0F),
                    new ScoreDoc(200, 1F, -1) };
                populateQuerySearchResult(queryResultShard2, shard2Results, shard2Docs);

                final int shard3Results = 0;
                final ScoreDoc[] shard3Docs = new ScoreDoc[0];
                populateQuerySearchResult(queryResultShard3, shard3Results, shard3Docs);

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
                        final ActionListener<RankFeatureResult> listener
                    ) {

                        RankFeatureResult rankFeatureResult = new RankFeatureResult();
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard1Target,
                                shard1Results,
                                shard1Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 11, 2, 200 })) {
                            // second shard

                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard2Target,
                                shard2Results,
                                shard2Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }

                    }
                };
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
                queryResultShard3.decRef();
            }
            RankFeaturePhase rankFeaturePhase = rankFeaturePhase(results, mockSearchPhaseContext, finalResults, phaseDone);
            try {
                rankFeaturePhase.run();

                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertTrue(phaseDone.get());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(3, rankPhaseResults.getAtomicArray().length());
                // one result is null
                assertEquals(2, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shard1Result = rankPhaseResults.getAtomicArray().get(0);
                List<ExpectedRankFeatureDoc> expectedShard1Results = List.of(new ExpectedRankFeatureDoc(1, 1, 110.0F, "ranked_1"));
                assertShardResults(shard1Result, expectedShard1Results);

                SearchPhaseResult shard2Result = rankPhaseResults.getAtomicArray().get(1);
                List<ExpectedRankFeatureDoc> expectedShard2Results = List.of(
                    new ExpectedRankFeatureDoc(11, 1, 200.0F, "ranked_11"),
                    new ExpectedRankFeatureDoc(2, 2, 109.0F, "ranked_2"),
                    new ExpectedRankFeatureDoc(200, 3, 101.0F, "ranked_200")

                );
                assertShardResults(shard2Result, expectedShard2Results);

                SearchPhaseResult shard3Result = rankPhaseResults.getAtomicArray().get(2);
                assertNull(shard3Result);

                List<ExpectedRankFeatureDoc> expectedFinalResults = List.of(new ExpectedRankFeatureDoc(1, 2, 110.0F, "ranked_1"));
                assertFinalResults(finalResults[0], expectedFinalResults);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    public void testRankFeatureCollectOnlyRankWindowSizeFeatures() {
        // request params used within SearchSourceBuilder and *RankContext classes
        final int rankWindowSize = 2;
        AtomicBoolean phaseDone = new AtomicBoolean(false);
        final ScoreDoc[][] finalResults = new ScoreDoc[1][1];

        // build the appropriate RankBuilder
        RankBuilder rankBuilder = rankBuilder(
            rankWindowSize,
            defaultQueryPhaseRankShardContext(Collections.emptyList(), rankWindowSize),
            defaultQueryPhaseRankCoordinatorContext(rankWindowSize),
            defaultRankFeaturePhaseRankShardContext(DEFAULT_FIELD),
            defaultRankFeaturePhaseRankCoordinatorContext(DEFAULT_SIZE, DEFAULT_FROM, rankWindowSize)
        );
        // create a SearchSource to attach to the request
        SearchSourceBuilder searchSourceBuilder = searchSourceWithRankBuilder(rankBuilder);

        SearchPhaseController controller = searchPhaseController();
        SearchShardTarget shard1Target = new SearchShardTarget("node0", new ShardId("test", "na", 0), null);
        SearchShardTarget shard2Target = new SearchShardTarget("node1", new ShardId("test", "na", 1), null);
        SearchShardTarget shard3Target = new SearchShardTarget("node2", new ShardId("test", "na", 2), null);

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(3);
        mockSearchPhaseContext.getRequest().source(searchSourceBuilder);
        try (SearchPhaseResults<SearchPhaseResult> results = searchPhaseResults(controller, mockSearchPhaseContext)) {
            // generate the QuerySearchResults that the RankFeaturePhase would have received from QueryPhase
            // here we have 3 results, with doc ids 1, and (11, 2) found on shards 0 and 1 respectively
            final ShardSearchContextId ctxShard1 = new ShardSearchContextId(UUIDs.base64UUID(), 123);
            final ShardSearchContextId ctxShard2 = new ShardSearchContextId(UUIDs.base64UUID(), 456);
            final ShardSearchContextId ctxShard3 = new ShardSearchContextId(UUIDs.base64UUID(), 789);

            QuerySearchResult queryResultShard1 = new QuerySearchResult(ctxShard1, shard1Target, null);
            QuerySearchResult queryResultShard2 = new QuerySearchResult(ctxShard2, shard2Target, null);
            QuerySearchResult queryResultShard3 = new QuerySearchResult(ctxShard3, shard2Target, null);

            try {
                queryResultShard1.setShardIndex(shard1Target.getShardId().getId());
                queryResultShard2.setShardIndex(shard2Target.getShardId().getId());
                queryResultShard3.setShardIndex(shard3Target.getShardId().getId());

                final int shard1Results = randomIntBetween(1, 100);
                final ScoreDoc[] shard1Docs = new ScoreDoc[] { new ScoreDoc(1, 10.0F) };
                populateQuerySearchResult(queryResultShard1, shard1Results, shard1Docs);

                final int shard2Results = randomIntBetween(1, 100);
                final ScoreDoc[] shard2Docs = new ScoreDoc[] { new ScoreDoc(11, 100.0F), new ScoreDoc(2, 9.0F) };
                populateQuerySearchResult(queryResultShard2, shard2Results, shard2Docs);

                final int shard3Results = 0;
                final ScoreDoc[] shard3Docs = new ScoreDoc[0];
                populateQuerySearchResult(queryResultShard3, shard3Results, shard3Docs);

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
                        final ActionListener<RankFeatureResult> listener
                    ) {
                        RankFeatureResult rankFeatureResult = new RankFeatureResult();
                        // make sure to match the context id generated above, otherwise we throw
                        // first shard
                        if (request.contextId().getId() == 123 && Arrays.equals(request.getDocIds(), new int[] { 1 })) {
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard1Target,
                                shard1Results,
                                shard1Docs
                            );
                            listener.onResponse(rankFeatureResult);
                        } else if (request.contextId().getId() == 456 && Arrays.equals(request.getDocIds(), new int[] { 11 })) {
                            // second shard
                            buildRankFeatureResult(
                                mockSearchPhaseContext.getRequest().source().rankBuilder(),
                                rankFeatureResult,
                                shard2Target,
                                shard2Results,
                                new ScoreDoc[] { shard2Docs[0] }
                            );
                            listener.onResponse(rankFeatureResult);
                        } else {
                            listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                        }
                    }
                };
            } finally {
                queryResultShard1.decRef();
                queryResultShard2.decRef();
                queryResultShard3.decRef();
            }
            RankFeaturePhase rankFeaturePhase = rankFeaturePhase(results, mockSearchPhaseContext, finalResults, phaseDone);
            try {
                rankFeaturePhase.run();
                mockSearchPhaseContext.assertNoFailure();
                assertTrue(mockSearchPhaseContext.failures.isEmpty());
                assertTrue(phaseDone.get());
                SearchPhaseResults<SearchPhaseResult> rankPhaseResults = rankFeaturePhase.rankPhaseResults;
                assertNotNull(rankPhaseResults.getAtomicArray());
                assertEquals(3, rankPhaseResults.getAtomicArray().length());
                // one result is null
                assertEquals(2, rankPhaseResults.getSuccessfulResults().count());

                SearchPhaseResult shard1Result = rankPhaseResults.getAtomicArray().get(0);
                List<ExpectedRankFeatureDoc> expectedShardResults = List.of(new ExpectedRankFeatureDoc(1, 1, 110.0F, "ranked_1"));
                assertShardResults(shard1Result, expectedShardResults);

                SearchPhaseResult shard2Result = rankPhaseResults.getAtomicArray().get(1);
                List<ExpectedRankFeatureDoc> expectedShard2Results = List.of(new ExpectedRankFeatureDoc(11, 1, 200.0F, "ranked_11"));
                assertShardResults(shard2Result, expectedShard2Results);

                SearchPhaseResult shard3Result = rankPhaseResults.getAtomicArray().get(2);
                assertNull(shard3Result);

                List<ExpectedRankFeatureDoc> expectedFinalResults = List.of(
                    new ExpectedRankFeatureDoc(11, 1, 200.0F, "ranked_11"),
                    new ExpectedRankFeatureDoc(1, 2, 110.0F, "ranked_1")
                );
                assertFinalResults(finalResults[0], expectedFinalResults);
            } finally {
                rankFeaturePhase.rankPhaseResults.close();
            }
        } finally {
            if (mockSearchPhaseContext.searchResponse.get() != null) {
                mockSearchPhaseContext.searchResponse.get().decRef();
            }
        }
    }

    private RankFeaturePhaseRankCoordinatorContext defaultRankFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize) {
        return new RankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize) {

            @Override
            protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                // no-op
                // this one is handled directly in rankGlobalResults to create a RankFeatureDoc
                // and avoid modifying in-place the ScoreDoc's rank
            }

            @Override
            public void computeRankScoresForGlobalResults(
                List<RankFeatureResult> rankSearchResults,
                ActionListener<RankFeatureDoc[]> rankListener
            ) {
                List<RankFeatureDoc> features = new ArrayList<>();
                for (RankFeatureResult rankFeatureResult : rankSearchResults) {
                    RankFeatureShardResult shardResult = rankFeatureResult.shardResult();
                    features.addAll(Arrays.stream(shardResult.rankFeatureDocs).toList());
                }
                rankListener.onResponse(features.toArray(new RankFeatureDoc[0]));
            }

            @Override
            public RankFeatureDoc[] rankAndPaginate(RankFeatureDoc[] rankFeatureDocs) {
                Arrays.sort(rankFeatureDocs, Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed());
                RankFeatureDoc[] topResults = new RankFeatureDoc[Math.max(0, Math.min(size, rankFeatureDocs.length - from))];
                // perform pagination
                for (int rank = 0; rank < topResults.length; ++rank) {
                    RankFeatureDoc rfd = rankFeatureDocs[from + rank];
                    topResults[rank] = new RankFeatureDoc(rfd.doc, rfd.score, rfd.shardIndex);
                    topResults[rank].rank = from + rank + 1;
                }
                return topResults;
            }
        };
    }

    private QueryPhaseRankCoordinatorContext negatingScoresQueryFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize) {
        return new QueryPhaseRankCoordinatorContext(rankWindowSize) {
            @Override
            public ScoreDoc[] rankQueryPhaseResults(
                List<QuerySearchResult> rankSearchResults,
                SearchPhaseController.TopDocsStats topDocsStats
            ) {
                List<ScoreDoc> docScores = new ArrayList<>();
                for (QuerySearchResult phaseResults : rankSearchResults) {
                    docScores.addAll(Arrays.asList(phaseResults.topDocs().topDocs.scoreDocs));
                }
                ScoreDoc[] sortedDocs = docScores.toArray(new ScoreDoc[0]);
                // negating scores
                Arrays.stream(sortedDocs).forEach(doc -> doc.score *= -1);

                Arrays.sort(sortedDocs, Comparator.comparing((ScoreDoc doc) -> doc.score).reversed());
                sortedDocs = Arrays.stream(sortedDocs).limit(rankWindowSize).toArray(ScoreDoc[]::new);
                RankFeatureDoc[] topResults = new RankFeatureDoc[Math.max(0, Math.min(size, sortedDocs.length - from))];
                // perform pagination
                for (int rank = 0; rank < topResults.length; ++rank) {
                    ScoreDoc base = sortedDocs[from + rank];
                    topResults[rank] = new RankFeatureDoc(base.doc, base.score, base.shardIndex);
                    topResults[rank].rank = from + rank + 1;
                }
                topDocsStats.fetchHits = topResults.length;
                return topResults;
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
            public boolean isCompoundBuilder() {
                return true;
            }

            @Override
            public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
                // no-op
                return baseExplanation;
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

    private SearchPhaseResults<SearchPhaseResult> searchPhaseResults(
        SearchPhaseController controller,
        MockSearchPhaseContext mockSearchPhaseContext
    ) {
        return controller.newSearchPhaseResults(
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            () -> false,
            SearchProgressListener.NOOP,
            mockSearchPhaseContext.getRequest(),
            mockSearchPhaseContext.numShards,
            exc -> {}
        );
    }

    private void buildRankFeatureResult(
        RankBuilder shardRankBuilder,
        RankFeatureResult rankFeatureResult,
        SearchShardTarget shardTarget,
        int totalHits,
        ScoreDoc[] scoreDocs
    ) {
        rankFeatureResult.setSearchShardTarget(shardTarget);
        // these are the SearchHits generated by the FetchFieldPhase processor
        SearchHit[] searchHits = new SearchHit[scoreDocs.length];
        float maxScore = Float.MIN_VALUE;
        for (int i = 0; i < searchHits.length; i++) {
            searchHits[i] = SearchHit.unpooled(scoreDocs[i].doc);
            searchHits[i].shard(shardTarget);
            searchHits[i].score(scoreDocs[i].score);
            searchHits[i].setDocumentField(DEFAULT_FIELD, new DocumentField(DEFAULT_FIELD, Collections.singletonList(scoreDocs[i].doc)));
            if (scoreDocs[i].score > maxScore) {
                maxScore = scoreDocs[i].score;
            }
        }
        SearchHits hits = null;
        try {
            hits = SearchHits.unpooled(searchHits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);
            // construct the appropriate RankFeatureDoc objects based on the rank builder
            RankFeaturePhaseRankShardContext rankFeaturePhaseRankShardContext = shardRankBuilder.buildRankFeaturePhaseShardContext();
            RankFeatureShardResult rankShardResult = (RankFeatureShardResult) rankFeaturePhaseRankShardContext.buildRankFeatureShardResult(
                hits,
                shardTarget.getShardId().id()
            );
            rankFeatureResult.shardResult(rankShardResult);
        } finally {
            if (hits != null) {
                hits.decRef();
            }
        }
    }

    private void populateQuerySearchResult(QuerySearchResult queryResult, int totalHits, ScoreDoc[] scoreDocs) {
        // this would have been populated during the QueryPhase by the appropriate QueryPhaseShardContext
        float maxScore = Float.MIN_VALUE;
        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            if (scoreDocs[i].score > maxScore) {
                maxScore = scoreDocs[i].score;
            }
            rankFeatureDocs[i] = new RankFeatureDoc(scoreDocs[i].doc, scoreDocs[i].score, scoreDocs[i].shardIndex);
        }
        queryResult.setRankShardResult(new RankFeatureShardResult(rankFeatureDocs));
        queryResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs),
                maxScore

            ),
            new DocValueFormat[0]
        );
        queryResult.size(totalHits);
    }

    private RankFeaturePhase rankFeaturePhase(
        SearchPhaseResults<SearchPhaseResult> results,
        MockSearchPhaseContext mockSearchPhaseContext,
        ScoreDoc[][] finalResults,
        AtomicBoolean phaseDone
    ) {
        // override the RankFeaturePhase to skip moving to next phase
        return new RankFeaturePhase(results, null, mockSearchPhaseContext, null) {
            @Override
            public void moveToNextPhase(
                SearchPhaseResults<SearchPhaseResult> phaseResults,
                SearchPhaseController.ReducedQueryPhase reducedQueryPhase
            ) {
                // this is called after the RankFeaturePhaseCoordinatorContext has been executed
                phaseDone.set(true);
                finalResults[0] = reducedQueryPhase.sortedTopDocs().scoreDocs();
                logger.debug("Skipping moving to next phase");
            }
        };
    }

    private void assertRankFeatureResults(RankFeatureShardResult rankFeatureShardResult, List<ExpectedRankFeatureDoc> expectedResults) {
        assertEquals(expectedResults.size(), rankFeatureShardResult.rankFeatureDocs.length);
        for (int i = 0; i < expectedResults.size(); i++) {
            ExpectedRankFeatureDoc expected = expectedResults.get(i);
            RankFeatureDoc actual = rankFeatureShardResult.rankFeatureDocs[i];
            assertEquals(expected.doc, actual.doc);
            assertEquals(expected.rank, actual.rank);
            assertEquals(expected.score, actual.score, 10E-5);
            assertEquals(expected.featureData, actual.featureData);
        }
    }

    private void assertFinalResults(ScoreDoc[] finalResults, List<ExpectedRankFeatureDoc> expectedResults) {
        assertEquals(expectedResults.size(), finalResults.length);
        for (int i = 0; i < expectedResults.size(); i++) {
            ExpectedRankFeatureDoc expected = expectedResults.get(i);
            RankFeatureDoc actual = (RankFeatureDoc) finalResults[i];
            assertEquals(expected.doc, actual.doc);
            assertEquals(expected.rank, actual.rank);
            assertEquals(expected.score, actual.score, 10E-5);
        }
    }

    private void assertShardResults(SearchPhaseResult shardResult, List<ExpectedRankFeatureDoc> expectedShardResults) {
        assertTrue(shardResult instanceof RankFeatureResult);
        RankFeatureResult rankResult = (RankFeatureResult) shardResult;
        assertNotNull(rankResult.rankFeatureResult());
        assertNull(rankResult.queryResult());
        assertNotNull(rankResult.rankFeatureResult().shardResult());
        RankFeatureShardResult rankFeatureShardResult = rankResult.rankFeatureResult().shardResult();
        assertRankFeatureResults(rankFeatureShardResult, expectedShardResults);
    }
}

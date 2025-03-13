/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.SequenceCriterion;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceMatcher;
import org.elasticsearch.xpack.eql.execution.sequence.TumblingWindow;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.xpack.eql.EqlTestUtils.booleanArrayOf;

public class PITAwareQueryClientTests extends ESTestCase {

    private final List<HitExtractor> keyExtractors = emptyList();
    private static final QueryBuilder[] FILTERS = new QueryBuilder[] {
        rangeQuery("some_timestamp_field").gte("2023-12-07"),
        termQuery("tier", "hot"),
        idsQuery().addIds("1", "2", "3") };
    private static final String[] INDICES = new String[] { "test1", "test2", "test3" };

    public void testQueryFilterUsedInPitAndSearches() {
        try (var threadPool = createThreadPool()) {
            final var filter = frequently() ? randomFrom(FILTERS) : null;
            int stages = randomIntBetween(2, 5);
            final var esClient = new ESMockClient(threadPool, filter, stages);

            EqlConfiguration eqlConfiguration = new EqlConfiguration(
                INDICES,
                org.elasticsearch.xpack.ql.util.DateUtils.UTC,
                "nobody",
                "cluster",
                filter,
                emptyMap(),
                null,
                TimeValue.timeValueSeconds(30),
                null,
                123,
                1,
                randomBoolean(),
                randomBoolean(),
                "",
                new TaskId("test", 123),
                new EqlSearchTask(
                    randomLong(),
                    "transport",
                    EqlSearchAction.NAME,
                    "",
                    null,
                    emptyMap(),
                    emptyMap(),
                    new AsyncExecutionId("", new TaskId(randomAlphaOfLength(10), 1)),
                    TimeValue.timeValueDays(5)
                )
            );
            IndexResolver indexResolver = new IndexResolver(esClient, "cluster", DefaultDataTypeRegistry.INSTANCE, () -> emptySet());
            CircuitBreaker cb = new NoopCircuitBreaker("testcb");
            EqlSession eqlSession = new EqlSession(
                esClient,
                eqlConfiguration,
                indexResolver,
                new PreAnalyzer(),
                new PostAnalyzer(),
                new EqlFunctionRegistry(),
                new Verifier(new Metrics()),
                new Optimizer(),
                new Planner(),
                cb
            );
            QueryClient eqlClient = new PITAwareQueryClient(eqlSession) {
                @Override
                public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
                    List<List<SearchHit>> searchHits = new ArrayList<>();
                    for (List<HitReference> ref : refs) {
                        List<SearchHit> hits = new ArrayList<>(ref.size());
                        for (HitReference hitRef : ref) {
                            hits.add(SearchHit.unpooled(-1, hitRef.id()));
                        }
                        searchHits.add(hits);
                    }
                    listener.onResponse(searchHits);
                }
            };

            List<SequenceCriterion> criteria = new ArrayList<>(stages);
            for (int i = 0; i < stages; i++) {
                final int j = i;
                criteria.add(
                    new SequenceCriterion(
                        i,
                        new BoxedQueryRequest(
                            () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()).terminateAfter(j),
                            "@timestamp",
                            emptyList(),
                            emptySet()
                        ),
                        keyExtractors,
                        TimestampExtractor.INSTANCE,
                        null,
                        ImplicitTiebreakerHitExtractor.INSTANCE,
                        false,
                        false
                    )
                );
            }

            SequenceMatcher matcher = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, booleanArrayOf(stages, false), cb);
            TumblingWindow window = new TumblingWindow(
                eqlClient,
                criteria,
                null,
                matcher,
                Collections.emptyList(),
                randomBoolean(),
                randomBoolean()
            );
            window.execute(wrap(response -> {
                // do nothing, we don't care about the query results
            }, ex -> { fail("Shouldn't have failed"); }));
        }
    }

    /**
     *  This class is used by {@code PITFailureTests.testPitCloseOnFailure} method
     *  to test that PIT close is never (wrongly) invoked if PIT open failed.
     */
    private class ESMockClient extends NoOpClient {
        private final QueryBuilder filter;
        private final BytesReference pitId = new BytesArray("test_pit_id");
        private boolean openedPIT = false;
        private int searchRequestsRemainingCount;

        ESMockClient(ThreadPool threadPool, QueryBuilder filter, int stages) {
            super(threadPool);
            this.filter = filter;
            this.searchRequestsRemainingCount = stages;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof OpenPointInTimeRequest openPIT) {
                assertFalse(openedPIT);
                assertEquals(filter, openPIT.indexFilter()); // check that the filter passed on to the eql query is used in opening the pit
                assertArrayEquals(INDICES, openPIT.indices()); // indices for opening pit should be the same as for the eql query itself

                openedPIT = true;
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(pitId, 1, 1, 0, 0);
                listener.onResponse((Response) response);
            } else if (request instanceof ClosePointInTimeRequest closePIT) {
                assertTrue(openedPIT);
                assertEquals(pitId, closePIT.getId());

                openedPIT = false;
                ClosePointInTimeResponse response = new ClosePointInTimeResponse(true, 1);
                listener.onResponse((Response) response);
            } else if (request instanceof SearchRequest searchRequest) {
                assertTrue(openedPIT);
                searchRequestsRemainingCount--;
                assertTrue(searchRequestsRemainingCount >= 0);

                assertEquals(pitId, searchRequest.source().pointInTimeBuilder().getEncodedId());
                assertEquals(0, searchRequest.indices().length); // no indices set in the search request
                assertEquals(1, searchRequest.source().subSearches().size());

                BoolQueryBuilder actualQuery = (BoolQueryBuilder) searchRequest.source().subSearches().get(0).getQueryBuilder();
                assertEquals(3, actualQuery.filter().size());
                assertTrue(actualQuery.filter().get(0) instanceof MatchAllQueryBuilder); // the match_all we used when building the criteria
                assertTrue(actualQuery.filter().get(1) instanceof RangeQueryBuilder);
                QueryBuilder expectedQuery = termsQuery("_index", INDICES); // indices should be used as a filter further on
                assertEquals(expectedQuery, actualQuery.filter().get(2));

                handleSearchRequest(listener, searchRequest);
            } else {
                super.doExecute(action, request, listener);
            }
        }

        @SuppressWarnings("unchecked")
        <Response extends ActionResponse> void handleSearchRequest(ActionListener<Response> listener, SearchRequest searchRequest) {
            int ordinal = searchRequest.source().terminateAfter();
            SearchHit searchHit = SearchHit.unpooled(ordinal, String.valueOf(ordinal));
            searchHit.sortValues(
                new SearchSortValues(new Long[] { (long) ordinal, 1L }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW })
            );

            SearchHits searchHits = SearchHits.unpooled(new SearchHit[] { searchHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 0.0f);
            SearchResponse response = new SearchResponse(
                searchHits,
                null,
                null,
                false,
                false,
                null,
                0,
                null,
                2,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                searchRequest.pointInTimeBuilder().getEncodedId()
            );

            ActionListener.respondAndRelease(listener, (Response) response);
        }
    }

    private static class TimestampExtractor implements HitExtractor {

        static final TimestampExtractor INSTANCE = new TimestampExtractor();

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public String hitName() {
            return null;
        }

        @Override
        public Timestamp extract(SearchHit hit) {
            return Timestamp.of(String.valueOf(hit.docId()));
        }
    }
}

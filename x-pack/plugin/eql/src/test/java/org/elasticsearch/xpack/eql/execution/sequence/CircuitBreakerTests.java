/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.Criterion;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.PITAwareQueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.Timestamp;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.elasticsearch.xpack.ql.util.Holder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.eql.plugin.EqlPlugin.CIRCUIT_BREAKER_LIMIT;
import static org.elasticsearch.xpack.eql.plugin.EqlPlugin.CIRCUIT_BREAKER_NAME;
import static org.elasticsearch.xpack.eql.plugin.EqlPlugin.CIRCUIT_BREAKER_OVERHEAD;

public class CircuitBreakerTests extends ESTestCase {

    private static final TestCircuitBreaker CIRCUIT_BREAKER = new TestCircuitBreaker();

    private final List<HitExtractor> keyExtractors = emptyList();
    private final HitExtractor tsExtractor = TimestampExtractor.INSTANCE;
    private final HitExtractor implicitTbExtractor = ImplicitTiebreakerHitExtractor.INSTANCE;
    private final int stages = randomIntBetween(3, 10);

    static class TestQueryClient implements QueryClient {

        @Override
        public void query(QueryRequest r, ActionListener<SearchResponse> l) {
            int ordinal = r.searchSource().terminateAfter();
            SearchHit searchHit = new SearchHit(ordinal, String.valueOf(ordinal), null, null);
            searchHit.sortValues(
                new SearchSortValues(new Long[] { (long) ordinal, 1L }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW })
            );
            SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, Relation.EQUAL_TO), 0.0f);
            SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
            SearchResponse s = new SearchResponse(internal, null, 0, 1, 0, 0, null, Clusters.EMPTY);
            l.onResponse(s);
        }

        @Override
        public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
            List<List<SearchHit>> searchHits = new ArrayList<>();
            for (List<HitReference> ref : refs) {
                List<SearchHit> hits = new ArrayList<>(ref.size());
                for (HitReference hitRef : ref) {
                    hits.add(new SearchHit(-1, hitRef.id(), null, null));
                }
                searchHits.add(hits);
            }
            listener.onResponse(searchHits);
        }
    }

    public void testCircuitBreakerTumblingWindow() {
        QueryClient client = new TestQueryClient();
        List<Criterion<BoxedQueryRequest>> criteria = new ArrayList<>(stages);

        for (int i = 0; i < stages; i++) {
            final int j = i;
            criteria.add(
                new Criterion<>(
                    i,
                    new BoxedQueryRequest(
                        () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()).terminateAfter(j),
                        "@timestamp",
                        emptyList(),
                        emptySet()
                    ),
                    keyExtractors,
                    tsExtractor,
                    null,
                    implicitTbExtractor,
                    false
                )
            );
        }

        SequenceMatcher matcher = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, CIRCUIT_BREAKER);
        TumblingWindow window = new TumblingWindow(client, criteria, null, matcher);
        window.execute(wrap(p -> {}, ex -> { throw ExceptionsHelper.convertToRuntime(ex); }));

        CIRCUIT_BREAKER.startBreaking();

        Holder<Exception> thrownException = new Holder<>();
        window.execute(wrap(p -> { fail("this shouldn't have been called"); }, ex -> { thrownException.set(ex); }));
        assertNotNull(thrownException.get());
        assertEquals(CircuitBreakingException.class, thrownException.get().getClass());

        CIRCUIT_BREAKER.stopBreaking();

        window.execute(wrap(p -> {}, ex -> { throw ExceptionsHelper.convertToRuntime(ex); }));
    }

    public void testCircuitBreakerSequenceMatcher() {
        List<Tuple<KeyAndOrdinal, HitReference>> hits = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            hits.add(
                new Tuple<>(
                    new KeyAndOrdinal(new SequenceKey(i), new Ordinal(Timestamp.of(String.valueOf(i)), o -> 1, 0)),
                    new HitReference("index", i + "")
                )
            );
        }

        // Break on first iteration
        SequenceMatcher matcher1 = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, new EqlTestCircuitBreaker(10000));
        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> matcher1.match(0, hits));
        assertEquals("sequence_inflight", e.getMessage());

        // Break on second iteration
        SequenceMatcher matcher2 = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, new EqlTestCircuitBreaker(15000));
        matcher2.match(0, hits);
        e = expectThrows(CircuitBreakingException.class, () -> matcher2.match(0, hits));
        assertEquals("sequence_inflight", e.getMessage());

        // Break on 3rd iteration with clear() called in between
        SequenceMatcher matcher3 = new SequenceMatcher(stages, false, TimeValue.MINUS_ONE, null, new EqlTestCircuitBreaker(15000));
        matcher3.match(0, hits);
        matcher3.clear();
        matcher3.match(0, hits);
        e = expectThrows(CircuitBreakingException.class, () -> matcher3.match(0, hits));
        assertEquals("sequence_inflight", e.getMessage());
    }

    public void testMemoryClearedOnSuccessfulRequest() {
        assertMemoryCleared(2, SuccessfulESMockClient::new);
    }

    public void testMemoryClearedOnShardsException() {
        assertMemoryCleared(stages, FailureESMockClient::new);
    }

    private void assertMemoryCleared(int sequenceFiltersCount, BiFunction<CircuitBreaker, Integer, ESMockClient> esClientSupplier) {
        final int SEARCH_REQUESTS_EXPECTED_COUNT = 2;
        List<BreakerSettings> eqlBreakerSettings = Collections.singletonList(
            new BreakerSettings(
                CIRCUIT_BREAKER_NAME,
                CIRCUIT_BREAKER_LIMIT,
                CIRCUIT_BREAKER_OVERHEAD,
                CircuitBreaker.Type.MEMORY,
                CircuitBreaker.Durability.TRANSIENT
            )
        );
        try (
            CircuitBreakerService service = new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                eqlBreakerSettings,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            ESMockClient esClient = esClientSupplier.apply(service.getBreaker(CIRCUIT_BREAKER_NAME), SEARCH_REQUESTS_EXPECTED_COUNT);
        ) {
            CircuitBreaker eqlCircuitBreaker = service.getBreaker(CIRCUIT_BREAKER_NAME);
            EqlConfiguration eqlConfiguration = new EqlConfiguration(
                new String[] { "test" },
                org.elasticsearch.xpack.ql.util.DateUtils.UTC,
                "nobody",
                "cluster",
                null,
                emptyMap(),
                null,
                TimeValue.timeValueSeconds(30),
                null,
                123,
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
                ),
                x -> Collections.emptySet()
            );
            IndexResolver indexResolver = new IndexResolver(
                esClient,
                "cluster",
                DefaultDataTypeRegistry.INSTANCE,
                () -> { return emptySet(); }
            );
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
                eqlCircuitBreaker
            );
            QueryClient eqlClient = new PITAwareQueryClient(eqlSession);
            List<Criterion<BoxedQueryRequest>> criteria = new ArrayList<>(sequenceFiltersCount);

            for (int i = 0; i < sequenceFiltersCount; i++) {
                final int j = i;
                criteria.add(
                    new Criterion<>(
                        i,
                        new BoxedQueryRequest(
                            () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()).terminateAfter(j),
                            "@timestamp",
                            emptyList(),
                            emptySet()
                        ),
                        keyExtractors,
                        tsExtractor,
                        null,
                        implicitTbExtractor,
                        false
                    )
                );
            }

            SequenceMatcher matcher = new SequenceMatcher(sequenceFiltersCount, false, TimeValue.MINUS_ONE, null, eqlCircuitBreaker);
            TumblingWindow window = new TumblingWindow(eqlClient, criteria, null, matcher);
            window.execute(wrap(p -> {}, ex -> {}));

            assertTrue(esClient.searchRequestsRemainingCount() == 0); // ensure all the search requests have been asked for
            assertEquals(0, eqlCircuitBreaker.getTrippedCount()); // the circuit breaker shouldn't trip
            assertEquals(0, eqlCircuitBreaker.getUsed()); // the circuit breaker memory should be clear
        }
    }

    /**
     *  A type of internal Node client that deals with three types of requests: open PIT, close PIT and SearchRequest.
     *  This class is used by {@code CircuitBreakerTests#testMemoryClearedOnSuccessfulRequest()} and
     *  {@code CircuitBreakerTests#testMemoryClearedOnShardsException()} methods to test the circuit breaker memory usage
     *  in case of a successful sequence request but also for a failed sequence request.
     */
    private abstract class ESMockClient extends NoOpClient {
        private final AtomicLong pitContextCounter = new AtomicLong();
        protected final CircuitBreaker circuitBreaker;
        // private final String pitId;
        private int searchRequestsRemainingCount;
        private final String pitId = "test_pit_id";

        ESMockClient(CircuitBreaker circuitBreaker, int searchRequestsRemainingCount) {
            super(getTestName());
            this.circuitBreaker = circuitBreaker;
            this.searchRequestsRemainingCount = searchRequestsRemainingCount;
        }

        abstract <Response extends ActionResponse> void handleSearchRequest(ActionListener<Response> listener, SearchRequest searchRequest);

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof OpenPointInTimeRequest) {
                pitContextCounter.incrementAndGet();
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(pitId);
                listener.onResponse((Response) response);
            } else if (request instanceof ClosePointInTimeRequest) {
                ClosePointInTimeResponse response = new ClosePointInTimeResponse(true, 1);
                assert pitContextCounter.get() > 0;
                pitContextCounter.decrementAndGet();
                listener.onResponse((Response) response);
            } else if (request instanceof SearchRequest searchRequest) {
                searchRequestsRemainingCount--;
                assertTrue(searchRequestsRemainingCount >= 0);
                assertEquals(0, circuitBreaker.getTrippedCount());
                handleSearchRequest(listener, searchRequest);
                assert pitContextCounter.get() == 0;
            } else {
                super.doExecute(action, request, listener);
            }
        }

        int searchRequestsRemainingCount() {
            return searchRequestsRemainingCount;
        }
    }

    /*
     * For a successful sequence request, there will be two search requests expected
     */
    private class SuccessfulESMockClient extends ESMockClient {

        SuccessfulESMockClient(CircuitBreaker circuitBreaker, int expectedSearchRequestsCount) {
            super(circuitBreaker, expectedSearchRequestsCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        <Response extends ActionResponse> void handleSearchRequest(ActionListener<Response> listener, SearchRequest searchRequest) {
            int ordinal = searchRequest.source().terminateAfter();
            SearchHit searchHit = new SearchHit(ordinal, String.valueOf(ordinal), null, null);
            searchHit.sortValues(
                new SearchSortValues(new Long[] { (long) ordinal, 1L }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW })
            );

            SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, Relation.EQUAL_TO), 0.0f);
            SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
            SearchResponse response = new SearchResponse(
                internal,
                null,
                2,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                searchRequestsRemainingCount() == 1 ? searchRequest.pointInTimeBuilder().getEncodedId() : null
            );

            if (searchRequestsRemainingCount() == 1) {
                assertEquals(0, circuitBreaker.getUsed()); // this is the first response, so no memory usage so far
            } else {
                assertTrue(circuitBreaker.getUsed() > 0); // at this point the algorithm already started adding up to memory usage
            }

            listener.onResponse((Response) response);
        }
    }

    /*
     * For a failed sequence request, there'll be a successful first request followed by a second one that throws an exception
     * the first search request is designed to return valid results to allow the tumbling window to start the algorithm
     */
    private class FailureESMockClient extends ESMockClient {

        FailureESMockClient(CircuitBreaker circuitBreaker, int expectedSearchRequestsCount) {
            super(circuitBreaker, expectedSearchRequestsCount);
        }

        @SuppressWarnings("unchecked")
        @Override
        <Response extends ActionResponse> void handleSearchRequest(ActionListener<Response> listener, SearchRequest searchRequest) {
            // this is the first response, so no memory usage so far
            if (searchRequestsRemainingCount() == 1) {
                assertEquals(0, circuitBreaker.getUsed());

                int ordinal = searchRequest.source().terminateAfter();
                SearchHit searchHit = new SearchHit(ordinal, String.valueOf(ordinal), null, null);
                searchHit.sortValues(
                    new SearchSortValues(new Long[] { (long) ordinal, 1L }, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW })
                );
                SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, Relation.EQUAL_TO), 0.0f);
                SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
                SearchResponse response = new SearchResponse(
                    internal,
                    null,
                    2,
                    0,
                    0,
                    0,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY,
                    searchRequest.pointInTimeBuilder().getEncodedId()
                );
                listener.onResponse((Response) response);
            } else {
                assertTrue(circuitBreaker.getUsed() > 0); // at this point the algorithm already started adding up to memory usage
                ShardSearchFailure[] failures = new ShardSearchFailure[] {
                    new ShardSearchFailure(
                        new ParsingException(1, 2, "foobar", null),
                        new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null)
                    ) };

                if (randomBoolean()) {
                    // simulate an all shards exception - SearchPhaseExecutionException
                    listener.onFailure(new SearchPhaseExecutionException("search", "all shards failed", failures));
                } else {
                    // or a partial shard failure
                    SearchResponse response = new SearchResponse(
                        new InternalSearchResponse(
                            new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
                            null,
                            new Suggest(Collections.emptyList()),
                            new SearchProfileResults(Collections.emptyMap()),
                            false,
                            false,
                            1
                        ),
                        null,
                        2,
                        1,
                        0,
                        0,
                        failures,
                        SearchResponse.Clusters.EMPTY,
                        null
                    );

                    // this should still be caught and the exception handled properly and circuit breaker cleared
                    listener.onResponse((Response) response);
                }
            }
        }

    }

    private static class EqlTestCircuitBreaker extends NoopCircuitBreaker {

        private final long limitInBytes;
        private long ramBytesUsed = 0;

        private EqlTestCircuitBreaker(long limitInBytes) {
            super("eql_test");
            this.limitInBytes = limitInBytes;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            ramBytesUsed += bytes;
            if (ramBytesUsed > limitInBytes) {
                throw new CircuitBreakingException(label, getDurability());
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            ramBytesUsed += bytes;
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

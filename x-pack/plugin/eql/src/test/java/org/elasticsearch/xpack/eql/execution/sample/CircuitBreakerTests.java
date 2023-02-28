/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sample;

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
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.execution.assembler.SampleCriterion;
import org.elasticsearch.xpack.eql.execution.assembler.SampleQueryRequest;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.execution.search.PITAwareQueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.eql.execution.assembler.SampleQueryRequest.COMPOSITE_AGG_NAME;
import static org.elasticsearch.xpack.eql.execution.sample.SampleIterator.CB_STACK_SIZE_PRECISION;
import static org.elasticsearch.xpack.eql.plugin.EqlPlugin.CIRCUIT_BREAKER_NAME;

public class CircuitBreakerTests extends ESTestCase {

    private static final TestCircuitBreaker CIRCUIT_BREAKER = new TestCircuitBreaker();

    public void testCircuitBreakerOnStackPush() {
        SampleIterator iterator = new SampleIterator(new QueryClient() {
            @Override
            public void query(QueryRequest r, ActionListener<SearchResponse> l) {}

            @Override
            public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {}
        }, mockCriteria(), randomIntBetween(10, 500), new Limit(1000, 0), CIRCUIT_BREAKER, 1);

        CIRCUIT_BREAKER.startBreaking();
        iterator.pushToStack(new SampleIterator.Page(CB_STACK_SIZE_PRECISION - 1));
        expectThrows(CircuitBreakingException.class, () -> iterator.pushToStack(new SampleIterator.Page(1)));
    }

    public void testMemoryCleared() {
        testMemoryCleared(randomBoolean());
    }

    private void testMemoryCleared(boolean fail) {
        try (
            CircuitBreakerService service = new HierarchyCircuitBreakerService(
                Settings.EMPTY,
                Collections.singletonList(EqlTestUtils.circuitBreakerSettings(Settings.EMPTY)),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            ESMockClient esClient = new ESMockClient(service.getBreaker(CIRCUIT_BREAKER_NAME));
        ) {
            CircuitBreaker eqlCircuitBreaker = service.getBreaker(CIRCUIT_BREAKER_NAME);
            IndexResolver indexResolver = new IndexResolver(esClient, "cluster", DefaultDataTypeRegistry.INSTANCE, () -> emptySet());
            EqlSession eqlSession = new EqlSession(
                esClient,
                EqlTestUtils.randomConfiguration(),
                indexResolver,
                new PreAnalyzer(),
                new PostAnalyzer(),
                new EqlFunctionRegistry(),
                new Verifier(new Metrics()),
                new Optimizer(),
                new Planner(),
                eqlCircuitBreaker
            );

            QueryClient eqlClient = new PITAwareQueryClient(eqlSession) {
                @Override
                public void fetchHits(Iterable<List<HitReference>> refs, ActionListener<List<List<SearchHit>>> listener) {
                    if (fail) {
                        throw new IllegalStateException("Let the request fail");
                    }
                    List<List<SearchHit>> result = new ArrayList<>();
                    result.add(emptyList());
                    listener.onResponse(result);
                }
            };

            SampleIterator iterator = new SampleIterator(
                eqlClient,
                mockCriteria(),
                randomIntBetween(10, 500),
                new Limit(1000, 0),
                eqlCircuitBreaker,
                1
            );

            // unfortunately, mocking an actual result set it extremely complicated
            // so we have to simulate some execution steps manually
            // - add a sample manually to force the iterator to use the QueryClient
            iterator.samples.add(mockSample());
            // - and force the circuit breaker to run
            iterator.pushToStack(new SampleIterator.Page(CB_STACK_SIZE_PRECISION + 1));
            iterator.stack.clear();

            assertNotEquals(0, eqlCircuitBreaker.getUsed());

            iterator.execute(wrap(p -> {
                if (fail) {
                    fail();
                }
            }, ex -> {
                if (fail == false) {
                    fail();
                }
            }));

            assertEquals(0, eqlCircuitBreaker.getTrippedCount()); // the circuit breaker shouldn't trip
            assertEquals(0, eqlCircuitBreaker.getUsed()); // the circuit breaker memory should be clear
        }
    }

    private Sample mockSample() {
        List<SearchHit> searchHits = new ArrayList<>();
        searchHits.add(new SearchHit(1, String.valueOf(1)));
        searchHits.add(new SearchHit(2, String.valueOf(2)));
        return new Sample(new SequenceKey(randomAlphaOfLength(10)), searchHits);
    }

    private SampleQueryRequest mockQueryRequest() {
        return new SampleQueryRequest(
            () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()).terminateAfter(1000),
            List.of("foo", "bar"),
            List.of(new FieldAttribute(Source.EMPTY, "foo", new EsField("foo", DataTypes.KEYWORD, null, false))),
            randomIntBetween(10, 500)
        );
    }

    private List<SampleCriterion> mockCriteria() {
        return List.of(new SampleCriterion(mockQueryRequest(), mockQueryRequest(), mockQueryRequest(), emptyList(), emptyList()));
    }

    private class ESMockClient extends NoOpClient {
        protected final CircuitBreaker circuitBreaker;
        private final String pitId = "test_pit_id";

        ESMockClient(CircuitBreaker circuitBreaker) {
            super(getTestName());
            this.circuitBreaker = circuitBreaker;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof OpenPointInTimeRequest) {
                OpenPointInTimeResponse response = new OpenPointInTimeResponse(pitId);
                listener.onResponse((Response) response);
            } else if (request instanceof ClosePointInTimeRequest) {
                ClosePointInTimeResponse response = new ClosePointInTimeResponse(true, 1);
                listener.onResponse((Response) response);
            } else if (request instanceof SearchRequest searchRequest) {
                handleSearchRequest(listener, searchRequest);
            } else {
                super.doExecute(action, request, listener);
            }
        }

        @SuppressWarnings("unchecked")
        <Response extends ActionResponse> void handleSearchRequest(ActionListener<Response> listener, SearchRequest searchRequest) {
            Aggregations aggs = new Aggregations(List.of(newInternalComposite()));

            SearchResponseSections internal = new SearchResponseSections(null, aggs, null, false, false, null, 0);
            SearchResponse response = new SearchResponse(
                internal,
                null,
                2,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                Clusters.EMPTY,
                searchRequest.pointInTimeBuilder().getEncodedId()
            );

            listener.onResponse((Response) response);
        }

        private InternalComposite newInternalComposite() {
            // InternalComposite(name, size, ...) is not public
            try {
                return new InternalComposite(new StreamInput() {
                    @Override
                    public byte readByte() throws IOException {
                        return 0;
                    }

                    @Override
                    public void readBytes(byte[] b, int offset, int len) throws IOException {

                    }

                    @Override
                    public void close() throws IOException {

                    }

                    @Override
                    public int available() throws IOException {
                        return 0;
                    }

                    @Override
                    protected void ensureCanReadBytes(int length) throws EOFException {

                    }

                    @Override
                    public int read() throws IOException {
                        return 0;
                    }

                    @Override
                    public int readVInt() throws IOException {
                        return 0;
                    }

                    @Override
                    public String readString() throws IOException {
                        return COMPOSITE_AGG_NAME; // we need it as aggs name, see SampleIterator.queryForCompositeAggPage()
                    }

                    @Override
                    public List<String> readStringList() throws IOException {
                        return emptyList();
                    }

                    @Override
                    public Map<String, Object> readMap() throws IOException {
                        return emptyMap();
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

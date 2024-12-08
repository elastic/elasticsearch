/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
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
import org.elasticsearch.xpack.eql.execution.search.PITAwareQueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryClient;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.eql.EqlTestUtils.booleanArrayOf;

public class PITFailureTests extends ESTestCase {

    public static final String PIT_EXCEPTION_MESSAGE = "test - PIT open did not succeed";
    private final List<HitExtractor> keyExtractors = emptyList();

    public void testHandlingPitFailure() {
        try (var threadPool = createThreadPool()) {
            final var esClient = new ESMockClient(threadPool);

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
            QueryClient eqlClient = new PITAwareQueryClient(eqlSession);
            List<SequenceCriterion> criteria = new ArrayList<>();
            criteria.add(
                new SequenceCriterion(
                    0,
                    new BoxedQueryRequest(
                        () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()).terminateAfter(0),
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

            SequenceMatcher matcher = new SequenceMatcher(1, false, TimeValue.MINUS_ONE, null, booleanArrayOf(1, false), cb);
            TumblingWindow window = new TumblingWindow(
                eqlClient,
                criteria,
                null,
                matcher,
                Collections.emptyList(),
                randomBoolean(),
                randomBoolean()
            );
            window.execute(
                wrap(
                    p -> { fail("Search succeeded despite PIT failure"); },
                    ex -> { assertEquals(PIT_EXCEPTION_MESSAGE, ex.getMessage()); }
                )
            );
        }
    }

    /**
     *  This class is used by {@code PITFailureTests.testPitCloseOnFailure} method
     *  to test that PIT close is never (wrongly) invoked if PIT open failed.
     */
    private class ESMockClient extends NoOpClient {

        ESMockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof OpenPointInTimeRequest) {
                shardFailure(listener);
            } else if (request instanceof ClosePointInTimeRequest) {
                fail("Request for PIT close, despite PIT open did not succeed");
            } else {
                super.doExecute(action, request, listener);
            }
        }

        @SuppressWarnings("unchecked")
        <Response extends ActionResponse> void shardFailure(ActionListener<Response> listener) {
            ShardSearchFailure[] failures = new ShardSearchFailure[] {
                new ShardSearchFailure(
                    new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null)
                ) };

            // simulate a shard failure
            listener.onFailure(new SearchPhaseExecutionException("search", PIT_EXCEPTION_MESSAGE, failures));
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

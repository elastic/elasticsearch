/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchTask;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.eql.util.SearchHitUtils;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

public class CrossProjectQueryClientTests extends ESTestCase {

    public void testEventSearchReplaysTheOriginalPatternIncludingExclusions() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            BasicQueryClient queryClient = new BasicQueryClient(session(client));
            queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), ActionListener.noop());
            assertPattern(client.eventSearch.get());
        }
    }

    public void testSequencePitReplaysTheOriginalPatternIncludingExclusions() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            PITAwareQueryClient queryClient = new PITAwareQueryClient(session(client));
            queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), ActionListener.noop());
            assertPattern(client.pit.get());
        }
    }

    public void testAnEmptyOriginalPatternOpensThePitOnAllIndices() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            PITAwareQueryClient queryClient = new PITAwareQueryClient(session(client, new String[0]));
            queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), ActionListener.noop());
            assertThat(client.pit.get().indices(), arrayContaining(Metadata.ALL));
        }
    }

    public void testAnEmptyOriginalPatternSearchesAllIndices() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            BasicQueryClient queryClient = new BasicQueryClient(session(client, new String[0]));
            queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), ActionListener.noop());
            assertThat(client.eventSearch.get().indices(), arrayContaining(Metadata.ALL));
        }
    }

    public void testAMissingOriginalPatternKeepsTheRewrittenIndices() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            BasicQueryClient queryClient = new BasicQueryClient(session(client, null));
            queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), ActionListener.noop());
            assertThat(client.eventSearch.get().indices(), arrayContaining("logs-0001"));
        }
    }

    public void testEventSearchRejectsChangedAuthorizedScope() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            EqlSession session = session(client);
            session.configuration().targetProjects(EqlProjectScopeTests.scope("a", "id-a"));
            client.resolvedTargets = EqlProjectScopeTests.scope("b", "id-b");
            client.response = SearchResponseUtils.response().build();
            try {
                PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
                new BasicQueryClient(session).query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), future);
                expectThrows(EqlIllegalArgumentException.class, future::actionGet);
                assertSame(client.resolvedTargets, client.eventSearch.get().getResolvedTargetProjects());
            } finally {
                client.response.decRef();
            }
        }
    }

    public void testPitIsClosedWhenAuthorizedScopeChanges() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            EqlSession session = session(client);
            session.configuration().targetProjects(EqlProjectScopeTests.scope("a", "id-a"));
            client.resolvedTargets = EqlProjectScopeTests.scope("b", "id-b");
            client.pitResponse = new OpenPointInTimeResponse(new BytesArray("pit"), 1, 1, 0, 0, SearchResponse.Clusters.EMPTY);
            PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
            new PITAwareQueryClient(session).query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), future);
            expectThrows(EqlIllegalArgumentException.class, future::actionGet);
            assertEquals(1, client.closedPits);
            assertNull(client.eventSearch.get());
        }
    }

    public void testSkippedPitProjectsSurviveSuccessfulSubsequentSearch() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            EqlSession session = session(client, new String[] { "logs-*" }, true);
            client.pitResponse = partialPitResponse();
            client.response = SearchResponseUtils.response().pointInTimeId(client.pitResponse.getPointInTimeId()).build();
            PITAwareQueryClient queryClient = new PITAwareQueryClient(session);
            try {
                PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
                queryClient.query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), future);
                assertSame(client.response, future.actionGet());
                assertTrue(client.response.getShardFailures().isEmpty());
                assertEquals(1, queryClient.searchFailures(client.response).size());
                assertEquals("linked", queryClient.searchFailures(client.response).getFirst().shard().getClusterAlias());
                Map<String, ShardSearchFailure> failures = new HashMap<>();
                SearchHitUtils.addShardFailures(failures, queryClient.searchFailures(client.response));
                assertEquals(1, failures.size());
            } finally {
                queryClient.close(ActionListener.noop());
                client.response.decRef();
            }
        }
    }

    public void testStrictQueryClosesAPartialPit() {
        try (var threadPool = createThreadPool()) {
            CapturingClient client = new CapturingClient(threadPool);
            client.pitResponse = partialPitResponse();
            PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
            new PITAwareQueryClient(session(client)).query(() -> SearchSourceBuilder.searchSource().query(matchAllQuery()), future);
            expectThrows(EqlIllegalArgumentException.class, future::actionGet);
            assertEquals(1, client.closedPits);
            assertNull(client.eventSearch.get());
        }
    }

    private static OpenPointInTimeResponse partialPitResponse() {
        SearchResponse.Cluster skipped = new SearchResponse.Cluster.Builder(new SearchResponse.Cluster("linked", "logs-*", true, null))
            .setStatus(SearchResponse.Cluster.Status.SKIPPED)
            .setFailures(List.of(new ShardSearchFailure(new IllegalStateException("linked PIT unavailable"))))
            .build();
        return new OpenPointInTimeResponse(
            new BytesArray("pit"),
            1,
            1,
            0,
            0,
            new SearchResponse.Clusters(Map.of("linked", skipped), false)
        );
    }

    private static void assertPattern(SearchRequest request) {
        assertThat(request.indices(), arrayContaining("logs-*", "-logs-test"));
        assertThat(request.getProjectRouting(), equalTo("_alias:_origin"));
        assertThat(request.indicesOptions().resolveCrossProjectIndexExpression(), equalTo(true));
    }

    private static void assertPattern(OpenPointInTimeRequest request) {
        assertThat(request.indices(), arrayContaining("logs-*", "-logs-test"));
        assertThat(request.getProjectRouting(), equalTo("_alias:_origin"));
        assertThat(request.indicesOptions().resolveCrossProjectIndexExpression(), equalTo(true));
        assertThat(request.allowPartialSearchResults(), equalTo(false));
    }

    private static EqlSession session(CapturingClient client) {
        return session(client, new String[] { "logs-*", "-logs-test" });
    }

    private static EqlSession session(CapturingClient client, String[] original) {
        return session(client, original, false);
    }

    private static EqlSession session(CapturingClient client, String[] original, boolean partial) {
        String[] rewritten = new String[] { "logs-0001" };
        ResolvedIndexExpressions resolved = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs-*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs-0001"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of()
                )
            ),
            null
        );
        IndicesOptions options = IndicesOptions.builder(IndicesOptions.strictExpandOpen())
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();
        EqlConfiguration configuration = new EqlConfiguration(
            rewritten,
            original,
            DateUtils.UTC,
            "nobody",
            "cluster",
            null,
            Map.of(),
            null,
            TimeValue.timeValueSeconds(30),
            options,
            123,
            1,
            partial,
            false,
            "_alias:_origin",
            "",
            new TaskId("test", 1),
            new EqlSearchTask(
                1L,
                "transport",
                EqlSearchAction.NAME,
                "",
                null,
                Map.of(),
                Map.of(),
                new AsyncExecutionId("", new TaskId("node", 1)),
                TimeValue.timeValueDays(5)
            ),
            true,
            resolved,
            null
        );
        return new EqlSession(
            client,
            configuration,
            new IndexResolver(client, "cluster", DefaultDataTypeRegistry.INSTANCE, () -> Set.of()),
            new PreAnalyzer(),
            new PostAnalyzer(),
            new EqlFunctionRegistry(),
            new Verifier(new Metrics()),
            new Optimizer(),
            new Planner(),
            NoopCircuitBreaker.INSTANCE
        );
    }

    private static final class CapturingClient extends NoOpClient {
        private final AtomicReference<SearchRequest> eventSearch = new AtomicReference<>();
        private final AtomicReference<OpenPointInTimeRequest> pit = new AtomicReference<>();
        private TargetProjects resolvedTargets;
        private SearchResponse response;
        private OpenPointInTimeResponse pitResponse;
        private int closedPits;

        private CapturingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked") // Each response below matches the captured request's action type.
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof SearchRequest searchRequest) {
                eventSearch.set(searchRequest);
                if (response != null) {
                    searchRequest.setResolvedTargetProjects(resolvedTargets);
                    listener.onResponse((Response) response);
                    return;
                }
            } else if (request instanceof OpenPointInTimeRequest open) {
                pit.set(open);
                if (pitResponse != null) {
                    open.setResolvedTargetProjects(resolvedTargets);
                    listener.onResponse((Response) pitResponse);
                    return;
                }
            } else if (request instanceof ClosePointInTimeRequest) {
                closedPits++;
                listener.onResponse((Response) new ClosePointInTimeResponse(true, 1));
                return;
            }
            listener.onFailure(new IllegalStateException("stop"));
        }
    }
}

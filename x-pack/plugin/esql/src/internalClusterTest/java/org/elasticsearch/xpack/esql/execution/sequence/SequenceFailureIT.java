/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.SequenceMatchDescription;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Native failure paths with a real snapshot and retained candidate pages. A transport gate targets only SEQUENCE's
 * second-stage search, after stage-one compute has transferred its candidates into the source operator.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceFailureIT extends AbstractSequenceIntegTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final int ANCHORS = 128;
    private static final String SOURCE_FAILURE = "injected sequence source search failure";

    /** Exercise the real REST status path while keeping transport interception for precise mutation timing. */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(SequenceBlockFactoryPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, "0b")
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, "0b")
            .build();
    }

    /** Tracks real compute blocks so failures cannot hide native page leaks behind successful breaker refunds. */
    public static class SequenceBlockFactoryPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    public void testParentCancellationWithOpenSnapshotAndRetainedCandidates() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request());
            gate.awaitStarted(future);
            assertSnapshotAndCandidatesLive(coordinator);
            List<TaskId> rootTasks = client(coordinator).admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks()
                .stream()
                .filter(task -> task.description() != null && task.description().contains("failure-a"))
                .map(task -> task.taskId())
                .toList();
            assertThat(rootTasks, hasSize(1));
            var cancellation = new CancelTasksRequest().setTargetTaskId(rootTasks.getFirst()).setReason("sequence failure test");
            cancellation.setWaitForCompletion(false);
            client(coordinator).execute(TransportCancelTasksAction.TYPE, cancellation).actionGet(TIMEOUT);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, TaskCancelledException.class), notNullValue());
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    public void testSourceSearchFailureCannotReturnPartialSequence() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            // Ordinary ES|QL may allow partial results. Missing sequence stages cannot be treated as a complete match.
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request().allowPartialResults(true));
            gate.awaitStarted(future);
            assertSnapshotAndCandidatesLive(coordinator);
            gate.release();
            Exception failure = expectFailure(future);
            var searchFailure = (SearchPhaseExecutionException) ExceptionsHelper.unwrap(failure, SearchPhaseExecutionException.class);
            assertThat(searchFailure, notNullValue());
            assertTrue(
                "the injected shard failure must be retained",
                java.util.Arrays.stream(searchFailure.shardFailures())
                    .anyMatch(shardFailure -> shardFailure.reason().contains(SOURCE_FAILURE))
            );
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    public void testSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete() throws Exception {
        assertSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete(false);
    }

    /** Full original sources must come from the same PIT as selection, even when live documents have changed or disappeared. */
    public void testStageSourcesRemainInSnapshotAfterUpdateAndDelete() throws Exception {
        assertSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete(true);
    }

    private void assertSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete(boolean withSource) throws Exception {
        populateIndices();
        Map<String, Object> originalAnchor = client().prepareGet("failure-a", "A" + (ANCHORS - 1)).get().getSource();
        Map<String, Object> originalCompletion = client().prepareGet("failure-b", "B").get().getSource();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            var query = withSource ? sourceRequest() : request();
            future = client(coordinator()).execute(EsqlQueryAction.INSTANCE, query);
            gate.awaitStarted(future);
            prepareIndex("failure-a").setId("A" + (ANCHORS - 1))
                .setSource("@timestamp", BASE.plusMillis(ANCHORS - 1).toString(), "key", "shared", "payload", "updated-anchor")
                .get();
            client().prepareDelete("failure-b", "B").get();
            prepareIndex("failure-b").setId("new-B")
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(ANCHORS + 1).toString(),
                    "key",
                    "shared",
                    "payload",
                    "new-completion",
                    "phase",
                    "target"
                )
                .get();
            refresh("failure-a", "failure-b");
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                // AbstractClient's future retains one response reference. Once this scope owns it, cleanup must
                // not release a producer reference that may still be live while its callback returns.
                future = null;
                if (withSource) {
                    List<List<Object>> rows = getValuesList(response);
                    assertThat(rows, hasSize(1));
                    assertEquals(List.of("shared", "anchor-" + (ANCHORS - 1), "completion"), rows.getFirst().subList(0, 3));
                    var description = SequenceMatchDescription.fromColumns(response.columns());
                    var decoded = description.decodeRow(rows.getFirst(), SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT);
                    assertEquals(
                        List.of(
                            new SequenceMatchDescription.Member(1, "failure-a", "A" + (ANCHORS - 1), originalAnchor),
                            new SequenceMatchDescription.Member(2, "failure-b", "B", originalCompletion)
                        ),
                        decoded.members()
                    );
                } else {
                    assertValues(response.values(), List.of(List.of("shared", "anchor-" + (ANCHORS - 1), "completion")));
                }
            }
            assertEquals("updated-anchor", client().prepareGet("failure-a", "A" + (ANCHORS - 1)).get().getSource().get("payload"));
            assertFalse(client().prepareGet("failure-b", "B").get().isExists());
        } finally {
            finish(gate, future);
        }
        assertReleased();
    }

    /** Cancelling while the second stage source fetch is parked must release the first member's already retained source. */
    public void testCancellationDuringStageSourceReplayReleasesSnapshotAndPayloads() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, stageSourceReplayRequest());
            gate.awaitStarted(future);
            assertTrue("payload or match allocations must be live before cancellation", requestBreaker(coordinator).getUsed() > 0);
            List<TaskId> rootTasks = client(coordinator).admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks()
                .stream()
                .filter(task -> task.description() != null && task.description().contains("failure-a"))
                .map(task -> task.taskId())
                .toList();
            assertThat(rootTasks, hasSize(1));
            var cancellation = new CancelTasksRequest().setTargetTaskId(rootTasks.getFirst())
                .setReason("sequence source cancellation test");
            cancellation.setWaitForCompletion(false);
            client(coordinator).execute(TransportCancelTasksAction.TYPE, cancellation).actionGet(TIMEOUT);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, TaskCancelledException.class), notNullValue());
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
        assertStageSourceReplayRecovery(coordinator);
    }

    public void testStageSourceSearchFailureCannotReturnPartialChain() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(true, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, stageSourceReplayRequest().allowPartialResults(true));
            gate.awaitStarted(future);
            assertTrue(requestBreaker(coordinator).getUsed() > 0);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, SearchPhaseExecutionException.class), notNullValue());
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
        assertStageSourceReplayRecovery(coordinator);
    }

    /** An unrelated definition added between stage reads must preserve completion on the original PIT. */
    public void testUnrelatedMappingChangeBetweenStageReadsCompletesAndFreshQueryRecovers() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, sourceRequest().allowPartialResults(true));
            gate.awaitStarted(future);
            assertSnapshotAndCandidatesLive(coordinator);
            for (String index : List.of("failure-a", "failure-b")) {
                assertAcked(
                    indicesAdmin().preparePutMapping(index).setSource(Map.of("runtime", Map.of("late_runtime", Map.of("type", "keyword"))))
                );
            }
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                future = null;
                assertFalse(response.isPartial());
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows, hasSize(1));
                assertEquals(List.of("shared", "anchor-" + (ANCHORS - 1), "completion"), rows.getFirst().subList(0, 3));
            }
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    /** A changed event-time definition is irrelevant when full replay needs only keys, predicates, payloads and source. */
    public void testUndemandedRuntimeTimestampChangeAfterSelectionPreservesFullReplay() throws Exception {
        assertUndemandedRuntimeTimestampMutation("timestamp", TimeValue.timeValueDays(1).millis());
    }

    /** Even a throwing deterministic timestamp must not be fetched implicitly after the match has been selected. */
    public void testUndemandedRuntimeTimestampFailureAfterSelectionPreservesFullReplay() throws Exception {
        assertUndemandedRuntimeTimestampMutation("failure", 0);
    }

    private void assertUndemandedRuntimeTimestampMutation(String script, long offset) throws Exception {
        populateMutationIndices();
        setRuntimeTimestamp("timestamp", 0);
        Map<String, Object> originalAnchor = client().prepareGet("failure-a", "A" + (ANCHORS - 1)).get().getSource();
        Map<String, Object> originalCompletion = client().prepareGet("failure-b", "B").get().getSource();
        String coordinator = coordinator();
        var gate = new SearchGate(false, true, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, sourceRequest().allowPartialResults(true));
            gate.awaitStarted(future);
            setRuntimeTimestamp(script, offset);
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                future = null;
                assertFalse(response.isPartial());
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows, hasSize(1));
                assertEquals(List.of("shared", "anchor-" + (ANCHORS - 1), "completion"), rows.getFirst().subList(0, 3));
                var description = SequenceMatchDescription.fromColumns(response.columns());
                assertEquals(
                    List.of(
                        new SequenceMatchDescription.Member(1, "failure-a", "A" + (ANCHORS - 1), originalAnchor),
                        new SequenceMatchDescription.Member(2, "failure-b", "B", originalCompletion)
                    ),
                    description.decodeRow(rows.getFirst(), SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT).members()
                );
            }
            assertIdentityPayloadRequest(gate, false);
        } finally {
            try {
                finish(gate, future);
            } finally {
                setRuntimeTimestamp("timestamp", 0);
            }
        }
        assertMutationResourcesReleased();
        assertRecovery(coordinator);
    }

    /** A genuinely demanded timestamp remains an ordinary mapped-value read and retains its loading failure. */
    public void testDemandedRuntimeTimestampFailureAfterSelectionCannotReturnPartialMatch() throws Exception {
        populateMutationIndices();
        setRuntimeTimestamp("timestamp", 0);
        String coordinator = coordinator();
        var gate = new SearchGate(false, true, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("""
                SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
                (FROM failure-a | KEEP key, payload, @timestamp),
                (FROM failure-b | WHERE phase=="target" | KEEP key, payload)
                | EVAL member2_payload = MEMBER(2, payload)
                | KEEP key, payload, member2_payload, @timestamp | LIMIT 1 | RETURN MATCHES
                """).allowPartialResults(true));
            gate.awaitStarted(future);
            assertTrue(mappedFields(gate.source).contains("@timestamp"));
            setRuntimeTimestamp("failure", 0);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, SearchPhaseExecutionException.class), notNullValue());
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("sequence mapped loading failure"));
            assertIdentityPayloadRequest(gate, true);
        } finally {
            try {
                finish(gate, future);
            } finally {
                setRuntimeTimestamp("timestamp", 0);
            }
        }
        assertMutationResourcesReleased();
        assertRecovery(coordinator);
    }

    /** Identity fetch cannot reapply either stage predicates, the request filter or event-time selection constraints. */
    public void testIdentityPayloadRequestOmitsSelectionOnlyDependencies() throws Exception {
        populateMutationIndices();
        var gate = new SearchGate(false, true, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator()).execute(
                EsqlQueryAction.INSTANCE,
                replayRequiredRequest().filter(org.elasticsearch.index.query.QueryBuilders.termQuery("raw_key", "shared"))
            );
            gate.awaitStarted(future);
            assertIdentityPayloadRequest(gate, false);
            assertEquals(3, mappedFields(gate.source).size());
            Map<String, Object> wrapper = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                Strings.toString(gate.source.query()),
                false
            );
            Map<?, ?> delegate = repeatabilityDelegate(wrapper);
            assertNotNull("the source authorization filter must retain its repeatability boundary", delegate);
            String query = effectiveQueryTree(wrapper).toString();
            for (String selectionOnly : List.of("@timestamp", "phase", "raw_key", "start", "target")) {
                assertFalse(
                    "identity query retained selection dependency [" + selectionOnly + "]: " + query,
                    query.contains(selectionOnly)
                );
            }
            assertThat(query, containsString("_index"));
            assertThat(query, containsString("failure-a"));
            assertThat(query, containsString(RankDocsQueryBuilder.NAME));
            assertThat(query, containsString("_doc="));
            assertThat(query, containsString("_shard="));
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                future = null;
                assertFalse(response.isPartial());
                assertEquals(List.of("shared", "ANCHOR-" + (ANCHORS - 1), "COMPLETION"), getValuesList(response).getFirst().subList(0, 3));
            }
        } finally {
            finish(gate, future);
        }
        assertMutationResourcesReleased();
    }

    /** A live key alias can invalidate full replay without turning a legitimate reconstruction limit into an internal error. */
    public void testKeyAliasChangeAfterSelectionReturnsControlledReconstructionFailure() throws Exception {
        assertAliasMutationFailsReconstruction("failure-a", "key", "changed_key", "raw_key", "correlation key no longer matches");
    }

    /** Full replay may lose a selected row after its predicate alias changes; the query must fail without partial output. */
    public void testPredicateAliasChangeAfterSelectionReturnsControlledReconstructionFailure() throws Exception {
        assertAliasMutationFailsReconstruction("failure-b", "phase", "changed_phase", "raw_phase", "selected expansion");
    }

    private void assertAliasMutationFailsReconstruction(String index, String alias, String target, String original, String reason)
        throws Exception {
        populateMutationIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false, true, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, replayRequiredRequest().allowPartialResults(true));
            gate.awaitStarted(future);
            setAliasTarget(index, alias, target);
            gate.release();
            Exception failure = expectFailure(future);
            // This checks the transport exception's REST status mapping, not an HTTP roundtrip.
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(failure));
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("SEQUENCE could not reconstruct"));
            assertThat(ExceptionsHelper.stackTrace(failure), containsString(reason));
        } finally {
            try {
                finish(gate, future);
            } finally {
                setAliasTarget(index, alias, original);
            }
        }
        assertMutationResourcesReleased();
        assertRecovery(coordinator);
    }

    /** A changed correlation key must reach the HTTP client as a reconstruction error even with partial results enabled. */
    public void testKeyAliasChangeAfterSelectionReturnsHttpBadRequest() throws Exception {
        assertAliasMutationReturnsHttpBadRequest("failure-a", "key", "changed_key", "raw_key", "correlation key no longer matches");
    }

    /** Losing a selected expansion during full replay must return an HTTP error rather than successful partial output. */
    public void testPredicateAliasChangeAfterSelectionReturnsHttpBadRequest() throws Exception {
        assertAliasMutationReturnsHttpBadRequest("failure-b", "phase", "changed_phase", "raw_phase", "selected expansion");
    }

    private void assertAliasMutationReturnsHttpBadRequest(String index, String alias, String target, String original, String reason)
        throws Exception {
        populateMutationIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false, true, true);
        var future = new PlainActionFuture<Response>();
        Cancellable request = null;
        Throwable originalFailure = null;
        try {
            var restRequest = new Request("POST", "/_query");
            restRequest.addParameter("allow_partial_results", "true");
            restRequest.addParameter("error_trace", "true");
            restRequest.setJsonEntity(
                Strings.toString(JsonXContent.contentBuilder().startObject().field("query", replayRequiredRequest().query()).endObject())
            );
            request = getRestClient().performRequestAsync(restRequest, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    future.onResponse(response);
                }

                @Override
                public void onFailure(Exception failure) {
                    future.onFailure(failure);
                }
            });
            gate.awaitStarted(future);
            setAliasTarget(index, alias, target);
            gate.release();
            var executionFailure = expectThrows(ExecutionException.class, () -> future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS));
            assertTrue(executionFailure.getCause() instanceof ResponseException);
            var response = ((ResponseException) executionFailure.getCause()).getResponse();
            String body = EntityUtils.toString(response.getEntity());
            assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            assertThat(body, containsString("SEQUENCE could not reconstruct"));
            assertThat(body, containsString(reason));
            Map<String, Object> error = XContentHelper.convertToMap(JsonXContent.jsonXContent, body, false);
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), error.get("status"));
            assertTrue(error.containsKey("error"));
            assertFalse("an HTTP error must not contain successful partial rows", error.containsKey("values"));
            assertFalse("an HTTP error must not contain a successful result schema", error.containsKey("columns"));
        } catch (Exception | AssertionError failure) {
            originalFailure = failure;
            throw failure;
        } finally {
            try {
                try {
                    finishRest(gate, future, request);
                } finally {
                    setAliasTarget(index, alias, original);
                }
            } catch (Exception | AssertionError cleanupFailure) {
                if (originalFailure != null) {
                    originalFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
        }
        assertMutationResourcesReleased();
        assertRecovery(coordinator);
    }

    /** Release the gate before draining the HTTP callback; cancellation also terminates a request after an earlier assertion failed. */
    private static void finishRest(SearchGate gate, ActionFuture<Response> future, Cancellable request) throws Exception {
        try {
            gate.close();
        } finally {
            if (request != null) {
                if (future.isDone() == false) {
                    request.cancel();
                }
                try {
                    EntityUtils.consume(future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS).getEntity());
                } catch (ExecutionException failure) {
                    if (failure.getCause() instanceof ResponseException responseFailure) {
                        EntityUtils.consume(responseFailure.getResponse().getEntity());
                    }
                }
            }
        }
    }

    private static Set<String> mappedFields(SearchSourceBuilder source) {
        return source.ext()
            .stream()
            .filter(SequenceDocValuesSearchExt.class::isInstance)
            .map(SequenceDocValuesSearchExt.class::cast)
            .flatMap(extension -> extension.fields().stream())
            .map(field -> field.field)
            .collect(java.util.stream.Collectors.toSet());
    }

    /** Source authorization can wrap the repeatability query without changing the selected-event query it guards. */
    private static Map<?, ?> repeatabilityDelegate(Object value) {
        if (value instanceof Map<?, ?> map) {
            if (map.get(SequenceRepeatabilityQueryBuilder.NAME) instanceof Map<?, ?> wrapper
                && wrapper.get("query") instanceof Map<?, ?> delegate) {
                return delegate;
            }
            for (Object child : map.values()) {
                Map<?, ?> delegate = repeatabilityDelegate(child);
                if (delegate != null) {
                    return delegate;
                }
            }
        } else if (value instanceof Iterable<?> children) {
            for (Object child : children) {
                Map<?, ?> delegate = repeatabilityDelegate(child);
                if (delegate != null) {
                    return delegate;
                }
            }
        }
        return null;
    }

    /** Ignore only RQB policy metadata; every surrounding authorization or selection clause remains part of the assertion. */
    private static Object effectiveQueryTree(Object value) {
        if (value instanceof Map<?, ?> map) {
            if (map.get(SequenceRepeatabilityQueryBuilder.NAME) instanceof Map<?, ?> wrapper) {
                assertEquals("an RQB query object must contain only its named query", 1, map.size());
                assertTrue(wrapper.get("query") instanceof Map<?, ?>);
                return effectiveQueryTree(wrapper.get("query"));
            }
            Map<Object, Object> effective = new LinkedHashMap<>();
            map.forEach((key, child) -> effective.put(key, effectiveQueryTree(child)));
            return effective;
        }
        if (value instanceof Iterable<?> children) {
            List<Object> effective = new ArrayList<>();
            children.forEach(child -> effective.add(effectiveQueryTree(child)));
            return effective;
        }
        return value;
    }

    private static void assertIdentityPayloadRequest(SearchGate gate, boolean timestampDemanded) {
        assertNotNull(gate.source);
        assertTrue(gate.readerPresent);
        assertFalse(gate.allowPartialSearchResults);
        assertEquals(1, gate.source.sorts().size());
        assertTrue(gate.source.sorts().getFirst() instanceof FieldSortBuilder);
        FieldSortBuilder sort = (FieldSortBuilder) gate.source.sorts().getFirst();
        assertEquals("_shard_doc", sort.getFieldName());
        assertEquals(SortOrder.ASC, sort.order());
        assertEquals(timestampDemanded, mappedFields(gate.source).contains("@timestamp"));
        assertFalse(gate.candidates.isEmpty());
        for (SearchSourceBuilder candidate : gate.candidates) {
            assertTrue(mappedFields(candidate).contains("@timestamp"));
            assertEquals(2, candidate.sorts().size());
            assertEquals("@timestamp", ((FieldSortBuilder) candidate.sorts().getFirst()).getFieldName());
        }
    }

    private void assertMutationResourcesReleased() throws Exception {
        assertReleased();
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertEquals("request breaker retained after sequence mapping mutation", 0, requestBreaker(node).getUsed());
            }
        });
    }

    private void setRuntimeTimestamp(String script, long offset) {
        for (String index : List.of("failure-a", "failure-b")) {
            long millis = BASE.toEpochMilli() + offset + (index.equals("failure-a") ? 0 : 1);
            assertAcked(
                indicesAdmin().preparePutMapping(index)
                    .setSource(
                        Map.of(
                            "runtime",
                            Map.of(
                                "@timestamp",
                                Map.of(
                                    "type",
                                    "date",
                                    "script",
                                    Map.of("lang", "sequence_failure", "source", script, "params", Map.of("millis", millis))
                                )
                            )
                        )
                    )
            );
        }
    }

    private void setAliasTarget(String index, String alias, String target) {
        assertAcked(
            indicesAdmin().preparePutMapping(index).setSource(Map.of("properties", Map.of(alias, Map.of("type", "alias", "path", target))))
        );
    }

    private void populateMutationIndices() {
        Map<String, Object> properties = Map.of(
            "@timestamp",
            Map.of("type", "date"),
            "key",
            Map.of("type", "alias", "path", "raw_key"),
            "raw_key",
            Map.of("type", "keyword"),
            "changed_key",
            Map.of("type", "keyword"),
            "phase",
            Map.of("type", "alias", "path", "raw_phase"),
            "raw_phase",
            Map.of("type", "keyword"),
            "changed_phase",
            Map.of("type", "keyword"),
            "payload",
            Map.of("type", "keyword")
        );
        for (String index : List.of("failure-a", "failure-b")) {
            assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(Map.of("properties", properties)));
            boolean first = index.equals("failure-a");
            prepareIndex(index).setId(first ? "A" + (ANCHORS - 1) : "B")
                .setSource(
                    Map.of(
                        "@timestamp",
                        BASE.plusMillis(first ? 0 : 1).toString(),
                        "raw_key",
                        "shared",
                        "changed_key",
                        "changed",
                        "raw_phase",
                        first ? "start" : "target",
                        "changed_phase",
                        "changed",
                        "payload",
                        first ? "anchor-" + (ANCHORS - 1) : "completion"
                    )
                )
                .get();
        }
        refresh("failure-a", "failure-b");
    }

    public void testDateMathClockIsFixedAcrossDelayedSearchAndReplay() throws Exception {
        populateIndices();
        Instant window = Instant.now();
        for (String index : List.of("failure-a", "failure-b")) {
            indicesAdmin().preparePutMapping(index).setSource("window", "type=date").get();
        }
        prepareIndex("failure-a").setId("clock-A")
            .setSource(
                "@timestamp",
                BASE.plusMillis(ANCHORS).toString(),
                "key",
                "clock",
                "payload",
                "clock-anchor",
                "window",
                window.toString()
            )
            .get();
        prepareIndex("failure-b").setId("clock-B")
            .setSource(
                "@timestamp",
                BASE.plusMillis(ANCHORS + 1).toString(),
                "key",
                "clock",
                "payload",
                "clock-completion",
                "phase",
                "target",
                "window",
                window.toString()
            )
            .get();
        refresh("failure-a", "failure-b");
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            var query = syncEsqlQueryRequest("""
                sequence by key with {"maxspan":"30s"} PRIMARY 1
                (from failure-a | eval request_started=NOW() | keep key, payload, request_started),
                (from failure-b | where phase=="target" | eval request_started=NOW() | keep key, payload, request_started)
                | EVAL member2_payload = MEMBER(2, payload), member2_request_started = MEMBER(2, request_started)
                | keep key, payload, member2_payload, request_started, member2_request_started
                | limit 1
                """).filter(org.elasticsearch.index.query.QueryBuilders.rangeQuery("window").gte("now-1m").lte("now+1m"));
            future = client(coordinator()).execute(EsqlQueryAction.INSTANCE, query);
            gate.awaitStarted(future);
            // An actual delay separates candidate admission from payload transport. The assertion pins the clock
            // crossing the wire rather than depending on an event expiring at a wall-clock boundary.
            Thread.sleep(25);
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                future = null;
                var rows = getValuesList(response);
                assertThat(rows, hasSize(1));
                assertEquals(List.of("clock", "clock-anchor", "clock-completion"), rows.getFirst().subList(0, 3));
                Object started = rows.getFirst().get(3);
                assertEquals(started, rows.getFirst().get(4));
                long startMillis = Instant.parse(started.toString()).toEpochMilli();
                assertTrue("candidate and payload shard searches must execute", gate.clocks.size() >= 4);
                for (long clock : gate.clocks) {
                    assertEquals("Search date math must share the ES|QL NOW() clock", startMillis, clock);
                }
            }
        } finally {
            finish(gate, future);
        }
        assertReleased();
    }

    public void testRequestBreakerAfterSnapshotAndCandidateAdmission() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        CircuitBreaker breaker = requestBreaker(coordinator);
        long previousLimit = breaker.getLimit();
        double previousOverhead = breaker.getOverhead();
        long previousTrips = breaker.getTrippedCount();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request());
            gate.awaitStarted(future);
            assertSnapshotAndCandidatesLive(coordinator);
            // Limit only the coordinator after field resolution, snapshot opening and stage-one candidate admission.
            // Data nodes retain normal limits; the pending Search response or its native conversion must fail admission.
            breaker.setLimitAndOverhead(1, 1.0);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, CircuitBreakingException.class), notNullValue());
            assertTrue("the real request breaker was not tripped", breaker.getTrippedCount() > previousTrips);
        } finally {
            breaker.setLimitAndOverhead(previousLimit, previousOverhead);
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    private void populateIndices() {
        for (String index : List.of("failure-a", "failure-b")) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(1, 0))
                    .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "payload", "type=keyword", "phase", "type=keyword")
            );
        }
        ensureGreen("failure-a", "failure-b");
        var bulk = client().prepareBulk();
        for (int event = 0; event < ANCHORS; event++) {
            bulk.add(
                prepareIndex("failure-a").setId("A" + event)
                    .setSource("@timestamp", BASE.plusMillis(event).toString(), "key", "shared", "payload", "anchor-" + event)
            );
        }
        bulk.add(
            prepareIndex("failure-b").setId("B")
                .setSource("@timestamp", BASE.plusMillis(ANCHORS).toString(), "key", "shared", "payload", "completion", "phase", "target")
        );
        assertFalse(bulk.get().hasFailures());
        refresh("failure-a", "failure-b");
    }

    private String coordinator() {
        for (String node : internalCluster().getNodeNames()) {
            var discoveryNode = internalCluster().getInstance(TransportService.class, node).getLocalNode();
            if (discoveryNode.canContainData() == false && discoveryNode.isMasterNode() == false) {
                return node;
            }
        }
        return internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
    }

    private static EsqlQueryRequest request() {
        return syncEsqlQueryRequest("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from failure-a | keep key, payload), (from failure-b | where phase=="target" | keep key, payload)
            | EVAL member2_payload = MEMBER(2, payload)
            | keep key, payload, member2_payload
            | limit 1
            """);
    }

    private static EsqlQueryRequest sourceRequest() {
        return syncEsqlQueryRequest("""
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM failure-a | KEEP key, payload),
            (FROM failure-b | WHERE phase=="target" | KEEP key, payload)
            | EVAL member2_payload = MEMBER(2, payload)
            | KEEP key, payload, member2_payload | LIMIT 1 | RETURN MATCHES
            """);
    }

    /** Both demanded output-only scalars require the source replay boundary exercised by cancellation and failure tests. */
    private static EsqlQueryRequest stageSourceReplayRequest() {
        return syncEsqlQueryRequest("""
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM failure-a | EVAL computed_payload = TO_UPPER(payload) | KEEP key, computed_payload),
            (FROM failure-b | WHERE phase=="target"
                | EVAL computed_payload = TO_UPPER(payload) | KEEP key, computed_payload)
            | EVAL member2_computed_payload = MEMBER(2, computed_payload)
            | KEEP key, computed_payload, member2_computed_payload | LIMIT 1 | RETURN MATCHES
            """);
    }

    /** Output-only computed values force full replay in both stages even if optional selection-output retention is enabled. */
    private static EsqlQueryRequest replayRequiredRequest() {
        return syncEsqlQueryRequest("""
            SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
            (FROM failure-a | WHERE phase=="start"
                | EVAL computed_payload = TO_UPPER(payload) | KEEP key, computed_payload),
            (FROM failure-b | WHERE phase=="target"
                | EVAL computed_payload = TO_UPPER(payload) | KEEP key, computed_payload)
            | EVAL member2_computed_payload = MEMBER(2, computed_payload)
            | KEEP key, computed_payload, member2_computed_payload | LIMIT 1 | RETURN MATCHES
            """);
    }

    private void assertSnapshotAndCandidatesLive(String coordinator) {
        int contexts = 0;
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            contexts += service.getActiveContexts();
        }
        assertEquals("both indexed sources must have live PIT readers", 2, contexts);
        assertTrue("stage-one candidate reservations must be retained", requestBreaker(coordinator).getUsed() > 32_768);
    }

    private CircuitBreaker requestBreaker(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST);
    }

    private static Exception expectFailure(ActionFuture<EsqlQueryResponse> future) {
        // If the query unexpectedly succeeds, finish() still owns and releases the future's response reference.
        return expectThrows(Exception.class, () -> future.actionGet(TIMEOUT));
    }

    private static void drain(ActionFuture<EsqlQueryResponse> future) {
        if (future == null) {
            return;
        }
        try {
            // Only unconsumed futures reach this method. hasReferences() cannot distinguish our reference from
            // a producer's reference while its response callback is still returning.
            future.actionGet(TIMEOUT).close();
        } catch (Exception e) {
            assertTrue("query did not finish after its transport gate was released: " + e, future.isDone());
        }
    }

    private static void finish(SearchGate gate, ActionFuture<EsqlQueryResponse> future) {
        try {
            gate.close();
        } finally {
            drain(future);
        }
    }

    private void assertRecovery(String coordinator) throws Exception {
        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request()).actionGet(TIMEOUT)) {
            assertValues(response.values(), List.of(List.of("shared", "anchor-" + (ANCHORS - 1), "completion")));
        }
        assertReleased();
    }

    /** Verify the same forced-replay request still returns both original sources after the injected terminal failure. */
    private void assertStageSourceReplayRecovery(String coordinator) throws Exception {
        Map<String, Object> anchor = client().prepareGet("failure-a", "A" + (ANCHORS - 1)).get().getSource();
        Map<String, Object> completion = client().prepareGet("failure-b", "B").get().getSource();
        try (
            var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, stageSourceReplayRequest().profile(true))
                .actionGet(TIMEOUT)
        ) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertEquals(List.of("shared", "ANCHOR-" + (ANCHORS - 1), "COMPLETION"), rows.getFirst().subList(0, 3));
            var description = SequenceMatchDescription.fromColumns(response.columns());
            var decoded = description.decodeRow(rows.getFirst(), SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT);
            assertEquals(
                List.of(
                    new SequenceMatchDescription.Member(1, "failure-a", "A" + (ANCHORS - 1), anchor),
                    new SequenceMatchDescription.Member(2, "failure-b", "B", completion)
                ),
                decoded.members()
            );
            assertFalse(response.isPartial());
            var statuses = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            assertThat(statuses, hasSize(1));
            assertEquals("both stages must replay their computed output", 4L, statuses.getFirst().stageDrivers());
            assertEquals(2L, statuses.getFirst().payloadSearchRequests());
        }
        assertReleased();
    }

    @After
    public void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals("PIT contexts retained after sequence failure", 0, service.getActiveContexts());
            }
            assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME, TransportSearchAction.TYPE.name() + "*")
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    /** Parks one second-stage shard query off the transport thread while preserving the real handler and task. */
    private final class SearchGate implements AutoCloseable {
        private final List<Long> clocks = new CopyOnWriteArrayList<>();
        private final List<SearchSourceBuilder> candidates = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final CountDownLatch handled = new CountDownLatch(1);
        private final AtomicBoolean claimed = new AtomicBoolean();
        private volatile boolean closed;
        private SearchSourceBuilder source;
        private boolean readerPresent;
        private boolean allowPartialSearchResults;

        private SearchGate(boolean failSearch) {
            this(failSearch, false);
        }

        private SearchGate(boolean failSearch, boolean payloadOnly) {
            this(failSearch, payloadOnly, false);
        }

        private SearchGate(boolean failSearch, boolean payloadOnly, boolean anyStage) {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transport = MockTransportService.getInstance(node);
                transport.<ShardSearchRequest>addRequestHandlingBehavior(
                    SearchTransportService.QUERY_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        clocks.add(request.nowInMillis());
                        boolean payloadSearch = request.source() != null
                            && request.source().fetchSource() != null
                            && request.source().fetchSource().fetchSource();
                        if (request.source() != null && payloadSearch == false) {
                            candidates.add(request.source().shallowCopy());
                        }
                        if (closed
                            || (anyStage == false && isSecondStageSearch(request) == false)
                            || (payloadOnly && payloadSearch == false)
                            || claimed.compareAndSet(false, true) == false) {
                            handler.messageReceived(request, channel, task);
                            return;
                        }
                        source = request.source().shallowCopy();
                        readerPresent = request.readerId() != null;
                        allowPartialSearchResults = request.allowPartialSearchResults();
                        request.incRef();
                        try {
                            transport.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                                @Override
                                protected void doRun() throws Exception {
                                    started.countDown();
                                    if (proceed.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS) == false) {
                                        throw new IllegalStateException("sequence shard search gate was not released");
                                    }
                                    if (failSearch) {
                                        channel.sendResponse(new IllegalStateException(SOURCE_FAILURE));
                                    } else {
                                        handler.messageReceived(request, channel, task);
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (e instanceof InterruptedException) {
                                        Thread.currentThread().interrupt();
                                    }
                                    channel.sendResponse(e);
                                }

                                @Override
                                public void onAfter() {
                                    request.decRef();
                                    handled.countDown();
                                }
                            });
                        } catch (Exception e) {
                            request.decRef();
                            handled.countDown();
                            throw e;
                        }
                    }
                );
            }
        }

        private void awaitStarted(ActionFuture<?> future) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT.millis());
            while (started.await(50, TimeUnit.MILLISECONDS) == false) {
                if (future.isDone()) {
                    try {
                        future.actionGet(TIMEOUT);
                    } catch (Exception failure) {
                        throw new AssertionError("query failed before reaching its sequence search gate", failure);
                    }
                    fail("query completed without reaching its sequence search gate");
                }
                assertTrue("sequence search gate did not start within the timeout", System.nanoTime() - deadline < 0);
            }
        }

        private void release() {
            proceed.countDown();
        }

        @Override
        public void close() {
            closed = true;
            release();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            if (claimed.get()) {
                safeAwait(handled, TIMEOUT);
            }
        }

        private static boolean isSecondStageSearch(ShardSearchRequest request) {
            if (request.source() == null) {
                return false;
            }
            // Only the second stage needs phase for its compute recheck. Unlike its _index query clauses, the physical
            // doc-value projection survives coordinator/shard query rewriting, so the gate cannot miss an optimized query.
            return request.source()
                .ext()
                .stream()
                .anyMatch(
                    extension -> extension instanceof SequenceDocValuesSearchExt projection
                        && projection.fields().stream().anyMatch(field -> field.field.equals("phase"))
                );
        }
    }
}

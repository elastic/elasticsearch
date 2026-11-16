/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/** Real HTTP error boundaries when a live mapping changes before a candidate read. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceDeploymentRecoveryIT extends AbstractSequenceIntegTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(45);
    private static final Instant BASE = Instant.parse("2026-11-13T00:00:00Z");
    private final List<String> ownedIndices = new ArrayList<>();

    /** Use real engines for the shard-side mapped-value loading boundary. */
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    /** HTTP failures are checked through the same client boundary that a detection runner receives. */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(SequenceFailureIT.SequenceBlockFactoryPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(CandidateGatePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, otherSettings))
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

    /** A shard-side producer mismatch is fatal even when the caller permits partial results. */
    public void testMappedIntegralAliasTypeChangeReturnsHttpBadRequestWithPartialResults() throws Exception {
        assertMappedTypeHttpFailure(true);
    }

    /** The same producer mismatch has the same client boundary when partial results are disabled. */
    public void testMappedIntegralAliasTypeChangeReturnsHttpBadRequestWithoutPartialResults() throws Exception {
        assertMappedTypeHttpFailure(false);
    }

    private void assertMappedTypeHttpFailure(boolean partial) throws Exception {
        Fixture fixture = createFixture("sequence-deploy-type-" + partial, dataNodes());
        assertResult(fixture, client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
        assertResourcesReleased();
        var future = new PlainActionFuture<Response>();
        Cancellable cancellable = null;
        try (var gate = new CandidateGate(fixture.first)) {
            Request request = new Request("POST", "/_query");
            request.addParameter("allow_partial_results", Boolean.toString(partial));
            request.addParameter("error_trace", "true");
            request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query(fixture)).endObject()));
            cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
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
            assertFalse("a fatal shard-side conversion must never become successful partial output", gate.allowPartialSearchResults);
            assertTrue(
                "the request must demand an integral value before its live alias changes",
                gate.source.ext()
                    .stream()
                    .filter(SequenceDocValuesSearchExt.class::isInstance)
                    .map(SequenceDocValuesSearchExt.class::cast)
                    .flatMap(ext -> ext.fields().stream())
                    .anyMatch(field -> field.field.equals("value") && SequenceHitReader.INTEGRAL_FORMAT.equals(field.format))
            );
            setValueAlias(fixture.first, "changed_value");
            gate.release();
            ExecutionException failure = expectThrows(ExecutionException.class, () -> future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause() instanceof ResponseException);
            Response response = ((ResponseException) failure.getCause()).getResponse();
            String body = EntityUtils.toString(response.getEntity());
            logger.info(
                "SEQUENCE mapped-type HTTP response partial={} status={} body={}",
                partial,
                response.getStatusLine().getStatusCode(),
                body
            );
            assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            assertThat(body, containsString("SEQUENCE integral field [value] no longer has a numeric type"));
            assertThat(
                "the failure must originate in the data-node conversion boundary",
                body,
                containsString("SequenceDocValuesFetchSubPhase")
            );
            Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, body, false);
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), parsed.get("status"));
            assertFalse(parsed.containsKey("values"));
            assertFalse(parsed.containsKey("columns"));
            assertTrue(parsed.get("error") instanceof Map<?, ?>);
            Map<?, ?> error = (Map<?, ?>) parsed.get("error");
            assertEquals("search_phase_execution_exception", error.get("type"));
            assertTrue(error.get("root_cause") instanceof List<?>);
            List<?> rootCauses = (List<?>) error.get("root_cause");
            assertFalse(rootCauses.isEmpty());
            for (Object rootCause : rootCauses) {
                assertTrue(rootCause instanceof Map<?, ?>);
                assertEquals("illegal_argument_exception", ((Map<?, ?>) rootCause).get("type"));
            }
            assertThat(rootCauses.toString(), containsString("no longer has a numeric type"));
            logger.info(
                "SEQUENCE mapped-type HTTP failure partial={} top_type={} root_cause={}",
                partial,
                error.get("type"),
                error.get("root_cause")
            );
        } finally {
            try {
                if (cancellable != null) {
                    if (future.isDone() == false) {
                        cancellable.cancel();
                    }
                    try {
                        EntityUtils.consume(future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS).getEntity());
                    } catch (ExecutionException failure) {
                        if (failure.getCause() instanceof ResponseException response) {
                            EntityUtils.consume(response.getResponse().getEntity());
                        }
                    }
                }
            } finally {
                setValueAlias(fixture.first, "raw_value");
            }
        }
        assertResourcesReleased();
        assertResult(fixture, client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
    }

    /** A demanded textual alias cannot silently stringify a numeric producer during the first read. */
    public void testMappedTextAliasTypeChangeReturnsHttpBadRequestWithPartialResults() throws Exception {
        assertMappedTextHttpFailure(true);
    }

    /** Caller partial-result preferences do not turn a coordinator conversion failure into rows. */
    public void testMappedTextAliasTypeChangeReturnsHttpBadRequestWithoutPartialResults() throws Exception {
        assertMappedTextHttpFailure(false);
    }

    private void assertMappedTextHttpFailure(boolean partial) throws Exception {
        Fixture fixture = createFixture("sequence-deploy-text-" + partial, dataNodes());
        setValueAlias(fixture.first, "payload");
        assertResult(
            fixture,
            client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT),
            "anchor"
        );
        assertResourcesReleased();
        var future = new PlainActionFuture<Response>();
        Cancellable cancellable = null;
        try (var gate = new CandidateGate(fixture.first)) {
            Request request = new Request("POST", "/_query");
            request.addParameter("allow_partial_results", Boolean.toString(partial));
            request.addParameter("error_trace", "true");
            request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query(fixture)).endObject()));
            cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
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
            assertFalse("candidate reads cannot return successful partial shards", gate.allowPartialSearchResults);
            assertTrue(
                "the frozen demand must be textual before its live alias changes",
                gate.source.ext()
                    .stream()
                    .filter(SequenceDocValuesSearchExt.class::isInstance)
                    .map(SequenceDocValuesSearchExt.class::cast)
                    .flatMap(ext -> ext.fields().stream())
                    .anyMatch(field -> field.field.equals("value") && field.format == null)
            );
            setValueAlias(fixture.first, "raw_value");
            gate.release();
            ExecutionException failure = expectThrows(ExecutionException.class, () -> future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS));
            assertTrue(failure.getCause() instanceof ResponseException);
            Response response = ((ResponseException) failure.getCause()).getResponse();
            String body = EntityUtils.toString(response.getEntity());
            logger.info(
                "SEQUENCE native-text HTTP response partial={} status={} body={}",
                partial,
                response.getStatusLine().getStatusCode(),
                body
            );
            assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), response.getStatusLine().getStatusCode());
            assertThat(body, containsString("SEQUENCE mapped [keyword] values require native textual bytes"));
            assertThat("the conversion must be checked on the coordinator's frozen demand", body, containsString("SequenceHitReader"));
            Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, body, false);
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), parsed.get("status"));
            assertFalse(parsed.containsKey("values"));
            assertFalse(parsed.containsKey("columns"));
            assertTrue(parsed.get("error") instanceof Map<?, ?>);
            Map<?, ?> error = (Map<?, ?>) parsed.get("error");
            assertEquals("illegal_argument_exception", error.get("type"));
            assertTrue(error.get("root_cause") instanceof List<?>);
            List<?> rootCauses = (List<?>) error.get("root_cause");
            assertFalse(rootCauses.isEmpty());
            for (Object rootCause : rootCauses) {
                assertTrue(rootCause instanceof Map<?, ?>);
                assertEquals("illegal_argument_exception", ((Map<?, ?>) rootCause).get("type"));
            }
            assertThat(rootCauses.toString(), containsString("require native textual bytes"));
            logger.info("SEQUENCE native-text HTTP failure partial={} top_type={} root_cause={}", partial, error.get("type"), rootCauses);
        } finally {
            try {
                if (cancellable != null) {
                    if (future.isDone() == false) {
                        cancellable.cancel();
                    }
                    try {
                        EntityUtils.consume(future.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS).getEntity());
                    } catch (ExecutionException failure) {
                        if (failure.getCause() instanceof ResponseException response) {
                            EntityUtils.consume(response.getResponse().getEntity());
                        }
                    }
                }
            } finally {
                setValueAlias(fixture.first, "raw_value");
            }
        }
        assertResourcesReleased();
        assertResult(fixture, client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query(fixture))).actionGet(TIMEOUT));
    }

    private Fixture createFixture(String prefix, String[] nodes) {
        String first = prefix + "-a";
        String second = prefix + "-b";
        for (int ordinal = 0; ordinal < 2; ordinal++) {
            String index = ordinal == 0 ? first : second;
            assertFalse(indexExists(index));
            ownedIndices.add(index);
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        indexSettings(1, 0).put("index.mode", "standard").put("index.routing.allocation.require._name", nodes[ordinal])
                    )
                    .setMapping(
                        Map.of(
                            "properties",
                            Map.of(
                                "@timestamp",
                                Map.of("type", "date"),
                                "key",
                                Map.of("type", "keyword"),
                                "payload",
                                Map.of("type", "keyword"),
                                "raw_value",
                                Map.of("type", "long"),
                                "changed_value",
                                Map.of("type", "date"),
                                "value",
                                Map.of("type", "alias", "path", "raw_value")
                            )
                        )
                    )
            );
            prepareIndex(index).setId(ordinal == 0 ? "A" : "B")
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(ordinal * 1000L).toString(),
                    "key",
                    "shared",
                    "payload",
                    ordinal == 0 ? "anchor" : "completion",
                    "raw_value",
                    7,
                    "changed_value",
                    BASE.toString()
                )
                .get();
        }
        refresh(first, second);
        ensureGreen(first, second);
        return new Fixture(first, second, client().prepareGet(first, "A").get().getSource());
    }

    private static String query(Fixture fixture) {
        return """
            SEQUENCE BY key WITHIN 10s PRIMARY 1
                (FROM %s METADATA _id,_source | EVAL computed=TO_UPPER(payload) | KEEP key,value,computed,_id,_source),
                (FROM %s METADATA _id | KEEP key,payload,_id)
            | EVAL member_payload=MEMBER(2,payload), member_id=MEMBER(2,_id)
            | KEEP key,value,computed,member_payload,_id,member_id,_source,sequence_provenance | LIMIT 1
            """.formatted(fixture.first, fixture.second);
    }

    /** Selected identities, ordinary values and explicitly requested source survive the first read. */
    private void assertResult(Fixture fixture, EsqlQueryResponse response) throws Exception {
        assertResult(fixture, response, 7L);
    }

    private void assertResult(Fixture fixture, EsqlQueryResponse response, Object expectedValue) throws Exception {
        try (response) {
            assertFalse(response.isPartial());
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertEquals(List.of("shared", expectedValue, "ANCHOR", "completion", "A", "B"), rows.getFirst().subList(0, 6));
            List<String> names = response.columns().stream().map(column -> column.name()).toList();
            int source = names.indexOf("_source");
            int provenance = names.indexOf("sequence_provenance");
            assertTrue(source >= 0 && provenance >= 0);
            assertEquals(fixture.firstSource, rows.getFirst().get(source));
            assertEquals(
                List.of(new SequenceProvenance.Member(1, fixture.first, "A"), new SequenceProvenance.Member(2, fixture.second, "B")),
                SequenceProvenance.decode(rows.getFirst().get(provenance))
            );
        }
    }

    private String[] dataNodes() {
        String[] nodes = Arrays.stream(internalCluster().getNodeNames())
            .filter(node -> internalCluster().getInstance(ClusterService.class, node).localNode().canContainData())
            .sorted()
            .toArray(String[]::new);
        assertEquals(2, nodes.length);
        return nodes;
    }

    private void setValueAlias(String index, String target) {
        assertAcked(
            indicesAdmin().preparePutMapping(index)
                .setSource(Map.of("properties", Map.of("value", Map.of("type", "alias", "path", target))))
        );
    }

    /** Check release before deleting the fixture, so deletion cannot disguise a leaked reader. */
    private void assertResourcesReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            assertThat(
                clusterAdmin().prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME, TransportSearchAction.TYPE.name() + "*")
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    /** Preserve leak evidence before deleting fixture-owned indices. */
    @After
    public void releaseFixtures() throws Exception {
        try {
            assertResourcesReleased();
        } finally {
            for (String index : ownedIndices) {
                if (indexExists(index)) {
                    assertAcked(indicesAdmin().prepareDelete(index));
                }
            }
            ownedIndices.clear();
        }
    }

    private record Fixture(String first, String second, Map<String, Object> firstSource) {}

    /** Parks a candidate search while preserving its real handler and request ownership. */
    public static class CandidateGatePlugin extends Plugin implements ActionPlugin {
        private volatile ThreadPool threadPool;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            threadPool = services.threadPool();
            return List.of();
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    CandidateGate gate = CandidateGate.ACTIVE.get();
                    if (gate == null
                        || action.equals(TransportSearchAction.NAME) == false
                        || request instanceof SearchRequest == false
                        || gate.claim((SearchRequest) request) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    request.mustIncRef();
                    var terminal = ActionListener.notifyOnce(ActionListener.runAfter(listener, () -> {
                        try {
                            request.decRef();
                        } finally {
                            gate.handled.countDown();
                        }
                    }));
                    try {
                        Supplier<ThreadContext.StoredContext> context = threadPool.getThreadContext().newRestorableContext(true);
                        gate.install(() -> {
                            try (var ignored = context.get()) {
                                chain.proceed(task, action, request, terminal);
                            } catch (Exception failure) {
                                terminal.onFailure(failure);
                            }
                        });
                    } catch (Exception failure) {
                        terminal.onFailure(failure);
                    }
                }
            });
        }
    }

    /** Parks the first-stage candidate request before shard loading. */
    private final class CandidateGate implements AutoCloseable {
        private static final AtomicReference<CandidateGate> ACTIVE = new AtomicReference<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch handled = new CountDownLatch(1);
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final String index;
        private volatile boolean closed;
        private volatile SearchSourceBuilder source;
        private boolean allowPartialSearchResults;
        private Runnable continuation;

        private CandidateGate(String index) {
            this.index = index;
            assertTrue("only one query owns the deployment gate", ACTIVE.compareAndSet(null, this));
        }

        private synchronized boolean claim(SearchRequest request) {
            SearchSourceBuilder candidate = request.source();
            boolean firstStage = request.getPointInTimeAuthorizationScope() != null
                && Arrays.asList(request.getPointInTimeAuthorizationScope().indices()).contains(index)
                && candidate != null
                && candidate.sorts() != null
                && candidate.sorts().size() == 2
                && candidate.sorts().getFirst() instanceof FieldSortBuilder first
                && first.getFieldName().equals("@timestamp")
                && candidate.sorts().get(1) instanceof FieldSortBuilder second
                && second.getFieldName().equals("_shard_doc")
                && candidate.ext()
                    .stream()
                    .filter(SequenceDocValuesSearchExt.class::isInstance)
                    .map(SequenceDocValuesSearchExt.class::cast)
                    .flatMap(ext -> ext.fields().stream())
                    .anyMatch(field -> field.field.equals("value"));
            if (closed || firstStage == false || claimed.compareAndSet(false, true) == false) {
                return false;
            }
            source = request.source().shallowCopy();
            allowPartialSearchResults = request.allowPartialSearchResults();
            return true;
        }

        /** Closing before installation still runs the continuation, so no request reference is stranded. */
        private void install(Runnable next) {
            boolean run;
            synchronized (this) {
                run = closed;
                if (run == false) {
                    continuation = next;
                }
            }
            started.countDown();
            if (run) {
                next.run();
            }
        }

        private void awaitStarted(ActionFuture<?> future) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT.millis());
            while (started.await(50, TimeUnit.MILLISECONDS) == false) {
                if (future.isDone()) {
                    future.actionGet(TIMEOUT);
                    fail("query completed before reaching its candidate gate");
                }
                assertTrue("candidate gate was not reached", System.nanoTime() - deadline < 0);
            }
        }

        private void release() {
            Runnable next;
            synchronized (this) {
                next = continuation;
                continuation = null;
            }
            if (next != null) {
                next.run();
            }
        }

        @Override
        public void close() {
            synchronized (this) {
                closed = true;
            }
            ACTIVE.compareAndSet(this, null);
            release();
            if (claimed.get()) {
                safeAwait(handled, TIMEOUT);
            }
        }
    }

}

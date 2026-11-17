/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateScriptFieldType;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlAsyncActionIT;
import org.elasticsearch.xpack.esql.action.EsqlAsyncGetResultAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Source time must use indexed values; accepted non-reading cases remain explicit. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceRuntimeTimestampIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-01-01T00:00:00Z");
    private static final String REFUSAL = "SEQUENCE does not support a runtime [@timestamp]";
    private static final String START = "sequence-timestamp-start";
    private static final String END = "sequence-timestamp-end";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Reuse the existing deterministic script engine: this fixture needs no Painless module, but keeps real runtime mappers/loaders.
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(
                CollectionUtils.appendToCopy(super.nodePlugins(), SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class),
                CandidateGatePlugin.class
            ),
            EsqlAsyncActionIT.LocalStateEsqlAsync.class
        );
    }

    /** Always install the delegate transport wrapper so the real FETCH handler can be paused on every seed. */
    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        List<Class<? extends Plugin>> mocks = new ArrayList<>(super.getMockPlugins());
        if (mocks.contains(MockTransportService.TestPlugin.class) == false) {
            mocks.add(MockTransportService.TestPlugin.class);
        }
        return List.copyOf(mocks);
    }

    /** Async acceptance has no rows; the stored terminal result must still fail rather than report partial success. */
    public void testAsyncRuntimeTimestampRefusesTerminalResultAndDeletesId() throws Exception {
        createMappedPair();
        SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        AtomicReference<String> id = new AtomicReference<>();
        boolean initialResponseConsumed = false;
        try (var gate = new CandidateGate(START, 1)) {
            var initial = client().execute(
                EsqlQueryAction.INSTANCE,
                asyncEsqlQueryRequest(sequence(START, END)).allowPartialResults(true)
                    .waitForCompletionTimeout(TimeValue.timeValueNanos(1))
                    .keepOnCompletion(true)
                    .keepAlive(TimeValue.timeValueMinutes(2))
            );
            try {
                try (var response = initial.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                    initialResponseConsumed = true;
                    // Preserve the exact ID before assertions so failure cleanup can always remove its stored result.
                    response.asyncExecutionId().ifPresent(id::set);
                    assertNotNull(id.get());
                    assertTrue(response.isAsync());
                    assertTrue(response.isRunning());
                    assertTrue(response.columns().isEmpty());
                    assertTrue(response.pages().isEmpty());
                }
                assertTrue("the actual async source must park before its first shard read", gate.started.await(30, TimeUnit.SECONDS));
                assertEquals(1, gate.pages.get());
                assertEquals(2, gate.pitShards.get());
                installCountedRuntime(START);
                gate.release();
                Exception failure = expectThrows(Exception.class, () -> {
                    try (
                        var response = client().execute(
                            EsqlAsyncGetResultAction.INSTANCE,
                            new GetAsyncResultRequest(id.get()).setWaitForCompletionTimeout(TimeValue.timeValueSeconds(60))
                        ).actionGet(DEFAULT_REQUEST_TIMEOUT)
                    ) {
                        getValuesList(response);
                    }
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString(REFUSAL));
                assertEquals(0, SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.get());
            } finally {
                gate.release();
                if (id.get() == null && initialResponseConsumed == false) {
                    try (var response = initial.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                        response.asyncExecutionId().ifPresent(id::set);
                    } catch (Exception ignored) {
                        // An initial request failure has no returned ID; release/drain the real source before cleanup.
                    }
                }
                if (id.get() != null) {
                    assertAcked(deleteAsyncId(client(), id.get()));
                    Exception missing = expectThrows(Exception.class, () -> {
                        try (
                            var response = client().execute(EsqlAsyncGetResultAction.INSTANCE, new GetAsyncResultRequest(id.get()))
                                .actionGet(DEFAULT_REQUEST_TIMEOUT)
                        ) {
                            getValuesList(response);
                        }
                    });
                    assertThat(ExceptionsHelper.unwrapCause(missing), instanceOf(ResourceNotFoundException.class));
                }
            }
        }
    }

    /** A separate fetch must validate its fresh mapping snapshot before loading a runtime shadow installed after query collection. */
    public void testRuntimeAddedBeforeActualFetchIsRefused() throws Exception {
        createMappedPair();
        SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        var future = new PlainActionFuture<EsqlQueryResponse>();
        List<MockTransportService> transports = new ArrayList<>();
        AtomicReference<String> receiver = new AtomicReference<>();
        AtomicReference<ShardId> shard = new AtomicReference<>();
        AtomicInteger documents = new AtomicInteger();
        AtomicInteger requestShards = new AtomicInteger();
        boolean submitted = false;
        // The gate observes the real shared PIT; MAX_VALUE leaves candidate dispatch untouched while FETCH claims its continuation.
        try (var gate = new CandidateGate(START, Integer.MAX_VALUE)) {
            try {
                for (TransportService transport : internalCluster().getInstances(TransportService.class)) {
                    // Internal cluster nodes use the transport test wrapper; every intercepted request still reaches its real handler.
                    MockTransportService mock = MockTransportService.class.cast(transport);
                    transports.add(mock);
                    mock.<ShardFetchSearchRequest>addRequestHandlingBehavior(
                        SearchTransportService.FETCH_ID_ACTION_NAME,
                        (handler, request, channel, task) -> {
                            var embedded = request.getShardSearchRequest();
                            if (embedded != null
                                && START.equals(embedded.shardId().getIndexName())
                                && request.docIds().length > 0
                                && gate.claimed.compareAndSet(false, true)) {
                                receiver.set(mock.getLocalNode().getName());
                                shard.set(embedded.shardId());
                                documents.set(request.docIds().length);
                                requestShards.set(embedded.numberOfShards());
                                request.mustIncRef();
                                var retained = Releasables.releaseOnce(request::decRef);
                                var failureListener = ActionListener.notifyOnce(new ChannelActionListener<>(channel));
                                try {
                                    Supplier<ThreadContext.StoredContext> saved = mock.getThreadPool()
                                        .getThreadContext()
                                        .newRestorableContext(true);
                                    gate.install(() -> {
                                        try {
                                            mock.getThreadPool().generic().execute(() -> {
                                                try (var ignored = saved.get()) {
                                                    handler.messageReceived(request, channel, task);
                                                } catch (Exception failure) {
                                                    failureListener.onFailure(failure);
                                                } finally {
                                                    retained.close();
                                                }
                                            });
                                        } catch (Exception failure) {
                                            try {
                                                failureListener.onFailure(failure);
                                            } finally {
                                                retained.close();
                                            }
                                        }
                                    });
                                } catch (Exception failure) {
                                    try {
                                        failureListener.onFailure(failure);
                                    } finally {
                                        retained.close();
                                    }
                                }
                            } else {
                                handler.messageReceived(request, channel, task);
                            }
                        }
                    );
                }
                client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(sequence(START, END)).allowPartialResults(true), future);
                submitted = true;
                assertTrue("a real nonempty FETCH must be held before its handler", gate.started.await(30, TimeUnit.SECONDS));
                assertEquals(2, gate.pitShards.get());
                assertTrue("one-shard query+fetch would reuse the old mapped context", requestShards.get() > 1);
                assertTrue(documents.get() > 0);
                assertNotNull(receiver.get());
                assertNotNull(shard.get());
                installCountedRuntime(START);
                var mapper = internalCluster().getInstance(IndicesService.class, receiver.get())
                    .indexServiceSafe(shard.get().getIndex())
                    .mapperService();
                assertBusy(() -> assertThat(mapper.mappingLookup().getFieldType("@timestamp"), instanceOf(DateScriptFieldType.class)));
                assertEquals(0, SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.get());
                gate.release();
                Exception failure = expectThrows(Exception.class, () -> {
                    try (var response = future.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                        getValuesList(response);
                    }
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString(REFUSAL));
                assertEquals(0, SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.get());
            } finally {
                gate.release();
                if (submitted && future.isDone() == false) {
                    try (var response = future.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                        getValuesList(response);
                    } catch (Exception ignored) {
                        // Drain the original source after releasing its real FETCH, including assertion-failure paths.
                    }
                }
                transports.forEach(MockTransportService::clearInboundRules);
            }
        }
    }

    private void installCountedRuntime(String index) {
        assertAcked(
            indicesAdmin().preparePutMapping(index)
                .setSource(
                    Map.of(
                        "runtime",
                        Map.of("@timestamp", Map.of("type", "date", "script", Map.of("lang", "sequence_failure", "source", "changing")))
                    )
                )
        );
    }

    private void assertQueryTasksReleased() throws Exception {
        assertBusy(
            () -> assertTrue(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME + "*", DriverTaskRunner.ACTION_NAME)
                    .get()
                    .getTasks()
                    .isEmpty()
            )
        );
    }

    /** Runtime-only timestamps, including constant scripts, cannot supply a sequence ordinal. */
    public void testStandaloneRuntimeTimestampIsRefused() {
        createRuntime(START);
        seedPair(START, "@timestamp");
        assertOrdinaryRuntimeDates(START, 2);
        assertRefused(sequence(START, START));
    }

    /** A mapped field still exists underneath a runtime shadow, so checking for its mere presence is insufficient. */
    public void testRuntimeShadowIsRefused() {
        createMapped(START, "date", false, 1);
        seedPair(START, "@timestamp");
        installRuntime(START);
        assertOrdinaryRuntimeDates(START, 2);
        assertRefused(sequence(START, START));
    }

    /** Every concrete index in the searched alias is checked, and partial-result opt-in cannot turn refusal into success. */
    public void testMixedAliasFailsWholeQueryIncludingPartialOptIn() {
        createMapped(START, "date", false, 1);
        createRuntime(END);
        seedPair(START, "@timestamp");
        seedPair(END, "@timestamp");
        String alias = "sequence-timestamp-mixed";
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(START, alias).addAlias(END, alias));
        assertOrdinaryRuntimeDates(alias, 4);
        assertRefused(sequence(alias, alias));
    }

    /** Resolve a mapped alias through its canonical target instead of mistaking the alias mapper for a runtime field. */
    public void testMappedDateNanosAndFieldAliasesRemainSupported() {
        for (String type : List.of("date", "date_nanos")) {
            for (boolean alias : List.of(false, true)) {
                String index = "sequence-timestamp-" + type.replace('_', '-') + "-" + alias;
                createMapped(index, type, alias, 1);
                seedPair(index, alias ? "event_time" : "@timestamp");
                try (var response = run(sequence(index, index))) {
                    assertEquals(List.of(List.of("k")), getValuesList(response));
                }
            }
        }
    }

    /** EXPLAIN is an accepted miss: analysis renders the sequence without executing a stage reader. */
    public void testExplainDoesNotRefuseRuntimeTimestamp() {
        createRuntime(START);
        seedPair(START, "@timestamp");
        try (var gate = new CandidateGate(START, Integer.MAX_VALUE)) {
            try (var response = run("EXPLAIN (" + sequence(START, START) + ")")) {
                assertThat(getValuesList(response).toString().toLowerCase(java.util.Locale.ROOT), containsString("sequence"));
            }
            assertEquals(0, gate.pages.get());
            assertEquals(0, gate.pitShards.get());
        }
    }

    /** An unsearched stage is the other accepted miss; its PIT slots must be excluded before another stage inspects mappings. */
    public void testNeverSearchedRuntimeStageAndForeignPitScopeRemainEmpty() {
        createMapped(START, "date", false, 1);
        createRuntime(END);
        seed(START, "not-a-start", "@timestamp", BASE, "B", "k");
        seedPair(END, "@timestamp");
        refresh(START, END);
        try (var gate = new CandidateGate(END, Integer.MAX_VALUE)) {
            try (var response = run(sequence(START, END))) {
                assertEquals(List.of(), getValuesList(response));
            }
            assertEquals(0, gate.pages.get());
            assertEquals("both source indices must really be represented in the shared PIT", 2, gate.pitShards.get());
        }
    }

    /** The mapping can change after analysis and PIT open but before the first stage actually reads its events. */
    public void testRuntimeAddedBeforeFirstStageSearchIsRefused() throws Exception {
        createMappedPair();
        assertRefusedAfterMappingChange(START, 1);
    }

    /** A completed first-stage read must not make the later stage's live mapping implicitly safe. */
    public void testRuntimeAddedBeforeLaterStageSearchIsRefused() throws Exception {
        createMappedPair();
        assertRefusedAfterMappingChange(END, 1);
    }

    /** Each continuation needs its own check, rather than a safe bit retained from the first page. */
    public void testRuntimeAddedBeforeContinuationIsRefused() throws Exception {
        createMapped(START, "date", false, 1);
        createMapped(END, "date", false, 1);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 300; i++) {
            bulk.add(
                prepareIndex(START).setId("a-" + i).setSource("@timestamp", BASE.plusMillis(i).toString(), "phase", "A", "key", "k-" + i)
            );
        }
        bulk.add(prepareIndex(END).setId("b").setSource("@timestamp", BASE.plusSeconds(1).toString(), "phase", "B", "key", "k-299"));
        assertFalse(bulk.get().hasFailures());
        refresh(START, END);
        assertRefusedAfterMappingChange(START, 2);
    }

    /** A PIT freezes documents, not mappings; even an explicitly cacheable empty request must validate its new context. */
    public void testExistingPitAndEmptyCachedRequestRecheckTimestamp() {
        createMapped(START, "date", false, 2);
        seedPair(START, "@timestamp");
        BytesReference pit = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(START).keepAlive(TimeValue.timeValueMinutes(2))
        ).actionGet().getPointInTimeId();
        try {
            for (QueryBuilder query : List.of(matchAllQuery(), new MatchNoneQueryBuilder())) {
                var response = guardedSearch(pit, query);
                try {
                    assertEquals(query.getWriteableName().equals("match_all") ? 1 : 0, response.getHits().getHits().length);
                    if (response.pointInTimeId() != null) {
                        pit = response.pointInTimeId();
                    }
                } finally {
                    response.decRef();
                }
            }
            installRuntime(START);
            for (QueryBuilder query : List.of(matchAllQuery(), new MatchNoneQueryBuilder())) {
                BytesReference snapshot = pit;
                Exception failure = expectThrows(Exception.class, () -> {
                    var response = guardedSearch(snapshot, query);
                    response.decRef();
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString(REFUSAL));
            }
        } finally {
            assertTrue(client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded());
        }
    }

    private org.elasticsearch.action.search.SearchResponse guardedSearch(BytesReference pit, QueryBuilder query) {
        return client().prepareSearch()
            .setSource(
                new SearchSourceBuilder().query(new SequenceSourceQueryBuilder(query, List.of(START)))
                    .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2)))
                    .fetchField("@timestamp")
                    .size(1)
            )
            .setRequestCache(true)
            .setPreference(null)
            .setPreFilterShardSize(Integer.MAX_VALUE)
            .setAllowPartialSearchResults(false)
            .get();
    }

    private void assertRefusedAfterMappingChange(String index, int page) throws Exception {
        var future = new PlainActionFuture<EsqlQueryResponse>();
        try (var gate = new CandidateGate(index, page)) {
            client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(sequence(START, END)).allowPartialResults(true), future);
            try {
                assertTrue("the selected stage search must be parked after analysis", gate.started.await(30, TimeUnit.SECONDS));
                assertEquals(page, gate.pages.get());
                assertEquals(2, gate.pitShards.get());
                installRuntime(index);
                gate.release();
                Exception failure = expectThrows(Exception.class, () -> {
                    try (var response = future.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                        getValuesList(response);
                    }
                });
                assertThat(ExceptionsHelper.stackTrace(failure), containsString(REFUSAL));
            } finally {
                gate.release();
                if (future.isDone() == false) {
                    try (var response = future.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                        getValuesList(response);
                    } catch (Exception ignored) {
                        // Drain the real query after releasing its request, including an assertion failure above.
                    }
                }
            }
        }
    }

    private void assertRefused(String query) {
        for (boolean partial : List.of(false, true)) {
            Exception failure = expectThrows(Exception.class, () -> {
                try (var response = run(syncEsqlQueryRequest(query).allowPartialResults(partial))) {
                    getValuesList(response);
                }
            });
            assertThat(ExceptionsHelper.stackTrace(failure), containsString(REFUSAL));
        }
    }

    private void assertOrdinaryRuntimeDates(String index, long count) {
        try (var response = run("FROM " + index + " | STATS n=COUNT(@timestamp)")) {
            assertEquals(List.of(List.of(count)), getValuesList(response));
        }
    }

    private void createMappedPair() {
        createMapped(START, "date", false, 1);
        createMapped(END, "date", false, 1);
        seed(START, "a", "@timestamp", BASE, "A", "k");
        seed(END, "b", "@timestamp", BASE.plusSeconds(1), "B", "k");
        refresh(START, END);
    }

    private void createMapped(String index, String type, boolean alias, int shards) {
        Map<String, Object> timestamp = Map.of("type", type);
        Map<String, Object> properties = alias
            ? Map.of(
                "event_time",
                timestamp,
                "@timestamp",
                Map.of("type", "alias", "path", "event_time"),
                "key",
                Map.of("type", "keyword"),
                "phase",
                Map.of("type", "keyword")
            )
            : Map.of("@timestamp", timestamp, "key", Map.of("type", "keyword"), "phase", Map.of("type", "keyword"));
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(shards, 0)).setMapping(Map.of("properties", properties)));
        ensureGreen(index);
    }

    private void createRuntime(String index) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of("key", Map.of("type", "keyword"), "phase", Map.of("type", "keyword")),
                        "runtime",
                        Map.of("@timestamp", runtimeTimestamp())
                    )
                )
        );
        ensureGreen(index);
    }

    private void installRuntime(String index) {
        assertAcked(indicesAdmin().preparePutMapping(index).setSource(Map.of("runtime", Map.of("@timestamp", runtimeTimestamp()))));
    }

    private static Map<String, Object> runtimeTimestamp() {
        return Map.of("type", "date", "script", Map.of("lang", "sequence_failure", "source", "constant"));
    }

    private void seedPair(String index, String timestamp) {
        seed(index, "a", timestamp, BASE, "A", "k");
        seed(index, "b", timestamp, BASE.plusSeconds(1), "B", "k");
        refresh(index);
    }

    private void seed(String index, String id, String timestamp, Instant time, String phase, String key) {
        prepareIndex(index).setId(id).setSource(timestamp, time.toString(), "phase", phase, "key", key).get();
    }

    private static String sequence(String first, String last) {
        return "SEQUENCE BY key WITHIN 30s PRIMARY 1 (FROM "
            + first
            + " | WHERE phase==\"A\"),"
            + "(FROM "
            + last
            + " | WHERE phase==\"B\") | KEEP key | LIMIT 10";
    }

    /** A paused action always transfers back to its real chain, including release racing installation. */
    private static final class CandidateGate implements AutoCloseable {
        private static final AtomicReference<CandidateGate> ACTIVE = new AtomicReference<>();
        private final String index;
        private final int page;
        private final AtomicInteger pages = new AtomicInteger();
        private final AtomicInteger pitShards = new AtomicInteger();
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final CountDownLatch started = new CountDownLatch(1);
        private boolean released;
        private Runnable continuation;

        private CandidateGate(String index, int page) {
            this.index = index;
            this.page = page;
            assertTrue(ACTIVE.compareAndSet(null, this));
        }

        private void install(Runnable next) {
            boolean run;
            synchronized (this) {
                run = released;
                if (run == false) {
                    continuation = next;
                }
            }
            started.countDown();
            if (run) {
                next.run();
            }
        }

        private void release() {
            Runnable next;
            synchronized (this) {
                released = true;
                next = continuation;
                continuation = null;
            }
            if (next != null) {
                next.run();
            }
        }

        @Override
        public void close() {
            ACTIVE.compareAndSet(this, null);
            release();
        }
    }

    /** Observe real PIT scopes and pause a selected candidate request without blocking a transport thread. */
    public static class CandidateGatePlugin extends Plugin implements ActionPlugin {
        private ThreadPool threadPool;

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
                    if (gate == null) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    if (request instanceof OpenPointInTimeRequest) {
                        listener = listener.delegateFailureAndWrap((next, response) -> {
                            if (response instanceof OpenPointInTimeResponse opened) {
                                gate.pitShards.set(opened.getTotalShards());
                            }
                            next.onResponse(response);
                        });
                    }
                    if (request instanceof SearchRequest search
                        && search.getPointInTimeAuthorizationScope() != null
                        && List.of(search.getPointInTimeAuthorizationScope().indices()).contains(gate.index)
                        && gate.pages.incrementAndGet() == gate.page
                        && gate.claimed.compareAndSet(false, true)) {
                        request.mustIncRef();
                        var terminal = ActionListener.notifyOnce(ActionListener.runAfter(listener, request::decRef));
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
                        return;
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    /** Refusal, accepted misses and live updates must all release real PIT contexts and retained blocks. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        assertNull(CandidateGate.ACTIVE.get());
        assertQueryTasksReleased();
        ensureBlocksReleased();
    }
}

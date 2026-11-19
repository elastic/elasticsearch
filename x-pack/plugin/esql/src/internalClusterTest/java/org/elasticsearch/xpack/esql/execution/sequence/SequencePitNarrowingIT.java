/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Actual native requests qualify physical PIT pruning independently of live mapped selection and logical authorization. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequencePitNarrowingIT extends AbstractSequenceIntegTestCase {
    private static final String FIRST = "sequence-pit-read-a";
    private static final String SECOND = "sequence-pit-read-b";
    private static final String FOREIGN = "sequence-pit-read-foreign";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private final List<String> owned = new ArrayList<>();
    private String coordinator;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), PitProbePlugin.class);
    }

    /** A newly discovered foreign index is not part of either final analyzed read set, even when its mapping is incompatible. */
    public void testForeignScopeCreatedAfterResolutionDoesNotRetainAReader() throws Exception {
        createPair();
        try (var gate = new Gate(EsqlSearchShardsAction.NAME)) {
            assertEquals(List.of(List.of("shared", "A", "B")), runGated(request("sequence-pit-read-*"), gate, List.of(() -> {
                createIndex(FOREIGN, "keyword");
                seed(FOREIGN, "F", "not-a-date", "A");
                refresh(FOREIGN);
            })));
            assertEquals(Set.of(FIRST, SECOND), decodedIndices(gate));
            assertEquals(3, gate.opened.get().total());
            assertEquals(1, gate.opened.get().skipped());
            assertEquals(List.of("sequence-pit-read-*"), gate.openIndices.get());
            assertFalse(gate.openFilter.get().contains(FOREIGN));
            assertFalse(gate.pagePrefilters.isEmpty());
            assertTrue(gate.pagePrefilters.stream().allMatch(value -> value == Integer.MAX_VALUE));
        }
    }

    public void testEachStageUsesItsOwnMembershipList() {
        createPair();
        String starts = "sequence-pit-start-list";
        String ends = "sequence-pit-end-list";
        createIndex(starts, "date");
        createIndex(ends, "date");
        seed(starts, "allow-a", BASE.toString(), "A");
        seed(ends, "allow-b", BASE.toString(), "B");
        refresh(starts, ends);
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM %s METADATA _id | WHERE phase IN (FROM %s | KEEP phase)),
            (FROM %s METADATA _id | WHERE phase IN (FROM %s | KEEP phase))
            | EVAL start_id = MEMBER(1, _id), start_phase = MEMBER(1, phase)
            | KEEP key, start_id, _id, start_phase, phase | SORT key | LIMIT 10
            """.formatted(FIRST, starts, SECOND, ends);
        var expected = List.of(List.of("shared", "A", "B", "A", "B"));
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            assertEquals(expected, getValuesList(response));
        }
        // Each decoy has a complete chain. One fails only the first list, the other only the second.
        for (String key : List.of("wrong-start", "wrong-end")) {
            client(coordinator).prepareIndex(FIRST)
                .setId(key + "-a")
                .setSource("@timestamp", BASE.toString(), "key", key, "phase", key.equals("wrong-start") ? "X" : "A")
                .get();
            client(coordinator).prepareIndex(SECOND)
                .setId(key + "-b")
                .setSource("@timestamp", BASE.plusMillis(1).toString(), "key", key, "phase", key.equals("wrong-end") ? "Y" : "B")
                .get();
        }
        refresh(FIRST, SECOND);
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            assertEquals(expected, getValuesList(response));
        }
    }

    public void testRuntimeEmptyMembershipPreservesSchemaWithoutEventReads() {
        createPair();
        String allow = "sequence-pit-empty-allow";
        createIndex(allow, "date");
        String predicate = " | WHERE key IN (FROM " + allow + " | KEEP key)";
        for (int stages : List.of(3, 1, 2)) {
            String query = """
                SEQUENCE BY key WITHIN 30s PRIMARY 2
                (FROM %s%s), (FROM %s%s)
                | KEEP sequence_provenance | LIMIT 10
                """.formatted(FIRST, (stages & 1) == 0 ? "" : predicate, SECOND, (stages & 2) == 0 ? "" : predicate);
            logger.info("Part B runtime-empty stage mask={}", stages);
            try (var gate = new Gate(); var response = run(query)) {
                assertFalse(response.isPartial());
                assertColumnNames(response.columns(), List.of("sequence_provenance"));
                assertColumnTypes(response.columns(), List.of("keyword"));
                assertEquals(List.of(), getValuesList(response));
                assertNull("the runtime-empty stage should skip PIT open", gate.openIndices.get());
                assertTrue("the runtime-empty stage should skip sequence event searches", gate.pagePrefilters.isEmpty());
            }
        }
    }

    private static EsqlQueryRequest request(String source) {
        return syncEsqlQueryRequest("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A" | KEEP key,_id),
            (FROM %s METADATA _id | WHERE phase=="B" | KEEP key,_id)
            | EVAL other_id=MEMBER(2,_id) | KEEP key,_id,other_id | LIMIT 1
            """.formatted(source, source)).allowPartialResults(false);
    }

    private List<List<Object>> runGated(EsqlQueryRequest request, Gate gate, List<CheckedRunnable<Exception>> changes) throws Exception {
        ActionFuture<EsqlQueryResponse> future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request);
        Throwable primary = null;
        try {
            for (int phase = 0; phase < changes.size(); phase++) {
                gate.await(phase);
                changes.get(phase).run();
                gate.release(phase);
            }
            var response = future.actionGet();
            future = null;
            try (response) {
                assertFalse(response.isPartial());
                // Only semantic String/Long/null cells escape the response; no borrowed block or BytesRef survives close.
                return getValuesList(response);
            }
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            gate.close();
            if (future != null) {
                try (var response = future.actionGet()) {
                    // A failure before response consumption still owns the future's successful response reference.
                } catch (Exception | Error cleanup) {
                    if (primary != null) {
                        if (primary != cleanup) {
                            primary.addSuppressed(cleanup);
                        }
                    } else {
                        throw cleanup;
                    }
                }
            }
        }
    }

    private Set<String> decodedIndices(Gate gate) {
        Opened opened = gate.opened.get();
        assertNotNull("actual native OpenPIT must complete", opened);
        return Set.copyOf(Arrays.asList(SearchContextId.decode(nodeRegistry(), opened.token()).getActualIndices()));
    }

    private NamedWriteableRegistry nodeRegistry() {
        return internalCluster().getInstance(NamedWriteableRegistry.class, coordinator);
    }

    private void createPair() {
        coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        createIndex(FIRST, "date");
        createIndex(SECOND, "date");
        seed(FIRST, "A", BASE.toString(), "A");
        seed(SECOND, "B", BASE.plusMillis(1).toString(), "B");
        refresh(FIRST, SECOND);
    }

    private void createIndex(String index, String timeType) {
        assertFalse("fixture must not own an existing namespace", indexExists(index));
        owned.add(index);
        var fields = Map.of("@timestamp", Map.of("type", timeType), "key", Map.of("type", "keyword"), "phase", Map.of("type", "keyword"));
        assertAcked(
            client(coordinator).admin()
                .indices()
                .prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(Map.of("properties", fields))
        );
    }

    private void seed(String index, String id, String timestamp, String phase) {
        client(coordinator).prepareIndex(index).setId(id).setSource(Map.of("@timestamp", timestamp, "key", "shared", "phase", phase)).get();
    }

    /** Fixture-owned indices are deleted only after actual terminal tasks, PITs, breakers and blocks have released. */
    @After
    public void releaseFixture() throws Exception {
        Gate gate = Gate.ACTIVE.get();
        if (gate != null) {
            gate.close();
        }
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals(0L, service.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
            assertTrue(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(
                        "indices:data/read/esql*",
                        "indices:data/read/search*",
                        TransportOpenPointInTimeAction.TYPE.name(),
                        TransportClosePointInTimeAction.TYPE.name()
                    )
                    .get()
                    .getTasks()
                    .isEmpty()
            );
        });
        ensureBlocksReleased();
        for (String index : owned) {
            if (indexExists(index)) {
                assertAcked(client().admin().indices().prepareDelete(index));
            }
        }
    }

    /** The token is deep-owned so later test-thread decoding outlives the producer's response reference. */
    private record Opened(int total, int skipped, BytesReference token) {}

    /** Owns only paused action continuations; closing resumes every installed continuation before query draining. */
    private static final class Gate implements AutoCloseable {
        private static final AtomicReference<Gate> ACTIVE = new AtomicReference<>();
        private final List<Pause> pauses;
        private final AtomicInteger phase = new AtomicInteger();
        private final AtomicReference<Opened> opened = new AtomicReference<>();
        private final AtomicReference<List<String>> openIndices = new AtomicReference<>();
        private final AtomicReference<String> openFilter = new AtomicReference<>();
        private final List<Integer> pagePrefilters = new CopyOnWriteArrayList<>();

        private Gate(String... actions) {
            pauses = Arrays.stream(actions).map(Pause::new).toList();
            assertTrue("only one query owns this fixture gate", ACTIVE.compareAndSet(null, this));
        }

        private void await(int phase) throws InterruptedException {
            assertTrue("native action did not reach phase " + phase, pauses.get(phase).started.await(30, TimeUnit.SECONDS));
        }

        private void release(int position) {
            phase.compareAndSet(position, position + 1);
            pauses.get(position).release();
        }

        private Pause claim(String action) {
            int position = phase.get();
            if (position < pauses.size()) {
                Pause pause = pauses.get(position);
                if (pause.action.equals(action) && pause.claimed.compareAndSet(false, true)) {
                    return pause;
                }
            }
            return null;
        }

        @Override
        public void close() {
            ACTIVE.compareAndSet(this, null);
            for (int position = 0; position < pauses.size(); position++) {
                release(position);
            }
        }
    }

    /** A close racing continuation installation must resume the action rather than abandon its owned request reference. */
    private static final class Pause {
        private final String action;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final CountDownLatch started = new CountDownLatch(1);
        private boolean released;
        private Runnable continuation;

        private Pause(String action) {
            this.action = action;
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
    }

    /** Resumes actual native actions with their original request reference and thread context; no assertion runs on callbacks. */
    public static class PitProbePlugin extends Plugin implements ActionPlugin {
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
                    Gate gate = Gate.ACTIVE.get();
                    if (gate == null) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    if (request instanceof OpenPointInTimeRequest open) {
                        gate.openIndices.set(List.copyOf(Arrays.asList(open.indices())));
                        gate.openFilter.set(Strings.toString(open.indexFilter()));
                        listener = listener.delegateFailureAndWrap((next, response) -> {
                            if (response instanceof OpenPointInTimeResponse opened) {
                                gate.opened.set(
                                    new Opened(
                                        opened.getTotalShards(),
                                        opened.getSkippedShards(),
                                        new BytesArray(BytesRef.deepCopyOf(opened.getPointInTimeId().toBytesRef()))
                                    )
                                );
                            }
                            next.onResponse(response);
                        });
                    } else if (request instanceof SearchRequest search && search.getPointInTimeAuthorizationIndices() != null) {
                        if (search.source() != null && search.source().size() != 0) {
                            gate.pagePrefilters.add(search.getPreFilterShardSize());
                        }
                    }
                    Pause pause = gate.claim(action);
                    if (pause == null) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    request.mustIncRef();
                    var terminal = ActionListener.notifyOnce(ActionListener.runAfter(listener, request::decRef));
                    try {
                        Supplier<ThreadContext.StoredContext> context = threadPool.getThreadContext().newRestorableContext(true);
                        pause.install(() -> {
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
}

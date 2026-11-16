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
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsRequest;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
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

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Actual native requests qualify physical PIT pruning independently of live mapped selection and logical authorization. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequencePitNarrowingIT extends AbstractSequenceIntegTestCase {
    private static final String FIRST = "sequence-pit-read-a";
    private static final String SECOND = "sequence-pit-read-b";
    private static final String FOREIGN = "sequence-pit-read-foreign";
    private static final String ALIAS = "sequence-pit-source";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final TimeValue KEEP_ALIVE = TimeValue.timeValueMinutes(2);
    private final List<String> owned = new ArrayList<>();
    private String coordinator;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), PitProbePlugin.class),
            SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.class
        );
    }

    /** A non-deterministic timestamp dependency is accepted while crossed physical scopes stay empty. */
    public void testCrossedScopesAcceptNondeterministicTimestampAndRemainEmpty() throws Exception {
        crossedScopePolicy(false);
    }

    /** The same crossed physical memberships produce an ordinary empty result when the mandatory time dependency is pure. */
    public void testCrossedScopesDeterministicControlIsEmpty() throws Exception {
        crossedScopePolicy(true);
    }

    private void crossedScopePolicy(boolean deterministic) throws Exception {
        createPair(false);
        String left = ALIAS + "-left";
        String right = ALIAS + "-right";
        assertAcked(
            client(coordinator).admin()
                .indices()
                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(FIRST, left)
                .addAlias(SECOND, right)
        );
        assertAcked(
            client(coordinator).admin()
                .indices()
                .preparePutMapping(FIRST)
                .setSource(
                    Map.of(
                        "runtime",
                        Map.of(
                            "@timestamp",
                            Map.of(
                                "type",
                                "date",
                                "script",
                                Map.of(
                                    "lang",
                                    "sequence_failure",
                                    "source",
                                    deterministic ? "timestamp" : "changing",
                                    "params",
                                    Map.of("millis", BASE.toEpochMilli())
                                )
                            )
                        )
                    )
                )
        );
        SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.set(0);
        EsqlQueryRequest request = syncEsqlQueryRequest("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A" | KEEP key,_id),
            (FROM %s METADATA _id | WHERE phase=="B" | KEEP key,_id)
            | EVAL other_id=MEMBER(2,_id) | KEEP key,_id,other_id | LIMIT 1
            """.formatted(left, right)).allowPartialResults(false);
        List<CheckedRunnable<Exception>> mutations = List.of(
            () -> assertAcked(
                client(coordinator).admin()
                    .indices()
                    .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .removeAlias(FIRST, left)
                    .removeAlias(SECOND, right)
                    .addAlias(SECOND, left)
                    .addAlias(FIRST, right)
            )
        );
        try (var gate = new Gate(EsqlSearchShardsAction.NAME)) {
            List<List<Object>> actual = runGated(request, gate, mutations);
            assertEquals(List.of(), actual);
            assertEquals(Set.of(FIRST, SECOND), decodedIndices(gate));
            System.out.println(
                "SEQUENCE_CROSSED_SCOPE_POLICY deterministic="
                    + deterministic
                    + " rows=empty native_indices="
                    + decodedIndices(gate)
                    + " value_evaluations="
                    + SequenceMappedFieldsIT.FailingRuntimeFieldPlugin.EXECUTIONS.get()
            );
            assertEquals(List.of(left, right), gate.openIndices.get());
            assertFalse(gate.completeScopes.isEmpty());
            assertTrue(gate.completeScopes.stream().allMatch(scope -> scope.equals(List.of(left, right))));
        }
    }

    /** A newly discovered foreign index is not part of either final analyzed read set, even when its mapping is incompatible. */
    public void testForeignScopeCreatedAfterResolutionDoesNotRetainAReader() throws Exception {
        createPair(false);
        try (var gate = new Gate(EsqlSearchShardsAction.NAME)) {
            assertEquals(List.of(List.of("shared", "A", "B")), runGated(request("sequence-pit-read-*", false), gate, List.of(() -> {
                createIndex(FOREIGN, "keyword", Map.of());
                seed(FOREIGN, "F", "not-a-date", "A", Map.of());
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

    /** Retargeting an original alias cannot turn a physically empty read intersection into an all-index open. */
    public void testAliasRetargetAfterResolutionKeepsLogicalOpenWithZeroContexts() throws Exception {
        createPair(false);
        createIndex(FOREIGN, "keyword", Map.of());
        seed(FOREIGN, "F", "not-a-date", "A", Map.of());
        assertAcked(
            client(coordinator).admin()
                .indices()
                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(FIRST, ALIAS)
                .addAlias(SECOND, ALIAS)
        );
        try (var gate = new Gate(EsqlSearchShardsAction.NAME)) {
            assertEquals(
                List.of(),
                runGated(
                    request(ALIAS, false),
                    gate,
                    List.of(
                        () -> assertAcked(
                            client(coordinator).admin()
                                .indices()
                                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                                .removeAlias(FIRST, ALIAS)
                                .removeAlias(SECOND, ALIAS)
                                .addAlias(FOREIGN, ALIAS)
                        )
                    )
                )
            );
            assertEquals(List.of(ALIAS), gate.openIndices.get());
            assertEquals(Set.of(), decodedIndices(gate));
            assertEquals(1, gate.opened.get().total());
            assertEquals(1, gate.opened.get().skipped());
            assertTrue(gate.openFilter.get().contains("match_none"));
            assertTrue(gate.completeScopes.stream().allMatch(scope -> scope.equals(List.of(ALIAS))));
            assertFalse(gate.completeScopes.isEmpty());
        }
    }

    /** An unmatched pure runtime metadata definition at open must not irreversibly remove a physically eligible read index. */
    public void testRuntimeIndexShadowBeforeOpenCanChangeBeforeFirstCandidate() throws Exception {
        createPair(false);
        for (String index : List.of(FIRST, SECOND)) {
            runtime(index, "_index", "unmatched");
        }
        assertEquals(List.of(), ordinaryIndexFilter(null));
        var opened = client(coordinator).execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(FIRST, SECOND).keepAlive(KEEP_ALIVE).allowPartialSearchResults(false)
        ).actionGet();
        var control = new Pit(new BytesArray(BytesRef.deepCopyOf(opened.getPointInTimeId().toBytesRef())));
        Throwable primary = null;
        try {
            assertEquals(2, SearchContextId.decode(nodeRegistry(), control.id).shards().size());
            List<List<Object>> actual;
            Set<String> nativeIndices;
            try (var gate = new Gate(TransportSearchAction.NAME)) {
                actual = runGated(request(FIRST + "," + SECOND, false), gate, List.of(() -> {
                    runtime(FIRST, "_index", FIRST);
                    runtime(SECOND, "_index", SECOND);
                }));
                nativeIndices = decodedIndices(gate);
            }
            assertEquals(List.of(FIRST + "/A", SECOND + "/B"), ordinaryIndexFilter(control));
            assertEquals(List.of(List.of("shared", "A", "B")), actual);
            assertEquals(Set.of(FIRST, SECOND), nativeIndices);
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                client(coordinator).execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(control.id)).actionGet();
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

    /** A timestamp mapping changed after PIT open can make a previously out-of-range source eligible. */
    public void testRequestTimeFilterKeepsRuntimeTimestampChangeAfterPitOpen() throws Exception {
        coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        createIndex(FIRST, "date", Map.of("marker", Map.of("type", "keyword")));
        createIndex(SECOND, "date", Map.of("only_b", Map.of("type", "long")));
        seed(FIRST, "A", BASE.plusSeconds(10).toString(), "A", Map.of("marker", "present"));
        seed(SECOND, "B", BASE.toString(), "B", Map.of("only_b", 7L));
        refresh(FIRST, SECOND);
        // A WRITE-only block forces PIT can-match at two shards while allowing the later mapping update.
        assertAcked(indicesAdmin().prepareUpdateSettings(FIRST).setSettings(Settings.builder().put("index.blocks.write", true)));
        var query = request(FIRST + "," + SECOND, true).filter(
            org.elasticsearch.index.query.QueryBuilders.rangeQuery("@timestamp")
                .gte(BASE.plusSeconds(9).toString())
                .lt(BASE.plusSeconds(12).toString())
        );
        try (var gate = new Gate(TransportSearchAction.NAME)) {
            assertEquals(List.of(Arrays.asList("shared", "A", "B", null, 7L)), runGated(query, gate, List.of(() -> {
                Opened opened = gate.opened.get();
                assertNotNull("the native PIT open must complete before the first candidate search", opened);
                logger.info("SEQUENCE G7 PIT open total={} skipped={}", opened.total(), opened.skipped());
                assertEquals("the baseline must retain both populated shard contexts", 2, opened.total());
                assertAcked(
                    client(coordinator).admin()
                        .indices()
                        .preparePutMapping(SECOND)
                        .setSource(
                            Map.of(
                                "runtime",
                                Map.of(
                                    "@timestamp",
                                    Map.of(
                                        "type",
                                        "date",
                                        "script",
                                        Map.of(
                                            "lang",
                                            "sequence_failure",
                                            "source",
                                            "timestamp",
                                            "params",
                                            Map.of("millis", BASE.plusSeconds(11).toEpochMilli())
                                        )
                                    )
                                )
                            )
                        )
                );
            })));
            assertEquals("the baseline scoped-index hint must skip neither populated shard", 0, gate.opened.get().skipped());
            assertEquals(Set.of(FIRST, SECOND), decodedIndices(gate));
            String expectedFilter = Strings.toString(query.filter());
            int filtered = -1;
            int unfiltered = -1;
            for (int attempt = 0; attempt < gate.fieldCaps.size(); attempt++) {
                FieldCapsCall call = gate.fieldCaps.get(attempt);
                if (Set.copyOf(call.indices()).equals(Set.of(FIRST, SECOND)) == false) {
                    continue;
                }
                if (expectedFilter.equals(call.filter())) {
                    filtered = attempt;
                } else if (call.filter() == null && filtered >= 0) {
                    unfiltered = attempt;
                    break;
                }
            }
            assertTrue("time-filtered field caps must be observed: " + gate.fieldCaps, filtered >= 0);
            assertTrue("the later unfiltered retry must be observed: " + gate.fieldCaps, unfiltered > filtered);
        }
    }

    /** DEFAULT must use the final unfiltered retry relation while an unsupported live filter remains a paging predicate. */
    public void testFieldCapsRetryKeepsLaterIndexWhoseLiveFilterChangesAfterOpen() throws Exception {
        createPair(true);
        var request = request(FIRST + "," + SECOND, true).filter(org.elasticsearch.index.query.QueryBuilders.existsQuery("marker"));
        try (var gate = new Gate(TransportSearchAction.NAME)) {
            assertEquals(
                List.of(Arrays.asList("shared", "A", "B", null, 7L)),
                runGated(request, gate, List.of(() -> runtime(SECOND, "marker", "present")))
            );
            assertEquals(Set.of(FIRST, SECOND), decodedIndices(gate));
            assertFalse(gate.openFilter.get().contains("marker"));
            assertTrue(gate.pageQueries.stream().anyMatch(query -> query.contains("marker")));
            String expectedFilter = Strings.toString(org.elasticsearch.index.query.QueryBuilders.existsQuery("marker"));
            int filtered = -1;
            int unfiltered = -1;
            for (int attempt = 0; attempt < gate.fieldCaps.size(); attempt++) {
                FieldCapsCall call = gate.fieldCaps.get(attempt);
                if (Set.copyOf(call.indices()).equals(Set.of(FIRST, SECOND)) == false) {
                    continue;
                }
                if (expectedFilter.equals(call.filter())) {
                    filtered = attempt;
                } else if (call.filter() == null && filtered >= 0) {
                    unfiltered = attempt;
                    break;
                }
            }
            System.out.println("SEQUENCE_PIT_FIELD_CAPS " + gate.fieldCaps);
            assertTrue("actual initial marker-filtered field caps must be observed: " + gate.fieldCaps, filtered >= 0);
            assertTrue("a later actual unfiltered request for the same source must be observed: " + gate.fieldCaps, unfiltered > filtered);
        }
    }

    private static EsqlQueryRequest request(String source, boolean extra) {
        String field = extra ? ",only_b" : "";
        String output = extra ? ",other_value=MEMBER(2,only_b) | KEEP key,_id,other_id,only_b,other_value" : " | KEEP key,_id,other_id";
        return syncEsqlQueryRequest("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A" | KEEP key,_id%s),
            (FROM %s METADATA _id | WHERE phase=="B" | KEEP key,_id%s)
            | EVAL other_id=MEMBER(2,_id)%s | LIMIT 1
            """.formatted(source, field, source, field, output)).allowPartialResults(false);
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

    private void createPair(boolean extra) {
        coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        createIndex(FIRST, "date", extra ? Map.of("marker", Map.of("type", "keyword")) : Map.of());
        createIndex(SECOND, "date", extra ? Map.of("only_b", Map.of("type", "long")) : Map.of());
        seed(FIRST, "A", BASE.toString(), "A", extra ? Map.of("marker", "present") : Map.of());
        seed(SECOND, "B", BASE.plusMillis(1).toString(), "B", extra ? Map.of("only_b", 7L) : Map.of());
        refresh(FIRST, SECOND);
    }

    private void createIndex(String index, String timeType, Map<String, Object> additional) {
        assertFalse("fixture must not own an existing namespace", indexExists(index));
        owned.add(index);
        var properties = new LinkedHashMap<String, Object>(additional);
        properties.putAll(
            Map.of("@timestamp", Map.of("type", timeType), "key", Map.of("type", "keyword"), "phase", Map.of("type", "keyword"))
        );
        assertAcked(
            client(coordinator).admin()
                .indices()
                .prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(Map.of("properties", properties))
        );
    }

    private void seed(String index, String id, String timestamp, String phase, Map<String, Object> extra) {
        var source = new LinkedHashMap<String, Object>(extra);
        source.putAll(Map.of("@timestamp", timestamp, "key", "shared", "phase", phase));
        client(coordinator).prepareIndex(index).setId(id).setSource(source).get();
    }

    private void runtime(String index, String field, String value) {
        assertAcked(
            client(coordinator).admin()
                .indices()
                .preparePutMapping(index)
                .setSource(
                    Map.of(
                        "runtime",
                        Map.of(
                            field,
                            Map.of(
                                "type",
                                "keyword",
                                "script",
                                Map.of("lang", "sequence_pit", "source", "constant", "params", Map.of("value", value))
                            )
                        )
                    )
                )
        );
    }

    private List<String> ordinaryIndexFilter(Pit token) {
        var source = new SearchSourceBuilder().query(termsQuery("_index", List.of(FIRST, SECOND))).size(10).fetchSource(false);
        var request = new SearchRequest().source(source).allowPartialSearchResults(false);
        if (token == null) {
            request.indices(FIRST, SECOND);
        } else {
            source.pointInTimeBuilder(new PointInTimeBuilder(token.id).setKeepAlive(KEEP_ALIVE));
            request.setPreFilterShardSize(Integer.MAX_VALUE);
        }
        var response = client(coordinator).search(request).actionGet();
        try {
            if (token != null && response.pointInTimeId() != null) {
                token.id = new BytesArray(BytesRef.deepCopyOf(response.pointInTimeId().toBytesRef()));
            }
            assertFalse(response.isTimedOut());
            assertEquals(0, response.getFailedShards());
            return Arrays.stream(response.getHits().getHits()).map(hit -> hit.getIndex() + "/" + hit.getId()).sorted().toList();
        } finally {
            response.decRef();
        }
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

    /** Copies the actual wrapped field-caps request so retry assertions do not infer attempts from final output. */
    private record FieldCapsCall(List<String> indices, String filter) {}

    /** Keeps the last token from the ordinary control so cleanup also covers a renewed or relocated reader. */
    private static final class Pit {
        private BytesReference id;

        private Pit(BytesReference id) {
            this.id = id;
        }
    }

    /** Owns only paused action continuations; closing resumes every installed continuation before query draining. */
    private static final class Gate implements AutoCloseable {
        private static final AtomicReference<Gate> ACTIVE = new AtomicReference<>();
        private final List<Pause> pauses;
        private final AtomicInteger phase = new AtomicInteger();
        private final AtomicReference<Opened> opened = new AtomicReference<>();
        private final AtomicReference<List<String>> openIndices = new AtomicReference<>();
        private final AtomicReference<String> openFilter = new AtomicReference<>();
        private final List<Integer> pagePrefilters = new CopyOnWriteArrayList<>();
        private final List<String> pageQueries = new CopyOnWriteArrayList<>();
        private final List<List<String>> completeScopes = new CopyOnWriteArrayList<>();
        private final List<FieldCapsCall> fieldCaps = new CopyOnWriteArrayList<>();

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
    public static class PitProbePlugin extends Plugin implements ActionPlugin, ScriptPlugin {
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
                    if (request instanceof EsqlResolveFieldsRequest resolve) {
                        var caps = resolve.fieldCapsRequest();
                        gate.fieldCaps.add(
                            new FieldCapsCall(
                                List.copyOf(Arrays.asList(caps.indices())),
                                caps.indexFilter() == null ? null : Strings.toString(caps.indexFilter())
                            )
                        );
                    } else if (request instanceof OpenPointInTimeRequest open) {
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
                        gate.completeScopes.add(List.copyOf(Arrays.asList(search.getPointInTimeAuthorizationIndices().indices())));
                        if (search.source() != null && search.source().size() != 0) {
                            gate.pagePrefilters.add(search.getPreFilterShardSize());
                            if (search.source().query() != null) {
                                gate.pageQueries.add(Strings.toString(search.source().query()));
                            }
                        }
                    }
                    boolean candidate = request instanceof SearchRequest search
                        && search.source() != null
                        && search.source().sorts() != null
                        && search.source().sorts().size() == 2;
                    Pause pause = action.equals(TransportSearchAction.NAME) && candidate == false ? null : gate.claim(action);
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

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "sequence_pit";
                }

                @Override
                public <T> T compile(String name, String source, ScriptContext<T> context, Map<String, String> params) {
                    if (context != StringFieldScript.CONTEXT || source.equals("constant") == false) {
                        throw new IllegalArgumentException("unsupported PIT qualification script/context");
                    }
                    var factory = new StringFieldScript.Factory() {
                        @Override
                        public boolean isResultDeterministic() {
                            return true;
                        }

                        @Override
                        public StringFieldScript.LeafFactory newFactory(
                            String fieldName,
                            Map<String, Object> parameters,
                            SearchLookup lookup,
                            OnScriptError onScriptError
                        ) {
                            return leaf -> new StringFieldScript(fieldName, parameters, lookup, onScriptError, leaf) {
                                @Override
                                public void execute() {
                                    emit((String) parameters.get("value"));
                                }
                            };
                        }
                    };
                    return context.factoryClazz.cast(factory);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(StringFieldScript.CONTEXT);
                }
            };
        }
    }
}

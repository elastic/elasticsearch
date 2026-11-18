/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;

/** Exercises reader liveness and time bounds when an unused start survives only through a remembered middle row. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceKeptStartReaderIT extends AbstractSequenceIntegTestCase {
    private static final String A = "sequence-kept-start-a";
    private static final String B = "sequence-kept-start-b";
    private static final String C = "sequence-kept-start-c";
    private static final String D = "sequence-kept-start-d";
    private static final List<String> INDICES = List.of(A, B, C, D);
    private static final Instant BASE = Instant.parse("2026-11-16T12:00:00Z");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(TrackingEsqlPlugin.class);
        return plugins;
    }

    /** Uses ordinary source searches and listeners, recording detached request and response identities for the witness. */
    public static class TrackingEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            var filters = new ArrayList<>(super.getActionFilters());
            filters.add(new ActionFilter() {
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
                    CandidateRequests capture = CandidateRequests.ACTIVE.get();
                    if (capture != null
                        && request instanceof SearchRequest search
                        && search.getPointInTimeAuthorizationScope() != null
                        && search.source() != null
                        && search.source().sorts() != null
                        && search.source().sorts().size() == 2
                        && search.source().sorts().getFirst() instanceof FieldSortBuilder temporal) {
                        Object[] after = search.source().searchAfter();
                        var recorded = new CandidateRequest(
                            List.of(search.getPointInTimeAuthorizationScope().indices()),
                            Strings.toString(search.source().query()),
                            after == null ? List.of() : Collections.unmodifiableList(Arrays.asList(after.clone())),
                            temporal.getNumericType(),
                            capture.order.incrementAndGet()
                        );
                        capture.requests.add(recorded);
                        chain.proceed(task, action, request, ActionListener.wrap(response -> {
                            if (response instanceof SearchResponse found) {
                                recorded.response = new CandidateResponse(
                                    Arrays.stream(found.getHits().getHits()).map(hit -> hit.getId()).toList(),
                                    capture.order.incrementAndGet()
                                );
                            }
                            listener.onResponse(response);
                        }, listener::onFailure));
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
            return filters;
        }
    }

    /**
     * R2 golden G35: C1 consumes both listed starts by substitution, but B1 still remembers the unused A1.
     * The finite 10ms window contains the entire golden history, so its unbounded-window outcome is unchanged.
     */
    public void testG35RememberedStartKeepsSourceReading() throws Exception {
        create();
        var bulk = client().prepareBulk();
        add(bulk, A, "A1", 0, "k1", 1, 0);
        add(bulk, B, "B1", 1_000_000, "k1", 0, 0);
        add(bulk, A, "A2", 2_000_000, "k1", 2, 0);
        add(bulk, A, "A3", 3_000_000, "k1", 2, 0);
        add(bulk, B, "B2", 4_000_000, "k1", 0, 0);
        add(bulk, C, "C1", 5_000_000, "k1", 2, 0);
        add(bulk, C, "C2", 6_000_000, "k1", 1, 0);
        finishSeed(bulk);

        String query = sequence("", "v == MEMBER(1,v)");
        for (boolean filter : List.of(false, true)) {
            Observed result = observe(query, Settings.builder().put("sequence_key_filter", filter));
            assertEquals(List.of(), result.warnings);
            assertRememberedRows(result);
            assertEquals(List.of(3L, 2L, 2L), result.status.runtime().rowsFetched());
            assertEquals(filter, result.requests.stream().anyMatch(SequenceKeptStartReaderIT::hasKeyFilter));
        }
    }

    /**
     * An idle second key keeps possibleMatch true while a middle continuation must use remembered A1's bound.
     * The first final page already contains C2; exact identities also reject an incorrect fallback through B0.
     */
    public void testTwoKeysRememberedStartBoundsMiddleContinuationWithBufferedFinal() throws Exception {
        assertTwoKeyContinuation(true);
    }

    public void testTwoKeysRememberedStartBoundsMiddleContinuationWithCompleteFallback() throws Exception {
        assertTwoKeyContinuation(false);
    }

    private void assertTwoKeyContinuation(boolean indexedKeys) throws Exception {
        create(indexedKeys);
        var bulk = client().prepareBulk();
        add(bulk, A, "unused", 0, "k2", 0, 0);
        add(bulk, A, "A1", 5_000_000, "k1", 1, 0);
        add(bulk, B, "B0", 6_000_000, "k1", 0, 0);
        add(bulk, A, "A2", 7_000_000, "k1", 2, 0);
        add(bulk, A, "A3", 8_000_000, "k1", 2, 0);
        add(bulk, B, "B2", 9_000_000, "k1", 0, 0);
        add(bulk, C, "C1", 9_500_000, "k1", 2, 0);
        Set<String> firstMiddlePage = new HashSet<>(Set.of("B0", "B2"));
        // These 254 hits survive ordinary stage execution. The correlated arrival predicate rejects them
        // only at merged consumption, after C1 and at the idle key's inclusive 10ms expiry boundary.
        for (int event = 0; event < 254; event++) {
            String id = "bad-" + event;
            firstMiddlePage.add(id);
            add(bulk, B, id, 10_000_000, "k1", 99, 0);
        }
        add(bulk, B, "B1", 12_000_000, "k1", 1, 0);
        add(bulk, C, "C2", 13_000_000, "k1", 1, 1);
        add(bulk, B, "startless", 14_000_000, "no-start", 0, 0);
        finishSeed(bulk);

        String query = sequence("v == 0 OR v == MEMBER(1,v)", "v == MEMBER(1,v) AND a == MEMBER(2,v)");
        Observed off = observe(query);
        Observed on = observe(query, true);
        assertTwoKeyResult(off, firstMiddlePage, false, false);
        assertTwoKeyResult(on, firstMiddlePage, true, indexedKeys);
        assertEquals(off.rows, on.rows);
        assertEquals(off.provenance, on.provenance);
    }

    private static void assertTwoKeyResult(Observed result, Set<String> firstMiddlePage, boolean keyFilter, boolean prunedStartless) {
        // B1 acquires A1 before evicting the still-live B0; B0 then has no holder and is a drop.
        assertEquals(
            List.of("SEQUENCE dropped [1] candidates at the limit of [2] per stage and key; matches may be missing"),
            result.warnings
        );
        assertEquals(1L, result.status.latestValidCounters().candidatesDroppedAtBound());
        assertRememberedRows(result);
        assertEquals(List.of(4L, prunedStartless ? 257L : 258L, 2L), result.status.runtime().rowsFetched());

        List<CandidateRequest> middle = result.requests.stream().filter(r -> r.indices.contains(B)).toList();
        assertEquals("the real 256-hit source page must force exactly one middle continuation", 2, middle.size());
        CandidateRequest first = middle.getFirst();
        CandidateRequest continuation = middle.getLast();
        assertTrue(first.after.isEmpty());
        assertNotNull(first.response);
        assertEquals(256, first.response.ids.size());
        assertEquals(firstMiddlePage, new HashSet<>(first.response.ids));
        assertEquals("date_nanos", continuation.numericType);
        assertEquals(2, continuation.after.size());
        assertEquals(Long.class, continuation.after.getFirst().getClass());
        assertEquals(Math.addExact(Math.multiplyExact(BASE.getEpochSecond(), 1_000_000_000L), 10_000_000L), continuation.after.getFirst());
        assertEquals(Long.class, continuation.after.getLast().getClass());
        assertEquals(List.of(BASE.plusMillis(15).toString()), timestampUppers(continuation.query));
        assertNotNull(continuation.response);
        assertEquals(keyFilter, hasKeyFilter(continuation));
        // This one-shard search sees the same in-window input in both mappings. The startless hit
        // survives only without the filter and when the unindexed mapping rejects the attached predicate.
        assertEquals(prunedStartless ? List.of("B1") : List.of("B1", "startless"), continuation.response.ids);

        List<CandidateRequest> finals = result.requests.stream().filter(r -> r.indices.contains(C)).toList();
        assertEquals(1, finals.size());
        CandidateRequest finalPage = finals.getFirst();
        assertNotNull(finalPage.response);
        assertEquals(List.of("C1", "C2"), finalPage.response.ids);
        assertTrue(
            "C2 must already be fetched before the middle continuation is dispatched",
            finalPage.response.order < continuation.order
        );
        // BASE+15ms also proves the bound was recomputed after C1 consumed A2/A3, whose bound was BASE+18ms.
    }

    private static void assertRememberedRows(Observed result) {
        assertEquals(List.of(List.of("k1", "A3", "B2", "C1"), List.of("k1", "A1", "B1", "C2")), result.rows);
        assertEquals(List.of(provenance("A3", "B2", "C1"), provenance("A1", "B1", "C2")), result.provenance);
        assertEquals(2L, result.status.completedMatches());
        assertEquals(2L, result.status.rowsEmitted());
    }

    public void testExhaustedKeyArrivalWarningCanBePruned() throws Exception {
        create();
        var bulk = client().prepareBulk();
        add(bulk, A, "A1", 0, "spent", 7, 0);
        add(bulk, A, "A2", 500_000, "live", 22, 0);
        add(bulk, B, "B1", 1_000_000, "spent", 7, 0);
        add(bulk, B, "B2", 1_500_000, "live", 22, 0);
        add(bulk, C, "C1", 2_000_000, "spent", 7, 0);
        // B-spare arrives after C1, so the first match cannot consume it by substitution.
        add(bulk, B, "B-spare", 3_000_000, "spent", 8, 0);
        add(bulk, D, "D1", 4_000_000, "spent", 7, 0);
        for (int event = 0; event < 255; event++) {
            add(bulk, C, "fill-" + event, 5_000_000, "live", 99, 0);
        }
        bulk.add(
            prepareIndex(C).setId("C-bad")
                .setSource(Map.of("@timestamp", BASE.plusMillis(6).toString(), "key", "spent", "v", 0, "a", 0, "text", "not-a-number"))
        );
        add(bulk, C, "C2", 7_000_000, "live", 22, 0);
        add(bulk, D, "D2", 8_000_000, "live", 22, 0);
        finishSeed(bulk);
        String query = "SEQUENCE BY key WITHIN 10ms PRIMARY 1 "
            + String.join(", ", stage(A, ""), stage(B, ""), stage(C, "TO_LONG(text) == MEMBER(2,v)"), stage(D, ""))
            + " | EVAL b=MEMBER(2,_id),c=MEMBER(3,_id),d=MEMBER(4,_id)"
            + " | KEEP key,_id,b,c,d,sequence_provenance | LIMIT 100";
        Observed off = observe(query);
        Observed on = observe(query, true);
        List<List<Object>> rows = List.of(List.of("spent", "A1", "B1", "C1", "D1"), List.of("live", "A2", "B2", "C2", "D2"));
        List<List<SequenceProvenance.Member>> provenance = List.of(provenance("A1", "B1", "C1", "D1"), provenance("A2", "B2", "C2", "D2"));
        assertEquals(rows, off.rows);
        assertEquals(rows, on.rows);
        assertEquals(provenance, off.provenance);
        assertEquals(provenance, on.provenance);

        String location = "Line 1:" + (query.indexOf("TO_LONG(text)") + 1) + ": ";
        assertEquals(
            List.of(
                location + "evaluation of [TO_LONG(text)] failed, treating result as null. Only first 20 failures recorded.",
                location + "org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [not-a-number]"
            ),
            off.warnings
        );
        assertEquals(List.of(), on.warnings);
        CandidateRequest offContinuation = thirdStageContinuation(off);
        CandidateRequest onContinuation = thirdStageContinuation(on);
        assertFalse(hasKeyFilter(offContinuation));
        assertTrue(hasKeyFilter(onContinuation));
        assertEquals(List.of("C-bad", "C2"), offContinuation.response.ids);
        assertEquals(List.of("C2"), onContinuation.response.ids);
        logger.info("exhausted key arrival: rows [{}], filter-off warnings [{}], filter-on warnings [{}]", rows, off.warnings, on.warnings);
    }

    private static boolean hasKeyFilter(CandidateRequest request) {
        return request.query.contains("__esql_sequence_waiting_keys_v1");
    }

    private static CandidateRequest thirdStageContinuation(Observed result) {
        List<CandidateRequest> continuations = result.requests.stream()
            .filter(request -> request.indices.contains(C) && request.after.isEmpty() == false)
            .toList();
        assertEquals(1, continuations.size());
        assertNotNull(continuations.getFirst().response);
        return continuations.getFirst();
    }

    private static List<SequenceProvenance.Member> provenance(String... ids) {
        return IntStream.range(0, ids.length).mapToObj(i -> new SequenceProvenance.Member(i + 1, INDICES.get(i), ids[i])).toList();
    }

    /**
     * Both row-dependent conjuncts belong to the same correlated arrival group. False AND a failed cast
     * must retain the ordinary warning even though the bad middle row is never kept or emitted.
     */
    public void testNotKeptArrivalConditionWarningReachesPublicResponse() throws Exception {
        create();
        var bulk = client().prepareBulk();
        add(bulk, A, "A1", 0, "k1", 7, 0);
        bulk.add(
            prepareIndex(B).setId("bad-middle")
                .setSource(Map.of("@timestamp", BASE.plusMillis(1).toString(), "key", "k1", "v", 0, "a", 0, "text", "not-a-number"))
        );
        add(bulk, B, "good-middle", 2_000_000, "k1", 7, 0);
        add(bulk, C, "C1", 3_000_000, "k1", 7, 0);
        finishSeed(bulk);
        String query = sequence("v == MEMBER(1,v) AND TO_LONG(text) == MEMBER(1,v)", "v == MEMBER(1,v)");

        Observed result = observe(query);
        List<List<Object>> expected = List.of(List.of("k1", "A1", "good-middle", "C1"));
        assertEquals(expected, result.rows);
        assertEquals(List.of(1L, 2L, 1L), result.status.runtime().rowsFetched());
        String location = "Line 1:" + (query.indexOf("TO_LONG(text)") + 1) + ": ";
        assertEquals(
            List.of(
                location + "evaluation of [TO_LONG(text)] failed, treating result as null. Only first 20 failures recorded.",
                location + "org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [not-a-number]"
            ),
            result.warnings
        );
        assertEquals(0L, result.status.latestValidCounters().candidatesDroppedAtBound());
        List<CandidateRequest> middle = result.requests.stream().filter(r -> r.indices.contains(B)).toList();
        assertEquals(1, middle.size());
        assertNotNull(middle.getFirst().response);
        assertEquals(List.of("bad-middle", "good-middle"), middle.getFirst().response.ids);

        // Removing a never-kept row preserves alerts and removes precisely the warning it generated.
        client().prepareDelete(B, "bad-middle").get();
        refresh(B);
        Observed withoutRejectedRow = observe(query);
        assertEquals(expected, withoutRejectedRow.rows);
        assertEquals(List.of(), withoutRejectedRow.warnings);
        assertEquals(List.of(1L, 1L, 1L), withoutRejectedRow.status.runtime().rowsFetched());
    }

    private Observed observe(String query) throws Exception {
        return observe(query, false);
    }

    private Observed observe(String query, boolean keyFilter) throws Exception {
        return observe(query, Settings.builder().put("sequence_key_filter", keyFilter));
    }

    private Observed observe(String query, Settings.Builder settings) throws Exception {
        String coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        var request = syncEsqlQueryRequest(query).profile(true)
            .pragmas(new QueryPragmas(settings.put("sequence_max_candidates", 2).build()));
        var future = new PlainActionFuture<Observed>();
        try (var capture = new CandidateRequests()) {
            client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
                try {
                    assertFalse(response.isPartial());
                    List<SequenceSourceStatus> statuses = response.profile()
                        .drivers()
                        .stream()
                        .flatMap(driver -> driver.operators().stream())
                        .map(OperatorStatus::status)
                        .filter(SequenceSourceStatus.class::isInstance)
                        .map(SequenceSourceStatus.class::cast)
                        .toList();
                    assertEquals("must execute the native source operator", 1, statuses.size());
                    SequenceSourceStatus status = statuses.getFirst();
                    assertNotNull(status.runtime());
                    var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
                    List<String> warnings = context.getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(w -> HeaderWarning.extractWarningValueFromWarningHeader(w, false))
                        .filter(w -> w.startsWith("Line ") || w.startsWith("SEQUENCE "))
                        .toList();
                    List<List<Object>> values = getValuesList(response);
                    List<List<Object>> rows = values.stream().map(row -> row.subList(0, row.size() - 1)).toList();
                    List<List<SequenceProvenance.Member>> provenance = values.stream()
                        .map(row -> SequenceProvenance.decode(row.getLast()))
                        .toList();
                    assertTrue("output cap reached", rows.size() < 100);
                    future.onResponse(new Observed(rows, provenance, status, List.copyOf(capture.requests), warnings));
                } catch (Exception e) {
                    future.onFailure(e);
                } catch (AssertionError e) {
                    future.onFailure(new IllegalStateException(e));
                }
            }, future::onFailure));
            return future.actionGet(TimeValue.timeValueMinutes(2));
        } finally {
            assertReleased();
        }
    }

    private static String sequence(String middleCondition, String finalCondition) {
        return "SEQUENCE BY key WITHIN 10ms PRIMARY 1 "
            + String.join(", ", stage(A, ""), stage(B, middleCondition), stage(C, finalCondition))
            + " | EVAL middle_id=MEMBER(2,_id),final_id=MEMBER(3,_id)"
            + " | KEEP key,_id,middle_id,final_id,sequence_provenance | LIMIT 100";
    }

    private static String stage(String index, String condition) {
        return "(FROM " + index + " METADATA _id" + (condition.isEmpty() ? "" : " | WHERE " + condition) + " | KEEP key,_id,v,a,text)";
    }

    private void create() {
        create(true);
    }

    private void create(boolean indexedKeys) {
        for (String index : INDICES) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(1, 0).put("index.mode", "standard"))
                    .setMapping(
                        "@timestamp",
                        "type=date_nanos",
                        "key",
                        "type=keyword,index=" + indexedKeys,
                        "v",
                        "type=long",
                        "a",
                        "type=long",
                        "text",
                        "type=keyword"
                    )
            );
        }
        ensureGreen(A, B, C, D);
    }

    private void add(BulkRequestBuilder bulk, String index, String id, long nanos, String key, long value, long argument) {
        bulk.add(
            prepareIndex(index).setId(id)
                .setSource(
                    Map.of(
                        "@timestamp",
                        BASE.plusNanos(nanos).toString(),
                        "key",
                        key,
                        "v",
                        value,
                        "a",
                        argument,
                        "text",
                        Long.toString(value)
                    )
                )
        );
    }

    private void finishSeed(BulkRequestBuilder bulk) {
        var response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        refresh(A, B, C, D);
    }

    private static List<String> timestampUppers(String query) {
        List<String> upper = new ArrayList<>();
        collectTimestampUppers(XContentHelper.convertToMap(JsonXContent.jsonXContent, query, false), upper);
        return List.copyOf(upper);
    }

    private static void collectTimestampUppers(Object value, List<String> upper) {
        if (value instanceof Map<?, ?> map) {
            if (map.get("range") instanceof Map<?, ?> fields && fields.get("@timestamp") instanceof Map<?, ?> range) {
                assertFalse("the source upper bound must be inclusive", range.containsKey("lt"));
                if (range.get("lte") != null) upper.add(range.get("lte").toString());
            }
            map.values().forEach(child -> collectTimestampUppers(child, upper));
        } else if (value instanceof List<?> list) {
            list.forEach(child -> collectTimestampUppers(child, upper));
        }
    }

    private void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals(0L, service.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
            assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME)
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    /** Check ownership before deleting the fixture, including assertion and query-failure paths. */
    @After
    public void cleanup() throws Exception {
        assertReleased();
        for (String index : INDICES) {
            if (indexExists(index)) assertAcked(indicesAdmin().prepareDelete(index));
        }
    }

    private record Observed(
        List<List<Object>> rows,
        List<List<SequenceProvenance.Member>> provenance,
        SequenceSourceStatus status,
        List<CandidateRequest> requests,
        List<String> warnings
    ) {}

    private record CandidateResponse(List<String> ids, long order) {}

    private static final class CandidateRequest {
        private final List<String> indices;
        private final String query;
        private final List<Object> after;
        private final String numericType;
        private final long order;
        private volatile CandidateResponse response;

        private CandidateRequest(List<String> indices, String query, List<Object> after, String numericType, long order) {
            this.indices = indices;
            this.query = query;
            this.after = after;
            this.numericType = numericType;
            this.order = order;
        }
    }

    private static final class CandidateRequests implements AutoCloseable {
        private static final AtomicReference<CandidateRequests> ACTIVE = new AtomicReference<>();
        private final List<CandidateRequest> requests = new CopyOnWriteArrayList<>();
        private final AtomicLong order = new AtomicLong();

        private CandidateRequests() {
            assertTrue(ACTIVE.compareAndSet(null, this));
        }

        @Override
        public void close() {
            assertTrue(ACTIVE.compareAndSet(this, null));
        }
    }
}

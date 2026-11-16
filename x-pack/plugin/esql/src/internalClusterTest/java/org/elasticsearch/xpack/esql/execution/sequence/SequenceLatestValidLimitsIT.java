/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Exercises native predicates, bounded work, warning headers and mandatory profiling on real nodes. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceLatestValidLimitsIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final String PREFIX = "sequence-limits-";

    /** Conditions retain their WHERE-site values through permitted projections, with default or overridden bounds. */
    public void testPrivateOwnValuesThroughProjections() {
        createIndices(2);
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex(PREFIX + "a").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "ALPHA"))
                .add(
                    prepareIndex(PREFIX + "b").setSource(
                        "@timestamp",
                        BASE.plusMillis(1).toString(),
                        "key",
                        "shared",
                        "payload",
                        List.of("miss", "ALPHA")
                    )
                )
                .get()
                .hasFailures()
        );
        refresh(PREFIX + "a", PREFIX + "b");
        for (String projection : List.of("KEEP key,payload", "DROP own", "RENAME own AS renamed | KEEP key,renamed")) {
            boolean renamed = projection.startsWith("RENAME");
            for (Integer bound : new Integer[] { null, 3 }) {
                Observed observed = observe("""
                    SEQUENCE BY key WITHIN 30s PRIMARY 1
                    (FROM sequence-limits-a | EVAL artifact=TO_LOWER(payload) | KEEP key,artifact) AS download,
                    (FROM sequence-limits-b | MV_EXPAND payload | EVAL own=TO_LOWER(payload)
                      | WHERE own == MEMBER(download,artifact) | %s) AS execution
                    | EVAL observed=MEMBER(execution,%s) | KEEP key,artifact,observed | LIMIT 10
                    """.formatted(projection, renamed ? "renamed" : "payload"), bound);
                assertEquals(List.of(List.of("shared", "alpha", renamed ? "alpha" : "ALPHA")), observed.values());
                assertEquals(1, observed.status().completedMatches());
                assertEquals(List.of(), observed.warnings());
            }
        }
        Exception removed = expectThrows(Exception.class, () -> observe("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM sequence-limits-a | KEEP key),(FROM sequence-limits-b | WHERE payload==MEMBER(1,payload))
            | KEEP key | LIMIT 10
            """));
        assertThat(ExceptionsHelper.unwrapCause(removed).getMessage(), containsString("payload"));
    }

    /** Every profile has seven counters; default N=8 drops the ninth start and hidden overrides remain bounded. */
    public void testDefaultEightCandidateBoundAndHiddenOverridesWithoutConditions() {
        createIndices(3);
        var bulk = client().prepareBulk();
        for (int stage = 0; stage < 3; stage++) {
            bulk.add(
                prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                    "@timestamp",
                    BASE.plusMillis(stage).toString(),
                    "key",
                    "shared",
                    "payload",
                    "row" + stage
                )
            );
        }
        for (int row = 0; row < 9; row++) {
            bulk.add(
                prepareIndex(PREFIX + "a").setSource(
                    "@timestamp",
                    BASE.plusMillis(10 + row).toString(),
                    "key",
                    "crowded",
                    "payload",
                    "start" + row
                )
            );
        }
        for (int stage = 1; stage < 3; stage++) {
            bulk.add(
                prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                    "@timestamp",
                    BASE.plusMillis(18 + stage).toString(),
                    "key",
                    "crowded",
                    "payload",
                    "final" + stage
                )
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh(PREFIX + "a", PREFIX + "b", PREFIX + "c");
        List<List<Object>> expected = List.of(List.of("shared", "row0"), List.of("crowded", "start8"));
        for (String suffix : List.of("", " AS completion WITHIN 30 seconds")) {
            Observed observed = observe("""
                SEQUENCE BY key WITHIN 30s PRIMARY 1
                (FROM sequence-limits-a | KEEP key,payload),(FROM sequence-limits-b | KEEP key)%s
                | KEEP key,payload | LIMIT 10
                """.formatted(suffix));
            assertEquals(expected, observed.values());
            assertEquals(1, observed.status().latestValidCounters().candidatesDroppedAtBound());
            assertEquals(1, observed.status().latestValidCounters().keysThatHitBound());
            assertEquals(8, observed.status().latestValidCounters().largestRingFill());
            assertEquals(
                List.of("SEQUENCE dropped [1] candidates at the limit of [8] per stage and key; matches may be missing"),
                observed.warnings()
            );
        }
        String threeStages = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-limits-a | KEEP key,payload),(FROM sequence-limits-b | KEEP key),(FROM sequence-limits-c | KEEP key)
            | KEEP key,payload | LIMIT 10
            """;
        for (int bound : List.of(3, 63, 380)) {
            Observed observed = observe(threeStages, bound);
            assertEquals(expected, observed.values());
            assertEquals(4, observed.status().latestValidCounters().searchSteps());
            assertEquals(bound == 3 ? 6 : 0, observed.status().latestValidCounters().candidatesDroppedAtBound());
            assertEquals(Math.min(bound, 9), observed.status().latestValidCounters().largestRingFill());
            assertEquals(
                bound == 3
                    ? List.of("SEQUENCE dropped [6] candidates at the limit of [3] per stage and key; matches may be missing")
                    : List.of(),
                observed.warnings()
            );
        }
        Exception excessive = expectThrows(Exception.class, () -> observe(threeStages, 1025));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("sequence_max_candidates"));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("1025"));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("1024"));
    }

    /** A single conjunct that reads two earlier stages waits until both values are bound. */
    public void testNativeF4AMultipleReferences() {
        createIndices(3);
        var bulk = client().prepareBulk();
        for (int stage = 0; stage < 3; stage++) {
            bulk.add(
                prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                    "@timestamp",
                    BASE.plusMillis(stage).toString(),
                    "key",
                    "shared",
                    "v",
                    stage + 1,
                    "label",
                    "row" + stage
                )
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh(PREFIX + "a", PREFIX + "b", PREFIX + "c");
        Observed observed = observe("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM sequence-limits-a | KEEP key,v,label),
            (FROM sequence-limits-b | KEEP key,v,label),
            (FROM sequence-limits-c | WHERE v==MEMBER(1,v)+MEMBER(2,v) | KEEP key,label)
            | EVAL middle=MEMBER(2,label),final=MEMBER(3,label) | KEEP label,middle,final | LIMIT 10
            """);
        assertEquals(List.of(List.of("row0", "row1", "row2")), observed.values());
        assertEquals(2, observed.status().latestValidCounters().searchSteps());
        assertEquals(List.of(), observed.warnings());
    }

    /** Frozen F4B grouping emits the oldest matching stage-3 row after exactly 66 unused-candidate visits. */
    public void testNativeF4BExactVisitsAndFourStageCeiling() {
        createIndices(4);
        var bulk = client().prepareBulk();
        for (int stage = 0; stage < 3; stage++) {
            for (int row = 0; row < 64; row++) {
                bulk.add(
                    prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                        "@timestamp",
                        BASE.plusMillis(stage * 64L + row).toString(),
                        "key",
                        "shared",
                        "a",
                        stage == 2 && row == 0 ? 1 : 0,
                        "b",
                        stage < 2 ? 1 : 0,
                        "label",
                        "s" + (stage + 1) + "_" + row
                    )
                );
            }
        }
        bulk.add(
            prepareIndex(PREFIX + "d").setSource(
                "@timestamp",
                BASE.plusMillis(192).toString(),
                "key",
                "shared",
                "a",
                1,
                "b",
                1,
                "label",
                "f"
            )
        );
        assertFalse(bulk.get().hasFailures());
        refresh(PREFIX + "a", PREFIX + "b", PREFIX + "c", PREFIX + "d");
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-limits-a | KEEP key,a,b,label),(FROM sequence-limits-b | KEEP key,a,b,label),
            (FROM sequence-limits-c | KEEP key,a,b,label),
            (FROM sequence-limits-d | WHERE a==MEMBER(3,a) AND b==MEMBER(1,b) | KEEP key,label)
            | EVAL middle=MEMBER(2,label),third=MEMBER(3,label),final=MEMBER(4,label)
            | KEEP label,middle,third,final | LIMIT 10
            """;
        Observed observed = observe(query, 64);
        assertEquals(List.of(List.of("s1_63", "s2_63", "s3_0", "f")), observed.values());
        assertEquals(66, observed.status().latestValidCounters().searchSteps());
        assertEquals(66, observed.status().latestValidCounters().maxSearchStepsPerFinalRow());
        assertEquals(64, observed.status().latestValidCounters().largestRingFill());
        assertEquals(0, observed.status().latestValidCounters().searchesAbandonedAtWorkLimit());
        assertEquals(List.of(), observed.warnings());
        Exception excessive = expectThrows(Exception.class, () -> observe(query, 65));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("sequence_max_candidates pragma [65]"));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("[64]"));
    }

    /** Native condition failures saturate their own allowance while both terminal resource summaries survive. */
    public void testNativeWorkLimitAndIndependentWarningBudgets() {
        createIndices(6);
        var bulk = client().prepareBulk();
        for (int stage = 0; stage < 6; stage++) {
            int perKey = stage == 0 ? 17 : 16;
            for (int key = 0; key < 2; key++) {
                if (stage == 5) {
                    bulk.add(
                        prepareIndex(PREFIX + "f").setSource(
                            "@timestamp",
                            BASE.plusMillis(162 + key).toString(),
                            "key",
                            "key" + key,
                            "payload",
                            "1"
                        )
                    );
                } else {
                    for (int row = 0; row < perKey; row++) {
                        long time = stage == 0 ? key * 17L + row : 34 + (stage - 1) * 32L + key * 16L + row;
                        bulk.add(
                            prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                                "@timestamp",
                                BASE.plusMillis(time).toString(),
                                "key",
                                "key" + key,
                                "payload",
                                stage == 0 ? "bad-" + key + "-" + row : "0"
                            )
                        );
                    }
                }
            }
        }
        assertFalse(bulk.get().hasFailures());
        refresh(PREFIX + "a", PREFIX + "b", PREFIX + "c", PREFIX + "d", PREFIX + "e", PREFIX + "f");
        Observed observed = observe("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM sequence-limits-a | KEEP key,payload),(FROM sequence-limits-b | KEEP key),(FROM sequence-limits-c | KEEP key),
            (FROM sequence-limits-d | KEEP key),(FROM sequence-limits-e | KEEP key),
            (FROM sequence-limits-f | WHERE TO_LONG(payload)==TO_LONG(MEMBER(1,payload)) | KEEP key)
            | KEEP key,payload | LIMIT 10
            """, 16);
        assertEquals(List.of(), observed.values());
        var counters = observed.status().latestValidCounters();
        assertEquals(2, counters.candidatesDroppedAtBound());
        assertEquals(2, counters.keysThatHitBound());
        assertEquals(2, counters.searchesAbandonedAtWorkLimit());
        assertEquals(262144, counters.searchSteps());
        assertEquals(131072, counters.maxSearchStepsPerFinalRow());
        assertEquals(20, observed.warnings().stream().filter(warning -> warning.contains("Cannot parse number [bad-")).count());
        assertEquals(1, observed.warnings().stream().filter(warning -> warning.contains("Only first 20 failures recorded.")).count());
        assertEquals(
            List.of(
                "SEQUENCE dropped [2] candidates at the limit of [16] per stage and key; matches may be missing",
                "SEQUENCE abandoned [2] searches at the work limit of [131072] steps; matches may be missing"
            ),
            observed.warnings().stream().filter(warning -> warning.startsWith("SEQUENCE ")).toList()
        );
    }

    /** Admitted N=8 still exercises the work guard after R6 lowers the seven-stage default; LIMIT closes before the tail. */
    public void testLimitOneKeepsDropAndWorkLimitSummaries() {
        createIndices(7);
        var bulk = client().prepareBulk();
        for (int stage = 0; stage < 6; stage++) {
            int rows = stage == 0 ? 9 : 8;
            for (int row = 0; row < rows; row++) {
                long time = stage == 0 ? row : 9 + (stage - 1) * 8L + row;
                bulk.add(
                    prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                        "@timestamp",
                        BASE.plusMillis(time).toString(),
                        "key",
                        "bad",
                        "v",
                        0,
                        "label",
                        "bad-" + stage + "-" + row
                    )
                );
            }
        }
        bulk.add(
            prepareIndex(PREFIX + "g").setSource("@timestamp", BASE.plusMillis(49).toString(), "key", "bad", "v", 1, "label", "bad-final")
        );
        for (String key : List.of("good", "tail")) {
            for (int stage = 0; stage < 7; stage++) {
                bulk.add(
                    prepareIndex(PREFIX + (char) ('a' + stage)).setSource(
                        "@timestamp",
                        BASE.plusMillis((key.equals("good") ? 60 : 70) + stage).toString(),
                        "key",
                        key,
                        "v",
                        1,
                        "label",
                        key + "-" + (stage + 1)
                    )
                );
            }
        }
        assertFalse(bulk.get().hasFailures());
        refresh(PREFIX + "a", PREFIX + "b", PREFIX + "c", PREFIX + "d", PREFIX + "e", PREFIX + "f", PREFIX + "g");
        Observed observed = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-limits-a | KEEP key,v,label),(FROM sequence-limits-b | KEEP key),
            (FROM sequence-limits-c | KEEP key),(FROM sequence-limits-d | KEEP key),
            (FROM sequence-limits-e | KEEP key),(FROM sequence-limits-f | KEEP key),
            (FROM sequence-limits-g | WHERE v==MEMBER(1,v) | KEEP key,label)
            | KEEP key,label | LIMIT 1
            """, 8);
        assertEquals(List.of(List.of("good", "good-1")), observed.values());
        assertEquals(1, observed.status().completedMatches());
        assertEquals(1, observed.status().rowsEmitted());
        var counters = observed.status().latestValidCounters();
        assertEquals(1, counters.candidatesDroppedAtBound());
        assertEquals(1, counters.keysThatHitBound());
        assertEquals(1, counters.searchesAbandonedAtWorkLimit());
        assertEquals(131078, counters.searchSteps());
        assertEquals(131072, counters.maxSearchStepsPerFinalRow());
        assertEquals(8, counters.largestRingFill());
        assertEquals(
            List.of(
                "SEQUENCE dropped [1] candidates at the limit of [8] per stage and key; matches may be missing",
                "SEQUENCE abandoned [1] searches at the work limit of [131072] steps; matches may be missing"
            ),
            observed.warnings()
        );
    }

    /** A nanosecond later event compares against an earlier date field through typed mandatory condition values. */
    public void testDateAndDateNanosConditionValues() {
        createIndices(2, "date", "date_nanos");
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex(PREFIX + "a").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex(PREFIX + "b").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh(PREFIX + "a", PREFIX + "b");
        Observed observed = observe("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM sequence-limits-a | KEEP key,@timestamp),
            (FROM sequence-limits-b | WHERE @timestamp>MEMBER(1,@timestamp) | KEEP key)
            | KEEP key | LIMIT 10
            """);
        assertEquals(List.of(List.of("shared")), observed.values());
        assertEquals(1, observed.status().latestValidCounters().searchSteps());
        assertEquals(List.of(), observed.warnings());
    }

    private Observed observe(String query) {
        return observe(query, null);
    }

    private Observed observe(String query, Integer candidateBound) {
        var coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var future = new PlainActionFuture<Observed>();
        var pragmas = Settings.builder().put("page_size", 17);
        if (candidateBound != null) {
            pragmas.put("sequence_max_candidates", candidateBound);
        }
        var request = syncEsqlQueryRequest(query).profile(true).pragmas(new QueryPragmas(pragmas.build()));
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            var threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                .getThreadPool()
                .getThreadContext();
            List<String> warnings = threadContext.getResponseHeaders()
                .getOrDefault("Warning", List.of())
                .stream()
                .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                .filter(warning -> warning.startsWith("Line ") || warning.startsWith("SEQUENCE "))
                .toList();
            List<SequenceSourceStatus> statuses = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            if (statuses.size() != 1) {
                future.onFailure(new IllegalStateException("expected one native sequence status, received [" + statuses.size() + "]"));
                return;
            }
            future.onResponse(new Observed(getValuesList(response), statuses.getFirst(), warnings));
        }, future::onFailure));
        Observed observed = future.actionGet(DEFAULT_REQUEST_TIMEOUT);
        assertNotNull(observed.status().latestValidCounters());
        String profile = Strings.toString(observed.status());
        for (String counter : List.of(
            "candidates_dropped_at_bound",
            "keys_that_hit_bound",
            "searches_abandoned_at_work_limit",
            "search_steps",
            "max_search_steps_per_final_row",
            "condition_evaluations",
            "largest_ring_fill"
        )) {
            assertThat(profile, containsString("\"" + counter + "\""));
        }
        assertFalse(profile.contains("candidates_used_without_report"));
        assertFalse(profile.contains("condition_false_or_null"));
        return observed;
    }

    private void createIndices(int stages, String... timestampTypes) {
        for (int stage = 0; stage < stages; stage++) {
            assertAcked(
                indicesAdmin().prepareCreate(PREFIX + (char) ('a' + stage))
                    .setSettings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping(
                        "@timestamp",
                        "type=" + (timestampTypes.length == 0 ? "date" : timestampTypes[stage]),
                        "key",
                        "type=keyword",
                        "payload",
                        "type=keyword",
                        "label",
                        "type=keyword",
                        "v",
                        "type=long",
                        "a",
                        "type=long",
                        "b",
                        "type=long"
                    )
            );
        }
    }

    /** No PIT, block or request-breaker ownership may outlive a completed or rejected public query. */
    @After
    public void checkSequenceResourcesReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
        for (CircuitBreakerService breakers : internalCluster().getInstances(CircuitBreakerService.class)) {
            assertBusy(() -> assertEquals(0, breakers.getBreaker(CircuitBreaker.REQUEST).getUsed()));
        }
    }

    /** Scalar snapshots are detached from the response's blocks and coordinator callback lifetime. */
    private record Observed(List<List<Object>> values, SequenceSourceStatus status, List<String> warnings) {}
}

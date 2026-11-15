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

/** Exercises public selection, native predicates, bounded work, warning headers and profiling on real nodes. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceLatestValidLimitsIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final String PREFIX = "sequence-limits-";

    /** A moved condition implicitly selects the same matcher as the explicit option and preserves WHERE-site values. */
    public void testImplicitSelectionAndPrivateOwnValuesThroughProjections() {
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
            for (String options : List.of("", ",\"pairing\":\"latest_valid\",\"max_candidates\":3")) {
                Observed observed = observe("""
                    SEQUENCE BY key WITH {"maxspan":"30s"%s} PRIMARY 1
                    (FROM sequence-limits-a | EVAL artifact=TO_LOWER(payload) | KEEP key,artifact) AS download,
                    (FROM sequence-limits-b | MV_EXPAND payload | EVAL own=TO_LOWER(payload)
                      | WHERE own == MEMBER(download,artifact) | %s) AS execution
                    | EVAL observed=MEMBER(execution,%s) | KEEP key,artifact,observed | LIMIT 10
                    """.formatted(options, projection, renamed ? "renamed" : "payload"));
                assertEquals(List.of(List.of("shared", "alpha", renamed ? "alpha" : "ALPHA")), observed.values());
                assertNotNull(observed.status().latestValidCounters());
                assertEquals(1, observed.status().completedMatches());
                assertEquals(List.of(), observed.warnings());
            }
        }
        Exception conflict = expectThrows(Exception.class, () -> observe("""
            SEQUENCE BY key WITH {"maxspan":"30s","pairing":"today"} PRIMARY 1
            (FROM sequence-limits-a),(FROM sequence-limits-b | WHERE payload==MEMBER(1,payload))
            | KEEP key | LIMIT 10
            """));
        assertThat(ExceptionsHelper.unwrapCause(conflict).getMessage(), containsString("today"));
        Exception removed = expectThrows(Exception.class, () -> observe("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM sequence-limits-a | KEEP key),(FROM sequence-limits-b | WHERE payload==MEMBER(1,payload))
            | KEEP key | LIMIT 10
            """));
        assertThat(ExceptionsHelper.unwrapCause(removed).getMessage(), containsString("payload"));
    }

    /** Default, explicit today and a nonbinding incoming limit retain the original profile shape. */
    public void testDefaultProfileAbsenceAndExplicitBoundsWithoutConditions() {
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
        assertFalse(bulk.get().hasFailures());
        refresh(PREFIX + "a", PREFIX + "b", PREFIX + "c");
        for (String suffix : List.of("", " AS completion WITHIN 30 seconds")) {
            Observed observed = observe("""
                SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY 1
                (FROM sequence-limits-a | KEEP key,payload),(FROM sequence-limits-b | KEEP key)%s
                | KEEP key,payload | LIMIT 10
                """.formatted(suffix));
            assertEquals(List.of(List.of("shared", "row0")), observed.values());
            assertNull(observed.status().latestValidCounters());
            assertFalse(Strings.toString(observed.status()).contains("search_steps"));
            assertEquals(List.of(), observed.warnings());
        }
        Observed today = observe("""
            SEQUENCE BY key WITH {"maxspan":"30s","pairing":"today"} PRIMARY 1
            (FROM sequence-limits-a | KEEP key,payload),(FROM sequence-limits-b | KEEP key)
            | KEEP key,payload | LIMIT 10
            """);
        assertNull(today.status().latestValidCounters());
        for (int bound : List.of(3, 63, 380)) {
            Observed latest = observe("""
                SEQUENCE BY key WITH {"maxspan":"30s","pairing":"latest_valid","max_candidates":%s} PRIMARY 1
                (FROM sequence-limits-a | KEEP key,payload),(FROM sequence-limits-b | KEEP key),(FROM sequence-limits-c | KEEP key)
                | KEEP key,payload | LIMIT 10
                """.formatted(bound));
            assertEquals(List.of(List.of("shared", "row0")), latest.values());
            assertEquals(2, latest.status().latestValidCounters().searchSteps());
            assertEquals(0, latest.status().latestValidCounters().candidatesDroppedAtBound());
            assertEquals(List.of(), latest.warnings());
        }
        Exception excessive = expectThrows(Exception.class, () -> observe("""
            SEQUENCE BY key WITH {"maxspan":"30s","pairing":"latest_valid","max_candidates":1025} PRIMARY 1
            (FROM sequence-limits-a),(FROM sequence-limits-b) | KEEP key | LIMIT 10
            """));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("max_candidates [1025]"));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("[1024]"));
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

    /** Frozen F4B grouping emits the oldest matching stage3 row after exactly66 unused-candidate visits. */
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
            SEQUENCE BY key WITH {"maxspan":"30s","max_candidates":64} PRIMARY 1
            (FROM sequence-limits-a | KEEP key,a,b,label),(FROM sequence-limits-b | KEEP key,a,b,label),
            (FROM sequence-limits-c | KEEP key,a,b,label),
            (FROM sequence-limits-d | WHERE a==MEMBER(3,a) AND b==MEMBER(1,b) | KEEP key,label)
            | EVAL middle=MEMBER(2,label),third=MEMBER(3,label),final=MEMBER(4,label)
            | KEEP label,middle,third,final | LIMIT 10
            """;
        Observed observed = observe(query);
        assertEquals(List.of(List.of("s1_63", "s2_63", "s3_0", "f")), observed.values());
        assertEquals(66, observed.status().latestValidCounters().searchSteps());
        assertEquals(66, observed.status().latestValidCounters().maxSearchStepsPerFinalRow());
        assertEquals(64, observed.status().latestValidCounters().largestRingFill());
        assertEquals(0, observed.status().latestValidCounters().searchesAbandonedAtWorkLimit());
        assertEquals(List.of(), observed.warnings());
        Exception excessive = expectThrows(Exception.class, () -> observe(query.replace("\"max_candidates\":64", "\"max_candidates\":65")));
        assertThat(ExceptionsHelper.unwrapCause(excessive).getMessage(), containsString("max_candidates [65]"));
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
            """);
        assertEquals(List.of(), observed.values());
        var counters = observed.status().latestValidCounters();
        assertEquals(2, counters.candidatesDroppedAtBound());
        assertEquals(2, counters.keysThatHitBound());
        assertEquals(2, counters.searchesAbandonedAtWorkLimit());
        assertEquals(262144, counters.searchSteps());
        assertEquals(131072, counters.maxSearchStepsPerFinalRow());
        assertEquals(0, counters.candidatesUsedWithoutReport());
        assertEquals(20, observed.warnings().stream().filter(warning -> warning.contains("Cannot parse number [bad-")).count());
        assertEquals(1, observed.warnings().stream().filter(warning -> warning.contains("Only first 20 failures recorded.")).count());
        assertEquals(
            List.of(
                "SEQUENCE latest_valid dropped [2] candidates at max_candidates [16] across [2] key lifetimes",
                "SEQUENCE latest_valid abandoned [2] final-row searches at the work limit [131072]"
            ),
            observed.warnings().stream().filter(warning -> warning.startsWith("SEQUENCE ")).toList()
        );
    }

    private Observed observe(String query) {
        var coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var future = new PlainActionFuture<Observed>();
        var request = syncEsqlQueryRequest(query).profile(true).pragmas(new QueryPragmas(Settings.builder().put("page_size", 17).build()));
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
        return future.actionGet(DEFAULT_REQUEST_TIMEOUT);
    }

    private void createIndices(int stages) {
        for (int stage = 0; stage < stages; stage++) {
            assertAcked(
                indicesAdmin().prepareCreate(PREFIX + (char) ('a' + stage))
                    .setSettings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping(
                        "@timestamp",
                        "type=date",
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

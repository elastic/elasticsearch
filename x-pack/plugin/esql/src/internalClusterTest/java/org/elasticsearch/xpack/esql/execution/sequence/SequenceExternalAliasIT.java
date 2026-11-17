/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Finding 5 witnesses distinguish an indexed alias from a genuinely computed runtime search. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceExternalAliasIT extends AbstractSequenceIntegTestCase {
    private static final String INDEX = "sequence-external-alias";

    /** Matching and nonmatching terms expose missing exact pushdown; ordinary ES|QL supplies the control. */
    public void testAliasMatchUsesExactSourcePredicates() {
        createEvents();
        for (String term : List.of("start", "absent")) {
            List<List<Object>> expected = term.equals("start") ? List.of(List.of("k", "start")) : List.of();
            String stage = "FROM "
                + INDEX
                + " | EVAL searched=label | WHERE phase==\"A\" AND MATCH(searched,\""
                + term
                + "\") | KEEP key,label";
            try (var ordinary = run(stage + " | LIMIT 10")) {
                assertEquals(expected, getValuesList(ordinary));
            }
            try (
                var sequence = run(
                    "SEQUENCE BY key WITHIN 10s PRIMARY 1 ("
                        + stage
                        + "),"
                        + "(FROM "
                        + INDEX
                        + " | WHERE phase==\"B\" | KEEP key) | KEEP key,label | LIMIT 10"
                )
            ) {
                assertEquals(expected, getValuesList(sequence));
            }
        }
    }

    /** Alias folding must retain the explicit indexed-score refusal rather than silently return filter-context zero. */
    public void testIndexedAliasWithScoreIsRefused() {
        createEvents();
        String stage = "FROM "
            + INDEX
            + " METADATA _score | EVAL searched=label"
            + " | WHERE phase==\"A\" AND MATCH(searched,\"start\") | KEEP key,_score";
        try (var ordinary = run(stage + " | LIMIT 10")) {
            var rows = getValuesList(ordinary);
            assertEquals(1, rows.size());
            assertTrue(((Number) rows.getFirst().get(1)).doubleValue() > 0);
        }
        Exception failure = expectThrows(Exception.class, () -> {
            try (
                var response = run(
                    "SEQUENCE BY key WITHIN 10s PRIMARY 1 ("
                        + stage
                        + "),"
                        + "(FROM "
                        + INDEX
                        + " | WHERE phase==\"B\" | KEEP key) | KEEP key,_score | LIMIT 10"
                )
            ) {
                getValuesList(response);
            }
        });
        Throwable cause = ExceptionsHelper.unwrapCause(failure);
        assertThat(cause, instanceOf(VerificationException.class));
        assertThat(cause.getMessage(), containsString("does not support METADATA [_score]"));
    }

    /** A computation that cannot fold back to a mapped field keeps the already accepted runtime score. */
    public void testComputedRuntimeMatchKeepsOrdinaryScore() {
        createEvents();
        String stage = "FROM "
            + INDEX
            + " METADATA _score | WHERE phase==\"A\""
            + " | EVAL searched=TO_UPPER(label) | WHERE MATCH(searched,\"START\") | KEEP key,_score";
        List<List<Object>> expected = List.of(List.of("k", 1.0));
        try (var ordinary = run(stage + " | LIMIT 10")) {
            assertEquals(expected, getValuesList(ordinary));
        }
        try (
            var sequence = run(
                "SEQUENCE BY key WITHIN 10s PRIMARY 1 ("
                    + stage
                    + "),"
                    + "(FROM "
                    + INDEX
                    + " | WHERE phase==\"B\" | KEEP key) | KEEP key,_score | LIMIT 10"
            )
        ) {
            assertEquals(expected, getValuesList(sequence));
        }
    }

    /** Alias folding must enforce the same OR/NOT placement restriction as the direct mapped-field spelling. */
    public void testIndexedAliasesUnderOrAndNotAreRefused() {
        createEvents();
        for (boolean alias : List.of(false, true)) {
            String field = alias ? "searched" : "label";
            for (String predicate : List.of("MATCH(" + field + ",\"start\") OR label==\"other\"", "NOT MATCH(" + field + ",\"absent\")")) {
                String stage = "FROM "
                    + INDEX
                    + (alias ? " | EVAL searched=label" : "")
                    + " | WHERE phase==\"A\" AND ("
                    + predicate
                    + ") | KEEP key,label";
                try (var ordinary = run(stage + " | LIMIT 10")) {
                    assertEquals(List.of(List.of("k", "start")), getValuesList(ordinary));
                }
                Exception failure = expectThrows(Exception.class, () -> {
                    try (
                        var response = run(
                            "SEQUENCE BY key WITHIN 10s PRIMARY 1 ("
                                + stage
                                + "),(FROM "
                                + INDEX
                                + " | WHERE phase==\"B\" | KEEP key) | KEEP key,label | LIMIT 10"
                        )
                    ) {
                        getValuesList(response);
                    }
                });
                Throwable cause = ExceptionsHelper.unwrapCause(failure);
                assertThat(cause, instanceOf(VerificationException.class));
                assertThat(cause.getMessage(), containsString("exact top-level AND term"));
                assertThat(cause.getMessage(), containsString("OR or NOT outside the search string"));
            }
        }
    }

    /** A real computation retains ordinary runtime boolean search semantics after the placement check. */
    public void testComputedRuntimeSearchUnderOrAndNotRemainsAccepted() {
        createEvents();
        for (String predicate : List.of("MATCH(searched,\"START\") OR searched==\"OTHER\"", "NOT MATCH(searched,\"ABSENT\")")) {
            String stage = "FROM "
                + INDEX
                + " | WHERE phase==\"A\" | EVAL searched=TO_UPPER(label) | WHERE "
                + predicate
                + " | KEEP key,label";
            try (
                var ordinary = run(stage + " | LIMIT 10");
                var sequence = run(
                    "SEQUENCE BY key WITHIN 10s PRIMARY 1 ("
                        + stage
                        + "),(FROM "
                        + INDEX
                        + " | WHERE phase==\"B\" | KEEP key) | KEEP key,label | LIMIT 10"
                )
            ) {
                assertEquals(List.of(List.of("k", "start")), getValuesList(ordinary));
                assertEquals(List.of(List.of("k", "start")), getValuesList(sequence));
            }
        }
    }

    private void createEvents() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "phase", "type=keyword", "label", "type=keyword")
        );
        ensureGreen(INDEX);
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex(INDEX).setId("A")
                        .setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "phase", "A", "label", "start")
                )
                .add(
                    prepareIndex(INDEX).setId("B")
                        .setSource("@timestamp", "2026-01-01T00:00:01Z", "key", "k", "phase", "B", "label", "finish")
                )
                .get()
                .hasFailures()
        );
        refresh(INDEX);
    }

    /** Every successful or rejected query must release its native reader contexts and blocks. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}

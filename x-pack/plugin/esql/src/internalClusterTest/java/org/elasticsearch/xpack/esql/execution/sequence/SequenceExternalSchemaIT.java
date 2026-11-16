/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Findings 2/3 compare materialized columns with the same ordinary schema, not merely their nullable values. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceExternalSchemaIT extends AbstractSequenceIntegTestCase {
    private static final String UNSUPPORTED = "sequence-external-schema-unsupported";
    private static final String LONG = "sequence-external-schema-conflict-long";
    private static final String KEYWORD = "sequence-external-schema-conflict-keyword";
    private static final String CONFLICT = "sequence-external-schema-conflict-*";
    private static final String TEXT = "sequence-external-schema-text";
    private static final String END = "sequence-external-schema-end";

    /** All event times are distinct; only the schema, never event identity or ordering, differs between probes. */
    @Before
    public void createEvents() {
        create(UNSUPPORTED, "bad", Map.of("type", "completion"));
        create(LONG, "bad", Map.of("type", "long"));
        create(KEYWORD, "bad", Map.of("type", "keyword"));
        create(TEXT, "label", Map.of("type", "text", "analyzer", "keyword"));
        create(END, "bad", Map.of("type", "keyword"));
        ensureGreen(UNSUPPORTED, LONG, KEYWORD, TEXT, END);
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex(UNSUPPORTED).setId("unsupported")
                        .setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "bad", "entry")
                )
                .add(prepareIndex(LONG).setId("long").setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "bad", 1L))
                .add(prepareIndex(KEYWORD).setId("keyword").setSource("@timestamp", "2026-01-01T00:00:00.100Z", "key", "k", "bad", "two"))
                .add(prepareIndex(TEXT).setId("text").setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "label", "alpha beta"))
                .add(prepareIndex(END).setId("end").setSource("@timestamp", "2026-01-01T00:00:01Z", "key", "k", "bad", "end"))
                .get()
                .hasFailures()
        );
        refresh(UNSUPPORTED, LONG, KEYWORD, TEXT, END);
    }

    /** PRIMARY use of an unsupported column must retain the ordinary refusal. */
    public void testUnsupportedPrimaryExpressionKeepsOrdinaryRefusal() {
        assertExpressionRefused(UNSUPPORTED, Boundary.PRIMARY, List.of("completion"));
    }

    /** MEMBER output must not turn an unsupported column into an ordinary usable NULL. */
    public void testUnsupportedMemberOutputKeepsOrdinaryRefusal() {
        assertExpressionRefused(UNSUPPORTED, Boundary.MEMBER_OUTPUT, List.of("completion"));
    }

    /** A stage condition has a separate binding site and must retain the same restriction. */
    public void testUnsupportedMemberConditionKeepsOrdinaryRefusal() {
        assertExpressionRefused(UNSUPPORTED, Boundary.MEMBER_CONDITION, List.of("completion"));
    }

    /** A multi-index long/keyword conflict is an unsupported column in ordinary expressions. */
    public void testConflictPrimaryExpressionKeepsOrdinaryRefusal() {
        assertExpressionRefused(CONFLICT, Boundary.PRIMARY, List.of("keyword", "long"));
    }

    /** A conflict cannot become usable merely by passing through MEMBER after the command. */
    public void testConflictMemberOutputKeepsOrdinaryRefusal() {
        assertExpressionRefused(CONFLICT, Boundary.MEMBER_OUTPUT, List.of("keyword", "long"));
    }

    /** The same conflicting field must be refused in the deferred earlier-member condition. */
    public void testConflictMemberConditionKeepsOrdinaryRefusal() {
        assertExpressionRefused(CONFLICT, Boundary.MEMBER_CONDITION, List.of("keyword", "long"));
    }

    /** Projection is allowed to return NULL, but its unsupported type and original_types must remain visible. */
    public void testPrimaryProjectionKeepsUnsupportedAndConflictMetadata() {
        for (String index : List.of(UNSUPPORTED, CONFLICT)) {
            Projection ordinary = project("FROM " + index + " | KEEP bad | LIMIT 10");
            assertEquals("unsupported", ordinary.type());
            assertEquals(index.equals(UNSUPPORTED) ? List.of("completion") : List.of("keyword", "long"), ordinary.originalTypes());
            assertEquals(index.equals(UNSUPPORTED) ? 1 : 2, ordinary.rows().size());
            assertNullCells(ordinary.rows());
            Projection sequence = project(sequence(index, 1, "") + " | KEEP bad | LIMIT 10");
            assertEquals("unsupported", sequence.type());
            assertEquals(ordinary.originalTypes(), sequence.originalTypes());
            assertEquals(1, sequence.rows().size());
            assertNullCells(sequence.rows());
        }
    }

    /** The unsupported mapping fails first in the combined witness, so conflict projection needs an independent observation. */
    public void testConflictProjectionKeepsOriginalTypes() {
        Projection ordinary = project("FROM " + CONFLICT + " | KEEP bad | LIMIT 10");
        assertEquals("unsupported", ordinary.type());
        assertEquals(List.of("keyword", "long"), ordinary.originalTypes());
        assertEquals(2, ordinary.rows().size());
        assertNullCells(ordinary.rows());
        Projection sequence = project(sequence(CONFLICT, 1, "") + " | KEEP bad | LIMIT 10");
        assertEquals("unsupported", sequence.type());
        assertEquals(ordinary.originalTypes(), sequence.originalTypes());
        assertEquals(1, sequence.rows().size());
        assertNullCells(sequence.rows());
    }

    /** MEMBER's alias follows ordinary alias semantics, including an ordinary verification refusal if aliasing is a use. */
    public void testMemberAliasProjectionMatchesOrdinarySchema() {
        for (String index : List.of(UNSUPPORTED, CONFLICT)) {
            String ordinaryQuery = "FROM " + index + " | EVAL observed=bad | KEEP observed | LIMIT 10";
            String sequenceQuery = sequence(index, 2, "") + " | EVAL observed=MEMBER(1,bad) | KEEP observed | LIMIT 10";
            Projection ordinary;
            try {
                ordinary = project(ordinaryQuery);
            } catch (Exception failure) {
                assertThat(ExceptionsHelper.unwrapCause(failure), instanceOf(VerificationException.class));
                verification(sequenceQuery, index.equals(UNSUPPORTED) ? List.of("completion") : List.of("keyword", "long"));
                continue;
            }
            Projection sequence = project(sequenceQuery);
            assertEquals(ordinary.type(), sequence.type());
            assertEquals(ordinary.originalTypes(), sequence.originalTypes());
            assertEquals(1, sequence.rows().size());
            assertNullCells(ordinary.rows());
            assertNullCells(sequence.rows());
        }
    }

    /** The keyword analyzer treats alpha beta as one token; standard runtime analysis would wrongly match alpha. */
    public void testDeclaredAnalyzerSurvivesPrimaryOutput() {
        assertAnalyzer(false);
    }

    /** The same analyzer must cross the separate MEMBER output reference. */
    public void testDeclaredAnalyzerSurvivesMemberOutput() {
        assertAnalyzer(true);
    }

    private void assertExpressionRefused(String index, Boundary boundary, List<String> types) {
        String control = boundary == Boundary.MEMBER_OUTPUT
            ? "FROM " + index + " | EVAL observed=bad | WHERE observed IS NULL | KEEP key | LIMIT 10"
            : "FROM " + index + " | WHERE bad IS NULL | KEEP key | LIMIT 10";
        verification(control, types);
        String query = switch (boundary) {
            case PRIMARY -> sequence(index, 1, "") + " | WHERE bad IS NULL | KEEP key | LIMIT 10";
            case MEMBER_OUTPUT -> sequence(index, 2, "") + " | EVAL observed=MEMBER(1,bad) | WHERE observed IS NULL | KEEP key | LIMIT 10";
            case MEMBER_CONDITION -> sequence(index, 2, " | WHERE MEMBER(1,bad) IS NULL | KEEP key") + " | KEEP key | LIMIT 10";
        };
        verification(query, types);
    }

    private void verification(String query, List<String> types) {
        Exception failure = expectThrows(Exception.class, () -> {
            try (var response = run(query)) {
                getValuesList(response);
            }
        });
        Throwable cause = ExceptionsHelper.unwrapCause(failure);
        assertThat("ordinary-compatible verification for " + query, cause, instanceOf(VerificationException.class));
        assertThat(cause.getMessage(), containsString("bad"));
        for (String type : types) {
            assertThat(cause.getMessage(), containsString(type));
        }
    }

    private static String sequence(String first, int primary, String finalCommands) {
        return "SEQUENCE BY key WITHIN 10s PRIMARY " + primary + " (FROM " + first + "),(FROM " + END + finalCommands + ")";
    }

    private void assertAnalyzer(boolean member) {
        for (String term : List.of("alpha", "alpha beta")) {
            List<List<Object>> expected = term.equals("alpha") ? List.of() : List.of(List.of("k"));
            String ordinary = "FROM "
                + TEXT
                + (member ? " | EVAL observed=label" : "")
                + " | WHERE MATCH("
                + (member ? "observed" : "label")
                + ",\""
                + term
                + "\") | KEEP key | LIMIT 10";
            try (var response = run(ordinary)) {
                assertEquals(expected, getValuesList(response));
            }
            String query = sequence(TEXT, member ? 2 : 1, "")
                + (member ? " | EVAL observed=MEMBER(1,label)" : "")
                + " | WHERE MATCH("
                + (member ? "observed" : "label")
                + ",\""
                + term
                + "\") | KEEP key | LIMIT 10";
            try (var response = run(query)) {
                assertEquals(expected, getValuesList(response));
            }
        }
    }

    private Projection project(String query) {
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            assertEquals(1, response.columns().size());
            // Native response columns use this plugin implementation, which exposes the original ES mapping types.
            ColumnInfoImpl column = response.columns().getFirst();
            return new Projection(
                column.outputType(),
                column.originalTypes() == null ? null : column.originalTypes().stream().sorted().toList(),
                getValuesList(response)
            );
        }
    }

    private static void assertNullCells(List<List<Object>> rows) {
        for (List<Object> row : rows) {
            assertEquals(1, row.size());
            assertNull(row.getFirst());
        }
    }

    private void create(String index, String field, Map<String, Object> mapping) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of("@timestamp", Map.of("type", "date_nanos"), "key", Map.of("type", "keyword"), field, mapping)
                    )
                )
        );
    }

    /** Every independent schema probe leaves no source context or block behind, including rejected expressions. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

    private enum Boundary {
        PRIMARY,
        MEMBER_OUTPUT,
        MEMBER_CONDITION
    }

    private record Projection(String type, List<String> originalTypes, List<List<Object>> rows) {}
}

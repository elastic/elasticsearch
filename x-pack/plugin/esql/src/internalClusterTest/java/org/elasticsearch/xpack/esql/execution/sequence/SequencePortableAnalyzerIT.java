/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** A materialized text column needs one agreed portable analyzer, including through aliases and source unions. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequencePortableAnalyzerIT extends AbstractSequenceIntegTestCase {
    private static final String STANDARD = "sequence-portable-standard";
    private static final String STANDARD_TWO = "sequence-portable-standard-two";
    private static final String WHITE = "sequence-portable-white";
    private static final String WHITE_TWO = "sequence-portable-white-two";

    /** Real mapped analyzers, rather than synthetic attribute declarations, establish the source semantics. */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CommonAnalysisPlugin.class);
    }

    @Before
    public void createEvents() {
        create(STANDARD, "standard", "s");
        create(STANDARD_TWO, "standard", "t");
        create(WHITE, "whitespace", "w");
        create(WHITE_TWO, "whitespace", "v");
        ensureGreen(STANDARD, STANDARD_TWO, WHITE, WHITE_TWO);
        refresh(STANDARD, STANDARD_TWO, WHITE, WHITE_TWO);
    }

    private void create(String index, String analyzer, String key) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    "@timestamp",
                    "type=date_nanos",
                    "key",
                    "type=keyword",
                    "phase",
                    "type=keyword",
                    "message",
                    "type=text,analyzer=" + analyzer
                )
        );
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex(index).setId(key + "-A")
                        .setSource("@timestamp", "2026-01-01T00:00:00Z", "key", key, "phase", "A", "message", "Alpha Beta")
                )
                .add(
                    prepareIndex(index).setId(key + "-B")
                        .setSource("@timestamp", "2026-01-01T00:00:01Z", "key", key, "phase", "B", "message", "finish")
                )
                .get()
                .hasFailures()
        );
    }

    /** Known but different branch analyzers cannot be replaced by the runtime default in either branch order. */
    public void testConflictingMappedAnalyzersAreRefused() {
        for (String[] order : List.of(new String[] { STANDARD, WHITE }, new String[] { WHITE, STANDARD })) {
            String first = union(order[0], order[1], "", "message");
            String sources = order[0] + "," + order[1];
            assertRefused(base(first, sources, 1) + " | WHERE MATCH(message,\"alpha\") | KEEP key | LIMIT 10");
            assertRefused(
                base(first, sources, 2) + " | EVAL observed=MEMBER(1,message) | WHERE MATCH(observed,\"alpha\") | KEEP key | LIMIT 10"
            );
        }
    }

    public void testHighlightExplicitAnalyzerOverridesConflictingUnionAnalyzers() {
        String query = base(union(STANDARD, WHITE, "", "message"), STANDARD + "," + WHITE, 1) + """
             | HIGHLIGHT MATCH(message, "alpha") ON message WITH {"analyzer": "standard"}
            """;
        assertRows(
            query + " | KEEP key,highlight_message | SORT key | LIMIT 10",
            List.of(List.of("s", "<em>Alpha</em> Beta"), List.of("w", "<em>Alpha</em> Beta"))
        );
        assertRefused(query + " | WHERE MATCH(message,\"alpha\") | KEEP key | LIMIT 10");
    }

    /** A direct EVAL copy must preserve whitespace's case sensitivity across PRIMARY and MEMBER output. */
    public void testMappedAnalyzerSurvivesStageEvalAlias() {
        assertAlias(" | EVAL copied=message");
    }

    /** RENAME is the same value under a new binding, not permission to change its analyzer. */
    public void testMappedAnalyzerSurvivesStageRename() {
        assertAlias(" | RENAME message AS copied");
    }

    private void assertAlias(String command) {
        String first = stage(WHITE, command, "copied");
        assertRows(first + " | WHERE MATCH(copied,\"alpha\") | KEEP key | LIMIT 10", List.of());
        assertRows(first + " | WHERE MATCH(copied,\"Alpha\") | KEEP key | LIMIT 10", List.of(List.of("w")));
        for (boolean member : List.of(false, true)) {
            String value = member ? "MEMBER(1,copied)" : "copied";
            String query = base(first, WHITE, member ? 2 : 1) + " | EVAL observed=" + value;
            assertRows(query + " | WHERE MATCH(observed,\"alpha\") | KEEP key | LIMIT 10", List.of());
            assertRows(query + " | WHERE MATCH(observed,\"Alpha\") | KEEP key | LIMIT 10", List.of(List.of("w")));
        }
    }

    /** Equal portable analyzers across real branches remain usable, with their original case behavior. */
    public void testMatchingWhitespaceUnionKeepsAnalyzer() {
        for (String command : List.of("", " | EVAL copied=message")) {
            String field = command.isEmpty() ? "message" : "copied";
            String query = base(union(WHITE, WHITE_TWO, command, field), WHITE + "," + WHITE_TWO, 1) + " | EVAL observed=" + field;
            assertRows(query + " | WHERE MATCH(observed,\"alpha\") | KEEP key | SORT key | LIMIT 10", List.of());
            assertRows(query + " | WHERE MATCH(observed,\"Alpha\") | KEEP key | SORT key | LIMIT 10", List.of(List.of("v"), List.of("w")));
        }
    }

    /** The default analyzer is also an explicit agreeing source contract, rather than a special conflict exemption. */
    public void testMatchingStandardUnionKeepsAnalyzer() {
        String query = base(union(STANDARD, STANDARD_TWO, "", "message"), STANDARD + "," + STANDARD_TWO, 1);
        assertRows(query + " | WHERE MATCH(message,\"alpha\") | KEEP key | SORT key | LIMIT 10", List.of(List.of("s"), List.of("t")));
    }

    /** A branch whose aligned text is only null contributes no analyzer requirement. */
    public void testNullOnlyUnionBranchDoesNotChangeWhitespace() {
        String first = "FROM (" + stage(WHITE, "", "message") + "),(FROM " + WHITE_TWO + " | WHERE phase==\"A\" | KEEP key)";
        String query = base(first, WHITE + "," + WHITE_TWO, 1);
        assertRows(query + " | WHERE MATCH(message,\"alpha\") | KEEP key | SORT key | LIMIT 10", List.of());
        assertRows(query + " | WHERE MATCH(message,\"Alpha\") | KEEP key | SORT key | LIMIT 10", List.of(List.of("w")));
    }

    /** Refusing full-text use does not make the underlying text values or ordinary string operations unsupported. */
    public void testConflictingUnionPlainStringsRemainUsable() {
        String query = base(union(STANDARD, WHITE, "", "message"), STANDARD + "," + WHITE, 1);
        assertRows(
            query + " | EVAL upper=TO_UPPER(message) | KEEP key,message,upper | SORT key | LIMIT 10",
            List.of(List.of("s", "Alpha Beta", "ALPHA BETA"), List.of("w", "Alpha Beta", "ALPHA BETA"))
        );
    }

    /** An explicit analyzer on already materialized values deliberately supplies common runtime semantics. */
    public void testExplicitRuntimeAnalyzerOverridesMaterializedConflict() {
        String query = base(union(STANDARD, WHITE, "", "message"), STANDARD + "," + WHITE, 1);
        assertRows(
            query
                + " | EVAL observed=TO_TEXT(message,{\"analyzer\":\"standard\"})"
                + " | WHERE MATCH(observed,\"alpha\") | KEEP key | SORT key | LIMIT 10",
            List.of(List.of("s"), List.of("w"))
        );
    }

    private static String stage(String index, String command, String field) {
        return "FROM " + index + " | WHERE phase==\"A\"" + command + " | KEEP key," + field;
    }

    private static String union(String left, String right, String command, String field) {
        return "FROM (" + stage(left, command, field) + "),(" + stage(right, command, field) + ")";
    }

    private static String base(String first, String endSources, int primary) {
        return "SEQUENCE BY key WITHIN 10s PRIMARY "
            + primary
            + " ("
            + first
            + "),(FROM "
            + endSources
            + " | WHERE phase==\"B\" | KEEP key)";
    }

    private void assertRows(String query, List<List<Object>> expected) {
        try (var response = run(query)) {
            assertFalse(response.isPartial());
            assertEquals(query, expected, getValuesList(response));
        }
    }

    private void assertRefused(String query) {
        try (var response = run(query)) {
            fail("Expected conflicting-analyzer refusal, got " + getValuesList(response));
        } catch (Exception failure) {
            Throwable cause = ExceptionsHelper.unwrapCause(failure);
            assertThat(cause, instanceOf(VerificationException.class));
            assertThat(cause.getMessage(), containsString("conflicting analyzers"));
            assertThat(cause.getMessage(), containsString("standard"));
            assertThat(cause.getMessage(), containsString("whitespace"));
        }
    }

    /** Both rejected and successful queries release their PIT contexts and pooled blocks. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}

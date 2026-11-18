/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
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

/** Materialization must not silently replace an index-local text analyzer with standard runtime analysis. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceUnknownAnalyzerIT extends AbstractSequenceIntegTestCase {
    private static final String INDEX = "sequence-unknown-analyzer";

    /** The custom analyzer uses the real whitespace tokenizer supplied by the common analysis module. */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CommonAnalysisPlugin.class);
    }

    /** A case-preserving local analyzer cannot be reconstructed from a portable field-capabilities analyzer name. */
    @Before
    public void createEvents() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put(indexSettings(1, 0).build())
                        .put("index.analysis.analyzer.local_text.type", "custom")
                        .put("index.analysis.analyzer.local_text.tokenizer", "whitespace")
                )
                .setMapping(
                    "@timestamp",
                    "type=date_nanos",
                    "key",
                    "type=keyword",
                    "phase",
                    "type=keyword",
                    "message",
                    "type=text,analyzer=local_text"
                )
        );
        ensureGreen(INDEX);
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex(INDEX).setId("A")
                        .setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "phase", "A", "message", "Alpha Beta")
                )
                .add(
                    prepareIndex(INDEX).setId("B")
                        .setSource("@timestamp", "2026-01-01T00:00:01Z", "key", "k", "phase", "B", "message", "finish")
                )
                .get()
                .hasFailures()
        );
        refresh(INDEX);
    }

    /** Direct and stage-aliased PRIMARY text retain the same full-text restriction after output aliases. */
    public void testUnknownAnalyzerPrimaryAndStageAliasRefused() {
        assertFullTextRefused(false);
    }

    /** MEMBER creates its own output binding, which must still trace to the original mapped analyzer. */
    public void testUnknownAnalyzerMemberAndOuterAliasesRefused() {
        assertFullTextRefused(true);
    }

    public void testHighlightExplicitAnalyzerOverridesUnknownMappedAnalyzer() {
        String query = sequence(false, false) + """
             | HIGHLIGHT MATCH(value, "alpha") ON value WITH {"analyzer": "standard"}
            """;
        try (var response = run(query + " | KEEP key,highlight_value | LIMIT 10")) {
            assertFalse(response.isPartial());
            assertEquals(List.of(List.of("k", "<em>Alpha</em> Beta")), getValuesList(response));
        }
        assertAnalyzerRefusal(query + " | WHERE MATCH(value,\"Alpha\") | KEEP key | LIMIT 10");
    }

    private void assertFullTextRefused(boolean member) {
        for (boolean stageAlias : List.of(false, true)) {
            String field = stageAlias ? "copied" : "message";
            try (
                var plain = run(
                    "FROM "
                        + INDEX
                        + " | WHERE phase==\"A\""
                        + (stageAlias ? " | EVAL copied=message" : "")
                        + " | WHERE MATCH("
                        + field
                        + ",\"Alpha\") | KEEP key | LIMIT 10"
                )
            ) {
                assertEquals(List.of(List.of("k")), getValuesList(plain));
            }
            for (String alias : List.of("", " | EVAL searched=value", " | RENAME value AS searched")) {
                String query = sequence(member, stageAlias)
                    + alias
                    + " | WHERE MATCH("
                    + (alias.isEmpty() ? "value" : "searched")
                    + ",\"Alpha\") | KEEP key | LIMIT 10";
                Exception failure = expectThrows(Exception.class, () -> {
                    try (var response = run(query)) {
                        getValuesList(response);
                    }
                });
                Throwable cause = ExceptionsHelper.unwrapCause(failure);
                assertThat(cause, instanceOf(VerificationException.class));
                assertThat(cause.getMessage(), containsString("cannot preserve the mapped analyzer"));
                assertThat(cause.getMessage(), containsString("message"));
            }
        }
    }

    /** Unknown full-text metadata does not make ordinary string values unsupported. */
    public void testUnknownAnalyzerKeepsPlainValuesAndStringFunctions() {
        for (boolean member : List.of(false, true)) {
            for (boolean stageAlias : List.of(false, true)) {
                String query = sequence(member, stageAlias);
                try (
                    var kept = run(query + " | KEEP value | LIMIT 10");
                    var computed = run(query + " | EVAL searched=value | EVAL upper=TO_UPPER(searched) | KEEP upper | LIMIT 10")
                ) {
                    assertEquals(List.of(List.of("Alpha Beta")), getValuesList(kept));
                    assertEquals(List.of(List.of("ALPHA BETA")), getValuesList(computed));
                }
            }
        }
    }

    /** An undeclared TO_TEXT wrapper cannot hide the original mapped analyzer. */
    public void testUnknownAnalyzerThroughUndeclaredTextConversionsRefused() {
        for (String stage : List.of("", " | EVAL copied=TO_TEXT(message)")) {
            String field = stage.isEmpty() ? "message" : "copied";
            String query = "SEQUENCE BY key WITHIN 10s PRIMARY 1 (FROM "
                + INDEX
                + " | WHERE phase==\"A\""
                + stage
                + " | KEEP key,"
                + field
                + "),(FROM "
                + INDEX
                + " | WHERE phase==\"B\" | KEEP key) | EVAL value="
                + field;
            for (String suffix : List.of(
                " | WHERE MATCH(TO_TEXT(value),\"Alpha\")",
                " | EVAL searched=TO_TEXT(value) | WHERE MATCH(searched,\"Alpha\")"
            )) {
                assertAnalyzerRefusal(query + suffix + " | KEEP key | LIMIT 10");
            }
        }
    }

    /** A union stage must inspect each branch's mapped producer rather than trust the merged reference. */
    public void testUnknownAnalyzerThroughUnionStageRefused() {
        String first = "FROM (FROM "
            + INDEX
            + " | WHERE phase==\"A\" | KEEP key,message),"
            + "(FROM "
            + INDEX
            + " | WHERE phase==\"A\" AND key==\"missing\" | KEEP key,message)";
        String query = "SEQUENCE BY key WITHIN 10s PRIMARY 1 ("
            + first
            + "),(FROM "
            + INDEX
            + " | WHERE phase==\"B\" | KEEP key) | WHERE MATCH(message,\"Alpha\") | KEEP key | LIMIT 10";
        assertAnalyzerRefusal(query);
    }

    private void assertAnalyzerRefusal(String query) {
        Exception failure = expectThrows(Exception.class, () -> {
            try (var response = run(query)) {
                getValuesList(response);
            }
        });
        Throwable cause = ExceptionsHelper.unwrapCause(failure);
        assertThat(cause, instanceOf(VerificationException.class));
        assertThat(cause.getMessage(), containsString("cannot preserve the mapped analyzer"));
        assertThat(cause.getMessage(), containsString("message"));
    }

    private static String sequence(boolean member, boolean stageAlias) {
        String field = stageAlias ? "copied" : "message";
        return "SEQUENCE BY key WITHIN 10s PRIMARY "
            + (member ? 2 : 1)
            + " (FROM "
            + INDEX
            + " | WHERE phase==\"A\""
            + (stageAlias ? " | EVAL copied=message" : "")
            + " | KEEP key,"
            + field
            + "),(FROM "
            + INDEX
            + " | WHERE phase==\"B\" | KEEP key)"
            + " | EVAL value="
            + (member ? "MEMBER(1," + field + ")" : field);
    }

    /** Rejected searches and ordinary output both drain their source and pooled blocks. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}

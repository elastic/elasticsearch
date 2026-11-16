/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/** Finding 2's explicit TO_TEXT declaration must survive materialization without becoming an indexed field. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceExternalDeclaredAnalyzerIT extends AbstractSequenceIntegTestCase {
    private static final String INDEX = "sequence-external-declared-analyzer";

    /** Whitespace preserves case, so accepting lowercase alpha would expose an accidental standard-analyzer fallback. */
    public void testToTextAnalyzerSurvivesPrimaryOutput() {
        assertDeclaredAnalyzer(false);
    }

    /** MEMBER uses a distinct binding and aliasing path from the PRIMARY projection. */
    public void testToTextAnalyzerSurvivesMemberOutput() {
        assertDeclaredAnalyzer(true);
    }

    private void assertDeclaredAnalyzer(boolean member) {
        createEvents();
        String first = "FROM "
            + INDEX
            + " | WHERE phase==\"A\""
            + " | EVAL text=TO_TEXT(TO_UPPER(label),{\"analyzer\":\"whitespace\"}) | KEEP key,text";
        for (String term : List.of("ALPHA", "alpha")) {
            List<List<Object>> expected = term.equals("ALPHA") ? List.of(List.of("k")) : List.of();
            String ordinary = first
                + (member ? " | EVAL observed=text" : "")
                + " | WHERE MATCH("
                + (member ? "observed" : "text")
                + ",\""
                + term
                + "\") | KEEP key | LIMIT 10";
            try (var response = run(ordinary)) {
                assertEquals("plain declared-analyzer control", expected, getValuesList(response));
            }
            String sequence = "SEQUENCE BY key WITHIN 10s PRIMARY "
                + (member ? 2 : 1)
                + " ("
                + first
                + "),"
                + "(FROM "
                + INDEX
                + " | WHERE phase==\"B\" | KEEP key)"
                + (member ? " | EVAL observed=MEMBER(1,text)" : "")
                + " | WHERE MATCH("
                + (member ? "observed" : "text")
                + ",\""
                + term
                + "\") | KEEP key | LIMIT 10";
            try (var response = run(sequence)) {
                assertEquals("SEQUENCE keeps the explicit whitespace analyzer", expected, getValuesList(response));
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
                        .setSource("@timestamp", "2026-01-01T00:00:00Z", "key", "k", "phase", "A", "label", "Alpha Beta")
                )
                .add(
                    prepareIndex(INDEX).setId("B").setSource("@timestamp", "2026-01-01T00:00:01Z", "key", "k", "phase", "B", "label", "end")
                )
                .get()
                .hasFailures()
        );
        refresh(INDEX);
    }

    /** Both accepted and rejected match terms must release native readers and blocks. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}

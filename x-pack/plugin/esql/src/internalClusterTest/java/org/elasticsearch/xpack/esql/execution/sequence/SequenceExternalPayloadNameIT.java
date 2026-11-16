/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.junit.After;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/** Finding 9 requires internal payload slots to coexist with a legal dotted user key. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceExternalPayloadNameIT extends AbstractSequenceIntegTestCase {
    private static final String INDEX = "sequence-external-payload-name";

    /** Ordinary dotted access and an unrelated key alias control the same two-document witness. */
    public void testLegalDottedKeyDoesNotCollideWithPayloadName() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    Map.of(
                        "properties",
                        Map.of(
                            "@timestamp",
                            Map.of("type", "date_nanos"),
                            "phase",
                            Map.of("type", "keyword"),
                            "label",
                            Map.of("type", "keyword"),
                            "stage1",
                            Map.of("properties", Map.of("label", Map.of("type", "keyword")))
                        )
                    )
                )
        );
        ensureGreen(INDEX);
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex(INDEX).setId("A")
                        .setSource(
                            Map.of("@timestamp", "2026-01-01T00:00:00Z", "phase", "A", "label", "left", "stage1", Map.of("label", "k"))
                        )
                )
                .add(
                    prepareIndex(INDEX).setId("B")
                        .setSource(
                            Map.of("@timestamp", "2026-01-01T00:00:01Z", "phase", "B", "label", "right", "stage1", Map.of("label", "k"))
                        )
                )
                .get()
                .hasFailures()
        );
        refresh(INDEX);
        List<List<Object>> expected = List.of(List.of("k", "left"));
        try (var ordinary = run("FROM " + INDEX + " | WHERE phase==\"A\" | KEEP stage1.label,label | LIMIT 10")) {
            assertEquals(expected, getValuesList(ordinary));
        }
        String first = "FROM " + INDEX + " | WHERE phase==\"A\"";
        String second = "FROM " + INDEX + " | WHERE phase==\"B\"";
        try (
            var alias = run(
                "SEQUENCE BY safe_key WITHIN 10s PRIMARY 1 ("
                    + first
                    + " | EVAL safe_key=stage1.label | KEEP safe_key,label),("
                    + second
                    + " | EVAL safe_key=stage1.label | KEEP safe_key,label) | KEEP safe_key,label | LIMIT 10"
            )
        ) {
            assertEquals(expected, getValuesList(alias));
        }
        try (
            var sequence = run(
                "SEQUENCE BY stage1.label WITHIN 10s PRIMARY 1 ("
                    + first
                    + " | KEEP stage1.label,label),("
                    + second
                    + " | KEEP stage1.label,label)"
                    + " | KEEP stage1.label,label | LIMIT 10"
            )
        ) {
            assertEquals(expected, getValuesList(sequence));
        }
    }

    /** A failed analysis or successful match must not retain native resources. */
    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.search.SearchHit;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";

    private static final String MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "k":  { "type": "keyword" },
            "l":  { "type": "long" },
            "d":  { "type": "double" },
            "b":  { "type": "boolean" },
            "dt": { "type": "date" },
            "ip": { "type": "ip" }
          }
        }
        """;

    @Before
    public void checkCapability() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testSingleRowTypes() {
        createDestination(DESTINATION);

        assertIntoStats("""
            ROW k = "hello", l = 42::long, d = 1.5, b = true, dt = TO_DATETIME("2026-01-15T12:34:56.789Z"), ip = TO_IP("192.168.1.1")
            | INTO dest-into
            """, 1L, 1L);

        refresh(DESTINATION);
        assertThat(
            queryRows("FROM dest-into | KEEP k, l, d, b, dt, ip"),
            equalTo(List.of(List.of("hello", 42L, 1.5, true, "2026-01-15T12:34:56.789Z", "192.168.1.1")))
        );
    }

    public void testMultipleRows() {
        createDestination(DESTINATION);

        assertIntoStats("""
            ROW k = ["alpha", "beta", "gamma"]
            | MV_EXPAND k
            | EVAL l = LENGTH(k)::long
            | INTO dest-into
            """, 3L, 3L);

        refresh(DESTINATION);
        assertThat(
            queryRows("FROM dest-into | KEEP k, l | SORT k"),
            equalTo(List.of(List.of("alpha", 5L), List.of("beta", 4L), List.of("gamma", 5L)))
        );
    }

    public void testMultiValues() {
        createDestination(DESTINATION);

        assertIntoStats("ROW k = [\"a\", \"b\"], l = [10, 20] | INTO dest-into", 1L, 1L);

        refresh(DESTINATION);
        assertThat(queryRows("FROM dest-into | KEEP k, l"), equalTo(List.of(List.of(List.of("a", "b"), List.of(10L, 20L)))));
    }

    public void testNullColumns() {
        createDestination(DESTINATION);

        assertIntoStats("ROW k = \"only_k\", l = null::long, d = null::double | INTO dest-into", 1L, 1L);

        refresh(DESTINATION);
        assertThat(queryRows("FROM dest-into | KEEP k, l, d"), equalTo(List.of(Arrays.asList("only_k", null, null))));
        var searchResponse = client().prepareSearch(DESTINATION).get();
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(Arrays.asList(hits), hasSize(1));
            assertThat(hits[0].getSourceAsMap(), equalTo(Map.of("k", "only_k")));
        } finally {
            searchResponse.decRef();
        }
    }

    public void testSecondRunAppends() {
        createDestination(DESTINATION);

        assertIntoStats("ROW k = \"first\", l = 1::long | INTO dest-into", 1L, 1L);
        assertIntoStats("ROW k = \"second\", l = 2::long | INTO dest-into", 1L, 1L);

        refresh(DESTINATION);
        assertThat(queryRows("FROM dest-into | KEEP k, l | SORT l"), equalTo(List.of(List.of("first", 1L), List.of("second", 2L))));
    }

    public void testQuotedDestination() {
        String quoted = "dest-into-quoted";
        createDestination(quoted);

        assertIntoStats("ROW k = \"quoted\", l = 7::long | INTO \"dest-into-quoted\"", 1L, 1L);

        refresh(quoted);
        assertThat(queryRows("FROM \"dest-into-quoted\" | KEEP k, l"), equalTo(List.of(List.of("quoted", 7L))));
    }

    public void testZeroRows() {
        createDestination(DESTINATION);

        assertIntoStats("ROW k = \"x\", l = 1::long | WHERE l > 10 | INTO dest-into", 0L, 0L);

        refresh(DESTINATION);
        assertThat(queryRows("FROM dest-into | KEEP k, l"), empty());
    }

    public void testUnmappedColumnUnderStrictMappingFails() {
        createDestination(DESTINATION);

        Exception failure = expectThrows(Exception.class, () -> run("ROW k = \"ok\", unmapped_col = \"nope\" | INTO dest-into").close());
        List<String> messages = new ArrayList<>();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null) {
                messages.add(cause.getMessage());
            }
        }
        assertThat(messages, hasItem(containsString("unmapped_col")));

        refresh(DESTINATION);
        assertThat(queryRows("FROM dest-into | KEEP k"), empty());
    }

    public void testFromSourceRejected() {
        createDestination(DESTINATION);

        Exception failure = expectThrows(Exception.class, () -> run("FROM dest-into | INTO dest-into").close());
        assertThat(failure.getMessage(), containsString("INTO currently only supports ROW as a source"));
    }

    private void createDestination(String indexName) {
        assertAcked(indicesAdmin().prepareCreate(indexName).setMapping(MAPPING));
    }

    private void assertIntoStats(String query, long expectedTotal, long expectedCreated) {
        try (EsqlQueryResponse response = run(query)) {
            assertOk(response);
            assertColumnNames(response.columns(), List.of("total", "created"));
            assertColumnTypes(response.columns(), List.of("long", "long"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(expectedTotal, expectedCreated))));
        }
    }

    private List<List<Object>> queryRows(String query) {
        try (EsqlQueryResponse response = run(query)) {
            assertOk(response);
            return getValuesList(response);
        }
    }
}

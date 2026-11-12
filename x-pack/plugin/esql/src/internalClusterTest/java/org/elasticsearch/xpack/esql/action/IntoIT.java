/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";

    @Before
    public void createDestination() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setMapping("""
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
            """));
    }

    public void testTypes() {
        assertInto(
            "ROW k = \"hello\", l = 42, d = 1.5, b = true, dt = TO_DATETIME(\"2026-01-15T12:34:56.789Z\"), ip = TO_IP(\"192.168.1.1\")",
            1
        );
        assertThat(
            readBack("KEEP k, l, d, b, dt, ip"),
            equalTo(List.of(List.of("hello", 42L, 1.5, true, "2026-01-15T12:34:56.789Z", "192.168.1.1")))
        );
    }

    public void testRowsAndMultiValues() {
        assertInto("ROW k = [\"alpha\", \"beta\"], l = [1, 2] | MV_EXPAND k", 2);
        assertThat(readBack("KEEP k, l | SORT k"), equalTo(List.of(List.of("alpha", List.of(1L, 2L)), List.of("beta", List.of(1L, 2L)))));
    }

    public void testSourceOmitsNullsAndHasDatesInUtc() {
        assertInto(
            "SET time_zone = \"Europe/Paris\"; ROW k = \"only_k\", l = null::long, dt = TO_DATETIME(\"2026-01-15T12:34:56.789Z\")",
            1
        );
        refresh(DESTINATION);
        assertResponse(
            prepareSearch(DESTINATION),
            response -> assertThat(
                Arrays.stream(response.getHits().getHits()).map(SearchHit::getSourceAsMap).toList(),
                equalTo(List.of(Map.of("k", "only_k", "dt", "2026-01-15T12:34:56.789Z")))
            )
        );
    }

    public void testInlineStats() {
        assertInto("ROW k = [\"a\", \"b\"] | MV_EXPAND k | INLINE STATS l = COUNT(*)", 2);
        assertThat(readBack("KEEP k, l | SORT k"), equalTo(List.of(List.of("a", 2L), List.of("b", 2L))));
    }

    public void testZeroRows() {
        assertInto("ROW k = \"x\" | WHERE k == \"y\"", 0);
    }

    public void testRefusedRowFailsTheQuery() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("ROW k = \"ok\", unmapped_col = \"nope\" | INTO " + DESTINATION).close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("unmapped_col"));
    }

    private void assertInto(String producer, long rows) {
        try (EsqlQueryResponse response = run(producer + " | INTO " + DESTINATION)) {
            assertColumnNames(response.columns(), List.of("total", "created"));
            assertColumnTypes(response.columns(), List.of("long", "long"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(rows, rows))));
        }
    }

    private List<List<Object>> readBack(String commands) {
        refresh(DESTINATION);
        try (EsqlQueryResponse response = run("FROM " + DESTINATION + " | " + commands)) {
            return getValuesList(response);
        }
    }
}

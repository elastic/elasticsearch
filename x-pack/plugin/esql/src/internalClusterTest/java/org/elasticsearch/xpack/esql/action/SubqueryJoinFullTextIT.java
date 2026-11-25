/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.VerificationException;
import org.junit.Before;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Full-text functions after a {@code JOIN} on a subquery. The join runs on the coordinator, and its output no longer carries the left
 * side's documents, so a search on an index field must be pushed below the join or rejected. A search on a subquery column scans the
 * values in the page and runs after the join.
 */
public class SubqueryJoinFullTextIT extends AbstractEsqlIntegTestCase {

    /**
     * Short enough that a query which hangs fails the test instead of the suite.
     */
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    @Before
    public void requireJoinOnSubquery() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
    }

    @Before
    public void createIndices() {
        assertAcked(client().admin().indices().prepareCreate("data").setMapping("key", "type=keyword", "country", "type=text"));
        client().prepareBulk()
            .add(prepareIndex("data").setSource(Map.of("key", "us", "country", "United States")))
            .add(prepareIndex("data").setSource(Map.of("key", "uk", "country", "United Kingdom")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertAcked(client().admin().indices().prepareCreate("lookup").setMapping("key", "type=keyword", "info", "type=keyword"));
        client().prepareBulk()
            .add(prepareIndex("lookup").setSource(Map.of("key", "us", "info", "America")))
            .add(prepareIndex("lookup").setSource(Map.of("key", "uk", "info", "Britain")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    /**
     * A search on an index field that stays above the join would run on the coordinator, which has no documents to search, so it is
     * rejected.
     */
    public void testIndexSearchThatStaysAboveTheJoinIsRejected() {
        for (String search : List.of("match(country, \"United States\")", "country : \"United States\"")) {
            String query = String.format(Locale.ROOT, """
                FROM data
                | LEFT OUTER JOIN (FROM lookup) ON key
                | WHERE %s OR info IS NOT NULL
                | KEEP key, country, info
                | SORT key
                """, search);
            var e = expectThrows(VerificationException.class, () -> run(query, TIMEOUT).close());
            assertThat(e.getMessage(), containsString("cannot be used after a JOIN on a subquery when it targets an indexed field"));
        }
    }

    /**
     * The aggregation of a {@code STATS} after the join runs on the coordinator too, so a search in the filter of an aggregate
     * function is rejected for the same reason.
     */
    public void testIndexSearchInAggregateFilterAfterTheJoinIsRejected() {
        var e = expectThrows(VerificationException.class, () -> run("""
            FROM data
            | LEFT OUTER JOIN (FROM lookup) ON key
            | STATS n = COUNT(*), us = COUNT(*) WHERE MATCH(country, "United States")
            """, TIMEOUT).close());
        assertThat(
            e.getMessage(),
            containsString("[MATCH] function cannot be used after a JOIN on a subquery when it targets an indexed field")
        );
    }

    public void testIndexSearchOnLeftFieldIsPushedBelowTheJoin() {
        try (var response = run("""
            FROM data
            | LEFT OUTER JOIN (FROM lookup) ON key
            | WHERE match(country, "States")
            | KEEP key, country, info
            """, TIMEOUT)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("us", "United States", "America"))));
        }
    }

    public void testSearchOnSubqueryColumnRunsAfterTheJoin() {
        try (var response = run("""
            FROM data
            | LEFT OUTER JOIN (FROM lookup) ON key
            | WHERE match(info, "Britain")
            | KEEP key, country, info
            """, TIMEOUT)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("uk", "United Kingdom", "Britain"))));
        }
    }
}

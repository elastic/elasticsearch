/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.Build;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.analysis.PlanSize;
import org.elasticsearch.xpack.esql.plan.logical.Filter;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;

/**
 * Classifies parsed plans, which is what the session classifies too, once views and datasets are resolved.
 */
public class QuerySourceCommandTests extends ESTestCase {

    public void testSingleSourceCommand() {
        assertSourceCommand("FROM logs | WHERE x > 1 | STATS count(*)", QuerySourceCommand.FROM);
        assertSourceCommand("TS metrics | STATS max(cpu)", QuerySourceCommand.TS);
        // PROMQL reads a time series index too, but is told apart from TS before analysis translates it into the same plan.
        assertSourceCommand("PROMQL index=metrics step=1m rate(http_requests_total[5m])", QuerySourceCommand.PROMQL);
    }

    public void testNoDataSource() {
        assertSourceCommand("ROW a = 1", QuerySourceCommand.OTHER);
    }

    public void testExternal() {
        assumeTrue("EXTERNAL requires snapshot builds", Build.current().isSnapshot());
        assertSourceCommand("EXTERNAL \"s3://bucket/table\" | LIMIT 10", QuerySourceCommand.EXTERNAL);
    }

    public void testLookupJoinIsNotASource() {
        assertSourceCommand("TS metrics | LOOKUP JOIN hosts ON host | STATS max(cpu)", QuerySourceCommand.TS);
    }

    public void testSubqueriesInFrom() {
        assumeTrue("subqueries in FROM require snapshot builds", Build.current().isSnapshot());
        assertSourceCommand("FROM logs, (FROM other | LIMIT 1)", QuerySourceCommand.FROM);
        assertSourceCommand("FROM logs, (TS metrics | LIMIT 1)", QuerySourceCommand.MIXED);
    }

    public void testInSubqueryIsNotASource() {
        // The session resolves IN subqueries into joins with view resolution, before classifying the plan.
        Filter filter = as(TEST_PARSER.parseQuery("TS metrics | WHERE host IN (FROM inventory | KEEP host)"), Filter.class);
        var resolved = InSubqueryResolver.resolveInSubqueryInFilter(filter, PlanSize.unlimited());
        assertThat(QuerySourceCommand.of(resolved), equalTo(QuerySourceCommand.TS));
    }

    private static void assertSourceCommand(String query, QuerySourceCommand expected) {
        assertThat(query, QuerySourceCommand.of(TEST_PARSER.parseQuery(query)), equalTo(expected));
    }
}

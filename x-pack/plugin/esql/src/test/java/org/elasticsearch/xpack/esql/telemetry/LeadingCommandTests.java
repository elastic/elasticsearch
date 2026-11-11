/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.is;

public class LeadingCommandTests extends ESTestCase {

    private static final EsqlParser DEV_PARSER = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    public void testFrom() {
        assertThat(leadingCommand("FROM idx | WHERE a > 1 | STATS c = COUNT(*)"), is(LeadingCommand.FROM));
    }

    public void testFromWithMetadata() {
        assertThat(leadingCommand("FROM idx METADATA _id | KEEP _id"), is(LeadingCommand.FROM));
    }

    public void testTs() {
        assertThat(leadingCommand("TS metrics | STATS max(rate(counter)) BY host"), is(LeadingCommand.TS));
    }

    public void testPromql() {
        assertThat(leadingCommand("PROMQL index=metrics step=1m (sum(rate(http_requests_total[5m])))"), is(LeadingCommand.PROMQL));
    }

    public void testRow() {
        assertThat(leadingCommand("ROW a = 1 | EVAL b = a + 1"), is(LeadingCommand.ROW));
    }

    public void testShow() {
        assertThat(leadingCommand("SHOW INFO"), is(LeadingCommand.SHOW));
    }

    public void testExplain() {
        assertThat(leadingCommand("EXPLAIN (FROM idx | LIMIT 1)"), is(LeadingCommand.EXPLAIN));
    }

    public void testExternal() {
        assumeTrue("EXTERNAL command must be enabled", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        assertThat(leadingCommand("EXTERNAL \"s3://bucket/table\" | LIMIT 1"), is(LeadingCommand.EXTERNAL));
    }

    public void testSetPrefixIsIgnored() {
        assertThat(leadingCommand("SET time_zone = \"+01:00\"; TS metrics | LIMIT 1"), is(LeadingCommand.TS));
    }

    public void testLetCountsTheMainStatement() {
        assertThat(leadingCommand("LET b = (TS metrics | LIMIT 3); FROM b | LIMIT 1"), is(LeadingCommand.FROM));
    }

    public void testForkCountsTheCommandBeforeIt() {
        assertThat(leadingCommand("FROM idx | FORK (WHERE a > 1) (WHERE a < 1)"), is(LeadingCommand.FROM));
    }

    public void testLookupJoinCountsTheLeftSide() {
        assertThat(leadingCommand("ROW a = 1 | LOOKUP JOIN lookup_idx ON a"), is(LeadingCommand.ROW));
    }

    public void testFromWithIndexAndSubquery() {
        assumeTrue("subqueries in FROM must be enabled", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertThat(leadingCommand("FROM idx, (ROW a = 1) | LIMIT 1"), is(LeadingCommand.FROM));
    }

    /**
     * The parser drops the outer {@code FROM} when its only source is one subquery, so the subquery's command leads.
     */
    public void testFromWithOnlyOneSubqueryCountsTheSubquery() {
        assumeTrue("subqueries in FROM must be enabled", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertThat(leadingCommand("FROM (ROW a = 1) | LIMIT 1"), is(LeadingCommand.ROW));
    }

    public void testFromWithOnlySubqueries() {
        assumeTrue("subqueries in FROM must be enabled", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertThat(leadingCommand("FROM (ROW a = 1), (FROM idx) | LIMIT 1"), is(LeadingCommand.FROM));
        assertThat(leadingCommand("FROM (TS metrics | LIMIT 1), (FROM idx) | LIMIT 1"), is(LeadingCommand.FROM));
    }

    public void testFromWithOneSubqueryAndMetadata() {
        assumeTrue("subqueries in FROM must be enabled", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertThat(leadingCommand("FROM (ROW a = 1) METADATA _id | LIMIT 1"), is(LeadingCommand.FROM));
    }

    public void testNoPlanIsUnknown() {
        assertThat(LeadingCommand.of(null), is(LeadingCommand.UNKNOWN));
    }

    private static LeadingCommand leadingCommand(String query) {
        return LeadingCommand.of(DEV_PARSER.createStatement(query).plan());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.search.crossproject.ProjectRoutingRequestInfo;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ProjectRoutingUsageHolderTests extends ESTestCase {

    private static ProjectRoutingRequestInfo info(boolean aliasOrigin, boolean aliasWildcard, boolean namedExpr, boolean customTags) {
        return new ProjectRoutingRequestInfo(customTags, namedExpr, aliasWildcard, aliasOrigin);
    }

    // -----------------------------------------------------------------------
    // hasLinkedProjects = false → all calls are no-ops
    // -----------------------------------------------------------------------

    public void testNoLinkedProjects_searchIsNoOp() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(true, true, true, true), false);
        holder.recordSearch(null, false);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(0L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(0L));
    }

    public void testNoLinkedProjects_esqlIsNoOp() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(true, true, true, true), true, false);
        holder.recordEsql(null, true, false);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlQueriesTotal(), equalTo(0L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(0L));
        assertThat(snap.getEsqlWithSet(), equalTo(0L));
    }

    // -----------------------------------------------------------------------
    // null info → only total_queries increments, no sub-counters
    // -----------------------------------------------------------------------

    public void testNullInfo_searchOnlyIncrementsTotalQueries() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(null, true);
        holder.recordSearch(null, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(2L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(0L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(0L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
        assertThat(snap.getSearchWithNamedExpression(), equalTo(0L));
    }

    public void testNullInfo_esqlOnlyIncrementsTotalQueries() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(null, false, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlQueriesTotal(), equalTo(1L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(0L));
        assertThat(snap.getEsqlWithAliasOrigin(), equalTo(0L));
        assertThat(snap.getEsqlWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getEsqlWithCustomTags(), equalTo(0L));
        assertThat(snap.getEsqlWithNamedExpression(), equalTo(0L));
        assertThat(snap.getEsqlWithSet(), equalTo(0L));
    }

    // -----------------------------------------------------------------------
    // with_project_routing and sub-counter flags — _search
    // -----------------------------------------------------------------------

    public void testSearch_noneInfoIncrementsWithProjectRoutingOnly() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(ProjectRoutingRequestInfo.NONE, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(1L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(1L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(0L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
        assertThat(snap.getSearchWithNamedExpression(), equalTo(0L));
    }

    public void testSearch_aliasOriginFlag() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(true, false, false, false), true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchWithProjectRouting(), equalTo(1L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(1L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
    }

    public void testSearch_aliasWildcardFlag() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, true, false, false), true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(1L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(0L));
    }

    public void testSearch_namedExpressionFlag() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, true, false), true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchWithNamedExpression(), equalTo(1L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
    }

    // -----------------------------------------------------------------------
    // custom-tag flag
    // -----------------------------------------------------------------------

    public void testSearch_noCustomTags() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, false, false), true);

        assertThat(holder.getSnapshot().getSearchWithCustomTags(), equalTo(0L));
    }

    public void testSearch_withCustomTags() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, false, true), true);

        assertThat(holder.getSnapshot().getSearchWithCustomTags(), equalTo(1L));
    }

    // -----------------------------------------------------------------------
    // ES|QL: with_SET increments independently of info nullness
    // -----------------------------------------------------------------------

    public void testEsql_setClauseWithNullInfo_doesNotIncrementWithSet() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(null, true, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlQueriesTotal(), equalTo(1L));
        assertThat(snap.getEsqlWithSet(), equalTo(0L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(0L));
    }

    public void testEsql_setClauseWithInfo() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(false, false, false, false), true, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlWithSet(), equalTo(1L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(1L));
    }

    public void testEsql_noSetClause() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(true, false, false, false), false, true);

        assertThat(holder.getSnapshot().getEsqlWithSet(), equalTo(0L));
    }

    public void testEsql_subCounterFlags() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(true, false, true, true), false, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(1L));
        assertThat(snap.getEsqlWithAliasOrigin(), equalTo(1L));
        assertThat(snap.getEsqlWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getEsqlWithNamedExpression(), equalTo(1L));
        assertThat(snap.getEsqlWithCustomTags(), equalTo(1L));
    }

    // -----------------------------------------------------------------------
    // Failure-recording methods
    // -----------------------------------------------------------------------

    public void testRecordSearchFailure_noOp_when_hasLinkedProjects_false() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearchProjectRoutingFailure(false);
        assertThat(holder.getSnapshot(), equalTo(new ProjectRoutingUsageSnapshot()));
    }

    public void testRecordSearchFailure_increments_queries_and_queries_project_routing_and_failures() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearchProjectRoutingFailure(true);
        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(1L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(1L));
        assertThat(snap.getSearchProjectRoutingFailures(), equalTo(1L));
        // mode sub-counters must remain at zero
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(0L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
        assertThat(snap.getSearchWithNamedExpression(), equalTo(0L));
        // esql counters untouched
        assertThat(snap.getEsqlQueriesTotal(), equalTo(0L));
        assertThat(snap.getEsqlProjectRoutingFailures(), equalTo(0L));
    }

    public void testRecordEsqlFailure_noOp_when_hasLinkedProjects_false() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsqlProjectRoutingFailure(false);
        assertThat(holder.getSnapshot(), equalTo(new ProjectRoutingUsageSnapshot()));
    }

    public void testRecordEsqlFailure_increments_queries_and_queries_project_routing_and_failures() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsqlProjectRoutingFailure(true);
        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlQueriesTotal(), equalTo(1L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(1L));
        assertThat(snap.getEsqlProjectRoutingFailures(), equalTo(1L));
        // mode sub-counters must remain at zero
        assertThat(snap.getEsqlWithAliasOrigin(), equalTo(0L));
        assertThat(snap.getEsqlWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getEsqlWithCustomTags(), equalTo(0L));
        assertThat(snap.getEsqlWithNamedExpression(), equalTo(0L));
        assertThat(snap.getEsqlWithSet(), equalTo(0L));
        // search counters untouched
        assertThat(snap.getSearchQueriesTotal(), equalTo(0L));
        assertThat(snap.getSearchProjectRoutingFailures(), equalTo(0L));
    }

    // -----------------------------------------------------------------------
    // Accumulation across multiple calls
    // -----------------------------------------------------------------------

    public void testSearch_accumulatesCorrectly() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(null, true);                                    // total only
        holder.recordSearch(info(true, false, false, false), true);         // + with_project_routing, alias_origin
        holder.recordSearch(info(false, false, true, false), true);         // + with_project_routing, named_expr
        holder.recordSearch(info(false, false, false, true), false);        // gated out — hasLinkedProjects=false

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(3L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(2L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(1L));
        assertThat(snap.getSearchWithNamedExpression(), equalTo(1L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
    }
}

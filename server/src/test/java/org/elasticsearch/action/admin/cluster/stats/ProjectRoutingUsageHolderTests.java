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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ProjectRoutingUsageHolderTests extends ESTestCase {

    private static ProjectRoutingRequestInfo info(boolean aliasOrigin, boolean aliasWildcard, boolean namedExpr, String... tags) {
        return new ProjectRoutingRequestInfo(List.of(tags), namedExpr, aliasWildcard, aliasOrigin);
    }

    // -----------------------------------------------------------------------
    // hasLinkedProjects = false → all calls are no-ops
    // -----------------------------------------------------------------------

    public void testNoLinkedProjects_searchIsNoOp() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(true, true, true, "mytag"), false);
        holder.recordSearch(null, false);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(0L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(0L));
    }

    public void testNoLinkedProjects_esqlIsNoOp() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(true, true, true, "mytag"), true, false);
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
        holder.recordSearch(info(true, false, false, "_alias"), true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchWithProjectRouting(), equalTo(1L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(1L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
    }

    public void testSearch_aliasWildcardFlag() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, true, false, "_alias"), true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(1L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(0L));
    }

    public void testSearch_namedExpressionFlag() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, true, "_alias"), true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchWithNamedExpression(), equalTo(1L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
    }

    // -----------------------------------------------------------------------
    // custom-tag detection: names starting with '_' are predefined
    // -----------------------------------------------------------------------

    public void testSearch_predefinedTagsOnly() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, false, "_alias", "_region", "_csp"), true);

        assertThat(holder.getSnapshot().getSearchWithCustomTags(), equalTo(0L));
    }

    public void testSearch_singleCustomTag() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, false, "mytag"), true);

        assertThat(holder.getSnapshot().getSearchWithCustomTags(), equalTo(1L));
    }

    public void testSearch_mixedPredefinedAndCustom() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, false, "_alias", "mytag"), true);

        assertThat(holder.getSnapshot().getSearchWithCustomTags(), equalTo(1L));
    }

    public void testSearch_emptyTagList() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(info(false, false, false /* no tags */), true);

        assertThat(holder.getSnapshot().getSearchWithCustomTags(), equalTo(0L));
    }

    // -----------------------------------------------------------------------
    // ES|QL: with_SET increments independently of info nullness
    // -----------------------------------------------------------------------

    public void testEsql_setClauseWithNullInfo() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(null, true, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlQueriesTotal(), equalTo(1L));
        assertThat(snap.getEsqlWithSet(), equalTo(1L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(0L));
    }

    public void testEsql_setClauseWithInfo() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(false, false, false, "_alias"), true, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlWithSet(), equalTo(1L));
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(1L));
    }

    public void testEsql_noSetClause() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(true, false, false, "_alias"), false, true);

        assertThat(holder.getSnapshot().getEsqlWithSet(), equalTo(0L));
    }

    public void testEsql_subCounterFlags() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordEsql(info(true, false, true, "_alias", "custom"), false, true);

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getEsqlWithProjectRouting(), equalTo(1L));
        assertThat(snap.getEsqlWithAliasOrigin(), equalTo(1L));
        assertThat(snap.getEsqlWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getEsqlWithNamedExpression(), equalTo(1L));
        assertThat(snap.getEsqlWithCustomTags(), equalTo(1L));
    }

    // -----------------------------------------------------------------------
    // Accumulation across multiple calls
    // -----------------------------------------------------------------------

    public void testSearch_accumulatesCorrectly() {
        ProjectRoutingUsageHolder holder = new ProjectRoutingUsageHolder();
        holder.recordSearch(null, true);                                   // total only
        holder.recordSearch(info(true, false, false, "_alias"), true);     // + with_project_routing, alias_origin
        holder.recordSearch(info(false, false, true, "_alias"), true);     // + with_project_routing, named_expr
        holder.recordSearch(info(false, false, false, "custom"), false);   // gated out — hasLinkedProjects=false

        ProjectRoutingUsageSnapshot snap = holder.getSnapshot();
        assertThat(snap.getSearchQueriesTotal(), equalTo(3L));
        assertThat(snap.getSearchWithProjectRouting(), equalTo(2L));
        assertThat(snap.getSearchWithAliasOrigin(), equalTo(1L));
        assertThat(snap.getSearchWithNamedExpression(), equalTo(1L));
        assertThat(snap.getSearchWithAliasWildcard(), equalTo(0L));
        assertThat(snap.getSearchWithCustomTags(), equalTo(0L));
    }
}

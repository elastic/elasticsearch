/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.crossproject.ProjectRoutingRequestInfo;

import java.util.concurrent.atomic.LongAdder;

/**
 * Accumulates per-node project-routing telemetry counters. Follows the same pattern as {@link CCSUsageTelemetry}.
 * Thread-safe via {@link LongAdder}. Obtain a point-in-time snapshot with {@link #getSnapshot()}.
 *
 * <p>All counters are gated on {@code hasLinkedProjects}: they only increment while the project has at least one
 * configured linked project. This ensures percentages can be computed from the data
 * (e.g. {@code queries_project_routing / queries}).
 */
public class ProjectRoutingUsageHolder {

    // _search, _async_search, _msearch (per sub-request), _search/template, _msearch/template
    private final LongAdder searchQueriesTotal = new LongAdder();
    private final LongAdder searchWithProjectRouting = new LongAdder();
    private final LongAdder searchWithAliasOrigin = new LongAdder();
    private final LongAdder searchWithAliasWildcard = new LongAdder();
    private final LongAdder searchWithCustomTags = new LongAdder();
    private final LongAdder searchWithNamedExpression = new LongAdder();
    private final LongAdder searchProjectRoutingFailures = new LongAdder();

    // ES|QL endpoint
    private final LongAdder esqlQueriesTotal = new LongAdder();
    private final LongAdder esqlWithProjectRouting = new LongAdder();
    private final LongAdder esqlWithAliasOrigin = new LongAdder();
    private final LongAdder esqlWithAliasWildcard = new LongAdder();
    private final LongAdder esqlWithCustomTags = new LongAdder();
    private final LongAdder esqlWithNamedExpression = new LongAdder();
    private final LongAdder esqlWithSet = new LongAdder();
    private final LongAdder esqlProjectRoutingFailures = new LongAdder();

    /**
     * Records a {@code _search} request. {@code queries} is always incremented (subject to the
     * {@code hasLinkedProjects} gate). The {@code queries_project_routing} counter and its sub-counters are only
     * incremented when {@code info} is non-null, i.e. the request carried a {@code project_routing} expression.
     *
     * @param info routing metadata from the resolver; null when the request had no {@code project_routing} header,
     *             or until the resolver is upgraded to populate it (Ticket 2)
     * @param hasLinkedProjects true when the project had at least one linked project at the time of the request;
     *                          when false all counters are skipped
     */
    public void recordSearch(@Nullable ProjectRoutingRequestInfo info, boolean hasLinkedProjects) {
        if (hasLinkedProjects == false) return;
        searchQueriesTotal.increment();
        if (info == null) return;
        searchWithProjectRouting.increment();
        if (info.usedAliasOrigin()) searchWithAliasOrigin.increment();
        if (info.usedAliasWildcard()) searchWithAliasWildcard.increment();
        if (info.usedNamedExpression()) searchWithNamedExpression.increment();
        if (info.tagsUsedInRouting().stream().anyMatch(t -> t.startsWith("_") == false)) searchWithCustomTags.increment();
    }

    /**
     * Records an ES|QL request. {@code queries} is always incremented (subject to the
     * {@code hasLinkedProjects} gate). The {@code queries_project_routing} counter and its sub-counters are only
     * incremented when {@code info} is non-null, i.e. the request carried a {@code project_routing} expression.
     *
     * @param info routing metadata from the resolver; null when the request had no {@code project_routing} expression,
     *             or until the resolver is upgraded to populate it (Ticket 2)
     * @param setClauseUsed true when the routing expression came from an in-query {@code SET project_routing = ...} clause
     * @param hasLinkedProjects true when the project had at least one linked project at the time of the request;
     *                          when false all counters are skipped
     */
    public void recordEsql(@Nullable ProjectRoutingRequestInfo info, boolean setClauseUsed, boolean hasLinkedProjects) {
        if (hasLinkedProjects == false) return;
        esqlQueriesTotal.increment();
        if (setClauseUsed) esqlWithSet.increment();
        if (info == null) return;
        esqlWithProjectRouting.increment();
        if (info.usedAliasOrigin()) esqlWithAliasOrigin.increment();
        if (info.usedAliasWildcard()) esqlWithAliasWildcard.increment();
        if (info.usedNamedExpression()) esqlWithNamedExpression.increment();
        if (info.tagsUsedInRouting().stream().anyMatch(t -> t.startsWith("_") == false)) esqlWithCustomTags.increment();
    }

    /**
     * Records a routing failure for a {@code _search}-family request. Increments {@code queries},
     * {@code queries_project_routing}, and {@code failures}. Called by Ticket 5 from
     * {@code AuthorizationService.onAuthorizedResourceLoadFailure()}.
     *
     * @param hasLinkedProjects true when the project had at least one linked project; when false this is a no-op
     */
    public void recordSearchProjectRoutingFailure(boolean hasLinkedProjects) {
        if (hasLinkedProjects == false) return;
        searchQueriesTotal.increment();
        searchWithProjectRouting.increment();
        searchProjectRoutingFailures.increment();
    }

    /**
     * Records a routing failure for an ES|QL request. Increments {@code queries},
     * {@code queries_project_routing}, and {@code failures}. Called by Ticket 5 from
     * {@code AuthorizationService.onAuthorizedResourceLoadFailure()}.
     *
     * @param hasLinkedProjects true when the project had at least one linked project; when false this is a no-op
     */
    public void recordEsqlProjectRoutingFailure(boolean hasLinkedProjects) {
        if (hasLinkedProjects == false) return;
        esqlQueriesTotal.increment();
        esqlWithProjectRouting.increment();
        esqlProjectRoutingFailures.increment();
    }

    /**
     * Returns a point-in-time snapshot of the current counters.
     */
    public ProjectRoutingUsageSnapshot getSnapshot() {
        return new ProjectRoutingUsageSnapshot(
            searchQueriesTotal.sum(),
            searchWithProjectRouting.sum(),
            searchWithAliasOrigin.sum(),
            searchWithAliasWildcard.sum(),
            searchWithCustomTags.sum(),
            searchWithNamedExpression.sum(),
            searchProjectRoutingFailures.sum(),
            esqlQueriesTotal.sum(),
            esqlWithProjectRouting.sum(),
            esqlWithAliasOrigin.sum(),
            esqlWithAliasWildcard.sum(),
            esqlWithCustomTags.sum(),
            esqlWithNamedExpression.sum(),
            esqlWithSet.sum(),
            esqlProjectRoutingFailures.sum()
        );
    }
}

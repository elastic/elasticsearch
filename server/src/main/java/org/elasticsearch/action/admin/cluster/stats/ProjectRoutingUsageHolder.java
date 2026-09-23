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
 *
 * <p>Common per-endpoint counters are grouped in {@link RoutingCounters}. Adding a new endpoint (e.g. EQL, SQL)
 * requires adding a new {@link RoutingCounters} instance and a corresponding {@code record*()} method.
 * The ES|QL-specific {@code in_SET} counter ({@link #esqlWithSet}) is tracked separately.
 */
public class ProjectRoutingUsageHolder {

    /**
     * Groups the counters that are common across all tracked endpoints. Each endpoint ({@code _search},
     * {@code _esql}, and any future additions) gets its own instance.
     */
    private static class RoutingCounters {
        final LongAdder total = new LongAdder();
        final LongAdder withProjectRouting = new LongAdder();
        final LongAdder withAliasOrigin = new LongAdder();
        final LongAdder withAliasWildcard = new LongAdder();
        final LongAdder withCustomTags = new LongAdder();
        final LongAdder withNamedExpression = new LongAdder();
        final LongAdder failures = new LongAdder();

        /**
         * Records a query. Always increments {@code total}. When {@code info} is non-null (the request
         * carried a {@code project_routing} expression), also increments {@code withProjectRouting} and
         * any applicable sub-counters.
         */
        void record(@Nullable ProjectRoutingRequestInfo info) {
            total.increment();
            if (info == null) {
                return;
            }
            withProjectRouting.increment();
            if (info.usedAliasOrigin()) {
                withAliasOrigin.increment();
            }
            if (info.usedAliasWildcard()) {
                withAliasWildcard.increment();
            }
            if (info.usedNamedExpression()) {
                withNamedExpression.increment();
            }
            if (info.usedCustomTags()) {
                withCustomTags.increment();
            }
        }

        /**
         * Records a routing failure. Increments {@code total}, {@code withProjectRouting}, and
         * {@code failures}. Called by Ticket 5 from {@code AuthorizationService.onAuthorizedResourceLoadFailure()}.
         */
        void recordFailure() {
            total.increment();
            withProjectRouting.increment();
            failures.increment();
        }
    }

    // _search, _async_search, _msearch (per sub-request), _search/template, _msearch/template, _count, _cat/count
    private final RoutingCounters search = new RoutingCounters();

    // ES|QL endpoint
    private final RoutingCounters esql = new RoutingCounters();
    private final LongAdder esqlWithSet = new LongAdder();  // in_SET: routing came from SET clause, not request body

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
        if (hasLinkedProjects == false) {
            return;
        }
        search.record(info);
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
        if (hasLinkedProjects == false) {
            return;
        }
        esql.record(info);
        if (setClauseUsed && info != null) {
            esqlWithSet.increment();
        }
    }

    /**
     * Records a routing failure for a {@code _search}-family request. Increments {@code queries},
     * {@code queries_project_routing}, and {@code failures}. Called by Ticket 5 from
     * {@code AuthorizationService.onAuthorizedResourceLoadFailure()}.
     *
     * @param hasLinkedProjects true when the project had at least one linked project; when false this is a no-op
     */
    public void recordSearchProjectRoutingFailure(boolean hasLinkedProjects) {
        if (hasLinkedProjects == false) {
            return;
        }
        search.recordFailure();
    }

    /**
     * Records a routing failure for an ES|QL request. Increments {@code queries},
     * {@code queries_project_routing}, and {@code failures}. Called by Ticket 5 from
     * {@code AuthorizationService.onAuthorizedResourceLoadFailure()}.
     *
     * @param hasLinkedProjects true when the project had at least one linked project; when false this is a no-op
     */
    public void recordEsqlProjectRoutingFailure(boolean hasLinkedProjects) {
        if (hasLinkedProjects == false) {
            return;
        }
        esql.recordFailure();
    }

    /**
     * Returns a point-in-time snapshot of the current counters.
     */
    public ProjectRoutingUsageSnapshot getSnapshot() {
        return new ProjectRoutingUsageSnapshot(
            search.total.sum(),
            search.withProjectRouting.sum(),
            search.withAliasOrigin.sum(),
            search.withAliasWildcard.sum(),
            search.withCustomTags.sum(),
            search.withNamedExpression.sum(),
            search.failures.sum(),
            esql.total.sum(),
            esql.withProjectRouting.sum(),
            esql.withAliasOrigin.sum(),
            esql.withAliasWildcard.sum(),
            esql.withCustomTags.sum(),
            esql.withNamedExpression.sum(),
            esqlWithSet.sum(),
            esql.failures.sum()
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The fetch phase decisions of one query, one for each plan the coordinator optimizes. A query optimizes its sub plans
 * and its main plan with the same optimizer, so a single query can record several decisions, in planning order.
 */
public final class FetchPhaseOutcomes {

    /**
     * One decision.
     *
     * @param reason why the plan did not get the fetch phase, {@code null} when the outcome says it all
     */
    public record Decision(Outcome outcome, @Nullable String reason) {
        @Override
        public String toString() {
            return reason == null ? outcome.toString() : outcome + ": " + reason;
        }
    }

    // sub plans are optimized one after the other, but on whatever thread the previous one completed on
    private final List<Decision> decisions = new CopyOnWriteArrayList<>();

    public void record(Outcome outcome, @Nullable String reason) {
        decisions.add(new Decision(outcome, reason));
    }

    public List<Decision> all() {
        return List.copyOf(decisions);
    }

    /**
     * {@link Outcome#APPLIED} when any plan of the query got the fetch phase, otherwise the first decision. {@code null}
     * when nothing was recorded.
     */
    @Nullable
    public Decision summary() {
        Decision first = null;
        for (Decision decision : decisions) {
            if (decision.outcome() == Outcome.APPLIED) {
                return decision;
            }
            if (first == null) {
                first = decision;
            }
        }
        return first;
    }

    /**
     * The decision {@code EXPLAIN} shows, {@code null} when the query left the fetch phase alone: the build lacks the
     * feature, or the cluster setting is off and the query sets no pragma. Such queries keep their {@code EXPLAIN} output.
     */
    @Nullable
    public Decision explained() {
        Decision summary = summary();
        if (summary == null) {
            return null;
        }
        return switch (summary.outcome()) {
            case DISABLED_FEATURE_FLAG, DISABLED_SETTING -> null;
            case ENABLED, DISABLED_PRAGMA, MIXED_VERSION_FALLBACK, APPLIED, INELIGIBLE_SHAPE, INELIGIBLE_NO_DEFERRABLE_FIELDS,
                INELIGIBLE_REMOTE_CLUSTER, INCONSISTENT_PROJECTION -> summary;
        };
    }
}

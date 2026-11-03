/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FetchPhaseStatsTests extends ESTestCase {

    public void testNoOutcomeBeforeThePlannerRuns() {
        assertThat(new FetchPhaseStats().outcome(), nullValue());
    }

    /**
     * The stats read the decisions as the planner records them, and report the one {@code EXPLAIN} shows: a plan that got
     * the fetch phase wins over one that didn't.
     */
    public void testTheOutcomeIsTheOneExplainShows() {
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        FetchPhaseStats stats = new FetchPhaseStats();
        stats.plannedBy(outcomes);
        outcomes.record(Outcome.INELIGIBLE_SHAPE, "a sub plan aggregates");
        outcomes.record(Outcome.APPLIED, null);
        assertThat(stats.outcome(), equalTo(Outcome.APPLIED));
    }

    /**
     * A query that left the fetch phase alone has no outcome, so nothing counts it.
     */
    public void testNoOutcomeWhenTheQueryLeftTheFetchPhaseAlone() {
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        outcomes.record(randomFrom(Outcome.DISABLED_FEATURE_FLAG, Outcome.DISABLED_SETTING), null);
        FetchPhaseStats stats = new FetchPhaseStats();
        stats.plannedBy(outcomes);
        assertThat(stats.outcome(), nullValue());
    }

    public void testBatchesAddUpAndANodeCountsOnce() {
        FetchPhaseStats stats = new FetchPhaseStats();
        stats.batchEnded(5, List.of("n1", "n2"), 1_000);
        stats.batchEnded(3, List.of("n2"), 2_000);
        assertThat(stats.documents(), equalTo(8L));
        assertThat(stats.requests(), equalTo(3L));
        assertThat(stats.nodesContacted(), equalTo(2));
        assertThat(stats.tookNanos(), equalTo(3_000L));
    }
}

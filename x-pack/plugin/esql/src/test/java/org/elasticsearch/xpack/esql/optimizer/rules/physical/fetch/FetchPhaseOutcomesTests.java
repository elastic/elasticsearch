/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes.Decision;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;

import static org.hamcrest.Matchers.equalTo;

public class FetchPhaseOutcomesTests extends ESTestCase {
    /**
     * A query optimizes its sub plans and its main plan with one optimizer. Any plan that got the fetch phase makes
     * the query a fetch phase query.
     */
    public void testSummaryPrefersAnAppliedPlan() {
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        outcomes.record(Outcome.INELIGIBLE_SHAPE, "[Aggregate] runs before the cut");
        outcomes.record(Outcome.APPLIED, null);
        assertThat(outcomes.summary(), equalTo(new Decision(Outcome.APPLIED, null)));
    }

    public void testSummaryIsTheFirstDecisionWithoutAnAppliedPlan() {
        FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
        outcomes.record(Outcome.INELIGIBLE_SHAPE, "[Aggregate] runs before the cut");
        outcomes.record(Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS, null);
        assertThat(outcomes.summary().toString(), equalTo("INELIGIBLE_SHAPE: [Aggregate] runs before the cut"));
    }

    public void testExplainedHidesQueriesThatLeftTheFetchPhaseAlone() {
        assertNull(new FetchPhaseOutcomes().explained());
        for (Outcome outcome : new Outcome[] { Outcome.DISABLED_FEATURE_FLAG, Outcome.DISABLED_SETTING }) {
            FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
            outcomes.record(outcome, null);
            assertNull(outcome.toString(), outcomes.explained());
        }
    }

    public void testExplainedShowsTheSummaryOnceTheSettingOrThePragmaHasASay() {
        for (Outcome outcome : new Outcome[] {
            Outcome.DISABLED_PRAGMA,
            Outcome.MIXED_VERSION_FALLBACK,
            Outcome.APPLIED,
            Outcome.INELIGIBLE_SHAPE,
            Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS,
            Outcome.INELIGIBLE_REMOTE_CLUSTER,
            Outcome.INCONSISTENT_PROJECTION }) {
            FetchPhaseOutcomes outcomes = new FetchPhaseOutcomes();
            outcomes.record(outcome, null);
            assertThat(outcomes.explained(), equalTo(new Decision(outcome, null)));
        }
    }
}

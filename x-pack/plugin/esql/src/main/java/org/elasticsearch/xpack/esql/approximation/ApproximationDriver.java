/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Result;

/**
 * Drives approximation subplan execution and substitutes sample probabilities into the main plan.
 */
public interface ApproximationDriver {

    /**
     * Builds an approximation driver for the logical plan.
     */
    static ApproximationDriver create(LogicalPlan logicalPlan, ApproximationSettings settings) {
        ApproximationVerifier.QueryProperties queryProperties = ApproximationVerifier.verifyPlanOrThrow(logicalPlan);
        if (queryProperties.forkBranchProperties() == null) {
            return new Approximation(logicalPlan, queryProperties, settings);
        } else {
            return new ForkApproximation(logicalPlan, queryProperties, settings);
        }
    }

    /**
     * Returns the next subplan to execute for calibration, or {@code null} if the main plan can run.
     */
    LogicalPlan firstSubPlan();

    /**
     * Processes a subplan result and returns the main plan with placeholders substituted
     * when this calibration step is complete.
     */
    LogicalPlan newMainPlan(LogicalPlan mainPlan, Result result);
}

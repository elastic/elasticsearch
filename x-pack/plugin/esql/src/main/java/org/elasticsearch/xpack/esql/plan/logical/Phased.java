/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.List;

/**
 * Marks a {@link LogicalPlan} node as requiring multiple ESQL executions to run.
 * All logical plans are now run by:
 * We do this by:
 * <ol>
 *     <li>{@link Analyzer analyzing} the entire query</li>
 *     <li>{@link Phased#extractFirstPhase extracting} the first phase from the
 *         logical plan</li>
 *     <li>if there isn't a first phase, run the logical plan and return the results. you are done.</li>
 *     <li>if there is first phase, run that</li>
 *     <li>{@link Phased#applyResultsFromFirstPhase appling} the results from the
 *         first phase into the logical plan</li>
 *     <li>start over from step 2 using the new logical plan</li>
 * </ol>
 */
public interface Phased {
    LogicalPlan firstPhase();

    LogicalPlan nextPhase(List<Attribute> schema, List<Page> firstPhaseResult);

    static LogicalPlan extractFirstPhase(LogicalPlan plan) {
        assert plan.analyzed();
        var firstPhase = new Holder<LogicalPlan>();
        plan.forEachUp(t -> {
            if (firstPhase.get() == null && t instanceof Phased phased) {
                firstPhase.set(phased.firstPhase());
            }
        });
        LogicalPlan firstPhasePlan = firstPhase.get();
        if (firstPhasePlan != null) {
            firstPhasePlan.setAnalyzed();
        }
        return firstPhasePlan;
    }

    static LogicalPlan applyResultsFromFirstPhase(LogicalPlan plan, List<Attribute> schema, List<Page> result) {
        assert plan.analyzed();
        Holder<Boolean> seen = new Holder<>(false);
        LogicalPlan applied = plan.transformUp(logicalPlan -> {
            if (seen.get() == false && logicalPlan instanceof Phased phased) {
                seen.set(true);
                return phased.nextPhase(schema, result);
            }
            return logicalPlan;
        });
        applied.setAnalyzed();
        return applied;
    }
}

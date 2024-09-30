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
 * <ol>
 *     <li>{@link Analyzer analyzing} the entire query</li>
 *     <li>{@link Phased#extractFirstPhase extracting} the first phase from the
 *         logical plan</li>
 *     <li>if there isn't a first phase, run the entire logical plan and return the
 *         results. you are done.</li>
 *     <li>if there is first phase, run that</li>
 *     <li>{@link Phased#applyResultsFromFirstPhase applying} the results from the
 *         first phase into the logical plan</li>
 *     <li>start over from step 2 using the new logical plan</li>
 * </ol>
 * <p>For example, {@code INLINESTATS} is written like this:</p>
 * <pre>{@code
 * FROM foo
 * | EVAL bar = a * b
 * | INLINESTATS m = MAX(bar) BY b
 * | WHERE m = bar
 * | LIMIT 1
 * }</pre>
 * <p>And it's split into:</p>
 * <pre>{@code
 * FROM foo
 * | EVAL bar = a * b
 * | STATS m = MAX(bar) BY b
 * }</pre>
 * <p>and</p>
 * <pre>{@code
 * FROM foo
 * | EVAL bar = a * b
 * | LOOKUP (results of m = MAX(bar) BY b) ON b
 * | WHERE m = bar
 * | LIMIT 1
 * }</pre>
 * <p>If there are multiple {@linkplain Phased} nodes in the plan we always
 * operate on the lowest one first, counting from the data source "upwards".
 * Generally that'll read left to right in the query. So:</p>
 * <pre>{@code
 * FROM foo | INLINESTATS | INLINESTATS
 * }</pre>
 * becomes
 * <pre>{@code
 * FROM foo | STATS
 * }</pre>
 * and
 * <pre>{@code
 * FROM foo | HASHJOIN | INLINESTATS
 * }</pre>
 * which is further broken into
 * <pre>{@code
 * FROM foo | HASHJOIN | STATS
 * }</pre>
 * and finally:
 * <pre>{@code
 * FROM foo | HASHJOIN | HASHJOIN
 * }</pre>
 */
public interface Phased {
    /**
     * Return a {@link LogicalPlan} for the first "phase" of this operation.
     * The result of this phase will be provided to {@link #nextPhase}.
     */
    LogicalPlan firstPhase();

    /**
     * Use the results of plan provided from {@link #firstPhase} to produce the
     * next phase of the query.
     */
    LogicalPlan nextPhase(List<Attribute> schema, List<Page> firstPhaseResult);

    /**
     * Find the first {@link Phased} operation and return it's {@link #firstPhase}.
     * Or {@code null} if there aren't any {@linkplain Phased} operations.
     */
    static LogicalPlan extractFirstPhase(LogicalPlan plan) {
        if (false == plan.optimized()) {
            throw new IllegalArgumentException("plan must be optimized");
        }
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

    /**
     * Merge the results of {@link #extractFirstPhase} into a {@link LogicalPlan}
     * and produce a new {@linkplain LogicalPlan} that will execute the rest of the
     * query. This plan <strong>may</strong> contain <strong>another</strong>
     * {@link #firstPhase}. If it does then it will also need to be
     * {@link #extractFirstPhase extracted} and the results will need to be applied
     * again by calling this method. Eventually this will produce a plan which
     * does not have a {@link #firstPhase} and <strong>that</strong> is the "final"
     * phase of the plan.
     */
    static LogicalPlan applyResultsFromFirstPhase(LogicalPlan plan, List<Attribute> schema, List<Page> result) {
        if (false == plan.analyzed()) {
            throw new IllegalArgumentException("plan must be analyzed");
        }
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

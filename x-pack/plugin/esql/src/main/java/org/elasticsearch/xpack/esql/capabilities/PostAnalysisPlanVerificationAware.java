/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.function.BiConsumer;

/**
 * Interface implemented by expressions or plans that require validation after query plan analysis,
 * when the indices and references have been resolved, but before the plan is transformed further by optimizations.
 * The interface is similar to {@link PostAnalysisVerificationAware}, but focused on the tree structure, oftentimes covering semantic
 * checks.
 */
public interface PostAnalysisPlanVerificationAware {

    /**
     * Allows the implementer to return a consumer that will perform self-validation in the context of the tree structure the implementer
     * is part of. This usually involves checking the type and configuration of the children or that of the parent.
     * <p>
     * It is often more useful to perform the checks as extended as it makes sense, over stopping at the first failure. This will allow the
     * author to progress faster to a correct query.
     * </p>
     * <p>
     *     Example: a {@link GroupingFunction} instance, which models a function to group documents to aggregate over, can only be used in
     *     the context of the STATS command, modeled by the {@link Aggregate} class. This is how this verification is performed:
     *     <pre>
     *     {@code
     *      @Override
     *      public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
     *          return (p, failures) -> {
     *              if (p instanceof Aggregate == false) {
     *                  p.forEachExpression(
     *                      GroupingFunction.class,
     *                      gf -> failures.add(fail(gf, "cannot use grouping function [{}] outside of a STATS command", gf.sourceText()))
     *                  );
     *              }
     *          };
     *      }
     *     }
     *     </pre>
     *
     * @return a consumer that will receive a tree to check and an accumulator of failures found during inspection.
     */
    BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification();
}

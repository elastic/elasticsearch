/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.expression.function.scalar.math.AutoBucket;
import org.elasticsearch.xpack.esql.optimizer.OptimizerRules.LogicalPlanDependencyCheck;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.elasticsearch.xpack.ql.common.Failure.fail;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;

public final class LogicalVerifier {

    private static final LogicalPlanDependencyCheck DEPENDENCY_CHECK = new LogicalPlanDependencyCheck();
    public static final LogicalVerifier INSTANCE = new LogicalVerifier();

    private LogicalVerifier() {}

    /** Verifies the optimized logical plan. */
    public Collection<Failure> verify(LogicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        plan.forEachUp(p -> {
            // dependency check
            // FIXME: re-enable
            // DEPENDENCY_CHECK.checkPlan(p, failures);

            // post optimization folding check
            p.forEachExpression(AutoBucket.class, e -> {
                Expression.TypeResolution resolution = isFoldable(e.from(), e.sourceText(), THIRD);
                if (resolution.unresolved()) {
                    failures.add(fail(e, resolution.message()));
                }
                resolution = isFoldable(e.to(), e.sourceText(), FOURTH);
                if (resolution.unresolved()) {
                    failures.add(fail(e, resolution.message()));
                }
            });
        });

        return failures;
    }
}

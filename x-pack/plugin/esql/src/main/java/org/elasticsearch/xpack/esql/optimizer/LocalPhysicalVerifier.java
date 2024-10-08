/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushDownUtils;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource.canPushToSource;

/** Local physical plan verifier. */
public final class LocalPhysicalVerifier {

    public static final LocalPhysicalVerifier INSTANCE = new LocalPhysicalVerifier();

    private LocalPhysicalVerifier() {}

    /** Verifies the physical plan. */
    public Collection<Failure> verify(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        Set<Failure> failures = new LinkedHashSet<>();

        plan.forEachDown(p -> {
            checkFullTextQueryFunctions(p, context, failures);
        });

        return failures;
    }

    private static void checkFullTextQueryFunctions(PhysicalPlan plan, LocalPhysicalOptimizerContext ctx, Set<Failure> failures) {
        if (plan instanceof FilterExec fe) {
            checkFullTextFunctionsConditions(fe.condition(), ctx, failures);
        }
    }

    private static void checkFullTextFunctionsConditions(Expression condition, LocalPhysicalOptimizerContext ctx, Set<Failure> failures) {
        condition.forEachUp(Or.class, or -> {
            Expression left = or.left();
            Expression right = or.right();
            checkDisjunction(failures, or, left, right, ctx);
            checkDisjunction(failures, or, right, left, ctx);
        });
    }

    private static void checkDisjunction(
        Set<Failure> failures,
        Or or,
        Expression first,
        Expression second,
        LocalPhysicalOptimizerContext ctx
    ) {
        first.forEachDown(FullTextFunction.class, ftf -> {
            if (canPushToSource(second, x -> LucenePushDownUtils.hasIdenticalDelegate(x, ctx.searchStats())) == false) {
                failures.add(
                    fail(
                        or,
                        "Invalid condition [{}]. Function {} can't be used as part of an or condition that includes [{}]",
                        or.sourceText(),
                        ftf.functionName(),
                        second.sourceText()
                    )
                );
            }
        });
    }
}

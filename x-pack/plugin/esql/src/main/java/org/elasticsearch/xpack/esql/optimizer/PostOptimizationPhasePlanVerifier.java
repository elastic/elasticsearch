/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.List;

import static org.elasticsearch.index.IndexMode.LOOKUP;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.datatypeEquals;

/**
 * Verifies the plan after optimization.
 * This is invoked immediately after a Plan Optimizer completes its work.
 * Currently, it is called after LogicalPlanOptimizer, PhysicalPlanOptimizer,
 * LocalLogicalPlanOptimizer, and LocalPhysicalPlanOptimizer.
 * Note: Logical and Physical optimizers may override methods in this class to perform different checks.
 */
public abstract class PostOptimizationPhasePlanVerifier {

    /** Verifies the optimized plan */
    public Failures verify(QueryPlan<?> optimizedPlan, boolean skipRemoteEnrichVerification, List<Attribute> expectedOutputAttributes) {
        Failures failures = new Failures();
        Failures depFailures = new Failures();
        if (skipVerification(optimizedPlan, skipRemoteEnrichVerification)) {
            return failures;
        }

        checkPlanConsistency(optimizedPlan, failures, depFailures);

        verifyOutputNotChanged(optimizedPlan, expectedOutputAttributes, failures);

        if (depFailures.hasFailures()) {
            throw new IllegalStateException(depFailures.toString());
        }

        return failures;
    }

    abstract boolean skipVerification(QueryPlan<?> optimizedPlan, boolean skipRemoteEnrichVerification);

    abstract void checkPlanConsistency(QueryPlan<?> optimizedPlan, Failures failures, Failures depFailures);

    private static void verifyOutputNotChanged(QueryPlan<?> optimizedPlan, List<Attribute> expectedOutputAttributes, Failures failures) {
        if (datatypeEquals(expectedOutputAttributes, optimizedPlan.output()) == false) {
            // LookupJoinExec represents the lookup index with EsSourceExec and this is turned into EsQueryExec by
            // ReplaceSourceAttributes. Because InsertFieldExtractions doesn't apply to lookup indices, the
            // right hand side will only have the EsQueryExec providing the _doc attribute and nothing else.
            if ((optimizedPlan instanceof EsQueryExec esQueryExec && esQueryExec.indexMode() == LOOKUP) == false) {
                failures.add(
                    fail(
                        optimizedPlan,
                        "Output has changed from [{}] to [{}]. ",
                        expectedOutputAttributes.toString(),
                        optimizedPlan.output().toString()
                    )
                );
            }
        }
    }

}

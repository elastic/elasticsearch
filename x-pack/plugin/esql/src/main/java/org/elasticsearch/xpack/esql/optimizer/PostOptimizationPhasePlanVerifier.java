/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.List;

import static org.elasticsearch.index.IndexMode.LOOKUP;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.dataTypeEquals;

/**
 * Verifies the plan after optimization.
 * This is invoked immediately after a Plan Optimizer completes its work.
 * Currently, it is called after LogicalPlanOptimizer, PhysicalPlanOptimizer,
 * LocalLogicalPlanOptimizer, and LocalPhysicalPlanOptimizer.
 * Note: Logical and Physical optimizers may override methods in this class to perform different checks.
 */
public abstract class PostOptimizationPhasePlanVerifier<P extends QueryPlan<P>> {

    /** Verifies the optimized plan */
    public Failures verify(P optimizedPlan, boolean skipRemoteEnrichVerification, List<Attribute> expectedOutputAttributes) {
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

    abstract boolean skipVerification(P optimizedPlan, boolean skipRemoteEnrichVerification);

    abstract void checkPlanConsistency(P optimizedPlan, Failures failures, Failures depFailures);

    private static void verifyOutputNotChanged(QueryPlan<?> optimizedPlan, List<Attribute> expectedOutputAttributes, Failures failures) {
        // disable this check if there are other failures already
        // it is possible that some of the attributes are not resolved yet and that is reflected in the failures
        // we cannot get the datatype on an unresolved attribute
        // if we try it, it causes an exception and the exception hides the more detailed error message
        if (failures.hasFailures()) {
            return;
        }
        if (dataTypeEquals(expectedOutputAttributes, optimizedPlan.output()) == false) {
            // If the output level is empty we add a column called ProjectAwayColumns.ALL_FIELDS_PROJECTED
            // We will ignore such cases for output verification
            // TODO: this special casing is required due to https://github.com/elastic/elasticsearch/issues/121741, remove when fixed.
            boolean hasProjectAwayColumns = optimizedPlan.output()
                .stream()
                .anyMatch(x -> x.name().equals(ProjectAwayColumns.ALL_FIELDS_PROJECTED));
            // LookupJoinExec represents the lookup index with EsSourceExec and this is turned into EsQueryExec by
            // ReplaceSourceAttributes. Because InsertFieldExtractions doesn't apply to lookup indices, the
            // right hand side will only have the EsQueryExec providing the _doc attribute and nothing else.
            // We perform an optimizer run on every fragment. LookupJoinExec also contains such a fragment,
            // and currently it only contains an EsQueryExec after optimization.
            boolean hasLookupJoinExec = optimizedPlan instanceof EsQueryExec esQueryExec && esQueryExec.indexMode() == LOOKUP;
            boolean ignoreError = hasProjectAwayColumns || hasLookupJoinExec;
            if (ignoreError == false) {
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

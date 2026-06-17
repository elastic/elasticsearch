/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.xpack.esql.capabilities.PostPhysicalOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/** Physical plan verifier. */
public final class PhysicalVerifier extends PostOptimizationPhasePlanVerifier<PhysicalPlan> {
    public static final PhysicalVerifier LOCAL_INSTANCE = new PhysicalVerifier(true);
    public static final PhysicalVerifier INSTANCE = new PhysicalVerifier(false);

    private PhysicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    @Override
    protected void checkPlanConsistency(PhysicalPlan optimizedPlan, Failures failures, Failures depFailures) {
        optimizedPlan.forEachDown(p -> {
            if (p instanceof FieldExtractExec fieldExtractExec) {
                Attribute sourceAttribute = fieldExtractExec.sourceAttribute();
                if (sourceAttribute == null) {
                    failures.add(
                        fail(
                            fieldExtractExec,
                            "Need to add field extractor for [{}] but cannot detect source attributes from node [{}]",
                            Expressions.names(fieldExtractExec.attributesToExtract()),
                            fieldExtractExec.child()
                        )
                    );
                }
            }

            // This check applies only for coordinator physical plans (isLocal == false)
            if (isLocal == false && p instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.REMOTE) {
                failures.add(
                    fail(
                        p,
                        "Physical plan contains remote executing operation [{}] in local part. "
                            + "This usually means this command is incompatible with some of the preceding commands.",
                        p.nodeName()
                    )
                );
            }

            // An unbounded streaming sort can only be handled by the coordinator's SortedMergeSourceOperator
            // when the sort was successfully pushed to data nodes (inputOrdering == SORTED). If the sort
            // could NOT be pushed (e.g. a non-sortable field type), the data arrives unsorted and buffering
            // MAX_VALUE rows would OOM. Reject here.
            if (p instanceof TopNExec topNExec
                && topNExec.inputOrdering() != TopNOperator.InputOrdering.SORTED
                && topNExec.unboundedSort()) {
                failures.add(
                    fail(
                        topNExec,
                        "SORT [{}] with an unbounded limit is not supported for this field type. Add a LIMIT before the sort.",
                        topNExec.sourceText()
                    )
                );
            }

            PlanConsistencyChecker.checkPlan(p, depFailures);

            if (failures.hasFailures() == false) {
                if (p instanceof PostPhysicalOptimizationVerificationAware va) {
                    va.postPhysicalOptimizationVerification(failures);
                }
                p.forEachExpression(ex -> {
                    if (ex instanceof PostPhysicalOptimizationVerificationAware va) {
                        va.postPhysicalOptimizationVerification(failures);
                    }
                });
            }
        });
    }
}

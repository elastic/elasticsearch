/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.PostPhysicalOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/** Physical plan verifier. */
public final class PhysicalVerifier extends PostOptimizationPhasePlanVerifier<PhysicalPlan> {

    private PhysicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    public static PhysicalVerifier getLocalVerifier() {
        return new PhysicalVerifier(true);
    }

    public static PhysicalVerifier getGeneralVerifier() {
        return new PhysicalVerifier(false);
    }

    @Override
    void checkPlanConsistency(PhysicalPlan optimizedPlan, Failures failures, Failures depFailures) {
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

            // This check applies only for general physical plans (isLocal == false)
            if (isLocal == false && p instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.REMOTE) {
                failures.add(fail(p, "Physical plan contains remote executing operation [{}] in local part", p.nodeName()));
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

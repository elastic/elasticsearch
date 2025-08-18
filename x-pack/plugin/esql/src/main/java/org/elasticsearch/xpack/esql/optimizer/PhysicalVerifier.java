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
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/** Physical plan verifier. */
public final class PhysicalVerifier extends PostOptimizationPhasePlanVerifier<PhysicalPlan> {

    public static final PhysicalVerifier INSTANCE = new PhysicalVerifier();

    private PhysicalVerifier() {}

    @Override
    boolean skipVerification(PhysicalPlan optimizedPlan, boolean skipRemoteEnrichVerification) {
        if (skipRemoteEnrichVerification) {
            // AwaitsFix https://github.com/elastic/elasticsearch/issues/118531
            var enriches = optimizedPlan.collectFirstChildren(EnrichExec.class::isInstance);
            if (enriches.isEmpty() == false && ((EnrichExec) enriches.get(0)).mode() == Enrich.Mode.REMOTE) {
                return true;
            }
        }
        return false;
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

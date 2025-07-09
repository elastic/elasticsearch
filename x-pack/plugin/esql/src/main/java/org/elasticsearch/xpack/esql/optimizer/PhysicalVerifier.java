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
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import static org.elasticsearch.index.IndexMode.LOOKUP;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.datatypeEquals;

/** Physical plan verifier. */
public final class PhysicalVerifier {

    public static final PhysicalVerifier INSTANCE = new PhysicalVerifier();

    private PhysicalVerifier() {}

    /** Verifies the physical plan. */
    public Failures verify(PhysicalPlan planAfter, boolean skipRemoteEnrichVerification, PhysicalPlan planBefore) {
        Failures failures = new Failures();
        Failures depFailures = new Failures();

        if (skipRemoteEnrichVerification) {
            // AwaitsFix https://github.com/elastic/elasticsearch/issues/118531
            var enriches = planAfter.collectFirstChildren(EnrichExec.class::isInstance);
            if (enriches.isEmpty() == false && ((EnrichExec) enriches.get(0)).mode() == Enrich.Mode.REMOTE) {
                return failures;
            }
        }

        planAfter.forEachDown(p -> {
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

        if (datatypeEquals(planBefore.output(), planAfter.output()) == false) {
            if ((planAfter instanceof EsQueryExec esQueryExec && esQueryExec.indexMode() == LOOKUP) == false) {
                failures.add(
                    fail(planAfter, "Layout has changed from [{}] to [{}]. ", planBefore.output().toString(), planAfter.output().toString())
                );
            }
        }

        if (depFailures.hasFailures()) {
            throw new IllegalStateException(depFailures.toString());
        }

        return failures;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.OptimizerRules.PhysicalPlanDependencyCheck;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/** Physical plan verifier. */
public final class PhysicalVerifier {

    public static final PhysicalVerifier INSTANCE = new PhysicalVerifier();
    private static final PhysicalPlanDependencyCheck DEPENDENCY_CHECK = new PhysicalPlanDependencyCheck();

    private PhysicalVerifier() {}

    /** Verifies the physical plan. */
    public Collection<Failure> verify(PhysicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        plan.forEachDown(p -> {
            // FIXME: re-enable
            // DEPENDENCY_CHECK.checkPlan(p, failures);
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
        });

        return failures;
    }
}

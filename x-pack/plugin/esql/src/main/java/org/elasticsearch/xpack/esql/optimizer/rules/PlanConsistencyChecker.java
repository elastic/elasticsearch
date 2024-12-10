/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.physical.BinaryExec;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class PlanConsistencyChecker<P extends QueryPlan<P>> {

    /**
     * Check whether a single {@link QueryPlan} produces no duplicate attributes and its children provide all of its required
     * {@link QueryPlan#references() references}. Otherwise, add
     * {@link org.elasticsearch.xpack.esql.common.Failure Failure}s to the {@link Failures} object.
     */
    public void checkPlan(P p, Failures failures) {
        if (p instanceof BinaryPlan binaryPlan) {
            checkMissing(p, binaryPlan.leftReferences(), binaryPlan.left().outputSet(), "missing references from left hand side", failures);
            checkMissing(
                p,
                binaryPlan.rightReferences(),
                binaryPlan.right().outputSet(),
                "missing references from right hand side",
                failures
            );
        } else if (p instanceof BinaryExec binaryExec) {
            checkMissing(p, binaryExec.leftReferences(), binaryExec.left().outputSet(), "missing references from left hand side", failures);
            checkMissing(
                p,
                binaryExec.rightReferences(),
                binaryExec.right().outputSet(),
                "missing references from right hand side",
                failures
            );
        } else {
            checkMissing(p, p.references(), p.inputSet(), "missing references", failures);
        }

        Set<String> outputAttributeNames = new HashSet<>();
        Set<NameId> outputAttributeIds = new HashSet<>();
        for (Attribute outputAttr : p.output()) {
            if (outputAttributeNames.add(outputAttr.name()) == false || outputAttributeIds.add(outputAttr.id()) == false) {
                failures.add(
                    fail(p, "Plan [{}] optimized incorrectly due to duplicate output attribute {}", p.nodeString(), outputAttr.toString())
                );
            }
        }
    }

    private void checkMissing(P plan, AttributeSet references, AttributeSet input, String detailErrorMessage, Failures failures) {
        AttributeSet missing = references.subtract(input);
        if (missing.isEmpty() == false) {
            failures.add(fail(plan, "Plan [{}] optimized incorrectly due to {} [{}]", plan.nodeString(), detailErrorMessage, missing));
        }
    }
}

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
        AttributeSet refs = p.references();
        AttributeSet input = p.inputSet();
        AttributeSet missing = refs.subtract(input);
        // TODO: for Joins, we should probably check if the required fields from the left child are actually in the left child, not
        // just any child (and analogously for the right child).
        if (missing.isEmpty() == false) {
            failures.add(fail(p, "Plan [{}] optimized incorrectly due to missing references {}", p.nodeString(), missing));
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
}

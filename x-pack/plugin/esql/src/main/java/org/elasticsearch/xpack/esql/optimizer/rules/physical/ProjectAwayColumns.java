/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;

/**
 * Adds an explicit project to minimize the amount of attributes sent from the local plan to the coordinator.
 * This is done here to localize the project close to the data source and simplify the upcoming field
 * extraction.
 */
public class ProjectAwayColumns extends Rule<PhysicalPlan, PhysicalPlan> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        Holder<Boolean> keepTraversing = new Holder<>(TRUE);
        // Invariant: if we add a projection with these attributes after the current plan node, the plan remains valid
        // and the overall output will not change.
        Holder<AttributeSet> requiredAttributes = new Holder<>(plan.outputSet());

        // This will require updating should we choose to have non-unary execution plans in the future.
        return plan.transformDown(UnaryExec.class, currentPlanNode -> {
            if (keepTraversing.get() == false) {
                return currentPlanNode;
            }
            if (currentPlanNode instanceof ExchangeExec exec) {
                keepTraversing.set(FALSE);
                var child = exec.child();
                // otherwise expect a Fragment
                if (child instanceof FragmentExec fragmentExec) {
                    var logicalFragment = fragmentExec.fragment();

                    // no need for projection when dealing with aggs
                    if (logicalFragment instanceof Aggregate == false) {
                        List<Attribute> output = new ArrayList<>(requiredAttributes.get());
                        // if all the fields are filtered out, it's only the count that matters
                        // however until a proper fix (see https://github.com/elastic/elasticsearch/issues/98703)
                        // add a synthetic field (so it doesn't clash with the user defined one) to return a constant
                        // to avoid the block from being trimmed
                        if (output.isEmpty()) {
                            var alias = new Alias(logicalFragment.source(), "<all-fields-projected>", Literal.NULL, null, true);
                            List<Alias> fields = singletonList(alias);
                            logicalFragment = new Eval(logicalFragment.source(), logicalFragment, fields);
                            output = Expressions.asAttributes(fields);
                        }
                        // add a logical projection (let the local replanning remove it if needed)
                        FragmentExec newChild = new FragmentExec(
                            Source.EMPTY,
                            new Project(logicalFragment.source(), logicalFragment, output),
                            fragmentExec.esFilter(),
                            fragmentExec.estimatedRowSize(),
                            fragmentExec.reducer()
                        );
                        return new ExchangeExec(exec.source(), output, exec.inBetweenAggs(), newChild);
                    }
                }
            } else {
                AttributeSet childOutput = currentPlanNode.inputSet();
                AttributeSet addedAttributes = currentPlanNode.outputSet().subtract(childOutput);
                requiredAttributes.set(requiredAttributes.get().subtract(addedAttributes).combine(currentPlanNode.references()));
            }
            return currentPlanNode;
        });
    }
}

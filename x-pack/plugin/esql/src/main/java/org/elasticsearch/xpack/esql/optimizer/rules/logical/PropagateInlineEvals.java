/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin.replaceStub;
import static org.elasticsearch.xpack.esql.plan.logical.join.StubRelation.computeOutput;

/**
 * Replace any evaluation from the inlined aggregation side (right side) to the left side (source) to perform the matching.
 * In INLINE STATS m = MIN(x) BY a + b the right side contains STATS m = MIN(X) BY a + b.
 * As the grouping key is used to perform the join, the evaluation required for creating it has to be copied to the left side
 * as well.
 */
public class PropagateInlineEvals extends OptimizerRules.OptimizerRule<InlineJoin> {

    @Override
    protected LogicalPlan rule(InlineJoin plan) {
        // check if there's any grouping that uses a reference on the right side
        // if so, look for the source until finding a StubReference
        // then copy those on the left side as well
        LogicalPlan left = plan.left();
        LogicalPlan right = plan.right();

        // grouping references
        List<Alias> groupingAlias = new ArrayList<>();
        AttributeSet.Builder groupingRefs = AttributeSet.builder();

        // perform only one iteration that does two things
        // first checks any aggregate that declares expressions inside the grouping
        // second that checks any found references to collect their declaration
        right = right.transformDown(p -> {
            if (p instanceof Aggregate aggregate) {
                // collect references
                for (Expression g : aggregate.groupings()) {
                    if (g instanceof ReferenceAttribute ref) {
                        groupingRefs.add(ref);
                    }
                }
            }

            if (groupingRefs.isEmpty()) {
                return p;
            }

            // find their declaration and remove it
            if (p instanceof Eval eval) {
                List<Alias> fields = eval.fields();
                List<Alias> remainingEvals = new ArrayList<>(fields.size());
                for (Alias f : fields) {
                    // TODO: look into identifying refs by their NameIds instead
                    if (groupingRefs.remove(f.toAttribute())) {
                        groupingAlias.add(f);
                    } else {
                        remainingEvals.add(f);
                    }
                }
                if (remainingEvals.size() != fields.size()) {
                    // if all fields are moved, replace the eval
                    p = remainingEvals.size() == 0 ? eval.child() : new Eval(eval.source(), eval.child(), remainingEvals);
                }
            }
            return p;
        });

        // copy found evals on the left side
        if (groupingAlias.size() > 0) {
            left = new Eval(plan.source(), plan.left(), groupingAlias);
        }

        // check if any DATE_FORMAT has been optimized into DATE_TRUNC by ReplaceAggregateNestedExpressionWithEval
        List<Attribute> leftFields = plan.config().leftFields();
        List<Attribute> rightFields = plan.config().rightFields();
        AttributeSet.Builder shadowing = AttributeSet.builder();
        for (Alias a : groupingAlias) {
            if (rightFields.contains(a.toAttribute()) == false) {
                shadowing.add(a.toAttribute());
            }
        }
        if (shadowing.isEmpty() == false && right instanceof Project project) {
            AttributeSet.Builder builder = AttributeSet.builder();
            builder.addAll(project.output());
            AttributeSet projectOutput = builder.build();

            if (project.child() instanceof Eval eval && projectOutput.containsAll(leftFields)) {
                Map<Attribute, Attribute> replacements = new HashMap<>();

                for (Alias f : eval.fields()) {
                    if (f.child() instanceof DateFormat df && shadowing.remove(df.field())) {
                        Attribute original = (Attribute) df.field();
                        Attribute aliasAttr = f.toAttribute();
                        replacements.put(original, aliasAttr);

                        // Replace aliasAttr with original in rightFields
                        int rIndex = rightFields.indexOf(aliasAttr);
                        if (rIndex >= 0) rightFields.set(rIndex, original);

                        // Replace aliasAttr with original in leftFields
                        int lIndex = leftFields.indexOf(aliasAttr);
                        if (lIndex >= 0) leftFields.set(lIndex, original);
                    }
                }

                // If all shadowing attributes are handled
                if (shadowing.isEmpty()) {
                    right = eval.child();

                    List<Attribute> output = plan.output();
                    output.replaceAll(attr -> replacements.getOrDefault(attr, attr));

                    LogicalPlan join = plan.replaceChildren(
                        left,
                        InlineJoin.replaceStub(new StubRelation(right.source(), left.output()), right)
                    );
                    LogicalPlan evalPlan = eval.replaceChild(join);
                    return new Project(evalPlan.source(), evalPlan, output);
                }
            }
        }

        // replace the old stub with the new out to capture the new output
        return plan.replaceChildren(left, replaceStub(new StubRelation(right.source(), computeOutput(right, left)), right));
    }
}

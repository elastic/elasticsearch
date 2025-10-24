/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameUtils.locallyUniqueTemporaryName;

/**
 * Replace aliasing evals (eval x=a) with a projection which can be further combined / simplified.
 * The rule gets applied only if there's another project (Project/Stats) above it.
 * <p>
 * Needs to take into account shadowing of potentially intermediate fields:
 * eval x = a + 1, y = x, z = y + 1, y = z, w = y + 1
 * The output should be
 * eval x = a + 1, z = a + 1 + 1, w = a + 1 + 1
 * project x, z, z as y, w
 */
public final class ReplaceAliasingEvalWithProject extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        Holder<Boolean> enabled = new Holder<>(false);

        return logicalPlan.transformDown(p -> {
            // found projection, turn enable flag on
            if (p instanceof Aggregate || p instanceof Project) {
                enabled.set(true);
            } else if (enabled.get() && p instanceof Eval eval) {
                p = rule(eval);
            }

            return p;
        });
    }

    private LogicalPlan rule(Eval eval) {
        LogicalPlan plan = eval;

        // Mostly, holds simple aliases from the eval, such as b = a, c = b, d = c, so we can resolve them in subsequent eval fields
        AttributeMap.Builder<Expression> renamesToPropagate = AttributeMap.builder();
        // the aliases for the final projection - mostly, same as above but holds the final aliases rather than the original attributes
        AttributeMap.Builder<NamedExpression> projectionAliases = AttributeMap.builder();
        // The names of attributes that are required to perform the aliases in a subsequent projection - if the next eval field
        // shadows one of these names, the subsequent projection won't work, so we need to perform a temporary rename.
        Set<String> namesRequiredForProjectionAliases = new HashSet<>();

        List<Alias> newEvalFields = new ArrayList<>();

        var fields = eval.fields();
        for (int i = 0, size = fields.size(); i < size; i++) {
            Alias field = fields.get(i);
            var attribute = field.toAttribute();
            // propagate all previous aliases into the current field
            field = (Alias) field.transformUp(e -> renamesToPropagate.build().resolve(e, e));
            Expression child = field.child();

            if (child instanceof Attribute renamedAttribute) {
                // Basic renaming - let's do that in the subsequent projection
                renamesToPropagate.put(attribute, renamedAttribute);
                projectionAliases.put(attribute, field);
                namesRequiredForProjectionAliases.add(renamedAttribute.name());
            } else {
                // not a basic renaming, needs to remain in the eval

                // The field may shadow one of the attributes that we will need to correctly perform the subsequent projection.
                // If so, rename it in the eval!
                if (namesRequiredForProjectionAliases.contains(field.name())) {
                    Alias newField = new Alias(field.source(), locallyUniqueTemporaryName(field.name()), field.child(), null, true);
                    Alias reRenamedField = new Alias(field.source(), field.name(), newField.toAttribute(), field.id(), field.synthetic());
                    projectionAliases.put(field.toAttribute(), reRenamedField);
                    // the renaming also needs to be propagated to eval fields to the right
                    renamesToPropagate.put(field.toAttribute(), newField.toAttribute());

                    field = newField;
                }

                newEvalFields.add(field);
            }
        }

        // at least one alias encountered, move it into a project
        if (renamesToPropagate.build().size() > 0) {
            // preserve the eval output (takes care of shadowing and order) but replace the basic aliases
            List<NamedExpression> projections = new ArrayList<>(eval.output());
            var projectionAliasesMap = projectionAliases.build();
            // replace the removed aliases with their initial definition - however use the output to preserve the shadowing
            for (int i = projections.size() - 1; i >= 0; i--) {
                NamedExpression project = projections.get(i);
                projections.set(i, projectionAliasesMap.getOrDefault(project, project));
            }

            LogicalPlan child = eval.child();
            if (newEvalFields.size() > 0) {
                // replace the eval with just the kept fields
                child = new Eval(eval.source(), eval.child(), newEvalFields);
            }
            // put the projection in place
            plan = new Project(eval.source(), child, projections);
        }

        return plan;
    }
}

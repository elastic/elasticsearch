/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

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

        // holds simple aliases such as b = a, c = b, d = c
        AttributeMap<Expression> basicAliases = new AttributeMap<>();
        // same as above but keeps the original expression
        AttributeMap<NamedExpression> basicAliasSources = new AttributeMap<>();

        List<Alias> keptFields = new ArrayList<>();

        var fields = eval.fields();
        for (int i = 0, size = fields.size(); i < size; i++) {
            Alias field = fields.get(i);
            Expression child = field.child();
            var attribute = field.toAttribute();
            // put the aliases in a separate map to separate the underlying resolve from other aliases
            if (child instanceof Attribute) {
                basicAliases.put(attribute, child);
                basicAliasSources.put(attribute, field);
            } else {
                // be lazy and start replacing name aliases only if needed
                if (basicAliases.size() > 0) {
                    // update the child through the field
                    field = (Alias) field.transformUp(e -> basicAliases.resolve(e, e));
                }
                keptFields.add(field);
            }
        }

        // at least one alias encountered, move it into a project
        if (basicAliases.size() > 0) {
            // preserve the eval output (takes care of shadowing and order) but replace the basic aliases
            List<NamedExpression> projections = new ArrayList<>(eval.output());
            // replace the removed aliases with their initial definition - however use the output to preserve the shadowing
            for (int i = projections.size() - 1; i >= 0; i--) {
                NamedExpression project = projections.get(i);
                projections.set(i, basicAliasSources.getOrDefault(project, project));
            }

            LogicalPlan child = eval.child();
            if (keptFields.size() > 0) {
                // replace the eval with just the kept fields
                child = new Eval(eval.source(), eval.child(), keptFields);
            }
            // put the projection in place
            plan = new Project(eval.source(), child, projections);
        }

        return plan;
    }
}

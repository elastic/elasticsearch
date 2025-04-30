/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.isGuaranteedNull;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.INNER;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * The rule matches a plan pattern having a Join on top of a Project and/or Eval. It then checks if the join's performed on a field which
 * is aliased to null (in type or value); if that's the case, it prunes the join, replacing it with an Eval  - returning aliases to null
 * for all the fields added in by the right side of the Join - plus a Project on top of it.
 * The rule can apply on the coordinator already, but it's more likely to be effective on the data nodes, where null aliasing is inserted
 * due to locally missing fields. This rule relies on that behavior -- see {@link ReplaceMissingFieldWithNull}.
 */
public class PruneJoinOnNullMatchingField extends OptimizerRules.OptimizerRule<Join> {

    @Override
    protected LogicalPlan rule(Join join) {
        var joinType = join.config().type();
        if ((joinType == INNER || joinType == LEFT) == false) { // other types will have different replacement logic
            return join;
        }
        AttributeMap.Builder<Expression> attributeMapBuilder = AttributeMap.builder();
        loop: for (var child = join.left();; child = ((UnaryPlan) child).child()) { // cast is safe as both Project and Eval are UnaryPlans
            switch (child) {
                case Project project -> project.projections().forEach(projection -> {
                    if (projection instanceof Alias alias) {
                        attributeMapBuilder.put(alias.toAttribute(), alias.child());
                    }
                });
                case Eval eval -> eval.fields().forEach(alias -> attributeMapBuilder.put(alias.toAttribute(), alias.child()));
                default -> {
                    break loop;
                }
            }
        }
        for (var attr : AttributeSet.of(join.config().matchFields())) {
            var resolved = attributeMapBuilder.build().resolve(attr);
            if (resolved != null && isGuaranteedNull(resolved)) {
                return replaceJoin(join);
            }
        }
        return join;
    }

    private static LogicalPlan replaceJoin(Join join) {
        var joinRightOutput = join.rightOutputFields();
        if (joinRightOutput.isEmpty()) { // can be empty when the join key is null and the other right side entries pruned (by an agg)
            return join.left();
        }
        List<Alias> aliases = new ArrayList<>(joinRightOutput.size());
        // TODO: cache aliases by type, à la ReplaceMissingFieldWithNull#missingToNull (tho lookup indices won't have Ks of fields)
        joinRightOutput.forEach(a -> aliases.add(new Alias(a.source(), a.name(), Literal.of(a, null), a.id())));
        var eval = new Eval(join.source(), join.left(), aliases);
        return new Project(join.source(), eval, join.computeOutput(join.left().output(), Expressions.asAttributes(aliases)));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.isGuaranteedNull;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * The rule checks if the join's performed on a field which is aliased to null (in type or value); if that's the case, it prunes the join,
 * replacing it with an Eval - returning aliases to null for all the fields added in by the right side of the Join - plus a Project on top
 * of it. The rule can apply on the coordinator already, but it's more likely to be effective on the data nodes, where null aliasing is
 * inserted due to locally missing fields. This rule relies on that behavior -- see {@link ReplaceFieldWithConstantOrNull}.
 */
public class PruneLeftJoinOnNullMatchingField extends OptimizerRules.ParameterizedOptimizerRule<Join, LogicalOptimizerContext> {

    public PruneLeftJoinOnNullMatchingField() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Join join, LogicalOptimizerContext ctx) {
        LogicalPlan plan = join;
        if (join.config().type() == LEFT) { // other types will have different replacement logic
            AttributeMap<Expression> attributeMap = RuleUtils.foldableReferences(join, ctx);

            for (var attr : AttributeSet.of(join.config().matchFields())) {
                var resolved = attributeMap.resolve(attr);
                if (resolved != null && isGuaranteedNull(resolved)) {
                    plan = replaceJoin(join);
                    break;
                }
            }
        }
        return plan;
    }

    private static LogicalPlan replaceJoin(Join join) {
        var joinRightOutput = join.rightOutputFields();
        // can be empty when the join key is null and the rest of the right side entries pruned (such as by an agg)
        if (joinRightOutput.isEmpty()) {
            return join.left();
        }
        var aliasedNulls = RuleUtils.aliasedNulls(joinRightOutput, a -> true);
        var eval = new Eval(join.source(), join.left(), aliasedNulls.v1());
        return new Project(join.source(), eval, join.computeOutput(join.left().output(), Expressions.asAttributes(aliasedNulls.v2())));
    }
}

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
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
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
        // Other join types will have different replacement logic.
        // This rule should only apply to LOOKUP JOIN, not INLINE STATS. For INLINE STATS, the join key is always
        // the grouping, and since STATS supports GROUP BY null, pruning the join when the join key (grouping) is
        // null would incorrectly change the query results.
        // Note: We use `instanceof InlineJoin == false` rather than `instanceof LookupJoin` because LookupJoin is
        // converted to a regular Join during analysis (via LookupJoin.surrogate()). By the time this optimizer rule
        // runs, the LookupJoin has already become a plain Join instance, so checking for LookupJoin would fail to
        // match the intended targets. The negative check correctly excludes InlineJoin while accepting all other
        // LEFT joins, including those originally created as LookupJoin.
        if (join.config().type() == LEFT && join instanceof InlineJoin == false) {
            AttributeMap<Expression> attributeMap = RuleUtils.foldableReferences(join, ctx);

            for (var attr : AttributeSet.of(join.config().leftFields())) {
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
        return new Project(join.source(), eval, join.computeOutputExpressions(join.left().output(), aliasedNulls.v2()));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * A {@code JOIN} whose right side is a subquery written by the user, as in {@code FROM a | LEFT OUTER JOIN (FROM b | ...) ON key}.
 * <p>
 * The subquery is an independent query: it runs before the main query and only its result reaches the join, so plan walks that
 * reason about the main query stop at its right side (see {@link IndependentSubqueryJoin}). The coordinator replaces the subquery
 * with its result before the plan is mapped, so this node is never serialized.
 */
public class SubqueryJoin extends Join implements IndependentSubqueryJoin {

    public SubqueryJoin(Source source, LogicalPlan left, LogicalPlan subquery, JoinConfig config) {
        super(source, left, subquery, config, ExecuteLocation.ANY);
    }

    public SubqueryJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan subquery,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions
    ) {
        this(source, left, subquery, new JoinConfig(type, leftFields, rightFields, joinOnConditions));
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<Join> info() {
        // Split the config so that its expressions are visible to expression transformations.
        JoinConfig config = config();
        return NodeInfo.create(
            this,
            SubqueryJoin::new,
            left(),
            right(),
            config.type(),
            config.leftFields(),
            config.rightFields(),
            config.joinOnConditions()
        );
    }

    @Override
    public SubqueryJoin withConfig(JoinConfig config) {
        return new SubqueryJoin(source(), left(), right(), config);
    }

    @Override
    public SubqueryJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new SubqueryJoin(source(), left, right, config());
    }
}

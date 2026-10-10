/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * A LOOKUP JOIN whose lookup rows come from an independent subquery. The coordinator materializes the right side before mapping, so this
 * node is never serialized. Right-side outputs are references because they do not belong to the left index.
 */
public class SubqueryLookupJoin extends Join implements IndependentSubqueryJoin, TelemetryAware {

    public SubqueryLookupJoin(Source source, LogicalPlan left, LogicalPlan subquery, List<Attribute> joinFields) {
        this(source, left, subquery, new JoinConfig(LEFT, joinFields, joinFields, null), ExecuteLocation.ANY);
    }

    public SubqueryLookupJoin(Source source, LogicalPlan left, LogicalPlan subquery, JoinConfig config, ExecuteLocation mode) {
        super(source, left, subquery, config, mode);
    }

    public SubqueryLookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan subquery,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions,
        ExecuteLocation mode
    ) {
        this(source, left, subquery, new JoinConfig(type, leftFields, rightFields, joinOnConditions), mode);
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return Join.computeOutputExpressions(left, makeReference(Expressions.asAttributes(right)), config());
    }

    @Override
    public SubqueryLookupJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new SubqueryLookupJoin(source(), left, right, config(), executesOn());
    }

    @Override
    public SubqueryLookupJoin withConfig(JoinConfig config) {
        return new SubqueryLookupJoin(source(), left(), right(), config, executesOn());
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(
            this,
            SubqueryLookupJoin::new,
            left(),
            right(),
            config().type(),
            config().leftFields(),
            config().rightFields(),
            config().joinOnConditions(),
            executesOn()
        );
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
    public String telemetryLabel() {
        return "LOOKUP JOIN SUBQUERY";
    }

    @Override
    public LogicalPlan inlineSubqueryResult(LocalRelation result, SubqueryResultContext context) {
        throw new UnsupportedOperationException("subquery lookup join result inlining is not implemented");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.List;

/**
 * A LOOKUP JOIN whose independent subquery has been materialized for physical planning.
 */
public class MaterializedLookupJoin extends Join {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "MaterializedLookupJoin",
        MaterializedLookupJoin::new
    );

    private static final TransportVersion ESQL_LOOKUP_JOIN_SUBQUERY = Analyzer.ESQL_LOOKUP_JOIN_SUBQUERY;

    public MaterializedLookupJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config, ExecuteLocation mode) {
        super(source, left, right, config, mode);
    }

    public MaterializedLookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions,
        ExecuteLocation mode
    ) {
        this(source, left, right, new JoinConfig(type, leftFields, rightFields, joinOnConditions), mode);
    }

    public MaterializedLookupJoin(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return Join.computeOutputExpressions(left, makeReference(Expressions.asAttributes(right)), config());
    }

    @Override
    public MaterializedLookupJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new MaterializedLookupJoin(source(), left, right, config(), executesOn());
    }

    @Override
    public MaterializedLookupJoin withConfig(JoinConfig config) {
        return new MaterializedLookupJoin(source(), left(), right(), config, executesOn());
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(
            this,
            MaterializedLookupJoin::new,
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
    public void writeTo(StreamOutput out) throws IOException {
        if (right() instanceof LocalRelation == false) {
            throw new IllegalStateException("materialized LOOKUP JOIN must have a LocalRelation on the right");
        }
        if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_SUBQUERY) == false) {
            throw new IllegalStateException("materialized LOOKUP JOIN cannot be sent to a node without subquery join support");
        }
        super.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}

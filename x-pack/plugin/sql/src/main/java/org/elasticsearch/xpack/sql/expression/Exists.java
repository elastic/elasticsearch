/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;

public class Exists extends SubQueryExpression {

    public Exists(Source source, LogicalPlan query) {
        this(source, query, null);
    }

    public Exists(Source source, LogicalPlan query, NameId id) {
        super(source, query, id);
    }

    @Override
    protected NodeInfo<Exists> info() {
        return NodeInfo.create(this, Exists::new, query(), id());
    }

    @Override
    protected SubQueryExpression clone(LogicalPlan newQuery) {
        return new Exists(source(), newQuery);
    }

    @Override
    public DataType dataType() {
        return BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }
}

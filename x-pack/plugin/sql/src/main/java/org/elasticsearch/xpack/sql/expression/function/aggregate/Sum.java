/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends NumericAggregate implements EnclosedAgg {

    public Sum(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Sum> info() {
        return NodeInfo.create(this, Sum::new, field());
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        return new Sum(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType().isInteger() ? LONG : DOUBLE;
    }

    @Override
    public String innerName() {
        return "sum";
    }
}

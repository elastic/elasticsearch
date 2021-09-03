/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Objects;

/**
 * Count the number of documents matched ({@code COUNT})
 * <strong>OR</strong> count the number of distinct values
 * for a field that matched ({@code COUNT(DISTINCT}.
 */
public class Count extends AggregateFunction {

    private final boolean distinct;

    public Count(Source source, Expression field, boolean distinct) {
        super(source, field);
        this.distinct = distinct;
    }

    @Override
    protected NodeInfo<Count> info() {
        return NodeInfo.create(this, Count::new, field(), distinct);
    }

    @Override
    public Count replaceChildren(List<Expression> newChildren) {
        return new Count(source(), newChildren.get(0), distinct);
    }

    public boolean distinct() {
        return distinct;
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), distinct());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Count other = (Count) obj;
            return Objects.equals(other.distinct(), distinct());
        }
        return false;
    }
}

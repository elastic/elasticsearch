/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.UnaryExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

/**
 * A cast that can't convert its input, which is always {@code null}. The analyzer uses it instead of a {@code null} literal when the
 * input contains an aggregate function, as in {@code STATS x = COUNT(*)::ip}: a constant isn't valid there, while an expression over
 * the aggregation is, and the optimizer moves it after the aggregation before replacing it with a {@code null} literal.
 */
public class NullCast extends UnaryExpression {
    private final DataType dataType;

    public NullCast(Source source, Expression child, DataType dataType) {
        super(source, child);
        this.dataType = dataType;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("replaced on the coordinator");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("replaced on the coordinator");
    }

    @Override
    protected NullCast replaceChild(Expression newChild) {
        return new NullCast(source(), newChild, dataType);
    }

    @Override
    protected NodeInfo<NullCast> info() {
        return NodeInfo.create(this, NullCast::new, child(), dataType);
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && dataType == ((NullCast) obj).dataType;
    }
}

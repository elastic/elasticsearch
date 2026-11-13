/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/**
 * The keys of {@code NESTED_ANY(<aggregate> BY <keys>)}, passed as the last argument of {@code NESTED_ANY}. It keeps them apart from
 * ordinary arguments; analysis lowers it with its {@link Nested} before evaluation or serialization.
 */
public final class NestedGroupings extends Expression {
    public NestedGroupings(Source source, List<Expression> keys) {
        super(source, keys);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public DataType dataType() {
        return DataType.NULL;
    }

    @Override
    protected TypeResolution resolveType() {
        return childrenResolved() ? TypeResolution.TYPE_RESOLVED : new TypeResolution("Unresolved NESTED BY keys");
    }

    @Override
    protected NodeInfo<NestedGroupings> info() {
        return NodeInfo.create(this, NestedGroupings::new, children());
    }

    @Override
    public NestedGroupings replaceChildren(List<Expression> children) {
        return new NestedGroupings(source(), children);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("unlowered NESTED BY keys");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("unlowered NESTED BY keys");
    }
}

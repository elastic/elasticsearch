/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Represent a key-value pair.
 */
public class EntryExpression extends Expression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "EntryExpression",
        EntryExpression::new
    );

    private final Literal key;

    private final Literal value;

    public EntryExpression(Source source, Literal key, Literal value) {
        super(source, List.of(key, value));
        this.key = key;
        this.value = value;
    }

    private EntryExpression(StreamInput in) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Literal.class),
            in.readNamedWriteable(Literal.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(key);
        out.writeNamedWriteable(value);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new EntryExpression(source(), (Literal) newChildren.get(0), (Literal) newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, EntryExpression::new, key, value);
    }

    public Expression key() {
        return key;
    }

    public Expression value() {
        return value;
    }

    @Override
    public DataType dataType() {
        return value.dataType();
    }

    @Override
    public boolean foldable() {
        return key.foldable() && value.foldable();
    }

    @Override
    public Object fold() {
        return toString();
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EntryExpression other = (EntryExpression) obj;
        return Objects.equals(key, other.key) && Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        return key.fold() + ":" + value.fold();
    }
}

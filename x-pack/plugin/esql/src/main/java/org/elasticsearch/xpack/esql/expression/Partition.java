/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isExact;

public class Partition extends Expression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Partition",
        Partition::new
    );

    private final Expression child;

    public Partition(Source source, Expression child) {
        super(source, List.of(child));
        this.child = child;
    }

    public Partition(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (DataType.isString(child.dataType())) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return isExact(child, "BY cannot be applied to field of data type [{}]: {}");
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public Partition replaceChildren(List<Expression> newChildren) {
        return new Partition(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<Partition> info() {
        return NodeInfo.create(this, Partition::new, child);
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    public Expression child() {
        return child;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Partition other = (Partition) obj;
        return Objects.equals(child, other.child);
    }
}

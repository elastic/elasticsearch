/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.List;

/** Explicit object-scope syntax. Analysis lowers this marker before evaluation or serialization. */
public final class Nested extends Function {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Nested.class).unary(Nested::new).name("nested");

    @FunctionInfo(
        preview = true,
        returnType = { "boolean", "integer", "long", "double", "keyword", "text" },
        briefSummary = "Evaluates an expression in one explicit nested object scope.",
        description = "Returns one completed Boolean or scalar reduction for each input parent row."
    )
    public Nested(
        Source source,
        @Param(
            name = "expression",
            type = { "boolean", "integer", "long", "double", "keyword", "text", "_source" },
            description = "Expression using fields from one nested path."
        ) Expression child
    ) {
        super(source, List.of(child));
    }

    public Expression field() {
        return children().getFirst();
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return childrenResolved() ? TypeResolution.TYPE_RESOLVED : new TypeResolution("Unresolved nested expression");
    }

    @Override
    protected NodeInfo<Nested> info() {
        return NodeInfo.create(this, Nested::new, field());
    }

    @Override
    public Nested replaceChildren(List<Expression> children) {
        return new Nested(source(), children.getFirst());
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("unlowered NESTED expression");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("unlowered NESTED expression");
    }
}

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

/**
 * Names a nested collection of the row's parent document so that later commands can read it through the name.
 * {@code EVAL o = NESTED_OBJECT(items)} makes {@code NESTED_EXPAND o} expand {@code items} into columns called {@code o.price},
 * {@code o.sku} and so on. It reads nothing: analysis resolves those names through the reference and then removes it, so it is
 * never evaluated, serialized or returned.
 */
public final class NestedObject extends Function {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedObject.class)
        .unary(NestedObject::new)
        .name("nested_object");

    @FunctionInfo(
        preview = true,
        returnType = { "object" },
        briefSummary = "Returns a reference to one nested collection for later nested functions and commands.",
        description = "Reads nothing. A later NESTED_EXPAND reads the collection through the reference as it reads the collection "
            + "itself. The reference is not a returned column and cannot be used as a value."
    )
    public NestedObject(
        Source source,
        @Param(name = "collection", type = { "_source" }, description = "A nested field of the index.") Expression collection
    ) {
        super(source, List.of(collection));
    }

    /** The nested collection this reference stands for. */
    public Expression collection() {
        return children().getFirst();
    }

    @Override
    public String functionName() {
        return "NESTED_OBJECT";
    }

    @Override
    public DataType dataType() {
        return DataType.OBJECT;
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected NodeInfo<NestedObject> info() {
        return NodeInfo.create(this, NestedObject::new, collection());
    }

    @Override
    public NestedObject replaceChildren(List<Expression> children) {
        return new NestedObject(source(), children.getFirst());
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("NESTED_OBJECT is removed by analysis");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("NESTED_OBJECT is removed by analysis");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.List;

/**
 * Names a nested collection of the row's parent document so later commands can read it, without reading anything itself.
 * {@code EVAL o = NESTED_OBJECT(items)} makes {@code o.price} name {@code items.price} in the nested functions,
 * {@code NESTED_EXPAND} and {@code NESTED_FLATTEN}. Analysis resolves those names through the reference and then drops it,
 * so it is never evaluated, serialized or returned.
 */
public final class NestedObject extends Nested {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedObject.class)
        .unary(NestedObject::new)
        .name("nested_object");

    @FunctionInfo(
        preview = true,
        returnType = { "object" },
        briefSummary = "Returns a reference to one nested collection for later nested functions and commands.",
        description = "Reads nothing. Later NESTED, NESTED_ANY, NESTED_ALL, NESTED_JSON, NESTED_EXPAND and NESTED_FLATTEN read the "
            + "collection through the reference as they read the collection itself. The reference is not a returned column."
    )
    public NestedObject(
        Source source,
        @Param(name = "collection", type = { "_source" }, description = "A nested field of the index.") Expression collection
    ) {
        super(source, collection, List.of());
    }

    @Override
    public DataType dataType() {
        return DataType.OBJECT;
    }

    @Override
    public String functionName() {
        return "NESTED_OBJECT";
    }

    @Override
    protected NodeInfo<NestedObject> info() {
        return NodeInfo.create(this, NestedObject::new, body());
    }

    @Override
    public NestedObject replaceChildren(List<Expression> children) {
        return new NestedObject(source(), children.getFirst());
    }
}

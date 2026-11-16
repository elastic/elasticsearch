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
 * Returns a nested collection for display, since ES|QL has no column type for a list of objects. A {@link Nested}
 * marker, so it is resolved and lowered with the other nested scopes, and reads child values the way they do.
 */
public final class NestedJson extends Nested {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedJson.class).unary(NestedJson::new).name("nested_json");

    @FunctionInfo(
        preview = true,
        returnType = { "keyword" },
        briefSummary = "Returns the objects of one nested collection as a JSON array.",
        description = "Returns one keyword for each input parent row holding its nested objects in document order, read from doc "
            + "values: arrays are in doc-values order and missing fields are left out. Null when the collection is empty."
    )
    public NestedJson(
        Source source,
        @Param(name = "collection", type = { "_source" }, description = "A nested field of the index.") Expression collection
    ) {
        super(source, collection, List.of());
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public String functionName() {
        return "NESTED_JSON";
    }

    @Override
    protected NodeInfo<NestedJson> info() {
        return NodeInfo.create(this, NestedJson::new, body());
    }

    @Override
    public NestedJson replaceChildren(List<Expression> children) {
        return new NestedJson(source(), children.getFirst());
    }
}

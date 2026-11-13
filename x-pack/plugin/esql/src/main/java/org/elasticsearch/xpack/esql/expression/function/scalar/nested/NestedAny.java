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
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.List;

/**
 * Evaluates an expression in one nested collection: a predicate is true when any nested object matches, and an aggregate
 * reduces each parent's objects, optionally {@code BY} child fields.
 */
public final class NestedAny extends Nested implements OptionalArgument {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedAny.class)
        .unaryVariadic(NestedAny::new)
        .name("nested_any");

    @FunctionInfo(
        preview = true,
        returnType = { "boolean", "integer", "long", "double", "keyword", "text" },
        briefSummary = "Evaluates an expression in one explicit nested object scope.",
        description = "Returns one completed Boolean or scalar reduction for each input parent row. "
            + "With `BY`, returns a keyword holding a JSON object of the aggregate for each group of nested objects."
    )
    public NestedAny(
        Source source,
        @Param(
            name = "expression",
            type = { "boolean", "integer", "long", "double", "keyword", "text", "_source" },
            description = "Expression using fields from one nested path, or the nested collection the next expression reads."
        ) Expression child,
        @Param(
            name = "keys",
            type = { "boolean", "integer", "long", "double", "keyword", "text" },
            optional = true,
            description = "The expression over the named collection, then the child fields after `BY` to group the nested objects by."
        ) List<Expression> rest
    ) {
        super(source, child, rest);
    }

    @Override
    public String functionName() {
        return "NESTED_ANY";
    }

    @Override
    protected NodeInfo<NestedAny> info() {
        return NodeInfo.create(this, NestedAny::new, children().getFirst(), children().subList(1, children().size()));
    }

    @Override
    public NestedAny replaceChildren(List<Expression> children) {
        return new NestedAny(source(), children.getFirst(), children.subList(1, children.size()));
    }
}

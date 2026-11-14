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
 * True when a predicate holds for every one of a parent's nested objects; true when there are none, and false when the
 * predicate is null for any of them.
 */
public final class NestedAll extends Nested implements OptionalArgument {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedAll.class)
        .unaryVariadic(NestedAll::new)
        .name("nested_all");

    @FunctionInfo(
        preview = true,
        returnType = "boolean",
        briefSummary = "Returns true when a predicate holds for every nested object.",
        description = "Returns, for each input parent row, whether all of its nested objects match the predicate. "
            + "A parent without nested objects matches; a nested object for which the predicate is null does not."
    )
    public NestedAll(
        Source source,
        @Param(
            name = "predicate",
            type = { "boolean", "_source" },
            description = "Predicate over fields from one nested path, or the nested collection the next predicate reads."
        ) Expression child,
        @Param(
            name = "collectionPredicate",
            type = "boolean",
            optional = true,
            description = "The predicate over the named collection."
        ) List<Expression> rest
    ) {
        super(source, child, rest);
    }

    @Override
    public String functionName() {
        return "NESTED_ALL";
    }

    @Override
    protected NodeInfo<NestedAll> info() {
        return NodeInfo.create(this, NestedAll::new, children().getFirst(), children().subList(1, children().size()));
    }

    @Override
    public NestedAll replaceChildren(List<Expression> children) {
        return new NestedAll(source(), children.getFirst(), children.subList(1, children.size()));
    }
}

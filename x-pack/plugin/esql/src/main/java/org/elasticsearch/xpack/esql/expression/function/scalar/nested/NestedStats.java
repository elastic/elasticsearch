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

/** Reduces each parent's nested objects with an aggregate, optionally {@code BY} child fields, the way STATS reduces rows. */
public final class NestedStats extends Nested implements OptionalArgument {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedStats.class)
        .unaryVariadic(NestedStats::new)
        .name("nested");

    @FunctionInfo(
        preview = true,
        returnType = { "integer", "long", "double", "keyword", "text" },
        briefSummary = "Reduces the nested objects of each parent with an aggregate.",
        description = "Returns one completed reduction for each input parent row. "
            + "With `BY`, returns a keyword holding a JSON object of the aggregate for each group of nested objects."
    )
    public NestedStats(
        Source source,
        @Param(
            name = "expression",
            type = { "integer", "long", "double", "keyword", "text", "_source" },
            description = "Aggregate over fields from one nested path, or the nested collection the next expression reads."
        ) Expression child,
        @Param(
            name = "keys",
            type = { "integer", "long", "double", "keyword", "text" },
            optional = true,
            description = "The aggregate over the named collection, then the child fields after `BY` to group the nested objects by."
        ) List<Expression> rest
    ) {
        super(source, child, rest);
    }

    @Override
    public String functionName() {
        return "NESTED";
    }

    @Override
    protected NodeInfo<NestedStats> info() {
        return NodeInfo.create(this, NestedStats::new, children().getFirst(), children().subList(1, children().size()));
    }

    @Override
    public NestedStats replaceChildren(List<Expression> children) {
        return new NestedStats(source(), children.getFirst(), children.subList(1, children.size()));
    }
}

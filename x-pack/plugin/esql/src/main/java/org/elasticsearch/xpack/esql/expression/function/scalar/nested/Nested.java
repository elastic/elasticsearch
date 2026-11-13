/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.ArrayList;
import java.util.List;

/** Explicit object-scope syntax. Analysis lowers this marker before evaluation or serialization. */
public final class Nested extends Function implements OptionalArgument {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Nested.class).unaryVariadic(Nested::new).name("nested");

    @FunctionInfo(
        preview = true,
        returnType = { "boolean", "integer", "long", "double", "keyword", "text" },
        briefSummary = "Evaluates an expression in one explicit nested object scope.",
        description = "Returns one completed Boolean or scalar reduction for each input parent row. "
            + "With `BY`, returns a keyword holding a JSON object of the aggregate for each group of nested objects."
    )
    public Nested(
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
        super(source, children(child, rest));
    }

    private static List<Expression> children(Expression child, List<Expression> rest) {
        List<Expression> children = new ArrayList<>(1 + rest.size());
        children.add(child);
        children.addAll(rest);
        return children;
    }

    /** The arguments before the {@code BY} keys. */
    private List<Expression> beforeKeys() {
        List<Expression> children = children();
        return children.getLast() instanceof NestedGroupings ? children.subList(0, children.size() - 1) : children;
    }

    /** The nested collection named before the expression, or {@code null} when the expression's child fields give it. */
    @Nullable
    public Expression collection() {
        return beforeKeys().size() > 1 ? beforeKeys().getFirst() : null;
    }

    /** The expression evaluated over each parent's nested objects. */
    public Expression body() {
        return beforeKeys().get(collection() == null ? 0 : 1);
    }

    /** The {@code BY} keys, or empty when the reduction is not grouped. */
    public List<Expression> keys() {
        return children().getLast() instanceof NestedGroupings keys ? keys.children() : List.of();
    }

    /** Arguments after the collection and the expression, which NESTED does not accept. */
    public List<Expression> extraArguments() {
        List<Expression> arguments = beforeKeys();
        return arguments.subList(Math.min(2, arguments.size()), arguments.size());
    }

    @Override
    public DataType dataType() {
        return keys().isEmpty() ? body().dataType() : DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        return childrenResolved() ? TypeResolution.TYPE_RESOLVED : new TypeResolution("Unresolved nested expression");
    }

    @Override
    protected NodeInfo<Nested> info() {
        return NodeInfo.create(this, Nested::new, children().getFirst(), children().subList(1, children().size()));
    }

    @Override
    public Nested replaceChildren(List<Expression> children) {
        return new Nested(source(), children.getFirst(), children.subList(1, children.size()));
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

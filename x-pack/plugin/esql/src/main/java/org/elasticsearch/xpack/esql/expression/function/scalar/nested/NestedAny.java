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
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isBoolean;

/**
 * True when a predicate holds for at least one of a parent's nested objects. Analysis lowers it to
 * {@link NestedPredicate} before the plan is serialized.
 */
public final class NestedAny extends Function implements OptionalArgument {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedAny.class)
        .unaryVariadic(NestedAny::new)
        .name("nested_any");

    @FunctionInfo(
        preview = true,
        returnType = "boolean",
        briefSummary = "Returns true when a predicate holds for any nested object.",
        description = "Returns, for each input parent row, whether at least one of its nested objects matches the predicate. "
            + "A parent without nested objects does not match."
    )
    public NestedAny(
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
        super(source, children(child, rest));
    }

    private static List<Expression> children(Expression child, List<Expression> rest) {
        List<Expression> children = new ArrayList<>(1 + rest.size());
        children.add(child);
        children.addAll(rest);
        return children;
    }

    /** The nested collection named before the predicate, or {@code null} when the predicate's fields name it. */
    public Expression collection() {
        return children().size() > 1 ? children().getFirst() : null;
    }

    /** The predicate evaluated over each parent's nested objects. */
    public Expression body() {
        return children().get(collection() == null ? 0 : 1);
    }

    @Override
    public String functionName() {
        return "NESTED_ANY";
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved nested expression");
        }
        if (children().size() > 2) {
            return new TypeResolution("NESTED_ANY takes a predicate, or a collection and a predicate");
        }
        TypeResolution predicate = isBoolean(body(), sourceText(), collection() == null ? FIRST : SECOND);
        if (predicate.unresolved()) {
            return predicate;
        }
        if (body().anyMatch(e -> e instanceof NestedAny || e instanceof NestedPredicate || e instanceof FullTextFunction)) {
            return new TypeResolution("NESTED_ANY does not allow inner scopes or full-text functions");
        }
        if (body().anyMatch(AggregateFunction.class::isInstance)) {
            return new TypeResolution("NESTED_ANY takes a predicate; use NESTED for a reduction such as SUM");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected NodeInfo<NestedAny> info() {
        return NodeInfo.create(this, NestedAny::new, children().getFirst(), children().subList(1, children().size()));
    }

    @Override
    public NestedAny replaceChildren(List<Expression> children) {
        return new NestedAny(source(), children.getFirst(), children.subList(1, children.size()));
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("unlowered NESTED_ANY");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("unlowered NESTED_ANY");
    }
}

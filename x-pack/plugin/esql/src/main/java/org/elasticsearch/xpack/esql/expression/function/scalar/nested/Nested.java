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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * The nested object-scope functions, such as {@link NestedAny} and {@link NestedJson}. Not a function itself: each subclass is
 * its own syntax, and analysis resolves and lowers them all the same way before evaluation or serialization.
 */
public abstract class Nested extends Function {
    protected Nested(Source source, Expression child, List<Expression> rest) {
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

    /** Arguments after the collection and the expression, which the nested functions do not accept. */
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
    public abstract Nested replaceChildren(List<Expression> children);

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("unlowered nested expression");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("unlowered nested expression");
    }
}

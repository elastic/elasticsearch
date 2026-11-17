/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reduces each parent's nested objects with one aggregate. Analysis lowers it to {@link NestedReduce} before the plan
 * is serialized. Grouping with {@code BY} is a later slice.
 */
public final class NestedStats extends Function implements OptionalArgument {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedStats.class)
        .unaryVariadic(NestedStats::new)
        .name("nested");

    @FunctionInfo(
        preview = true,
        returnType = { "double", "integer", "long" },
        briefSummary = "Reduces the nested objects of each parent with an aggregate.",
        description = "Returns one completed reduction for each input parent row. "
            + "A parent without nested objects has no values: COUNT is 0 and the other reductions are null."
    )
    public NestedStats(
        Source source,
        @Param(
            name = "expression",
            type = { "double", "integer", "long", "_source" },
            description = "Aggregate over fields from one nested path, or the nested collection the next expression reads."
        ) Expression child,
        @Param(
            name = "aggregate",
            type = { "double", "integer", "long" },
            optional = true,
            description = "The aggregate over the named collection."
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

    /** The nested collection named before the aggregate, or {@code null} when the aggregate's fields name it. */
    public Expression collection() {
        return children().size() > 1 ? children().getFirst() : null;
    }

    /** The aggregate evaluated over each parent's nested objects. */
    public Expression body() {
        return children().get(collection() == null ? 0 : 1);
    }

    @Override
    public String functionName() {
        return "NESTED";
    }

    @Override
    public DataType dataType() {
        return childrenResolved() ? body().dataType() : DataType.NULL;
    }

    @Override
    public Nullability nullable() {
        return childrenResolved() && body() instanceof Count ? Nullability.FALSE : Nullability.TRUE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return TypeResolution.TYPE_RESOLVED;
        }
        if (children().size() > 2) {
            return new TypeResolution("NESTED takes a reduction, or a collection and a reduction");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected NodeInfo<NestedStats> info() {
        return NodeInfo.create(this, NestedStats::new, children().getFirst(), children().subList(1, children().size()));
    }

    @Override
    public NestedStats replaceChildren(List<Expression> children) {
        return new NestedStats(source(), children.getFirst(), children.subList(1, children.size()));
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("unlowered NESTED");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("unlowered NESTED");
    }
}

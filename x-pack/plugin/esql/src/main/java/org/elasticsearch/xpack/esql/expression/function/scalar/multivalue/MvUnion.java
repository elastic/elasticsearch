/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Returns the union of values from two multi-valued fields (all unique values from both inputs).
 * Example:
 *   Given set A = {"a","b","c"} and set B = {"b","c","d"}, MV_UNION(A, B) returns {"a", "b", "c", "d"}
 */
public class MvUnion extends MvSetOperationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvUnion", MvUnion::new);

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "geo_point",
            "geo_shape",
            "geohash",
            "geotile",
            "geohex",
            "integer",
            "ip",
            "keyword",
            "long",
            "unsigned_long",
            "version" },
        description = "Returns all unique values from the combined input fields (set union). "
            + "Null values are treated as empty sets; returns `null` only if both fields are null.",
        preview = true,
        examples = {
            @Example(file = "mv_union", tag = "testMvUnionWithIntValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithLongValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithBooleanValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithDoubleValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithBytesRefValues") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") }
    )
    public MvUnion(
        Source source,
        @Param(
            name = "field1",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Multivalue expression. Null values are treated as empty sets."
        ) Expression field1,
        @Param(
            name = "field2",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "Multivalue expression. Null values are treated as empty sets."
        ) Expression field2
    ) {
        super(source, field1, field2);
    }

    private MvUnion(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public Object fold(FoldContext ctx) {
        Object leftVal = left().fold(ctx);
        Object rightVal = right().fold(ctx);

        // If both are null, return null
        if (leftVal == null && rightVal == null) {
            return null;
        }

        // Treat null as empty set
        List<?> leftList = leftVal == null ? List.of() : (leftVal instanceof List<?> l ? l : List.of(leftVal));
        List<?> rightList = rightVal == null ? List.of() : (rightVal instanceof List<?> l ? l : List.of(rightVal));

        // Compute union using LinkedHashSet to maintain order
        Set<Object> result = new LinkedHashSet<>(leftList);
        result.addAll(rightList);

        if (result.isEmpty()) {
            return null;
        }
        return result.size() == 1 ? result.iterator().next() : new ArrayList<>(result);
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock field1, BooleanBlock field2) {
        processSetOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((BooleanBlock) b).getBoolean(p),
            builder::appendBoolean,
            Set::addAll
        );
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock field1, BytesRefBlock field2) {
        processSetOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((BytesRefBlock) b).getBytesRef(p, new BytesRef()),
            builder::appendBytesRef,
            Set::addAll
        );
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        processSetOperation(builder, position, field1, field2, (p, b) -> ((IntBlock) b).getInt(p), builder::appendInt, Set::addAll);
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        processSetOperation(builder, position, field1, field2, (p, b) -> ((LongBlock) b).getLong(p), builder::appendLong, Set::addAll);
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock field1, DoubleBlock field2) {
        processSetOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((DoubleBlock) b).getDouble(p),
            builder::appendDouble,
            Set::addAll
        );
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new MvUnion(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvUnion::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvUnionBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvUnionBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvUnionIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvUnionLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvUnionDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    public Nullability nullable() {
        // Return UNKNOWN to prevent the optimizer from replacing the entire
        // expression with null when one argument is null. MV_UNION treats
        // null as an empty set - only returns null if BOTH arguments are null.
        return Nullability.UNKNOWN;
    }
}

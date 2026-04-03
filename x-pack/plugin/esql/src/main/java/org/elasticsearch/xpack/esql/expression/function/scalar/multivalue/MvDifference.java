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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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


public class MvDifference extends MvSetOperationFunction{

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class, "MvDifference", MvDifference::new);

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
        description = "Returns all unique values from the left field (excluding right). "
            + "Null values are treated as empty sets; returns `null` if right field is null.",
        preview = true,
        examples = {
            @Example(file = "mv_difference", tag = "testMvDifferenceWithIntValues"),
            @Example(file = "mv_difference", tag = "testMvDifferenceWithLongValues"),
            @Example(file = "mv_difference", tag = "testMvDifferenceWithBooleanValues"),
            @Example(file = "mv_difference", tag = "testMvDifferenceWithDoubleValues"),
            @Example(file = "mv_difference", tag = "testMvDifferenceWithBytesRefValues") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") }
    )

    public MvDifference(
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
            description = "Multivalue expression."
        ) Expression field2
    ) {
        super(source, field1, field2);
    }

    private MvDifference(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public Object fold(FoldContext ctx) {
        Object left = left().fold(ctx);
        Object right = right().fold(ctx);

        if (left == null)
            return null;

        List<?> leftList = left instanceof List<?> l ? l : List.of(left);
        List<?> rightList = right == null ? List.of() : (right instanceof List<?> l ? l : List.of(right));

        Set<Object> result = new LinkedHashSet<>(leftList);
        result.removeAll(rightList);

        return result.size() == 1 ? result.iterator().next() : new ArrayList<>(result);

    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new MvDifference(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvDifference::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ElementType leftType = PlannerUtils.toElementType(left().dataType());
        ElementType rightType = PlannerUtils.toElementType(right().dataType());

        if (leftType == ElementType.NULL)
            return EvalOperator.CONSTANT_NULL_FACTORY;

        if (rightType == ElementType.NULL)
            return toEvaluator.apply(left());

        return switch (PlannerUtils.toElementType(dataType())){
            case BOOLEAN -> new MvDifferenceBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvDifferenceBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvDifferenceIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvDifferenceLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvDifferenceDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
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
            Set::removeAll
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
            Set::removeAll
        );
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        processSetOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((IntBlock) b).getInt(p),
            builder::appendInt,
            Set::removeAll);
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        processSetOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((LongBlock) b).getLong(p),
            builder::appendLong,
            Set::removeAll);
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
            Set::removeAll
        );
    }
}

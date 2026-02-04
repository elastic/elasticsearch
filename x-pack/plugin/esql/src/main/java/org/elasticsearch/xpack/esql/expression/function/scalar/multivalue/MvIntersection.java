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
import java.util.Set;

/**
 * Adds a function to return a result set with multivalued items that are contained in the input sets.
 * Example:
 *   Given set A = {"a","b","c"} and set B = {"b","c","d"}, MV_INTERSECTION(A, B) returns {"b", "c"}
 */
public class MvIntersection extends MvSetOperationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvIntersection",
        MvIntersection::new
    );

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
        description = "Returns the values that appear in both input fields. Returns `null` if either field is null or if no values match.",
        preview = true,
        examples = {
            @Example(file = "mv_intersection", tag = "testMvIntersectionWithIntValues"),
            @Example(file = "mv_intersection", tag = "testMvIntersectionWithLongValues"),
            @Example(file = "mv_intersection", tag = "testMvIntersectionWithBooleanValues"),
            @Example(file = "mv_intersection", tag = "testMvIntersectionWithDoubleValues"),
            @Example(file = "mv_intersection", tag = "testMvIntersectionWithBytesRefValues") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
    )
    public MvIntersection(
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
            description = "Multivalue expression. If null, the function returns null."
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
            description = "Multivalue expression. If null, the function returns null."
        ) Expression field2
    ) {
        super(source, field1, field2);
    }

    private MvIntersection(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
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
            Set::retainAll
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
            Set::retainAll
        );
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        processSetOperation(builder, position, field1, field2, (p, b) -> ((IntBlock) b).getInt(p), builder::appendInt, Set::retainAll);
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        processSetOperation(builder, position, field1, field2, (p, b) -> ((LongBlock) b).getLong(p), builder::appendLong, Set::retainAll);
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
            Set::retainAll
        );
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new MvIntersection(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvIntersection::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvIntersectionBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvIntersectionBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvIntersectionIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvIntersectionLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvIntersectionDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}

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
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

/**
 * Adds a function that takes two multivalued expressions that return a result where the values of the first multivalued expression are
 * returned except for values that exist as a value in the second expression.
 *
 * a = ["a","a","b","c"] b = ["b"] MV_DIFFERENCE(a,b) => ["a","a","c"]
 */
public class MvDifference extends MvSetOperationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvDifference",
        MvDifference::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvDifference.class)
        .binary(MvDifference::new)
        .name("mv_difference");

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
        description = "Returns the values that appear in the first field, except those that appear in the second. Returns `null` if the "
            + "first expression is null (nothing to remove) or the value of the first field if the second is null.",
        preview = true,
        examples = { @Example(file = "mv_difference", tag = "mv_difference") },
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
            description = "Expression that can be null, a single value, or multiple values. If null, the function returns null."
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
            description = "Expression that can be null, a single value, or multiple values. If null, the function returns field1."
        ) Expression field2
    ) {
        super(source, field1, field2);
    }

    private MvDifference(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock field1, BooleanBlock field2) {
        processListOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((BooleanBlock) b).getBoolean(p),
            builder::appendBoolean,
            List::removeAll
        );
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock field1, BytesRefBlock field2) {
        processListOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((BytesRefBlock) b).getBytesRef(p, new BytesRef()),
            builder::appendBytesRef,
            List::removeAll
        );
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        var field1ValueCount = field1.getValueCount(position);
        var field1FirstValueIndex = field1.getFirstValueIndex(position);
        if (field1.isNull(position)) {
            builder.appendNull();
            return;
        }
        boolean empty = true;
        for (int index = field1FirstValueIndex; index < field1FirstValueIndex + field1ValueCount; index++) {
            var value = field1.getInt(index);
            if (field2.hasValue(position, value)) continue;
            if (empty) builder.beginPositionEntry();
            builder.appendInt(value);
            empty = false;
        }
        if (empty) {
            builder.appendNull();
        } else {
            builder.endPositionEntry();
        }
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        processListOperation(builder, position, field1, field2, (p, b) -> ((LongBlock) b).getLong(p), builder::appendLong, List::removeAll);
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock field1, DoubleBlock field2) {
        processListOperation(
            builder,
            position,
            field1,
            field2,
            (p, b) -> ((DoubleBlock) b).getDouble(p),
            builder::appendDouble,
            List::removeAll
        );
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvDifferenceBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvDifferenceBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvDifferenceIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvDifferenceLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvDifferenceDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> ConstantEvaluators.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }
}

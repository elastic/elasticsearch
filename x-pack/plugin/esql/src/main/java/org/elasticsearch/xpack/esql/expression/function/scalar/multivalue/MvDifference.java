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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;

/**
 * Adds a function that takes two multivalued expressions that return a result where the values of the first multivalued expression are
 * returned except for values that exist as a value in the second expression.
 *
 * a = ["a","b","c"] b = ["b"] MV_DIFFERENCE(a,b) => ["a","c"]
 */
public class MvDifference extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvDifference", MvDifference::new);
    protected DataType dataType;

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
            description = "Multivalue expression. If null, the function returns field1."
        ) Expression field2
    ) {
        super(source, field1, field2);
    }

    private MvDifference(StreamInput in) throws IOException {
        super(in);
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock field1, BooleanBlock field2) {
        var field1ValueCount = field1.getValueCount(position);
        var field1FirstValueIndex = field1.getFirstValueIndex(position);
        if (field1.isNull(position)) {
            builder.appendNull();
            return;
        }
        boolean empty = true;
        for (int index = field1FirstValueIndex; index < field1FirstValueIndex + field1ValueCount; index++) {
            var value = field1.getBoolean(index);
            if (field2.hasValue(position, value)) continue;
            if (empty) builder.beginPositionEntry();
            builder.appendBoolean(value);
            empty = false;
        }
        if (empty) {
            builder.appendNull();
        } else {
            builder.endPositionEntry();
        }
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock field1, BytesRefBlock field2) {
        var field1ValueCount = field1.getValueCount(position);
        var field1FirstValueIndex = field1.getFirstValueIndex(position);
        var field2Scratch = new BytesRef();
        var value = new BytesRef();
        if (field1.isNull(position)) {
            builder.appendNull();
            return;
        }
        boolean empty = true;
        for (int index = field1FirstValueIndex; index < field1FirstValueIndex + field1ValueCount; index++) {
            value = field1.getBytesRef(index, value);
            if (field2.hasValue(position, value, field2Scratch)) continue;
            if (empty) builder.beginPositionEntry();
            builder.appendBytesRef(value);
            empty = false;
        }
        if (empty) {
            builder.appendNull();
        } else {
            builder.endPositionEntry();
        }
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
        var field1ValueCount = field1.getValueCount(position);
        var field1FirstValueIndex = field1.getFirstValueIndex(position);
        if (field1.isNull(position)) {
            builder.appendNull();
            return;
        }
        boolean empty = true;
        for (int index = field1FirstValueIndex; index < field1FirstValueIndex + field1ValueCount; index++) {
            var value = field1.getLong(index);
            if (field2.hasValue(position, value)) continue;
            if (empty) builder.beginPositionEntry();
            builder.appendLong(value);
            empty = false;
        }
        if (empty) {
            builder.appendNull();
        } else {
            builder.endPositionEntry();
        }
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock field1, DoubleBlock field2) {
        var field1ValueCount = field1.getValueCount(position);
        var field1FirstValueIndex = field1.getFirstValueIndex(position);
        if (field1.isNull(position)) {
            builder.appendNull();
            return;
        }
        boolean empty = true;
        for (int index = field1FirstValueIndex; index < field1FirstValueIndex + field1ValueCount; index++) {
            var value = field1.getDouble(index);
            if (field2.hasValue(position, value)) continue;
            if (empty) builder.beginPositionEntry();
            builder.appendDouble(value);
            empty = false;
        }
        if (empty) {
            builder.appendNull();
        } else {
            builder.endPositionEntry();
        }
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
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        var field1Type = PlannerUtils.toElementType(left().dataType());
        var field2Type = PlannerUtils.toElementType(right().dataType());

        if (field2Type == ElementType.NULL) {
            // throw new AssertionError("function should be optimized away");
        }

        if (field1Type != ElementType.NULL && field2Type != ElementType.NULL && field1Type != field2Type) {
            throw new EsqlIllegalArgumentException(
                "Incompatible data types for MvDifference, field1 type({}) value({}) and field2 type({}) value({}) don't match.",
                field1Type,
                left(),
                field2Type,
                right()
            );
        }
        return switch (field1Type) {
            case BOOLEAN -> new MvDifferenceBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvDifferenceBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvDifferenceIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvDifferenceLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvDifferenceDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        this.dataType = left().dataType().noText();
        return isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(left(), sourceText(), FIRST);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    public Object fold(FoldContext ctx) {
        if (Expressions.isGuaranteedNull(right())) {
            return left().fold(ctx);
        }
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }
}

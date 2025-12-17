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
import org.elasticsearch.compute.data.Block;
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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Returns the union of values from two multi-valued fields (all unique values from both inputs).
 * Example:
 *   Given set A = {"a","b","c"} and set B = {"b","c","d"}, MV_UNION(A, B) returns {"a", "b", "c", "d"}
 */
public class MvUnion extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvUnion", MvUnion::new);

    private DataType dataType;

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
        description = "Returns all unique values from both input fields. Returns `null` if either field is null.",
        preview = true,
        examples = {
            @Example(file = "mv_union", tag = "testMvUnionWithIntValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithLongValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithBooleanValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithDoubleValues"),
            @Example(file = "mv_union", tag = "testMvUnionWithBytesRefValues") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
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

    private MvUnion(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock field1, BooleanBlock field2) {
        processUnionSet(builder, position, field1, field2, (p, block) -> ((BooleanBlock) block).getBoolean(p), builder::appendBoolean);
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock field1, BytesRefBlock field2) {
        processUnionSet(builder, position, field1, field2, (p, block) -> {
            BytesRef value = new BytesRef();
            return ((BytesRefBlock) block).getBytesRef(p, value);
        }, builder::appendBytesRef);
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        processUnionSet(builder, position, field1, field2, (p, block) -> ((IntBlock) block).getInt(p), builder::appendInt);
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        processUnionSet(builder, position, field1, field2, (p, block) -> ((LongBlock) block).getLong(p), builder::appendLong);
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock field1, DoubleBlock field2) {
        processUnionSet(builder, position, field1, field2, (p, block) -> ((DoubleBlock) block).getDouble(p), builder::appendDouble);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (left().dataType() != DataType.NULL && right().dataType() != DataType.NULL) {
            this.dataType = left().dataType().noText();
            return isType(
                right(),
                t -> t.noText() == left().dataType().noText(),
                sourceText(),
                SECOND,
                left().dataType().noText().typeName()
            );
        }

        Expression evaluatedField = left().dataType() == DataType.NULL ? right() : left();
        this.dataType = evaluatedField.dataType().noText();

        TypeResolution resolution = isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram(
            evaluatedField,
            sourceText(),
            FIRST
        );
        if (resolution.unresolved()) {
            return resolution;
        }

        return resolution;
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
        return Nullability.TRUE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MvUnion other = (MvUnion) obj;
        return Objects.equals(other.left(), left()) && Objects.equals(other.right(), right());
    }

    static <T> void processUnionSet(
        Block.Builder builder,
        int position,
        Block field1,
        Block field2,
        BiFunction<Integer, Block, T> getValueFunction,
        Consumer<T> addValueFunction
    ) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);

        // If either field has no values (is null), return null
        if (firstValueCount == 0 || secondValueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        // Use LinkedHashSet to maintain insertion order
        Set<T> values = new LinkedHashSet<>();

        // Add all values from first field
        for (int i = 0; i < firstValueCount; i++) {
            values.add(getValueFunction.apply(firstValueIndex + i, field1));
        }

        // Add all values from second field (duplicates automatically ignored by Set)
        for (int i = 0; i < secondValueCount; i++) {
            values.add(getValueFunction.apply(secondValueIndex + i, field2));
        }

        // Build result
        builder.beginPositionEntry();
        for (T value : values) {
            addValueFunction.accept(value);
        }
        builder.endPositionEntry();
    }
}

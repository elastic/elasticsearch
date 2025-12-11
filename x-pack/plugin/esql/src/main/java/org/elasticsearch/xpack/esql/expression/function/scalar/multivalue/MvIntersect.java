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
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram;

/**
 * Adds a function to return a result set with multivalued items that are contained in the input sets.
 * Example:
 *   Given set A = {"a","b","c"} and set B = {"b","c","d"}, MV_INTERSECT(A, B) returns {"b", "c"}
 */
public class MvIntersect extends EsqlScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvIntersect",
        MvIntersect::new
    );

    private final Expression firstField;
    private final Expression secondField;

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
        description = """
            Returns a subset of the inputs sets that contains the intersection of values in provided mv arguments
            """,
        examples = { @Example(file = "mv_intersect", tag = "testMvIntersectWithIntValues") }
    )
    public MvIntersect(
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
                "version" }
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
                "version" }
        ) Expression field2
    ) {
        super(source, List.of(field1, field2));
        firstField = field1;
        secondField = field2;
    }

    private MvIntersect(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock firstValue, BooleanBlock secondValue) {
        int firstValueCount = firstValue.getValueCount(position);
        int secondValueCount = secondValue.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = firstValue.getFirstValueIndex(position);
        int secondValueIndex = secondValue.getFirstValueIndex(position);

        Set<Boolean> values = new HashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(firstValue.getBoolean(firstValueIndex + i));
        }

        Set<Boolean> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(secondValue.getBoolean(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            builder.appendNull();
            return;
        }

        builder.beginPositionEntry();
        for (Boolean value : values) {
            builder.appendBoolean(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock firstValue, BytesRefBlock secondValue) {
        int firstValueCount = firstValue.getValueCount(position);
        int secondValueCount = secondValue.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = firstValue.getFirstValueIndex(position);
        int secondValueIndex = secondValue.getFirstValueIndex(position);

        Set<BytesRef> values = new HashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            BytesRef value = new BytesRef();
            values.add(firstValue.getBytesRef(firstValueIndex + i, value));
        }

        Set<BytesRef> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            BytesRef value = new BytesRef();
            secondValues.add(secondValue.getBytesRef(secondValueIndex + i, value));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            builder.appendNull();
            return;
        }

        builder.beginPositionEntry();
        for (BytesRef value : values) {
            builder.appendBytesRef(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock firstValue, IntBlock secondValue) {
        int firstValueCount = firstValue.getValueCount(position);
        int secondValueCount = secondValue.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = firstValue.getFirstValueIndex(position);
        int secondValueIndex = secondValue.getFirstValueIndex(position);

        Set<Integer> values = new HashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(firstValue.getInt(firstValueIndex + i));
        }

        Set<Integer> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(secondValue.getInt(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            builder.appendNull();
            return;
        }

        builder.beginPositionEntry();
        for (Integer value : values) {
            builder.appendInt(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock firstValue, LongBlock secondValue) {
        int firstValueCount = firstValue.getValueCount(position);
        int secondValueCount = secondValue.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = firstValue.getFirstValueIndex(position);
        int secondValueIndex = secondValue.getFirstValueIndex(position);

        Set<Long> values = new HashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(firstValue.getLong(firstValueIndex + i));
        }

        Set<Long> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(secondValue.getLong(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            builder.appendNull();
            return;
        }

        builder.beginPositionEntry();
        for (Long value : values) {
            builder.appendLong(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock firstValue, DoubleBlock secondValue) {
        int firstValueCount = firstValue.getValueCount(position);
        int secondValueCount = secondValue.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            builder.appendNull();
            return;
        }

        int firstValueIndex = firstValue.getFirstValueIndex(position);
        int secondValueIndex = secondValue.getFirstValueIndex(position);

        Set<Double> values = new HashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(firstValue.getDouble(firstValueIndex + i));
        }

        Set<Double> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(secondValue.getDouble(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            builder.appendNull();
            return;
        }

        builder.beginPositionEntry();
        for (Double value : values) {
            builder.appendDouble(value);
        }
        builder.endPositionEntry();
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

        // ensure all children are the same type
        DataType childDataType = firstField.dataType();
        if (childDataType != secondField.dataType()) {
            return new TypeResolution("All child fields must be the same type");
        }

        this.dataType = childDataType.noText();

        TypeResolution resolution = isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram(
            this.children().getFirst(),
            sourceText(),
            FIRST
        );
        if (resolution.unresolved()) {
            return resolution;
        }

        return resolution;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        // TODO - validation
        return new MvIntersect(source(), newChildren.getFirst(), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        // TODO - validation
        return NodeInfo.create(this, MvIntersect::new, firstField, secondField);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().getFirst());
        // TODO - validation
        out.writeNamedWriteable(children().get(1));
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvIntersectBooleanEvaluator.Factory(
                source(),
                toEvaluator.apply(firstField),
                toEvaluator.apply(secondField)
            );
            case BYTES_REF -> new MvIntersectBytesRefEvaluator.Factory(
                source(),
                toEvaluator.apply(firstField),
                toEvaluator.apply(secondField)
            );
            case INT -> new MvIntersectIntEvaluator.Factory(source(), toEvaluator.apply(firstField), toEvaluator.apply(secondField));
            case LONG -> new MvIntersectLongEvaluator.Factory(source(), toEvaluator.apply(firstField), toEvaluator.apply(secondField));
            case DOUBLE -> new MvIntersectDoubleEvaluator.Factory(source(), toEvaluator.apply(firstField), toEvaluator.apply(secondField));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}

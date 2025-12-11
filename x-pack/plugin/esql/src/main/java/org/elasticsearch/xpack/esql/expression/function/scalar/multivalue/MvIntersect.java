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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
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
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
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

    private final Expression field1;
    private final Expression field2;

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
        description = "Returns a subset of the inputs sets that contains the intersection of values in provided mv arguments.",
        examples = {
            @Example(file = "mv_intersect", tag = "testMvIntersectWithIntValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithLongValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithBooleanValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithDoubleValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithBytesRefValues") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.3.0") }
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
        this.field1 = field1;
        this.field2 = field2;
    }

    private MvIntersect(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    static void appendEmptySet(Block.Builder builder) {
        builder.appendNull();
    }

    @Override
    public boolean foldable() {
        return field1.foldable() && field2.foldable();
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock field1, BooleanBlock field2) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            appendEmptySet(builder);
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        Set<Boolean> values = new LinkedHashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(field1.getBoolean(firstValueIndex + i));
        }

        Set<Boolean> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(field2.getBoolean(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            appendEmptySet(builder);
            return;
        }

        builder.beginPositionEntry();
        for (Boolean value : values) {
            builder.appendBoolean(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock field1, BytesRefBlock field2) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            appendEmptySet(builder);
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        Set<BytesRef> values = new LinkedHashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            BytesRef value = new BytesRef();
            values.add(field1.getBytesRef(firstValueIndex + i, value));
        }

        Set<BytesRef> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            BytesRef value = new BytesRef();
            secondValues.add(field2.getBytesRef(secondValueIndex + i, value));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            appendEmptySet(builder);
            return;
        }

        builder.beginPositionEntry();
        for (BytesRef value : values) {
            builder.appendBytesRef(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            appendEmptySet(builder);
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        Set<Integer> values = new LinkedHashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(field1.getInt(firstValueIndex + i));
        }

        Set<Integer> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(field2.getInt(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            appendEmptySet(builder);
            return;
        }

        builder.beginPositionEntry();
        for (Integer value : values) {
            builder.appendInt(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            appendEmptySet(builder);
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        Set<Long> values = new LinkedHashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(field1.getLong(firstValueIndex + i));
        }

        Set<Long> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(field2.getLong(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            appendEmptySet(builder);
            return;
        }

        builder.beginPositionEntry();
        for (Long value : values) {
            builder.appendLong(value);
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock field1, DoubleBlock field2) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            appendEmptySet(builder);
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        Set<Double> values = new LinkedHashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(field1.getDouble(firstValueIndex + i));
        }

        Set<Double> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(field2.getDouble(secondValueIndex + i));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            // nothing intersected
            appendEmptySet(builder);
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
        ElementType field1Type = PlannerUtils.toElementType(field1.dataType());
        ElementType field2Type = PlannerUtils.toElementType(field2.dataType());

        if (field1Type != field2Type && field1Type.equals(ElementType.NULL) == false && field2Type.equals(ElementType.NULL) == false) {
            return new TypeResolution("All child fields must be the same type");
        }

        Expression evaluatedField = field1Type.equals(ElementType.NULL) ? field2 : field1;

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
    public Expression replaceChildren(List<Expression> newChildren) {
        // TODO - validation
        return new MvIntersect(source(), newChildren.getFirst(), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        // TODO - validation
        return NodeInfo.create(this, MvIntersect::new, field1, field2);
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
            case BOOLEAN -> new MvIntersectBooleanEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case BYTES_REF -> new MvIntersectBytesRefEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case INT -> new MvIntersectIntEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case LONG -> new MvIntersectLongEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case DOUBLE -> new MvIntersectDoubleEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
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
        return Objects.hash(field1, field2);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MvIntersect other = (MvIntersect) obj;
        return Objects.equals(other.field1, field1) && Objects.equals(other.field2, field2);
    }
}

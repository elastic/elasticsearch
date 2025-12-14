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
import java.util.function.BiFunction;
import java.util.function.Consumer;

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
        preview = true,
        examples = {
            @Example(file = "mv_intersect", tag = "testMvIntersectWithIntValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithLongValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithBooleanValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithDoubleValues"),
            @Example(file = "mv_intersect", tag = "testMvIntersectWithBytesRefValues") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
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

    @Override
    public boolean foldable() {
        return field1.foldable() && field2.foldable();
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, @Position int position, BooleanBlock field1, BooleanBlock field2) {
        processIntersectSet(builder, position, field1, field2, (p, block) -> ((BooleanBlock) block).getBoolean(p), builder::appendBoolean);
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, @Position int position, BytesRefBlock field1, BytesRefBlock field2) {
        processIntersectSet(builder, position, field1, field2, (p, block) -> {
            BytesRef value = new BytesRef();
            return ((BytesRefBlock) block).getBytesRef(p, value);
        }, builder::appendBytesRef);
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock field1, IntBlock field2) {
        processIntersectSet(builder, position, field1, field2, (p, block) -> ((IntBlock) block).getInt(p), builder::appendInt);
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock field1, LongBlock field2) {
        processIntersectSet(builder, position, field1, field2, (p, block) -> ((LongBlock) block).getLong(p), builder::appendLong);
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock field1, DoubleBlock field2) {
        processIntersectSet(builder, position, field1, field2, (p, block) -> ((DoubleBlock) block).getDouble(p), builder::appendDouble);
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
        return new MvIntersect(source(), newChildren.getFirst(), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvIntersect::new, field1, field2);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field1);
        out.writeNamedWriteable(field2);
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

    static <T> void processIntersectSet(
        Block.Builder builder,
        int position,
        Block field1,
        Block field2,
        BiFunction<Integer, Block, T> getValueFunction,
        Consumer<T> addValueFunction
    ) {
        int firstValueCount = field1.getValueCount(position);
        int secondValueCount = field2.getValueCount(position);
        if (firstValueCount == 0 || secondValueCount == 0) {
            // if either block has no values, there will be no intersection
            builder.appendNull();
            return;
        }

        int firstValueIndex = field1.getFirstValueIndex(position);
        int secondValueIndex = field2.getFirstValueIndex(position);

        Set<T> values = new LinkedHashSet<>();
        for (int i = 0; i < firstValueCount; i++) {
            values.add(getValueFunction.apply(firstValueIndex + i, field1));
        }

        Set<T> secondValues = new HashSet<>();
        for (int i = 0; i < secondValueCount; i++) {
            secondValues.add(getValueFunction.apply(secondValueIndex + i, field2));
        }

        values.retainAll(secondValues);
        if (values.isEmpty()) {
            builder.appendNull();
            return;
        }

        builder.beginPositionEntry();
        for (T value : values) {
            addValueFunction.accept(value);
        }
        builder.endPositionEntry();
    }
}

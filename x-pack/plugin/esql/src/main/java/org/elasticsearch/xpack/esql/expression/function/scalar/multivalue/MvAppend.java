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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Appends values to a multi-value
 */
public class MvAppend extends EsqlScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvAppend", MvAppend::new);

    private final Expression field1, field2;
    private DataType dataType;

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "double",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "text",
            "version" },
        description = "Concatenates values of two multi-value fields."
    )
    public MvAppend(
        Source source,
        @Param(
            name = "field1",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" }
        ) Expression field1,
        @Param(
            name = "field2",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" }
        ) Expression field2
    ) {
        super(source, Arrays.asList(field1, field2));
        this.field1 = field1;
        this.field2 = field2;
    }

    private MvAppend(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(field1);
        out.writeNamedWriteable(field2);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(field1, DataType::isRepresentable, sourceText(), FIRST, "representable");
        if (resolution.unresolved()) {
            return resolution;
        }
        dataType = field1.dataType();
        if (dataType == DataType.NULL) {
            dataType = field2.dataType();
            return isType(field2, DataType::isRepresentable, sourceText(), SECOND, "representable");
        }
        return isType(field2, t -> t == dataType, sourceText(), SECOND, dataType.typeName());
    }

    @Override
    public boolean foldable() {
        return field1.foldable() && field2.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvAppendBooleanEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case BYTES_REF -> new MvAppendBytesRefEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case DOUBLE -> new MvAppendDoubleEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case INT -> new MvAppendIntEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case LONG -> new MvAppendLongEvaluator.Factory(source(), toEvaluator.apply(field1), toEvaluator.apply(field2));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvAppend(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvAppend::new, field1, field2);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
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
        MvAppend other = (MvAppend) obj;
        return Objects.equals(other.field1, field1) && Objects.equals(other.field2, field2);
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, int position, IntBlock field1, IntBlock field2) {
        int count1 = field1.getValueCount(position);
        int count2 = field2.getValueCount(position);
        if (count1 == 0 || count2 == 0) {
            builder.appendNull();
        } else {
            builder.beginPositionEntry();
            int first1 = field1.getFirstValueIndex(position);
            int first2 = field2.getFirstValueIndex(position);
            for (int i = 0; i < count1; i++) {
                builder.appendInt(field1.getInt(first1 + i));
            }
            for (int i = 0; i < count2; i++) {
                builder.appendInt(field2.getInt(first2 + i));
            }
            builder.endPositionEntry();
        }

    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, int position, BooleanBlock field1, BooleanBlock field2) {
        int count1 = field1.getValueCount(position);
        int count2 = field2.getValueCount(position);
        if (count1 == 0 || count2 == 0) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            int first2 = field2.getFirstValueIndex(position);
            builder.beginPositionEntry();
            for (int i = 0; i < count1; i++) {
                builder.appendBoolean(field1.getBoolean(first1 + i));
            }
            for (int i = 0; i < count2; i++) {
                builder.appendBoolean(field2.getBoolean(first2 + i));
            }
            builder.endPositionEntry();
        }

    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, int position, LongBlock field1, LongBlock field2) {
        int count1 = field1.getValueCount(position);
        int count2 = field2.getValueCount(position);
        if (count1 == 0 || count2 == 0) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            int first2 = field2.getFirstValueIndex(position);
            builder.beginPositionEntry();
            for (int i = 0; i < count1; i++) {
                builder.appendLong(field1.getLong(first1 + i));
            }
            for (int i = 0; i < count2; i++) {
                builder.appendLong(field2.getLong(first2 + i));
            }
            builder.endPositionEntry();
        }
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock field1, DoubleBlock field2) {
        int count1 = field1.getValueCount(position);
        int count2 = field2.getValueCount(position);
        if (count1 == 0 || count2 == 0) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            int first2 = field2.getFirstValueIndex(position);
            builder.beginPositionEntry();
            for (int i = 0; i < count1; i++) {
                builder.appendDouble(field1.getDouble(first1 + i));
            }
            for (int i = 0; i < count2; i++) {
                builder.appendDouble(field2.getDouble(first2 + i));
            }
            builder.endPositionEntry();
        }

    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, int position, BytesRefBlock field1, BytesRefBlock field2) {
        int count1 = field1.getValueCount(position);
        int count2 = field2.getValueCount(position);
        if (count1 == 0 || count2 == 0) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            int first2 = field2.getFirstValueIndex(position);
            builder.beginPositionEntry();
            BytesRef spare = new BytesRef();
            for (int i = 0; i < count1; i++) {
                builder.appendBytesRef(field1.getBytesRef(first1 + i, spare));
            }
            for (int i = 0; i < count2; i++) {
                builder.appendBytesRef(field2.getBytesRef(first2 + i, spare));
            }
            builder.endPositionEntry();
        }
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}

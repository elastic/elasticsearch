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
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Appends values to a multi-value
 */
public class MvAppend extends EsqlScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvAppend", MvAppend::new);

    private final Expression field1;
    private final List<? extends Expression> field2;
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
            "version" },
        description = "Concatenates values of two or more multi-value fields."
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
        ) List<? extends Expression> field2
    ) {
        super(source, Stream.concat(Stream.of(field1), field2.stream()).toList());
        this.field1 = field1;
        this.field2 = field2;
    }

    private MvAppend(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(field1);
        out.writeNamedWriteableCollection(field2);
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
        dataType = field1.dataType().noText();
        if (dataType == DataType.NULL) {
            for (Expression value : field2) {
                dataType = value.dataType().noText();
                resolution = isType(value, DataType::isRepresentable, sourceText(), DEFAULT, "representable");
                if (resolution.unresolved()) {
                    return resolution;
                }
            }
        }
        return resolution;
    }

    @Override
    public boolean foldable() {
        boolean foldable = field1.foldable();
        for (Expression expression : field2) {
            foldable &= expression.foldable();
        }
        return foldable;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvAppendBooleanEvaluator.Factory(source(), toEvaluator.apply(field1),
                field2.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new));
            case BYTES_REF -> new MvAppendBytesRefEvaluator.Factory(source(), toEvaluator.apply(field1),
                field2.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new));
            case DOUBLE -> new MvAppendDoubleEvaluator.Factory(source(), toEvaluator.apply(field1),
                field2.stream().map(toEvaluator::apply).toArray(EvalOperator.ExpressionEvaluator.Factory[]::new));
            case INT -> new MvAppendIntEvaluator.Factory(source(), toEvaluator.apply(field1), field2.stream().map(toEvaluator::apply).toArray(
                EvalOperator.ExpressionEvaluator.Factory[]::new));
            case LONG -> new MvAppendLongEvaluator.Factory(source(), toEvaluator.apply(field1), field2.stream().map(toEvaluator::apply).toArray(
                EvalOperator.ExpressionEvaluator.Factory[]::new));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType);
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvAppend(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
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
    static void process(IntBlock.Builder builder, int position, IntBlock field1, IntBlock... field2) {
        int count1 = field1.getValueCount(position);
        boolean field2AllCountZero = true;
        for (IntBlock block : field2) {
            if (block.getValueCount(position) > 0) {
                field2AllCountZero = false;
                break;
            }
        }
        if (count1 == 0 || field2AllCountZero) {
            builder.appendNull();
        } else {
            builder.beginPositionEntry();
            int first1 = field1.getFirstValueIndex(position);
            for (int i = 0; i < count1; i++) {
                builder.appendInt(field1.getInt(first1 + i));
            }

            for (int i = 0; i < field2.length; i++) {
                int first = field2[i].getFirstValueIndex(position);
                for (int j = 0; j < field2[i].getValueCount(position); j++) {
                    builder.appendInt(field2[i].getInt(first + j));
                }
            }
            builder.endPositionEntry();
        }

    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, int position, BooleanBlock field1, BooleanBlock... field2) {
        int count1 = field1.getValueCount(position);
        boolean field2AllCountZero = true;
        for (BooleanBlock block : field2) {
            if (block.getValueCount(position) > 0) {
                field2AllCountZero = false;
                break;
            }
        }
        if (count1 == 0 || field2AllCountZero) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            builder.beginPositionEntry();
            for (int i = 0; i < count1; i++) {
                builder.appendBoolean(field1.getBoolean(first1 + i));
            }

            for (int i = 0; i < field2.length; i++) {
                int first = field2[i].getFirstValueIndex(position);
                for (int j = 0; j < field2[i].getValueCount(position); j++) {
                    builder.appendBoolean(field2[i].getBoolean(first + j));
                }
            }
            builder.endPositionEntry();
        }

    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, int position, LongBlock field1, LongBlock... field2) {
        int count1 = field1.getValueCount(position);
        boolean field2AllCountZero = true;
        for (LongBlock block : field2) {
            if (block.getValueCount(position) > 0) {
                field2AllCountZero = false;
                break;
            }
        }
        if (count1 == 0 || field2AllCountZero) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            builder.beginPositionEntry();
            for (int i = 0; i < count1; i++) {
                builder.appendLong(field1.getLong(first1 + i));
            }

            for (int i = 0; i < field2.length; i++) {
                int first = field2[i].getFirstValueIndex(position);
                for (int j = 0; j < field2[i].getValueCount(position); j++) {
                    builder.appendLong(field2[i].getLong(first + j));
                }
            }
            builder.endPositionEntry();
        }
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock field1, DoubleBlock... field2) {
        int count1 = field1.getValueCount(position);
        boolean field2AllCountZero = true;
        for (DoubleBlock block : field2) {
            if (block.getValueCount(position) > 0) {
                field2AllCountZero = false;
                break;
            }
        }
        if (count1 == 0 || field2AllCountZero) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            builder.beginPositionEntry();
            for (int i = 0; i < count1; i++) {
                builder.appendDouble(field1.getDouble(first1 + i));
            }

            for (int i = 0; i < field2.length; i++) {
                int first = field2[i].getFirstValueIndex(position);
                for (int j = 0; j < field2[i].getValueCount(position); j++) {
                    builder.appendDouble(field2[i].getDouble(first + j));
                }
            }
            builder.endPositionEntry();
        }

    }

    @Evaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, int position, BytesRefBlock field1, BytesRefBlock... field2) {
        int count1 = field1.getValueCount(position);
        boolean field2AllCountZero = true;
        for (BytesRefBlock block : field2) {
            if (block.getValueCount(position) > 0) {
                field2AllCountZero = false;
                break;
            }
        }
        if (count1 == 0 || field2AllCountZero) {
            builder.appendNull();
        } else {
            int first1 = field1.getFirstValueIndex(position);
            builder.beginPositionEntry();
            BytesRef spare = new BytesRef();
            for (int i = 0; i < count1; i++) {
                builder.appendBytesRef(field1.getBytesRef(first1 + i, spare));
            }

            for (int i = 0; i < field2.length; i++) {
                int first = field2[i].getFirstValueIndex(position);
                for (int j = 0; j < field2[i].getValueCount(position); j++) {
                    builder.appendBytesRef(field2[i].getBytesRef(first + j, spare));
                }
            }
            builder.endPositionEntry();
        }
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}

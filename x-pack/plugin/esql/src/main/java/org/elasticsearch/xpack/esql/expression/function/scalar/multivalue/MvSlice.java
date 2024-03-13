/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Returns a subset of the multivalued field using the start and end index values.
 */
public class MvSlice extends ScalarFunction implements OptionalArgument, EvaluatorMapper {
    private final Expression field, start, end;

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
        description = "Returns a subset of the multivalued field using the start and end index values."
    )
    public MvSlice(
        Source source,
        @Param(
            name = "v",
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
                "version" },
            description = "A multivalued field"
        ) Expression field,
        @Param(name = "start", type = { "integer" }, description = "start index") Expression start,
        @Param(name = "end", type = { "integer" }, description = "end index (included)", optional = true) Expression end
    ) {
        super(source, end == null ? Arrays.asList(field, start, start) : Arrays.asList(field, start, end));
        this.field = field;
        this.start = start;
        this.end = end == null ? start : end;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(field, EsqlDataTypes::isRepresentable, sourceText(), FIRST, "representable");
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isInteger(start, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (end != null) {
            resolution = isInteger(end, sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolution;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && start.foldable() && (end == null || end.foldable());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        if (start.foldable() && end.foldable()) {
            int startOffset = Integer.parseInt(String.valueOf(start.fold()));
            int endOffset = Integer.parseInt(String.valueOf(end.fold()));
            checkStartEnd(startOffset, endOffset);
        }
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case BOOLEAN -> new MvSliceBooleanEvaluator.Factory(
                source(),
                toEvaluator.apply(field),
                toEvaluator.apply(start),
                toEvaluator.apply(end)
            );
            case BYTES_REF -> new MvSliceBytesRefEvaluator.Factory(
                source(),
                toEvaluator.apply(field),
                toEvaluator.apply(start),
                toEvaluator.apply(end)
            );
            case DOUBLE -> new MvSliceDoubleEvaluator.Factory(
                source(),
                toEvaluator.apply(field),
                toEvaluator.apply(start),
                toEvaluator.apply(end)
            );
            case INT -> new MvSliceIntEvaluator.Factory(
                source(),
                toEvaluator.apply(field),
                toEvaluator.apply(start),
                toEvaluator.apply(end)
            );
            case LONG -> new MvSliceLongEvaluator.Factory(
                source(),
                toEvaluator.apply(field),
                toEvaluator.apply(start),
                toEvaluator.apply(end)
            );
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvSlice(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvSlice::new, field, start, end);
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MvSlice other = (MvSlice) obj;
        return Objects.equals(other.field, field) && Objects.equals(other.start, start) && Objects.equals(other.end, end);
    }

    static int adjustIndex(int oldOffset, int fieldValueCount, int first) {
        return oldOffset < 0 ? oldOffset + fieldValueCount + first : oldOffset + first;
    }

    static void checkStartEnd(int start, int end) throws InvalidArgumentException {
        if (start > end) {
            throw new InvalidArgumentException("Start offset is greater than end offset");
        }
        if (start < 0 && end >= 0) {
            throw new InvalidArgumentException("Start and end offset have different signs");
        }
    }

    @Evaluator(extraName = "Boolean", warnExceptions = { InvalidArgumentException.class })
    static void process(BooleanBlock.Builder builder, int position, BooleanBlock field, int start, int end) {
        int fieldValueCount = field.getValueCount(position);
        checkStartEnd(start, end);
        int first = field.getFirstValueIndex(position);
        int mvStartIndex = adjustIndex(start, fieldValueCount, first);
        mvStartIndex = Math.max(first, mvStartIndex);
        int mvEndIndex = adjustIndex(end, fieldValueCount, first);
        mvEndIndex = Math.min(fieldValueCount + first - 1, mvEndIndex);
        if (mvStartIndex >= fieldValueCount + first || mvEndIndex < first) {
            builder.appendNull();
            return;
        }
        if (mvStartIndex == mvEndIndex) {
            builder.appendBoolean(field.getBoolean(mvStartIndex));
            return;
        }
        builder.beginPositionEntry();
        for (int i = mvStartIndex; i <= mvEndIndex; i++) {
            builder.appendBoolean(field.getBoolean(i));
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Int", warnExceptions = { InvalidArgumentException.class })
    static void process(IntBlock.Builder builder, int position, IntBlock field, int start, int end) {
        int fieldValueCount = field.getValueCount(position);
        checkStartEnd(start, end);
        int first = field.getFirstValueIndex(position);
        int mvStartIndex = adjustIndex(start, fieldValueCount, first);
        mvStartIndex = Math.max(first, mvStartIndex);
        int mvEndIndex = adjustIndex(end, fieldValueCount, first);
        mvEndIndex = Math.min(fieldValueCount + first - 1, mvEndIndex);
        if (mvStartIndex >= fieldValueCount + first || mvEndIndex < first) {
            builder.appendNull();
            return;
        }
        if (mvStartIndex == mvEndIndex) {
            builder.appendInt(field.getInt(mvStartIndex));
            return;
        }
        builder.beginPositionEntry();
        for (int i = mvStartIndex; i <= mvEndIndex; i++) {
            builder.appendInt(field.getInt(i));
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Long", warnExceptions = { InvalidArgumentException.class })
    static void process(LongBlock.Builder builder, int position, LongBlock field, int start, int end) {
        int fieldValueCount = field.getValueCount(position);
        checkStartEnd(start, end);
        int first = field.getFirstValueIndex(position);
        int mvStartIndex = adjustIndex(start, fieldValueCount, first);
        mvStartIndex = Math.max(first, mvStartIndex);
        int mvEndIndex = adjustIndex(end, fieldValueCount, first);
        mvEndIndex = Math.min(fieldValueCount + first - 1, mvEndIndex);
        if (mvStartIndex >= fieldValueCount + first || mvEndIndex < first) {
            builder.appendNull();
            return;
        }
        if (mvStartIndex == mvEndIndex) {
            builder.appendLong(field.getLong(mvStartIndex));
            return;
        }
        builder.beginPositionEntry();
        for (int i = mvStartIndex; i <= mvEndIndex; i++) {
            builder.appendLong(field.getLong(i));
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Double", warnExceptions = { InvalidArgumentException.class })
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock field, int start, int end) {
        int fieldValueCount = field.getValueCount(position);
        checkStartEnd(start, end);
        int first = field.getFirstValueIndex(position);
        int mvStartIndex = adjustIndex(start, fieldValueCount, first);
        mvStartIndex = Math.max(first, mvStartIndex);
        int mvEndIndex = adjustIndex(end, fieldValueCount, first);
        mvEndIndex = Math.min(fieldValueCount + first - 1, mvEndIndex);
        if (mvStartIndex >= fieldValueCount + first || mvEndIndex < first) {
            builder.appendNull();
            return;
        }
        if (mvStartIndex == mvEndIndex) {
            builder.appendDouble(field.getDouble(mvStartIndex));
            return;
        }
        builder.beginPositionEntry();
        for (int i = mvStartIndex; i <= mvEndIndex; i++) {
            builder.appendDouble(field.getDouble(i));
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "BytesRef", warnExceptions = { InvalidArgumentException.class })
    static void process(BytesRefBlock.Builder builder, int position, BytesRefBlock field, int start, int end) {
        int fieldValueCount = field.getValueCount(position);
        checkStartEnd(start, end); // append null here ?
        int first = field.getFirstValueIndex(position);
        int mvStartIndex = adjustIndex(start, fieldValueCount, first);
        mvStartIndex = Math.max(first, mvStartIndex);
        int mvEndIndex = adjustIndex(end, fieldValueCount, first);
        mvEndIndex = Math.min(fieldValueCount + first - 1, mvEndIndex);
        if (mvStartIndex >= fieldValueCount + first || mvEndIndex < first) {
            builder.appendNull();
            return;
        }
        BytesRef fieldScratch = new BytesRef();
        if (mvStartIndex == mvEndIndex) {
            builder.appendBytesRef(field.getBytesRef(mvStartIndex, fieldScratch));
            return;
        }
        builder.beginPositionEntry();
        for (int i = mvStartIndex; i <= mvEndIndex; i++) {
            builder.appendBytesRef(field.getBytesRef(i, fieldScratch));
        }
        builder.endPositionEntry();
    }
}

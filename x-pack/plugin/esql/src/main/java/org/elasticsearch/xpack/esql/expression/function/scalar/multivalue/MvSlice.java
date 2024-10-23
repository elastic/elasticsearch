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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToInt;

/**
 * Returns a subset of the multivalued field using the start and end index values.
 */
public class MvSlice extends EsqlScalarFunction implements OptionalArgument, EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvSlice", MvSlice::new);

    private final Expression field, start, end;

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
            "integer",
            "ip",
            "keyword",
            "long",
            "text",
            "version" },
        description = """
            Returns a subset of the multivalued field using the start and end index values.
            This is most useful when reading from a function that emits multivalued columns
            in a known order like <<esql-split>> or <<esql-mv_sort>>.""",
        detailedDescription = """
            The order that <<esql-multivalued-fields, multivalued fields>> are read from
            underlying storage is not guaranteed. It is *frequently* ascending, but don't
            rely on that.""",
        examples = { @Example(file = "ints", tag = "mv_slice_positive"), @Example(file = "ints", tag = "mv_slice_negative") }
    )
    public MvSlice(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "version" },
            description = "Multivalue expression. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "start",
            type = { "integer" },
            description = "Start position. If `null`, the function returns `null`. "
                + "The start argument can be negative. An index of -1 is used to specify the last value in the list."
        ) Expression start,
        @Param(
            name = "end",
            type = { "integer" },
            description = "End position(included). Optional; if omitted, the position at `start` is returned. "
                + "The end argument can be negative. An index of -1 is used to specify the last value in the list.",
            optional = true
        ) Expression end
    ) {
        super(source, end == null ? Arrays.asList(field, start, start) : Arrays.asList(field, start, end));
        this.field = field;
        this.start = start;
        this.end = end;
    }

    private MvSlice(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(start);
        out.writeOptionalNamedWriteable(end);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    Expression field() {
        return field;
    }

    Expression start() {
        return start;
    }

    Expression end() {
        return end;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(field, DataType::isRepresentable, sourceText(), FIRST, "representable");
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = TypeResolutions.isType(start, dt -> dt == INTEGER, sourceText(), SECOND, "integer");
        if (resolution.unresolved()) {
            return resolution;
        }

        if (end != null) {
            resolution = TypeResolutions.isType(end, dt -> dt == INTEGER, sourceText(), THIRD, "integer");
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
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (start.foldable() && end.foldable()) {
            int startOffset = stringToInt(String.valueOf(start.fold()));
            int endOffset = stringToInt(String.valueOf(end.fold()));
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

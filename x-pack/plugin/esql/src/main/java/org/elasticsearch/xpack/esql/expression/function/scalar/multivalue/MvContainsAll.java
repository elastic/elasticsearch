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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCounters;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the count of values.
 */
public class MvContainsAll extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvContainsAll",
        MvContainsAll::new
    );
    private DataType dataType;

    @FunctionInfo(
        returnType = "boolean",
        description = "Checks if the values yielded by multivalue value expression are all also present in the values yielded by another"
            + "multivalue expression. The result is a boolean representing the outcome or null if either of the expressions where null.",
        examples = { @Example(file = "string", tag = "mv_contains_all"), @Example(file = "string", tag = "mv_contains_all_bothsides"), }
    )
    public MvContainsAll(
        Source source,
        @Param(
            name = "superset",
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
                "unsigned_long",
                "version" },
            description = "Multivalue expression."
        ) Expression superset,
        @Param(
            name = "subset",
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
                "unsigned_long",
                "version" },
            description = "Multivalue expression."
        ) Expression subset
    ) {
        super(source, superset, subset);
    }

    private MvContainsAll(StreamInput in) throws IOException {
        super(in);
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

        TypeResolution resolution = isRepresentableExceptCounters(left(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        dataType = left().dataType() == DataType.NULL ? DataType.NULL : DataType.BOOLEAN;
        if (left().dataType() == DataType.NULL) {
            dataType = right().dataType() == DataType.NULL ? DataType.NULL : DataType.BOOLEAN;
            return isRepresentableExceptCounters(right(), sourceText(), SECOND);
        }
        return isType(right(), t -> t.noText() == left().dataType().noText(), sourceText(), SECOND, left().dataType().noText().typeName());
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new MvContainsAll(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvContainsAll::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var supersetType = PlannerUtils.toElementType(left().dataType());
        var subsetType = PlannerUtils.toElementType(right().dataType());
        if (supersetType != subsetType) {
            throw new EsqlIllegalArgumentException(
                "Incompatible data types for MvContainsAll, superset type({}) value({}) and subset type({}) value({}) don't match.",
                supersetType,
                left(),
                subsetType,
                right()
            );
        }
        return switch (supersetType) {
            case BOOLEAN -> new MvContainsAllBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvContainsAllBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvContainsAllDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvContainsAllIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvContainsAllLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    @Evaluator(extraName = "Int")
    static void process(BooleanBlock.Builder builder, int position, IntBlock field1, IntBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, IntBlock::getInt));
    }

    @Evaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, int position, BooleanBlock field1, BooleanBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, BooleanBlock::getBoolean));
    }

    @Evaluator(extraName = "Long")
    static void process(BooleanBlock.Builder builder, int position, LongBlock field1, LongBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, LongBlock::getLong));
    }

    @Evaluator(extraName = "Double")
    static void process(BooleanBlock.Builder builder, int position, DoubleBlock field1, DoubleBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, DoubleBlock::getDouble));
    }

    @Evaluator(extraName = "BytesRef")
    static void process(BooleanBlock.Builder builder, int position, BytesRefBlock field1, BytesRefBlock field2) {
        appendTo(builder, containsAll(field1, field2, position, (block, index) -> {
            var ref = new BytesRef();
            block.getBytesRef(index, ref);
            return ref;
        }));
    }

    static void appendTo(BooleanBlock.Builder builder, Boolean bool) {
        if (bool == null) {
            builder.appendNull();
        } else {
            builder.beginPositionEntry().appendBoolean(bool).endPositionEntry();
        }
    }

    /**
     * A block is considered a subset if the superset contains values that test equal for all the values in the subset, independent of
     * order. Duplicates are ignored in the sense that for each duplicate in the subset, we will search/match against the first/any value
     * in the superset.
     * @param superset block to check against
     * @param subset block containing values that should be present in the other block.
     * @return {@code true} if the given blocks are a superset and subset to each other, {@code false} if not and {@code null} if the subset
     * or superset contains only null values.
     */
    static <BlockType extends Block, Type> Boolean containsAll(
        BlockType superset,
        BlockType subset,
        final int position,
        ValueExtractor<BlockType, Type> valueExtractor
    ) {
        if (superset == subset) {
            return true;
        }
        if (subset.areAllValuesNull() || superset.areAllValuesNull()) {
            return null;
        }

        final var subsetCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        for (int subsetIndex = startIndex; subsetIndex < startIndex + subsetCount; subsetIndex++) {
            var value = valueExtractor.extractValue(subset, subsetIndex);
            if (hasValue(superset, position, value, valueExtractor) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if the block has the value at any of it's positions
     * @param superset Block to search
     * @param value to search for
     * @return true if the supplied long value is in the supplied Block
     */
    static <BlockType extends Block, Type> boolean hasValue(
        BlockType superset,
        final int position,
        Type value,
        ValueExtractor<BlockType, Type> valueExtractor
    ) {
        final var supersetCount = superset.getValueCount(position);
        final var startIndex = superset.getFirstValueIndex(position);
        for (int supersetIndex = startIndex; supersetIndex < startIndex + supersetCount; supersetIndex++) {
            var element = valueExtractor.extractValue(superset, supersetIndex);
            if (element.equals(value)) {
                return true;
            }
        }
        return false;
    }

    interface ValueExtractor<BlockType extends Block, Type> {
        Type extractValue(BlockType block, int position);
    }
}

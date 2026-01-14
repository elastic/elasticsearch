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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
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
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Function that takes two multivalued expressions and checks if any values of one expression(subset) are
 * present(equals) in the other (superset).
 * <p>
 * Given Set A = {"a","b","c"} and Set B = {"c","d"}, the relationship between first (row) and second (column) arguments is:
 * <ul>
 *     <li>A, B &rArr; true (A ∩ B is a non-empty set)</li>
 *     <li>B, A &rArr; true (A ∩ B is a non-empty set)</li>
 *     <li>A, A &rArr; true (A ∩ A is a non-empty set</li>
 *     <li>B, B &rArr; true (B ∩ B is a non-empty set</li>
 *     <li>A, null &rArr; false (A ∩ &empty; is an empty set)</li>
 *     <li>null, A &rArr; false (&empty; ∩ A is an empty set)</li>
 *     <li>B, null &rArr; false (B ∩ &empty; is an empty set)</li>
 *     <li>null, B &rArr; false (&empty; ∩ B  is an empty set)</li>
 *     <li>null, null &rArr; false (&empty; ∩ &empty; is an empty set)</li>
 * </ul>
 */
public class MvOverlaps extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvOverlaps",
        MvOverlaps::new
    );

    @FunctionInfo(
        returnType = "boolean",
        description = "Checks if any value yielded by the second multivalue expression is present in the values yielded by "
            + "the first multivalue expression. Returns a boolean. Null values are treated as an empty set.",
        examples = {
            @Example(file = "mv_overlaps", tag = "mv_overlaps"),
            @Example(file = "mv_overlaps", tag = "mv_overlaps_bothsides"),
            @Example(file = "mv_overlaps", tag = "mv_overlaps_where"), },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
    )
    public MvOverlaps(
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
            description = "Multivalue expression."
        ) Expression subset
    ) {
        super(source, superset, subset);
    }

    private MvOverlaps(StreamInput in) throws IOException {
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

        TypeResolution resolution = isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(left(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (left().dataType() == DataType.NULL) {
            return isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(right(), sourceText(), SECOND);
        }
        return isType(right(), t -> t.noText() == left().dataType().noText(), sourceText(), SECOND, left().dataType().noText().typeName());
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected MvOverlaps replaceChildren(Expression newLeft, Expression newRight) {
        return new MvOverlaps(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvOverlaps::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var lefType = PlannerUtils.toElementType(left().dataType());
        var rightType = PlannerUtils.toElementType(right().dataType());

        if (lefType == ElementType.NULL || rightType == ElementType.NULL) {
            return EvalOperator.CONSTANT_FALSE_FACTORY;
        }

        if (lefType != rightType) {
            throw new EsqlIllegalArgumentException(
                "Incompatible data types for mv_overlaps, left type({}) value({}) and right type({}) value({}) don't match.",
                lefType,
                left(),
                rightType,
                right()
            );
        }

        return switch (lefType) {
            case BOOLEAN -> new MvOverlapsBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvOverlapsBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvOverlapsDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvOverlapsIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvOverlapsLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    @Evaluator(extraName = "Int", allNullsIsNull = false)
    static boolean process(@Position int position, IntBlock left, IntBlock right) {
        if (left == right) {
            return true;
        }

        final var valueCount = right.getValueCount(position);
        final var startIndex = right.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = right.getInt(valueIndex);
            if (left.hasValue(position, value)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Boolean", allNullsIsNull = false)
    static boolean process(@Position int position, BooleanBlock left, BooleanBlock right) {
        if (left == right) {
            return true;
        }

        final var valueCount = right.getValueCount(position);
        final var startIndex = right.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = right.getBoolean(valueIndex);
            if (left.hasValue(position, value)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(@Position int position, LongBlock left, LongBlock right) {
        if (left == right) {
            return true;
        }

        final var valueCount = right.getValueCount(position);
        final var startIndex = right.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = right.getLong(valueIndex);
            if (left.hasValue(position, value)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean process(@Position int position, DoubleBlock left, DoubleBlock right) {
        if (left == right) {
            return true;
        }

        final var valueCount = right.getValueCount(position);
        final var startIndex = right.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = right.getDouble(valueIndex);
            if (left.hasValue(position, value)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean process(@Position int position, BytesRefBlock left, BytesRefBlock right) {
        if (left == right) {
            return true;
        }

        final var valueCount = right.getValueCount(position);
        final var startIndex = right.getFirstValueIndex(position);
        var value = new BytesRef();
        var scratch = new BytesRef();
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            // we pass in a reference, but sometimes we only get a return value, see ConstantBytesRefVector.getBytesRef
            value = right.getBytesRef(valueIndex, value);
            if (left.hasValue(position, value, scratch)) {
                return true;
            }
        }
        return false;
    }
}

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
public class MvIntersects extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvIntersects",
        MvIntersects::new
    );

    @FunctionInfo(
        returnType = "boolean",
        description = "Checks if any value yielded by the second multivalue expression is present in the values yielded by "
            + "the first multivalue expression. Returns a boolean. Null values are treated as an empty set.",
        examples = {
            @Example(file = "mv_intersects", tag = "mv_intersects"),
            @Example(file = "mv_intersects", tag = "mv_intersects_bothsides"),
            @Example(file = "mv_intersects", tag = "mv_intersects_where"), },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
    )
    public MvIntersects(
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
            description = "Multivalue expression."
        ) Expression superset,
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
            description = "Multivalue expression."
        ) Expression subset
    ) {
        super(source, superset, subset);
    }

    private MvIntersects(StreamInput in) throws IOException {
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
    protected MvIntersects replaceChildren(Expression newLeft, Expression newRight) {
        return new MvIntersects(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvIntersects::new, left(), right());
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
                "Incompatible data types for mv_intersects, left type({}) value({}) and right type({}) value({}) don't match.",
                lefType,
                left(),
                rightType,
                right()
            );
        }

        return switch (lefType) {
            case BOOLEAN -> new MvIntersectsBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvIntersectsBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvIntersectsDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvIntersectsIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvIntersectsLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    /*
     * process method, approach:
     *
     * for comparable values excluding boolean, is:
     * If quick equals reference check succeeds then they overlap and return true. If any side is ordered than
     * we can do something better than linear scanning. If only one side is ordered then we linear scan the other
     * side and call block#hasValue(..) which might perform a binary search. We call the process method again with
     * params reversed to ensure the ordered one is on the right.
     * If both sides are ordered we do a stepwise comparison.
     *
     * for booleans we scan for the first flip.
     */

    @Evaluator(extraName = "Int", allNullsIsNull = false)
    static boolean process(@Position int position, IntBlock left, IntBlock right) {
        if (left == right) {
            return true;
        }
        if (left.isNull(position) || right.isNull(position)) {
            return false;
        }
        final var leftStartIndex = left.getFirstValueIndex(position);
        final var leftEndIndex = leftStartIndex + left.getValueCount(position);
        if (left.mvSortedAscending()) {
            if (right.mvSortedAscending() == false) {
                return process(position, right, left);
            }
            var rightStartIndex = right.getFirstValueIndex(position);
            var rightEndIndex = rightStartIndex + right.getValueCount(position);
            var leftIndex = leftStartIndex;
            var rightIndex = rightStartIndex;
            if (leftIndex >= leftEndIndex || rightIndex >= rightEndIndex) {
                return false;
            }
            var leftValue = left.getInt(leftIndex);
            var rightValue = right.getInt(rightIndex);
            while (true) {
                if (leftValue == rightValue) {
                    return true;
                } else if (leftValue < rightValue) {
                    leftIndex++;
                    if (leftIndex >= leftEndIndex) {
                        return false;
                    }
                    leftValue = left.getInt(leftIndex);
                } else {
                    rightIndex++;
                    if (rightIndex >= rightEndIndex) {
                        return false;
                    }
                    rightValue = right.getInt(rightIndex);
                }
            }
        }
        for (int valueIndex = leftStartIndex; valueIndex < leftEndIndex; valueIndex++) {
            var value = left.getInt(valueIndex);
            if (right.hasValue(position, value)) {
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
        if (left.isNull(position) || right.isNull(position)) {
            return false;
        }
        if (left.getValueCount(position) < right.getValueCount(position)) {
            boolean value = right.getBoolean(right.getFirstValueIndex(position));
            if (left.hasValue(position, value)) {
                return true;
            }
            return right.hasValue(position, value == false);
        }
        boolean value = left.getBoolean(left.getFirstValueIndex(position));
        if (right.hasValue(position, value)) {
            return true;
        }
        return left.hasValue(position, value == false);
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(@Position int position, LongBlock left, LongBlock right) {
        if (left == right) {
            return true;
        }
        if (left.isNull(position) || right.isNull(position)) {
            return false;
        }
        final var leftStartIndex = left.getFirstValueIndex(position);
        final var leftEndIndex = leftStartIndex + left.getValueCount(position);
        if (left.mvSortedAscending()) {
            if (right.mvSortedAscending() == false) {
                return process(position, right, left);
            }
            var rightStartIndex = right.getFirstValueIndex(position);
            var rightEndIndex = rightStartIndex + right.getValueCount(position);
            var leftIndex = leftStartIndex;
            var rightIndex = rightStartIndex;
            if (leftIndex >= leftEndIndex || rightIndex >= rightEndIndex) {
                return false;
            }
            var leftValue = left.getLong(leftIndex);
            var rightValue = right.getLong(rightIndex);
            while (true) {
                if (leftValue == rightValue) {
                    return true;
                } else if (leftValue < rightValue) {
                    leftIndex++;
                    if (leftIndex >= leftEndIndex) {
                        return false;
                    }
                    leftValue = left.getLong(leftIndex);
                } else {
                    rightIndex++;
                    if (rightIndex >= rightEndIndex) {
                        return false;
                    }
                    rightValue = right.getLong(rightIndex);
                }
            }
        }
        for (int valueIndex = leftStartIndex; valueIndex < leftEndIndex; valueIndex++) {
            var value = left.getLong(valueIndex);
            if (right.hasValue(position, value)) {
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
        if (left.isNull(position) || right.isNull(position)) {
            return false;
        }
        final var leftStartIndex = left.getFirstValueIndex(position);
        final var leftEndIndex = leftStartIndex + left.getValueCount(position);
        if (left.mvSortedAscending()) {
            if (right.mvSortedAscending() == false) {
                return process(position, right, left);
            }
            var rightStartIndex = right.getFirstValueIndex(position);
            var rightEndIndex = rightStartIndex + right.getValueCount(position);
            var leftIndex = leftStartIndex;
            var rightIndex = rightStartIndex;
            if (leftIndex >= leftEndIndex || rightIndex >= rightEndIndex) {
                return false;
            }
            var leftValue = left.getDouble(leftIndex);
            var rightValue = right.getDouble(rightIndex);
            while (true) {
                if (leftValue == rightValue) {
                    return true;
                } else if (leftValue < rightValue) {
                    leftIndex++;
                    if (leftIndex >= leftEndIndex) {
                        return false;
                    }
                    leftValue = left.getDouble(leftIndex);
                } else {
                    rightIndex++;
                    if (rightIndex >= rightEndIndex) {
                        return false;
                    }
                    rightValue = right.getDouble(rightIndex);
                }
            }
        }
        for (int valueIndex = leftStartIndex; valueIndex < leftEndIndex; valueIndex++) {
            var value = left.getDouble(valueIndex);
            if (right.hasValue(position, value)) {
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
        if (left.isNull(position) || right.isNull(position)) {
            return false;
        }
        final var leftStartIndex = left.getFirstValueIndex(position);
        final var leftEndIndex = leftStartIndex + left.getValueCount(position);
        var leftValue = new BytesRef();
        var rightValue = new BytesRef();

        if (left.mvSortedAscending()) {
            if (right.mvSortedAscending() == false) {
                return process(position, right, left);
            }
            var rightStartIndex = right.getFirstValueIndex(position);
            var rightEndIndex = rightStartIndex + right.getValueCount(position);
            var leftIndex = leftStartIndex;
            var rightIndex = rightStartIndex;

            if (leftIndex >= leftEndIndex || rightIndex >= rightEndIndex) {
                return false;
            }

            leftValue = left.getBytesRef(leftIndex, leftValue);
            rightValue = right.getBytesRef(rightIndex, rightValue);

            while (true) {
                int compare = leftValue.compareTo(rightValue);
                if (compare == 0) {
                    return true;
                } else if (compare < 0) {
                    leftIndex++;
                    if (leftIndex >= leftEndIndex) {
                        return false;
                    }
                    leftValue = left.getBytesRef(leftIndex, leftValue);
                } else {
                    rightIndex++;
                    if (rightIndex >= rightEndIndex) {
                        return false;
                    }
                    rightValue = right.getBytesRef(rightIndex, rightValue);
                }
            }
        }

        for (int valueIndex = leftStartIndex; valueIndex < leftEndIndex; valueIndex++) {
            // we pass in a reference, but sometimes we only get a return value, see ConstantBytesRefVector.getBytesRef
            leftValue = left.getBytesRef(valueIndex, leftValue);
            if (right.hasValue(position, leftValue, rightValue)) {
                return true;
            }
        }
        return false;
    }
}

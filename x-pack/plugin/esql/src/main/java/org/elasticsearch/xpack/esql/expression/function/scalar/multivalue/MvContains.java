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
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
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
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Function that takes two multivalued expressions and checks if values of one expression(subset) are
 * all present(equals) in the other (superset). Duplicates are ignored in the sense that for each
 * duplicate in the subset, we will search/match against the first/any value in the superset.
 * <p>
 * Given Set A = {"a","b","c"} and Set B = {"b","c"}, the relationship between first (row) and second (column) arguments is:
 * <ul>
 *     <li>A, B &rArr; true  (A &sube; B)</li>
 *     <li>B, A &rArr; false (A &#8840; B)</li>
 *     <li>A, A &rArr; true (A &equiv; A)</li>
 *     <li>B, B &rArr; true (B &equiv; B)</li>
 *     <li>A, null &rArr; true (B &sube; &empty;)</li>
 *     <li>null, A &rArr; false (&empty; &#8840; B)</li>
 *     <li>B, null &rArr; true (B &sube; &empty;)</li>
 *     <li>null, B &rArr; false (&empty; &#8840; B)</li>
 *     <li>null, null &rArr; true (&empty; &equiv; &empty;)</li>
 * </ul>
 */
public class MvContains extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvContains",
        MvContains::new
    );

    @FunctionInfo(
        returnType = "boolean",
        description = "Checks if all values yielded by the second multivalue expression are present in the values yielded by "
            + "the first multivalue expression. Returns a boolean. Null values are treated as an empty set.",
        examples = {
            @Example(file = "string", tag = "mv_contains"),
            @Example(file = "string", tag = "mv_contains_bothsides"),
            @Example(file = "string", tag = "mv_contains_where"), },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") }
    )
    public MvContains(
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

    private MvContains(StreamInput in) throws IOException {
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

        TypeResolution resolution = isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram(
            left(),
            sourceText(),
            FIRST
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        if (left().dataType() == DataType.NULL) {
            return isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndExponentialHistogram(right(), sourceText(), SECOND);
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
    protected MvContains replaceChildren(Expression newLeft, Expression newRight) {
        return new MvContains(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvContains::new, left(), right());
    }

    @Override
    public Object fold(ExpressionContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var supersetType = PlannerUtils.toElementType(left().dataType());
        var subsetType = PlannerUtils.toElementType(right().dataType());

        if (subsetType == ElementType.NULL) {
            return EvalOperator.CONSTANT_TRUE_FACTORY;
        }

        if (supersetType != ElementType.NULL && supersetType != subsetType) {
            throw new EsqlIllegalArgumentException(
                "Incompatible data types for MvContains, superset type({}) value({}) and subset type({}) value({}) don't match.",
                supersetType,
                left(),
                subsetType,
                right()
            );
        }

        return switch (supersetType) {
            case BOOLEAN -> new MvContainsBooleanEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case BYTES_REF -> new MvContainsBytesRefEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case DOUBLE -> new MvContainsDoubleEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case INT -> new MvContainsIntEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case LONG -> new MvContainsLongEvaluator.Factory(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            case NULL -> new IsNull.IsNullEvaluatorFactory(toEvaluator.apply(right()));
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    @Evaluator(extraName = "Int", allNullsIsNull = false)
    static boolean process(@Position int position, IntBlock superset, IntBlock subset) {
        if (superset == subset || subset.areAllValuesNull()) {
            return true;
        }

        final var valueCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = subset.getInt(valueIndex);
            if (superset.hasValue(position, value) == false) {
                return false;
            }
        }
        return true;
    }

    @Evaluator(extraName = "Boolean", allNullsIsNull = false)
    static boolean process(@Position int position, BooleanBlock superset, BooleanBlock subset) {
        if (superset == subset || subset.areAllValuesNull()) {
            return true;
        }

        final var valueCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = subset.getBoolean(valueIndex);
            if (superset.hasValue(position, value) == false) {
                return false;
            }
        }
        return true;
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(@Position int position, LongBlock superset, LongBlock subset) {
        if (superset == subset || subset.areAllValuesNull()) {
            return true;
        }

        final var valueCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = subset.getLong(valueIndex);
            if (superset.hasValue(position, value) == false) {
                return false;
            }
        }
        return true;
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean process(@Position int position, DoubleBlock superset, DoubleBlock subset) {
        if (superset == subset || subset.areAllValuesNull()) {
            return true;
        }

        final var valueCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            var value = subset.getDouble(valueIndex);
            if (superset.hasValue(position, value) == false) {
                return false;
            }
        }
        return true;
    }

    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean process(@Position int position, BytesRefBlock superset, BytesRefBlock subset) {
        if (superset == subset || subset.areAllValuesNull()) {
            return true;
        }

        final var valueCount = subset.getValueCount(position);
        final var startIndex = subset.getFirstValueIndex(position);
        var value = new BytesRef();
        var scratch = new BytesRef();
        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            // we pass in a reference, but sometimes we only get a return value, see ConstantBytesRefVector.getBytesRef
            value = subset.getBytesRef(valueIndex, value);
            if (superset.hasValue(position, value, scratch) == false) {
                return false;
            }
        }
        return true;
    }
}

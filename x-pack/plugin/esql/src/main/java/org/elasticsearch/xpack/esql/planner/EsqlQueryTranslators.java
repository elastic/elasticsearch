/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

public class EsqlQueryTranslators {
    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = Stream.concat(
        List.of(new TrivialBinaryComparisons()).stream(),
        ExpressionTranslators.QUERY_TRANSLATORS.stream()
    ).toList();

    public static class TrivialBinaryComparisons extends ExpressionTranslator<BinaryComparison> {
        @Override
        protected Query asQuery(BinaryComparison bc, TranslatorHandler handler) {
            ExpressionTranslators.BinaryComparisons.checkBinaryComparison(bc);
            Query translated = translate(bc, handler);
            return translated == null ? null : handler.wrapFunctionQuery(bc, bc.left(), () -> translated);
        }

        static Query translate(BinaryComparison bc, TranslatorHandler handler) {
            if ((bc.left() instanceof FieldAttribute) == false || bc.right().foldable() == false) {
                return null;
            }
            FieldAttribute attribute = (FieldAttribute) bc.left();
            Source source = bc.source();
            Object value = ExpressionTranslators.valueOf(bc.right());

            // Comparisons with multi-values always return null in ESQL.
            if (value instanceof List<?>) {
                return new MatchAll(Source.EMPTY).negate(source);
            }

            DataType valueType = bc.right().dataType();
            DataType attributeDataType = attribute.dataType();
            if (valueType == UNSIGNED_LONG && value instanceof Long ul) {
                value = unsignedLongAsNumber(ul);
            }
            if ((value instanceof Number) == false || isInRange(attributeDataType, valueType, (Number) value)) {
                return null;
            }
            Number num = (Number) value;

            if (Double.isNaN(((Number) value).doubleValue())) {
                return new MatchAll(Source.EMPTY).negate(source);
            }

            boolean matchAllOrNone;
            if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) {
                matchAllOrNone = (num.doubleValue() > 0) == false;
            } else if (bc instanceof LessThan || bc instanceof LessThanOrEqual) {
                matchAllOrNone = (num.doubleValue() > 0);
            } else if (bc instanceof Equals || bc instanceof NullEquals) {
                matchAllOrNone = false;
            } else if (bc instanceof NotEquals) {
                matchAllOrNone = true;
            } else {
                throw new QlIllegalArgumentException("Unknown binary comparison [{}]", bc);
            }

            return matchAllOrNone ? new MatchAll(source) : new MatchAll(Source.EMPTY).negate(source);
        }

        private static final BigDecimal HALF_FLOAT_MAX = BigDecimal.valueOf(65504);
        private static final BigDecimal UNSIGNED_LONG_MAX = BigDecimal.valueOf(2).pow(64).subtract(BigDecimal.ONE);

        private static boolean isInRange(DataType numericFieldDataType, DataType valueDataType, Number value) {
            double doubleValue = value.doubleValue();
            if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                return false;
            }

            BigDecimal decimalValue;
            if (value instanceof BigInteger bigIntValue) {
                // Unsigned longs may be represented as BigInteger.
                decimalValue = new BigDecimal(bigIntValue);
            } else {
                decimalValue = valueDataType.isRational() ? BigDecimal.valueOf(doubleValue) : BigDecimal.valueOf(value.longValue());
            }

            // Determine min/max for dataType. Use BigDecimals as doubles will have rounding errors for long/ulong.
            BigDecimal minValue;
            BigDecimal maxValue;
            if (numericFieldDataType == DataTypes.BYTE) {
                minValue = BigDecimal.valueOf(Byte.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Byte.MAX_VALUE);
            } else if (numericFieldDataType == DataTypes.SHORT) {
                minValue = BigDecimal.valueOf(Short.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Short.MAX_VALUE);
            } else if (numericFieldDataType == DataTypes.INTEGER) {
                minValue = BigDecimal.valueOf(Integer.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Integer.MAX_VALUE);
            } else if (numericFieldDataType == DataTypes.LONG) {
                minValue = BigDecimal.valueOf(Long.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Long.MAX_VALUE);
            } else if (numericFieldDataType == DataTypes.UNSIGNED_LONG) {
                minValue = BigDecimal.ZERO;
                maxValue = UNSIGNED_LONG_MAX;
            } else if (numericFieldDataType == DataTypes.HALF_FLOAT) {
                minValue = HALF_FLOAT_MAX.negate();
                maxValue = HALF_FLOAT_MAX;
            } else if (numericFieldDataType == DataTypes.FLOAT) {
                minValue = BigDecimal.valueOf(-Float.MAX_VALUE);
                maxValue = BigDecimal.valueOf(Float.MAX_VALUE);
            } else if (numericFieldDataType == DataTypes.DOUBLE || numericFieldDataType == DataTypes.SCALED_FLOAT) {
                // Scaled floats are represented as doubles in ESQL.
                minValue = BigDecimal.valueOf(-Double.MAX_VALUE);
                maxValue = BigDecimal.valueOf(Double.MAX_VALUE);
            } else {
                throw new QlIllegalArgumentException("Data type [{}] unsupported for numeric range check", numericFieldDataType);
            }

            return minValue.compareTo(decimalValue) <= 0 && maxValue.compareTo(decimalValue) >= 0;
        }
    }
}

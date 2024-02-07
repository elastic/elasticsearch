/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
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
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Check;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

public final class EsqlExpressionTranslators {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new EqualsIgnoreCaseTranslator(),
        new BinaryComparisons(),
        new ExpressionTranslators.Ranges(),
        new ExpressionTranslators.BinaryLogic(),
        new ExpressionTranslators.IsNulls(),
        new ExpressionTranslators.IsNotNulls(),
        new ExpressionTranslators.Nots(),
        new ExpressionTranslators.Likes(),
        new ExpressionTranslators.InComparisons(),
        new ExpressionTranslators.StringQueries(),
        new ExpressionTranslators.Matches(),
        new ExpressionTranslators.MultiMatches(),
        new Scalars()
    );

    public static Query toQuery(Expression e, TranslatorHandler handler) {
        Query translation = null;
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, handler);
            if (translation != null) {
                return translation;
            }
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    public static class EqualsIgnoreCaseTranslator extends ExpressionTranslator<InsensitiveEquals> {

        @Override
        protected Query asQuery(InsensitiveEquals bc, TranslatorHandler handler) {
            return doTranslate(bc, handler);
        }

        public static Query doTranslate(InsensitiveEquals bc, TranslatorHandler handler) {
            checkInsensitiveComparison(bc);
            return handler.wrapFunctionQuery(bc, bc.left(), () -> translate(bc));
        }

        public static void checkInsensitiveComparison(InsensitiveEquals bc) {
            Check.isTrue(
                bc.right().foldable(),
                "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
                bc.right().sourceLocation().getLineNumber(),
                bc.right().sourceLocation().getColumnNumber(),
                Expressions.name(bc.right()),
                bc.symbol()
            );
        }

        static Query translate(InsensitiveEquals bc) {
            TypedAttribute attribute = checkIsPushableAttribute(bc.left());
            Source source = bc.source();
            BytesRef value = BytesRefs.toBytesRef(ExpressionTranslators.valueOf(bc.right()));
            String name = pushableAttributeName(attribute);
            return new TermQuery(source, name, value.utf8ToString(), true);
        }
    }

    public static class BinaryComparisons extends ExpressionTranslator<BinaryComparison> {
        @Override
        protected Query asQuery(BinaryComparison bc, TranslatorHandler handler) {
            return doTranslate(bc, handler);
        }

        public static Query doTranslate(BinaryComparison bc, TranslatorHandler handler) {
            ExpressionTranslators.BinaryComparisons.checkBinaryComparison(bc);
            Query translated = translateOutOfRangeComparisons(bc);
            return translated == null
                ? ExpressionTranslators.BinaryComparisons.doTranslate(bc, handler)
                : handler.wrapFunctionQuery(bc, bc.left(), () -> translated);
        }

        private static Query translateOutOfRangeComparisons(BinaryComparison bc) {
            if ((bc.left() instanceof FieldAttribute) == false
                || bc.left().dataType().isNumeric() == false
                || bc.right().foldable() == false) {
                return null;
            }
            Source source = bc.source();
            Object value = ExpressionTranslators.valueOf(bc.right());

            // Comparisons with multi-values always return null in ESQL.
            if (value instanceof List<?>) {
                return new MatchAll(source).negate(source);
            }

            DataType valueType = bc.right().dataType();
            DataType attributeDataType = bc.left().dataType();
            if (valueType == UNSIGNED_LONG && value instanceof Long ul) {
                value = unsignedLongAsNumber(ul);
            }
            Number num = (Number) value;
            if (isInRange(attributeDataType, valueType, num)) {
                return null;
            }

            if (Double.isNaN(((Number) value).doubleValue())) {
                return new MatchAll(source).negate(source);
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

            return matchAllOrNone ? new MatchAll(source) : new MatchAll(source).negate(source);
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

    public static class Scalars extends ExpressionTranslator<ScalarFunction> {
        @Override
        protected Query asQuery(ScalarFunction f, TranslatorHandler handler) {
            return doTranslate(f, handler);
        }

        public static Query doTranslate(ScalarFunction f, TranslatorHandler handler) {
            if (f instanceof CIDRMatch cm) {
                if (cm.ipField() instanceof FieldAttribute fa && Expressions.foldable(cm.matches())) {
                    String targetFieldName = handler.nameOf(fa.exactAttribute());
                    Set<Object> set = new LinkedHashSet<>(Expressions.fold(cm.matches()));

                    Query query = new TermsQuery(f.source(), targetFieldName, set);
                    // CIDR_MATCH applies only to single values.
                    return handler.wrapFunctionQuery(f, cm.ipField(), () -> query);
                }
            }

            return ExpressionTranslators.Scalars.doTranslate(f, handler);
        }
    }
}

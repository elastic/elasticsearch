/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.ql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.versionfield.Version;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
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

    /**
     * This class is responsible for pushing the ES|QL Binary Comparison operators into Lucene.  It covers:
     *  <ul>
     *      <li>{@link Equals}</li>
     *      <li>{@link NotEquals}</li>
     *      <li>{@link NullEquals}</li>
     *      <li>{@link GreaterThanOrEqual}</li>
     *      <li>{@link GreaterThan}</li>
     *      <li>{@link LessThanOrEqual}</li>
     *      <li>{@link LessThan}</li>
     *  </ul>
     *
     *  In general, we are able to push these down when one of the arguments is a constant (i.e. is foldable).  This class assumes
     *  that an earlier pass through the query has rearranged things so that the foldable value will be the right hand side
     *  input to the operation.
     */
    public static class BinaryComparisons extends ExpressionTranslator<BinaryComparison> {
        @Override
        protected Query asQuery(BinaryComparison bc, TranslatorHandler handler) {
            // TODO: Pretty sure this check is redundant with the one at the beginning of translate
            ExpressionTranslators.BinaryComparisons.checkBinaryComparison(bc);
            Query translated = translateOutOfRangeComparisons(bc);
            if (translated != null) {
                return handler.wrapFunctionQuery(bc, bc.left(), () -> translated);
            }
            return handler.wrapFunctionQuery(bc, bc.left(), () -> translate(bc, handler));
        }

        static Query translate(BinaryComparison bc, TranslatorHandler handler) {
            Check.isTrue(
                bc.right().foldable(),
                "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
                bc.right().sourceLocation().getLineNumber(),
                bc.right().sourceLocation().getColumnNumber(),
                Expressions.name(bc.right()),
                bc.symbol()
            );
            TypedAttribute attribute = checkIsPushableAttribute(bc.left());
            Source source = bc.source();
            String name = handler.nameOf(attribute);
            Object result = bc.right().fold();
            Object value = result;
            String format = null;
            boolean isDateLiteralComparison = false;

            // TODO: This type coersion layer is copied directly from the QL counterpart code. It's probably not necessary or desireable
            // in the ESQL version. We should instead do the type conversions using our casting functions.
            // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
            // no matter the timezone provided by the user
            if (value instanceof ZonedDateTime || value instanceof OffsetTime) {
                DateFormatter formatter;
                if (value instanceof ZonedDateTime) {
                    formatter = DateFormatter.forPattern("strict_date_optional_time_nanos");
                    // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime instance
                    // which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                    // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format as well.
                    value = formatter.format((ZonedDateTime) value);
                } else {
                    formatter = DateFormatter.forPattern("strict_hour_minute_second_fraction");
                    value = formatter.format((OffsetTime) value);
                }
                format = formatter.pattern();
                isDateLiteralComparison = true;
            } else if (attribute.dataType() == IP && value instanceof BytesRef bytesRef) {
                value = DocValueFormat.IP.format(bytesRef);
            } else if (attribute.dataType() == VERSION) {
                // VersionStringFieldMapper#indexedValueForSearch() only accepts as input String or BytesRef with the String (i.e. not
                // encoded) representation of the version as it'll do the encoding itself.
                if (value instanceof BytesRef bytesRef) {
                    value = new Version(bytesRef).toString();
                } else if (value instanceof Version version) {
                    value = version.toString();
                }
            } else if (attribute.dataType() == UNSIGNED_LONG && value instanceof Long ul) {
                value = unsignedLongAsNumber(ul);
            }

            ZoneId zoneId = null;
            if (DataTypes.isDateTime(attribute.dataType())) {
                zoneId = bc.zoneId();
            }
            if (bc instanceof GreaterThan) {
                return new RangeQuery(source, name, value, false, null, false, format, zoneId);
            }
            if (bc instanceof GreaterThanOrEqual) {
                return new RangeQuery(source, name, value, true, null, false, format, zoneId);
            }
            if (bc instanceof LessThan) {
                return new RangeQuery(source, name, null, false, value, false, format, zoneId);
            }
            if (bc instanceof LessThanOrEqual) {
                return new RangeQuery(source, name, null, false, value, true, format, zoneId);
            }
            if (bc instanceof Equals || bc instanceof NullEquals || bc instanceof NotEquals) {
                name = pushableAttributeName(attribute);

                Query query;
                if (isDateLiteralComparison) {
                    // dates equality uses a range query because it's the one that has a "format" parameter
                    query = new RangeQuery(source, name, value, true, value, true, format, zoneId);
                } else {
                    query = new TermQuery(source, name, value);
                }
                if (bc instanceof NotEquals) {
                    query = new NotQuery(source, query);
                }
                return query;
            }

            throw new QlIllegalArgumentException("Don't know how to translate binary comparison [{}] in [{}]", bc.right().nodeString(), bc);
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

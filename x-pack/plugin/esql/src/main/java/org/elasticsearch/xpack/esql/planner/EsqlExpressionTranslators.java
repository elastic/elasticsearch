/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslators;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.querydsl.query.SpatialRelatesQuery;
import org.elasticsearch.xpack.versionfield.Version;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.core.planner.ExpressionTranslators.or;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.HOUR_MINUTE_SECOND;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

public final class EsqlExpressionTranslators {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new EqualsIgnoreCaseTranslator(),
        new BinaryComparisons(),
        new SpatialRelatesTranslator(),
        new InComparisons(),
        new Ranges(), // Create Range in PushFiltersToSource for qualified pushable filters on the same field.
        new ExpressionTranslators.BinaryLogic(),
        new ExpressionTranslators.IsNulls(),
        new ExpressionTranslators.IsNotNulls(),
        new ExpressionTranslators.Nots(),
        new ExpressionTranslators.Likes(),
        new ExpressionTranslators.StringQueries(),
        new ExpressionTranslators.Matches(),
        new ExpressionTranslators.MultiMatches(),
        new MatchFunctionTranslator(),
        new QueryStringFunctionTranslator(),
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
            BytesRef value = BytesRefs.toBytesRef(valueOf(bc.right()));
            String name = pushableAttributeName(attribute);
            return new TermQuery(source, name, value.utf8ToString(), true);
        }
    }

    /**
     * This class is responsible for pushing the ES|QL Binary Comparison operators into Lucene.  It covers:
     *  <ul>
     *      <li>{@link Equals}</li>
     *      <li>{@link NotEquals}</li>
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
            Check.isTrue(
                bc.right().foldable(),
                "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [{}]",
                bc.right().sourceLocation().getLineNumber(),
                bc.right().sourceLocation().getColumnNumber(),
                Expressions.name(bc.right()),
                bc.symbol()
            );

            Query translated = translateOutOfRangeComparisons(bc);
            if (translated != null) {
                return handler.wrapFunctionQuery(bc, bc.left(), () -> translated);
            }
            return handler.wrapFunctionQuery(bc, bc.left(), () -> translate(bc, handler));
        }

        static Query translate(BinaryComparison bc, TranslatorHandler handler) {
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
                    formatter = DEFAULT_DATE_TIME_FORMATTER;
                    // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime instance
                    // which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                    // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format as well.
                    value = formatter.format((ZonedDateTime) value);
                } else {
                    formatter = HOUR_MINUTE_SECOND;
                    value = formatter.format((OffsetTime) value);
                }
                format = formatter.pattern();
                isDateLiteralComparison = true;
            } else if (attribute.dataType() == IP && value instanceof BytesRef bytesRef) {
                value = ipToString(bytesRef);
            } else if (attribute.dataType() == VERSION) {
                // VersionStringFieldMapper#indexedValueForSearch() only accepts as input String or BytesRef with the String (i.e. not
                // encoded) representation of the version as it'll do the encoding itself.
                if (value instanceof BytesRef bytesRef) {
                    value = versionToString(bytesRef);
                } else if (value instanceof Version version) {
                    value = versionToString(version);
                }
            } else if (attribute.dataType() == UNSIGNED_LONG && value instanceof Long ul) {
                value = unsignedLongAsNumber(ul);
            }

            ZoneId zoneId = null;
            if (DataType.isDateTime(attribute.dataType())) {
                zoneId = bc.zoneId();
                value = dateTimeToString((Long) value);
                format = DEFAULT_DATE_TIME_FORMATTER.pattern();
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
            if (bc instanceof Equals || bc instanceof NotEquals) {
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
            Object value = valueOf(bc.right());

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
            } else if (bc instanceof Equals) {
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
                decimalValue = valueDataType.isRationalNumber() ? BigDecimal.valueOf(doubleValue) : BigDecimal.valueOf(value.longValue());
            }

            // Determine min/max for dataType. Use BigDecimals as doubles will have rounding errors for long/ulong.
            BigDecimal minValue;
            BigDecimal maxValue;
            if (numericFieldDataType == DataType.BYTE) {
                minValue = BigDecimal.valueOf(Byte.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Byte.MAX_VALUE);
            } else if (numericFieldDataType == DataType.SHORT) {
                minValue = BigDecimal.valueOf(Short.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Short.MAX_VALUE);
            } else if (numericFieldDataType == DataType.INTEGER) {
                minValue = BigDecimal.valueOf(Integer.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Integer.MAX_VALUE);
            } else if (numericFieldDataType == DataType.LONG) {
                minValue = BigDecimal.valueOf(Long.MIN_VALUE);
                maxValue = BigDecimal.valueOf(Long.MAX_VALUE);
            } else if (numericFieldDataType == DataType.UNSIGNED_LONG) {
                minValue = BigDecimal.ZERO;
                maxValue = UNSIGNED_LONG_MAX;
            } else if (numericFieldDataType == DataType.HALF_FLOAT) {
                minValue = HALF_FLOAT_MAX.negate();
                maxValue = HALF_FLOAT_MAX;
            } else if (numericFieldDataType == DataType.FLOAT) {
                minValue = BigDecimal.valueOf(-Float.MAX_VALUE);
                maxValue = BigDecimal.valueOf(Float.MAX_VALUE);
            } else if (numericFieldDataType == DataType.DOUBLE || numericFieldDataType == DataType.SCALED_FLOAT) {
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
            // TODO we could optimize starts_with as well

            throw new QlIllegalArgumentException("Cannot translate expression:[" + f.sourceText() + "]");
        }
    }

    public static class SpatialRelatesTranslator extends ExpressionTranslator<SpatialRelatesFunction> {

        @Override
        protected Query asQuery(SpatialRelatesFunction bc, TranslatorHandler handler) {
            return doTranslate(bc, handler);
        }

        public static void checkSpatialRelatesFunction(Expression constantExpression, ShapeRelation queryRelation) {
            Check.isTrue(
                constantExpression.foldable(),
                "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [ST_{}]",
                constantExpression.sourceLocation().getLineNumber(),
                constantExpression.sourceLocation().getColumnNumber(),
                Expressions.name(constantExpression),
                queryRelation
            );
        }

        public static Query doTranslate(SpatialRelatesFunction bc, TranslatorHandler handler) {
            if (bc.left().foldable()) {
                checkSpatialRelatesFunction(bc.left(), bc.queryRelation());
                return translate(bc, handler, bc.right(), bc.left());
            } else {
                checkSpatialRelatesFunction(bc.right(), bc.queryRelation());
                return translate(bc, handler, bc.left(), bc.right());
            }
        }

        static Query translate(
            SpatialRelatesFunction bc,
            TranslatorHandler handler,
            Expression spatialExpression,
            Expression constantExpression
        ) {
            TypedAttribute attribute = checkIsPushableAttribute(spatialExpression);
            String name = handler.nameOf(attribute);

            try {
                Geometry shape = SpatialRelatesUtils.makeGeometryFromLiteral(constantExpression);
                return new SpatialRelatesQuery(bc.source(), name, bc.queryRelation(), shape, attribute.dataType());
            } catch (IllegalArgumentException e) {
                throw new QlIllegalArgumentException(e.getMessage(), e);
            }
        }
    }

    public static class InComparisons extends ExpressionTranslator<In> {

        @Override
        protected Query asQuery(In in, TranslatorHandler handler) {
            return doTranslate(in, handler);
        }

        public static Query doTranslate(In in, TranslatorHandler handler) {
            return handler.wrapFunctionQuery(in, in.value(), () -> translate(in, handler));
        }

        private static boolean needsTypeSpecificValueHandling(DataType fieldType) {
            return DataType.isDateTime(fieldType) || fieldType == IP || fieldType == VERSION || fieldType == UNSIGNED_LONG;
        }

        private static Query translate(In in, TranslatorHandler handler) {
            TypedAttribute attribute = checkIsPushableAttribute(in.value());

            Set<Object> terms = new LinkedHashSet<>();
            List<Query> queries = new ArrayList<>();

            for (Expression rhs : in.list()) {
                if (DataType.isNull(rhs.dataType()) == false) {
                    if (needsTypeSpecificValueHandling(attribute.dataType())) {
                        // delegates to BinaryComparisons translator to ensure consistent handling of date and time values
                        Query query = BinaryComparisons.translate(new Equals(in.source(), in.value(), rhs), handler);

                        if (query instanceof TermQuery) {
                            terms.add(((TermQuery) query).value());
                        } else {
                            queries.add(query);
                        }
                    } else {
                        terms.add(valueOf(rhs));
                    }
                }
            }

            if (terms.isEmpty() == false) {
                String fieldName = pushableAttributeName(attribute);
                queries.add(new TermsQuery(in.source(), fieldName, terms));
            }

            return queries.stream().reduce((q1, q2) -> or(in.source(), q1, q2)).get();
        }
    }

    public static class Ranges extends ExpressionTranslator<Range> {

        @Override
        protected Query asQuery(Range r, TranslatorHandler handler) {
            return doTranslate(r, handler);
        }

        public static Query doTranslate(Range r, TranslatorHandler handler) {
            return handler.wrapFunctionQuery(r, r.value(), () -> translate(r, handler));
        }

        private static RangeQuery translate(Range r, TranslatorHandler handler) {
            Object lower = valueOf(r.lower());
            Object upper = valueOf(r.upper());
            String format = null;

            DataType dataType = r.value().dataType();
            if (DataType.isDateTime(dataType) && DataType.isDateTime(r.lower().dataType()) && DataType.isDateTime(r.upper().dataType())) {
                lower = dateTimeToString((Long) lower);
                upper = dateTimeToString((Long) upper);
                format = DEFAULT_DATE_TIME_FORMATTER.pattern();
            }

            if (dataType == IP) {
                if (lower instanceof BytesRef bytesRef) {
                    lower = ipToString(bytesRef);
                }
                if (upper instanceof BytesRef bytesRef) {
                    upper = ipToString(bytesRef);
                }
            } else if (dataType == VERSION) {
                // VersionStringFieldMapper#indexedValueForSearch() only accepts as input String or BytesRef with the String (i.e. not
                // encoded) representation of the version as it'll do the encoding itself.
                if (lower instanceof BytesRef bytesRef) {
                    lower = versionToString(bytesRef);
                } else if (lower instanceof Version version) {
                    lower = versionToString(version);
                }
                if (upper instanceof BytesRef bytesRef) {
                    upper = versionToString(bytesRef);
                } else if (upper instanceof Version version) {
                    upper = versionToString(version);
                }
            } else if (dataType == UNSIGNED_LONG) {
                if (lower instanceof Long ul) {
                    lower = unsignedLongAsNumber(ul);
                }
                if (upper instanceof Long ul) {
                    upper = unsignedLongAsNumber(ul);
                }
            }
            return new RangeQuery(
                r.source(),
                handler.nameOf(r.value()),
                lower,
                r.includeLower(),
                upper,
                r.includeUpper(),
                format,
                r.zoneId()
            );
        }
    }

    public static class MatchFunctionTranslator extends ExpressionTranslator<Match> {
        @Override
        protected Query asQuery(Match match, TranslatorHandler handler) {
            return new MatchQuery(match.source(), ((FieldAttribute) match.field()).name(), match.queryAsText());
        }
    }

    public static class QueryStringFunctionTranslator extends ExpressionTranslator<QueryString> {
        @Override
        protected Query asQuery(QueryString queryString, TranslatorHandler handler) {
            return new QueryStringQuery(queryString.source(), queryString.queryAsText(), Map.of(), null);
        }
    }
}

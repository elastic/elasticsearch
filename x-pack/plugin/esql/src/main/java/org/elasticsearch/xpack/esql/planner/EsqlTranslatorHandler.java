/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.MetadataAttribute;
import org.elasticsearch.xpack.ql.expression.TypedAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.planner.QlTranslatorHandler;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Check;
import org.elasticsearch.xpack.versionfield.Version;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

public final class EsqlTranslatorHandler extends QlTranslatorHandler {

    public static final List<ExpressionTranslator<?>> QUERY_TRANSLATORS = List.of(
        new EqualsIgnoreCaseTranslator(),
        new ExpressionTranslators.BinaryComparisons(),
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
        new ExpressionTranslators.Scalars()
    );

    @Override
    public Query asQuery(Expression e) {
        Query translation = null;
        for (ExpressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, this);
            if (translation != null) {
                return translation;
            }
        }

        throw new QlIllegalArgumentException("Don't know how to translate {} {}", e.nodeName(), e);
    }

    @Override
    public Object convert(Object value, DataType dataType) {
        return EsqlDataTypeConverter.convert(value, dataType);
    }

    @Override
    public Query wrapFunctionQuery(ScalarFunction sf, Expression field, Supplier<Query> querySupplier) {
        if (field instanceof FieldAttribute fa) {
            if (fa.getExactInfo().hasExact()) {
                var exact = fa.exactAttribute();
                if (exact != fa) {
                    fa = exact;
                }
            }
            // don't wrap is null/is not null with SVQ
            Query query = querySupplier.get();
            if ((sf instanceof IsNull || sf instanceof IsNotNull) == false) {
                query = new SingleValueQuery(query, fa.name());
            }
            return ExpressionTranslator.wrapIfNested(query, field);
        }
        if (field instanceof MetadataAttribute) {
            return querySupplier.get(); // MetadataAttributes are always single valued
        }
        throw new EsqlIllegalArgumentException("Expected a FieldAttribute or MetadataAttribute but received [" + field + "]");
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
            Object value = ExpressionTranslators.valueOf(bc.right());
            String format = null;
            boolean isDateLiteralComparison = false;

            // for a date constant comparison, we need to use a format for the date, to make sure that the format is the same
            // no matter the timezone provided by the user
            if (value instanceof ZonedDateTime || value instanceof OffsetTime) {
                DateFormatter formatter;
                if (value instanceof ZonedDateTime) {
                    formatter = DateFormatter.forPattern(ExpressionTranslators.DATE_FORMAT);
                    // RangeQueryBuilder accepts an Object as its parameter, but it will call .toString() on the ZonedDateTime instance
                    // which can have a slightly different format depending on the ZoneId used to create the ZonedDateTime
                    // Since RangeQueryBuilder can handle date as String as well, we'll format it as String and provide the format as well.
                    value = formatter.format((ZonedDateTime) value);
                } else {
                    formatter = DateFormatter.forPattern(ExpressionTranslators.TIME_FORMAT);
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

            String name = pushableAttributeName(attribute);

            Query query;
            if (isDateLiteralComparison) {
                // dates equality uses a range query because it's the one that has a "format" parameter
                query = new RangeQuery(source, name, value, true, value, true, format, zoneId);
            } else {
                query = new TermQuery(source, name, value, DataTypes.isString(attribute.dataType()));
            }
            return query;

        }
    }
}

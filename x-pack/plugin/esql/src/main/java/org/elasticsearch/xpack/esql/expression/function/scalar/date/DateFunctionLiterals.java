/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateNanosToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * Literal inspection for listing-time date folds and post-analysis comparison invert.
 * Listing: ImplicitCasting has not run, so KEYWORD / TEXT ISO strings are parsed here;
 * never calls {@code dataType()} on unresolved children. Invert: foldable right-hand
 * sides after analysis, typed as the field ({@code DATETIME} vs {@code DATE_NANOS}).
 */
final class DateFunctionLiterals {

    private DateFunctionLiterals() {}

    static Literal[] literalArgs(List<Expression> args, int arity) {
        if (args.size() != arity) {
            return null;
        }
        Literal[] literals = new Literal[arity];
        for (int i = 0; i < arity; i++) {
            Expression arg = args.get(i);
            if (arg instanceof Literal literal) {
                literals[i] = literal;
            } else {
                return null;
            }
        }
        return literals;
    }

    static ZoneId zoneId(Configuration config) {
        return QuerySettings.TIME_ZONE.get(config.resolvedSettings());
    }

    static ParsedDate parseDateLiteral(Literal literal, ZoneId zone) {
        Object value = literal.value();
        if (value == null) {
            return null;
        }
        DataType type = literal.dataType();
        if (type == DataType.DATETIME) {
            return value instanceof Number number ? new ParsedDate(number.longValue(), false) : null;
        }
        if (type == DataType.DATE_NANOS) {
            return value instanceof Number number ? new ParsedDate(number.longValue(), true) : null;
        }
        if (DataType.isString(type)) {
            long millis = dateTimeToLong(BytesRefs.toString(value), DEFAULT_DATE_TIME_FORMATTER.withZone(zone));
            return new ParsedDate(millis, false);
        }
        return null;
    }

    record ParsedDate(long epoch, boolean nanos) {}

    static Expression datetimeField(Expression field) {
        if (field instanceof TypedAttribute attr && attr.dataType().isDate()) {
            return attr;
        }
        return null;
    }

    /**
     * Fold a comparison literal to the field's epoch units. DATE_NANOS on a DATETIME field
     * is refused: flooring leftover nanos would lie about DATE_TRUNC alignment.
     */
    static Long foldDateToFieldEpoch(Expression expr, DataType fieldType, ZoneId zone, FoldContext ctx) {
        if (expr.foldable() == false) {
            return null;
        }
        Object value = expr.fold(ctx);
        if (value == null) {
            return null;
        }
        DataType type = expr.dataType();
        if (type == DataType.DATETIME && value instanceof Number number) {
            long millis = number.longValue();
            return fieldType == DataType.DATE_NANOS ? DateUtils.toNanoSeconds(millis) : millis;
        }
        if (type == DataType.DATE_NANOS && value instanceof Number number) {
            if (fieldType != DataType.DATE_NANOS) {
                return null;
            }
            return number.longValue();
        }
        if (DataType.isString(type)) {
            String text = BytesRefs.toString(value);
            if (fieldType == DataType.DATE_NANOS) {
                return dateNanosToLong(text, DEFAULT_DATE_NANOS_FORMATTER.withZone(zone));
            }
            return dateTimeToLong(text, DEFAULT_DATE_TIME_FORMATTER.withZone(zone));
        }
        return null;
    }

    static Long foldIntegralNumber(Expression expr, FoldContext ctx) {
        if (expr.foldable() == false) {
            return null;
        }
        Object value = expr.fold(ctx);
        // Integral boxes only. doubleValue() == longValue() lies for |n| > 2^53.
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        return null;
    }

    static long toMillis(long epoch, DataType fieldType) {
        return fieldType == DataType.DATE_NANOS ? DateUtils.toMilliSeconds(epoch) : epoch;
    }

    static long toFieldEpoch(long millis, DataType fieldType) {
        return fieldType == DataType.DATE_NANOS ? DateUtils.toNanoSeconds(millis) : millis;
    }

    static long toFieldEpoch(Instant instant, DataType fieldType) {
        return fieldType == DataType.DATE_NANOS ? DateUtils.toLong(instant) : instant.toEpochMilli();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneId;
import java.util.List;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * Literal inspection for listing-time date folds. ImplicitCasting has not run, so KEYWORD /
 * TEXT ISO strings are parsed here. Never calls {@code dataType()} on unresolved children.
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
}

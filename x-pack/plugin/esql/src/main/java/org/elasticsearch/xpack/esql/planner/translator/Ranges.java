/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

import static org.elasticsearch.xpack.esql.core.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

public class Ranges extends ExpressionTranslator<Range> {

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
        return new RangeQuery(r.source(), handler.nameOf(r.value()), lower, r.includeLower(), upper, r.includeUpper(), format, r.zoneId());
    }
}

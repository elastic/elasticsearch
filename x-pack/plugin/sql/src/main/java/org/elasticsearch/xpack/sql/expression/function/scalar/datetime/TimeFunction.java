/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDateOrTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public abstract class TimeFunction extends DateTimeFunction {

    TimeFunction(Source source, Expression field, ZoneId zoneId, DateTimeExtractor extractor) {
        super(source, field, zoneId, extractor);
    }

    public static Integer dateTimeChrono(OffsetTime time, String tzId, String chronoName) {
        return dateTimeChrono(asTimeAtZone(time, ZoneId.of(tzId)), ChronoField.valueOf(chronoName));
    }

    @Override
    protected TypeResolution resolveType() {
        return isDateOrTime(field(), sourceText(), Expressions.ParamOrdinal.DEFAULT);
    }

    @Override
    protected Processor makeProcessor() {
        return new TimeProcessor(extractor(), zoneId());
    }
}

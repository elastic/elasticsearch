/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDateOrTime;

public abstract class TimeFunction extends DateTimeFunction {

    TimeFunction(Source source, Expression field, ZoneId zoneId, DateTimeExtractor extractor) {
        super(source, field, zoneId, extractor);
    }

    @Override
    protected TypeResolution resolveType() {
        return isDateOrTime(field(), sourceText(), DEFAULT);
    }

    @Override
    protected Processor makeProcessor() {
        return new TimeProcessor(extractor(), zoneId());
    }
}

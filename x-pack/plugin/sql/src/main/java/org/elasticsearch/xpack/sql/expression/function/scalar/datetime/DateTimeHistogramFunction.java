/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.ZoneId;

/**
 * DateTimeFunctions that can be mapped as histogram. This means the dates order is maintained
 * Unfortunately this means only YEAR works since everything else changes the order
 */
public abstract class DateTimeHistogramFunction extends DateTimeFunction {

    DateTimeHistogramFunction(Source source, Expression field, ZoneId zoneId, DateTimeExtractor extractor) {
        super(source, field, zoneId, extractor);
    }

    /**
     * used for aggregration (date histogram)
     */
    public abstract long interval();
}

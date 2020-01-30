/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.ZoneId;

/**
 * Extract the year from a datetime.
 */
public class Year extends DateTimeHistogramFunction {
    
    public static String YEAR_INTERVAL = DateHistogramInterval.YEAR.toString();

    public Year(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.YEAR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return Year::new;
    }

    @Override
    protected Year replaceChild(Expression newChild) {
        return new Year(source(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "year";
    }

    @Override
    public String calendarInterval() {
        return YEAR_INTERVAL;
    }
}

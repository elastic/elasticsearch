/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.xpack.sql.type.DataType;

import java.time.Duration;

/**
 * Day/Hour/Minutes/Seconds (exact) interval.
 */
public class IntervalDayTime extends Interval<Duration> {

    public IntervalDayTime(Duration interval, DataType intervalType) {
        super(interval, intervalType);
    }
}

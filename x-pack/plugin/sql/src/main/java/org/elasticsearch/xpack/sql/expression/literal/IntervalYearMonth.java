/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.literal;

import org.elasticsearch.xpack.sql.type.DataType;

import java.time.Period;

/**
 * Year/Month (relative) interval.
 */
public class IntervalYearMonth extends Interval<Period> {

    public IntervalYearMonth(Period interval, DataType intervalType) {
        super(interval, intervalType);
    }
}

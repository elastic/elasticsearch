/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.joda.time.PeriodType;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class PeriodThrottler implements Throttler {

    private final TimeValue period;
    private final PeriodType periodType;

    public PeriodThrottler(TimeValue period) {
        this(period, PeriodType.minutes());
    }

    public PeriodThrottler(TimeValue period, PeriodType periodType) {
        this.period = period;
        this.periodType = periodType;
    }

    public TimeValue interval() {
        return period;
    }

    @Override
    public Result throttle(Alert alert, Trigger.Result result) {
        Alert.Status status = alert.status();
        TimeValue timeElapsed = new TimeValue(System.currentTimeMillis() - status.lastRan().getMillis());
        if (timeElapsed.getMillis() <= period.getMillis()) {
            return Result.throttle("throttling interval is set to [" + period.format(periodType) +
                    "] but time elapsed since last execution is [" + timeElapsed.format(periodType) + "]");
        }
        return Result.NO;
    }
}

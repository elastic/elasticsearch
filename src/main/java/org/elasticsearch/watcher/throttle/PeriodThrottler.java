/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.throttle;

import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.common.joda.time.PeriodType;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class PeriodThrottler implements Throttler {

    private final TimeValue period;
    private final PeriodType periodType;
    private final Clock clock;

    public PeriodThrottler(Clock clock, TimeValue period) {
        this(clock, period, PeriodType.minutes());
    }

    public PeriodThrottler(Clock clock, TimeValue period, PeriodType periodType) {
        this.period = period;
        this.periodType = periodType;
        this.clock = clock;
    }

    public TimeValue interval() {
        return period;
    }

    @Override
    public Result throttle(WatchExecutionContext ctx) {
        Watch.Status status = ctx.watch().status();
        if (status.lastExecuted() != null) {
            TimeValue timeElapsed = clock.timeElapsedSince(status.lastExecuted());
            if (timeElapsed.getMillis() <= period.getMillis()) {
                return Result.throttle("throttling interval is set to [" + period.format(periodType) +
                        "] but time elapsed since last execution is [" + timeElapsed.format(periodType) + "]");
            }
        }
        return Result.NO;
    }
}

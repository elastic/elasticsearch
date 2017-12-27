/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.throttler;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.joda.time.PeriodType;

import java.time.Clock;

import static org.elasticsearch.xpack.watcher.actions.throttler.Throttler.Type.PERIOD;

/**
 * This throttler throttles the action based on its last <b>successful</b> execution time. If the time passed since
 * the last successful execution is lower than the given period, the aciton will be throttled.
 */
public class PeriodThrottler implements Throttler {

    @Nullable private final TimeValue period;
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

    public TimeValue period() {
        return period;
    }

    @Override
    public Result throttle(String actionId, WatchExecutionContext ctx) {
        TimeValue period = this.period;
        if (period == null) {
            // falling back on the throttle period of the watch
            period = ctx.watch().throttlePeriod();
        }
        if (period == null) {
            // falling back on the default throttle period of watcher
            period = ctx.defaultThrottlePeriod();
        }
        ActionStatus status = ctx.watch().status().actionStatus(actionId);
        if (status.lastSuccessfulExecution() == null) {
            return Result.NO;
        }
        TimeValue timeElapsed = TimeValue.timeValueMillis(clock.millis() - status.lastSuccessfulExecution().timestamp().getMillis());
        if (timeElapsed.getMillis() <= period.getMillis()) {
            return Result.throttle(PERIOD, "throttling interval is set to [{}] but time elapsed since last execution is [{}]",
                    period.format(periodType), timeElapsed.format(periodType));
        }
        return Result.NO;
    }
}

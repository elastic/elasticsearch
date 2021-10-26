/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions.throttler;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;

import java.time.Clock;

import static org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler.Type.PERIOD;

/**
 * This throttler throttles the action based on its last <b>successful</b> execution time. If the time passed since
 * the last successful execution is lower than the given period, the aciton will be throttled.
 */
public class PeriodThrottler implements Throttler {

    @Nullable private final TimeValue period;
    private final Clock clock;

    public PeriodThrottler(Clock clock, TimeValue period) {
        this.period = period;
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
        long now = clock.millis();
        long executionTime = status.lastSuccessfulExecution().timestamp().toInstant().toEpochMilli();
        TimeValue timeElapsed = TimeValue.timeValueMillis(now - executionTime);
        if (timeElapsed.getMillis() <= period.getMillis()) {
            return Result.throttle(PERIOD, "throttling interval is set to [{}] but time elapsed since last execution is [{}]",
                    period, timeElapsed);
        }
        return Result.NO;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.throttler;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.WatcherLicensee;
import org.elasticsearch.watcher.support.clock.Clock;

/**
 *
 */
public class ActionThrottler implements Throttler {

    private static final AckThrottler ACK_THROTTLER = new AckThrottler();

    private final WatcherLicensee watcherLicensee;
    private final PeriodThrottler periodThrottler;
    private final AckThrottler ackThrottler;

    public ActionThrottler(Clock clock, @Nullable TimeValue throttlePeriod, WatcherLicensee watcherLicensee) {
        this(new PeriodThrottler(clock, throttlePeriod), ACK_THROTTLER, watcherLicensee);
    }

    ActionThrottler(PeriodThrottler periodThrottler, AckThrottler ackThrottler, WatcherLicensee watcherLicensee) {
        this.periodThrottler = periodThrottler;
        this.ackThrottler = ackThrottler;
        this.watcherLicensee = watcherLicensee;
    }

    public TimeValue throttlePeriod() {
        return periodThrottler != null ? periodThrottler.period() : null;
    }

    @Override
    public Result throttle(String actionId, WatchExecutionContext ctx) {
        if (!watcherLicensee.isExecutingActionsAllowed()) {
            return Result.throttle("watcher license does not allow action execution");
        }
        if (periodThrottler != null) {
            Result throttleResult = periodThrottler.throttle(actionId, ctx);
            if (throttleResult.throttle()) {
                return throttleResult;
            }
        }
        return ackThrottler.throttle(actionId, ctx);
    }
}

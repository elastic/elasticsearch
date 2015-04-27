/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.throttler;

import org.elasticsearch.watcher.actions.throttler.AckThrottler;
import org.elasticsearch.watcher.actions.throttler.PeriodThrottler;
import org.elasticsearch.watcher.actions.throttler.Throttler;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class ActionThrottler implements Throttler {

    private static final AckThrottler ACK_THROTTLER = new AckThrottler();

    private final LicenseService licenseService;
    private final PeriodThrottler periodThrottler;
    private final AckThrottler ackThrottler;

    public ActionThrottler(Clock clock, @Nullable TimeValue throttlePeriod, LicenseService licenseService) {
        this(new PeriodThrottler(clock, throttlePeriod), ACK_THROTTLER, licenseService);
    }

    ActionThrottler(PeriodThrottler periodThrottler, AckThrottler ackThrottler, LicenseService licenseService) {
        this.periodThrottler = periodThrottler;
        this.ackThrottler = ackThrottler;
        this.licenseService = licenseService;
    }

    public TimeValue throttlePeriod() {
        return periodThrottler != null ? periodThrottler.period() : null;
    }

    @Override
    public Result throttle(String actionId, WatchExecutionContext ctx) {
        if (!licenseService.enabled()) {
            return Result.throttle("watcher license expired");
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

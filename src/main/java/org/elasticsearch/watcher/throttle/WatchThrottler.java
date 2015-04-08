/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.throttle;

import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class WatchThrottler implements Throttler {

    private static final AckThrottler ACK_THROTTLER = new AckThrottler();

    private final LicenseService licenseService;
    private final PeriodThrottler periodThrottler;
    private final AckThrottler ackThrottler;

    public WatchThrottler(Clock clock, @Nullable TimeValue throttlePeriod, LicenseService licenseService) {
        this(throttlePeriod != null ? new PeriodThrottler(clock, throttlePeriod) : null, ACK_THROTTLER, licenseService);
    }

    WatchThrottler(PeriodThrottler periodThrottler, AckThrottler ackThrottler, LicenseService licenseService) {
        this.periodThrottler = periodThrottler;
        this.ackThrottler = ackThrottler;
        this.licenseService = licenseService;
    }

    @Override
    public Result throttle(WatchExecutionContext ctx) {
        if (!licenseService.enabled()) {
            return Result.throttle("watcher license expired");
        }
        if (periodThrottler != null) {
            Result throttleResult = periodThrottler.throttle(ctx);
            if (throttleResult.throttle()) {
                return throttleResult;
            }
        }
        return ackThrottler.throttle(ctx);
    }
}

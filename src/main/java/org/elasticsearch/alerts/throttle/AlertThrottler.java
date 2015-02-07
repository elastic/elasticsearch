/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.AlertContext;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class AlertThrottler implements Throttler {

    private static final AckThrottler ACK_THROTTLER = new AckThrottler();

    private final PeriodThrottler periodThrottler;

    public AlertThrottler(@Nullable TimeValue throttlePeriod) {
        this.periodThrottler = throttlePeriod != null ? new PeriodThrottler(throttlePeriod) : null;
    }

    @Override
    public Result throttle(AlertContext ctx, Trigger.Result result) {
        if (periodThrottler != null) {
            Result throttleResult = periodThrottler.throttle(ctx, result);
            if (throttleResult.throttle()) {
                return throttleResult;
            }
        }
        return ACK_THROTTLER.throttle(ctx, result);
    }
}

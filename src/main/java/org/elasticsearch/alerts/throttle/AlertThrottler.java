/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public class AlertThrottler implements Throttler {

    private static final AckThrottler ACK_THROTTLER = new AckThrottler();

    private final PeriodThrottler periodThrottler;
    private final AckThrottler ackThrottler;

    public AlertThrottler(@Nullable TimeValue throttlePeriod) {
        this(throttlePeriod != null ? new PeriodThrottler(throttlePeriod) : null, ACK_THROTTLER);
    }

    AlertThrottler(PeriodThrottler periodThrottler, AckThrottler ackThrottler) {
        this.periodThrottler = periodThrottler;
        this.ackThrottler = ackThrottler;
    }

    @Override
    public Result throttle(ExecutionContext ctx) {
        if (periodThrottler != null) {
            Result throttleResult = periodThrottler.throttle(ctx);
            if (throttleResult.throttle()) {
                return throttleResult;
            }
        }
        return ackThrottler.throttle(ctx);
    }
}

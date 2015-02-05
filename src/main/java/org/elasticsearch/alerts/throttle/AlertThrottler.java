/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.trigger.Trigger;

/**
 *
 */
public class AlertThrottler implements Throttler {

    private final PeriodThrottler periodThrottler;
    private final AckThrottler ackThrottler;

    public AlertThrottler(PeriodThrottler periodThrottler, AckThrottler ackThrottler) {
        this.periodThrottler = periodThrottler;
        this.ackThrottler = ackThrottler;
    }

    @Override
    public Result throttle(Alert alert, Trigger.Result result) {
        Result throttleResult = Result.NO;
        if (periodThrottler != null) {
            throttleResult = periodThrottler.throttle(alert, result);
            if (throttleResult.throttle()) {
                return throttleResult;
            }
        }
        if (ackThrottler != null) {
            throttleResult = ackThrottler.throttle(alert, result);
            if (throttleResult.throttle()) {
                return throttleResult;
            }
        }
        return throttleResult;
    }
}

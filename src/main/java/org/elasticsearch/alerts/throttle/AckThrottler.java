/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.trigger.Trigger;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;

/**
 *
 */
public class AckThrottler implements Throttler {



    @Override
    public Result throttle(Alert alert, Trigger.Result result) {
        if (alert.status().acked()) {
            return Result.throttle("alert [" + alert.name() + "] was acked at [" + formatDate(alert.status().ack().timestamp()) + "]");
        }
        return Result.NO;
    }
}

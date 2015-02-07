/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.AlertContext;
import org.elasticsearch.alerts.trigger.Trigger;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;

/**
 *
 */
public class AckThrottler implements Throttler {

    @Override
    public Result throttle(AlertContext ctx, Trigger.Result result) {
        if (ctx.alert().status().acked()) {
            return Result.throttle("alert [" + ctx.alert().name() + "] was acked at [" + formatDate(ctx.alert().status().ack().timestamp()) + "]");
        }
        return Result.NO;
    }
}

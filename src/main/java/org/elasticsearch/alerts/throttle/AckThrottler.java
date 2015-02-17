/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.condition.Condition;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;

/**
 *
 */
public class AckThrottler implements Throttler {

    @Override
    public Result throttle(ExecutionContext ctx, Condition.Result result) {
        if (ctx.alert().acked()) {
            return Result.throttle("alert [" + ctx.alert().name() + "] was acked at [" + formatDate(ctx.alert().status().ackStatus().timestamp()) + "]");
        }
        return Result.NO;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.throttle;

import org.elasticsearch.watcher.execution.WatchExecutionContext;

import static org.elasticsearch.watcher.support.WatcherDateUtils.formatDate;

/**
 *
 */
public class AckThrottler implements Throttler {

    @Override
    public Result throttle(WatchExecutionContext ctx) {
        if (ctx.watch().acked()) {
            return Result.throttle("watch [" + ctx.watch().id() + "] was acked at [" + formatDate(ctx.watch().status().ackStatus().timestamp()) + "]");
        }
        return Result.NO;
    }
}

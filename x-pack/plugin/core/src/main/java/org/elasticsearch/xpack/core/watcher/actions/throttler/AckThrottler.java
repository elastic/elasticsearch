/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.actions.throttler;

import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus.AckStatus;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;

import static org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler.Type.ACK;
import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.formatDate;

public class AckThrottler implements Throttler {

    @Override
    public Result throttle(String actionId, WatchExecutionContext ctx) {
        ActionStatus actionStatus = ctx.watch().status().actionStatus(actionId);
        AckStatus ackStatus = actionStatus.ackStatus();
        if (ackStatus.state() == AckStatus.State.ACKED) {
            return Result.throttle(ACK, "action [{}] was acked at [{}]", actionId, formatDate(ackStatus.timestamp()));
        }
        return Result.NO;
    }
}

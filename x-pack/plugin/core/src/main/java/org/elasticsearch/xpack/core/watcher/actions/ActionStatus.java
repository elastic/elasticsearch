/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.protocol.xpack.watcher.status.ActionAckStatus;
import org.elasticsearch.protocol.xpack.watcher.status.ActionStatusExecution;
import org.elasticsearch.protocol.xpack.watcher.status.ActionStatusThrottle;
import org.joda.time.DateTime;

import java.util.Objects;

public class ActionStatus {

    private ActionAckStatus ackStatus;
    @Nullable private ActionStatusExecution lastExecution;
    @Nullable private ActionStatusExecution lastSuccessfulExecution;
    @Nullable private ActionStatusThrottle lastThrottle;

    public ActionStatus(DateTime now) {
        this(new ActionAckStatus(now, ActionAckStatus.State.AWAITS_SUCCESSFUL_EXECUTION),
                null, null, null);
    }

    public ActionStatus(ActionAckStatus ackStatus, @Nullable ActionStatusExecution lastExecution,
                        @Nullable ActionStatusExecution lastSuccessfulExecution, @Nullable ActionStatusThrottle lastThrottle) {
        this.ackStatus = ackStatus;
        this.lastExecution = lastExecution;
        this.lastSuccessfulExecution = lastSuccessfulExecution;
        this.lastThrottle = lastThrottle;
    }

    public ActionStatus(org.elasticsearch.protocol.xpack.watcher.status.ActionStatus protocolStatus) {
        this.ackStatus = protocolStatus.ackStatus();
        this.lastExecution = protocolStatus.lastExecution();
        this.lastSuccessfulExecution = protocolStatus.lastSuccessfulExecution();
        this.lastThrottle = protocolStatus.lastThrottle();
    }

    public ActionAckStatus ackStatus() {
        return ackStatus;
    }

    public ActionStatusExecution lastExecution() {
        return lastExecution;
    }

    public ActionStatusExecution lastSuccessfulExecution() {
        return lastSuccessfulExecution;
    }

    public ActionStatusThrottle lastThrottle() {
        return lastThrottle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionStatus that = (ActionStatus) o;

        return Objects.equals(ackStatus, that.ackStatus) &&
                Objects.equals(lastExecution, that.lastExecution) &&
                Objects.equals(lastSuccessfulExecution, that.lastSuccessfulExecution) &&
                Objects.equals(lastThrottle, that.lastThrottle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    public void update(DateTime timestamp, Action.Result result) {
        switch (result.status()) {

            case FAILURE:
                String reason = result instanceof Action.Result.Failure ? ((Action.Result.Failure) result).reason() : "";
                lastExecution = ActionStatusExecution.failure(timestamp, reason);
                return;

            case THROTTLED:
                reason = result instanceof Action.Result.Throttled ? ((Action.Result.Throttled) result).reason() : "";
                lastThrottle = new ActionStatusThrottle(timestamp, reason);
                return;

            case SUCCESS:
            case SIMULATED:
                lastExecution = ActionStatusExecution.successful(timestamp);
                lastSuccessfulExecution = lastExecution;
                if (ackStatus.state() == ActionAckStatus.State.AWAITS_SUCCESSFUL_EXECUTION) {
                    ackStatus = new ActionAckStatus(timestamp, ActionAckStatus.State.ACKABLE);
                }
        }
    }

    public boolean onAck(DateTime timestamp) {
        if (ackStatus.state() == ActionAckStatus.State.ACKABLE) {
            ackStatus = new ActionAckStatus(timestamp, ActionAckStatus.State.ACKED);
            return true;
        }
        return false;
    }

    public boolean resetAckStatus(DateTime timestamp) {
        if (ackStatus.state() != ActionAckStatus.State.AWAITS_SUCCESSFUL_EXECUTION) {
            ackStatus = new ActionAckStatus(timestamp, ActionAckStatus.State.AWAITS_SUCCESSFUL_EXECUTION);
            return true;
        }
        return false;
    }

    public org.elasticsearch.protocol.xpack.watcher.status.ActionStatus toProtocolStatus() {
        return new org.elasticsearch.protocol.xpack.watcher.status.ActionStatus(ackStatus, lastExecution, lastSuccessfulExecution,
                lastThrottle);
    }
}

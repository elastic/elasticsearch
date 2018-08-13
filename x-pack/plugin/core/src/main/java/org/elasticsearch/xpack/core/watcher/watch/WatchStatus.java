/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.watch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.protocol.xpack.watcher.status.ActionAckStatus;
import org.elasticsearch.protocol.xpack.watcher.status.ExecutionState;
import org.elasticsearch.protocol.xpack.watcher.status.WatchStatusState;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents the status of the watch.<br>
 * The design decision of Watch API was to store {@link Watch} in Lucene index and do not hide that fact from the users,
 * i.e. some of the clients may search watch index to retrieve watches instead of using Watch API.<br>
 * Initially there was a single {@link WatchStatus} class that implements {@link ToXContentObject} for both creating XContent representation
 * to be stored in Lucene and to be used in REST response (methods such as get, activate/inactivate return the status).
 * This class was also implementing {@link Streamable} to be used in transport requests.
 * With the high-level REST client development, there was a decision to to move all XPack protocol classes to
 * {@link org.elasticsearch.protocol.xpack} package licensed with Apache 2 license.<br>
 * This is the reason why second {@link org.elasticsearch.protocol.xpack.watcher.status.WatchStatus} class has appeared.
 * That class does not contain any business logic - only things related to XContent/binary serialization/deserialization - copied from
 * previous implementation. <br>
 * For interoperability reasons, this class always delegates to protocol class whenever serialization is needed.
 */
public class WatchStatus implements ToXContentObject, Streamable {

    private WatchStatusState state;

    @Nullable private ExecutionState executionState;
    @Nullable private DateTime lastChecked;
    @Nullable private DateTime lastMetCondition;
    @Nullable private long version;
    @Nullable private Map<String, String> headers;
    private Map<String, ActionStatus> actions;

    // for serialization
    private WatchStatus() {
    }

    public WatchStatus(DateTime now, Map<String, ActionStatus> actions) {
        this(-1, new WatchStatusState(true, now), null, null, null, actions, Collections.emptyMap());
    }

    private WatchStatus(long version, WatchStatusState state, ExecutionState executionState, DateTime lastChecked,
                        DateTime lastMetCondition, Map<String, ActionStatus> actions, Map<String, String> headers) {
        this.version = version;
        this.lastChecked = lastChecked;
        this.lastMetCondition = lastMetCondition;
        this.actions = actions;
        this.state = state;
        this.executionState = executionState;
        this.headers = headers;
    }

    public WatchStatus(org.elasticsearch.protocol.xpack.watcher.status.WatchStatus protocolStatus) {
        fromProtocolStatus(protocolStatus);
    }

    private void fromProtocolStatus(org.elasticsearch.protocol.xpack.watcher.status.WatchStatus protocolStatus) {
        this.version = protocolStatus.version();
        this.lastChecked = protocolStatus.lastChecked();
        this.lastMetCondition = protocolStatus.lastMetCondition();
        this.actions = new HashMap<>();
        for (Map.Entry<String, org.elasticsearch.protocol.xpack.watcher.status.ActionStatus> protocolActionStatus:
                protocolStatus.actions().entrySet()) {
            actions.put(protocolActionStatus.getKey(), new ActionStatus(protocolActionStatus.getValue()));
        }
        this.state = protocolStatus.state();
        this.executionState = protocolStatus.executionState();
        this.headers = protocolStatus.headers();
    }

    public WatchStatusState state() {
        return state;
    }

    public boolean checked() {
        return lastChecked != null;
    }

    public DateTime lastChecked() {
        return lastChecked;
    }

    public ActionStatus actionStatus(String actionId) {
        return actions.get(actionId);
    }

    public long version() {
        return version;
    }

    public void version(long version) {
        this.version = version;
    }

    public void setExecutionState(ExecutionState executionState) {
        this.executionState = executionState;
    }

    public ExecutionState getExecutionState() {
        return executionState;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatchStatus that = (WatchStatus) o;

        return Objects.equals(lastChecked, that.lastChecked) &&
                Objects.equals(lastMetCondition, that.lastMetCondition) &&
                Objects.equals(version, that.version) &&
                Objects.equals(executionState, that.executionState) &&
                Objects.equals(actions, that.actions) &&
                Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastChecked, lastMetCondition, actions, version, executionState, headers);
    }

    /**
     * Called whenever an watch is checked, ie. the condition of the watch is evaluated to see if
     * the watch should be executed.
     *
     * @param metCondition  indicates whether the watch's condition was met.
     */
    public void onCheck(boolean metCondition, DateTime timestamp) {
        lastChecked = timestamp;
        if (metCondition) {
            lastMetCondition = timestamp;
        } else {
            for (ActionStatus status : actions.values()) {
                status.resetAckStatus(timestamp);
            }
        }
    }

    public void onActionResult(String actionId, DateTime timestamp, Action.Result result) {
        ActionStatus status = actions.get(actionId);
        status.update(timestamp, result);
    }

    /**
     * Notifies this status that the givne actions were acked. If the current state of one of these actions is
     * {@link ActionAckStatus.State#ACKABLE ACKABLE},
     * then we'll it'll change to {@link ActionAckStatus.State#ACKED ACKED}
     * (when set to {@link ActionAckStatus.State#ACKED ACKED}, the AckThrottler
     * will throttle the execution of the action.
     *
     * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
     */
    boolean onAck(DateTime timestamp, String... actionIds) {
        boolean changed = false;
        boolean containsAll = false;
        for (String actionId : actionIds) {
            if (actionId.equals(WatchField.ALL_ACTIONS_ID)) {
                containsAll = true;
                break;
            }
        }
        if (containsAll) {
            for (ActionStatus status : actions.values()) {
                changed |= status.onAck(timestamp);
            }
            return changed;
        }

        for (String actionId : actionIds) {
            ActionStatus status = actions.get(actionId);
            if (status != null) {
                changed |= status.onAck(timestamp);
            }
        }

        return changed;
    }

    boolean setActive(boolean active, DateTime now) {
        boolean change = this.state.isActive() != active;
        if (change) {
            this.state = new WatchStatusState(active, now);
        }
        return change;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.toProtocolStatus().writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        org.elasticsearch.protocol.xpack.watcher.status.WatchStatus protocolStatus = org.elasticsearch.protocol.xpack.watcher.status
                .WatchStatus.read(in);
        fromProtocolStatus(protocolStatus);
    }

    public static WatchStatus read(StreamInput in) throws IOException {
        WatchStatus status = new WatchStatus();
        status.readFrom(in);
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return this.toProtocolStatus().toXContent(builder, params);
    }

    public static WatchStatus parse(String watchId, WatcherXContentParser parser) throws IOException {
        org.elasticsearch.protocol.xpack.watcher.status.WatchStatus protocolStatus = org.elasticsearch.protocol.xpack.watcher.status
                .WatchStatus
                .parse(watchId, parser);
        WatchStatus status = new WatchStatus(protocolStatus);
        if (status.state == null) {
            status.state = new WatchStatusState(true, parser.getParseDateTime());
        }
        return status;
    }

    private Map<String, org.elasticsearch.protocol.xpack.watcher.status.ActionStatus> protocolActionsStatus(){
        Map<String, org.elasticsearch.protocol.xpack.watcher.status.ActionStatus> protocolMap =new HashMap<>();
        for (Map.Entry<String, ActionStatus> coreStatusEntry: actions.entrySet()){
            protocolMap.put(coreStatusEntry.getKey(), coreStatusEntry.getValue().toProtocolStatus());
        }
        return protocolMap;
    }

    public org.elasticsearch.protocol.xpack.watcher.status.WatchStatus toProtocolStatus() {
        return new org.elasticsearch.protocol.xpack.watcher.status.WatchStatus(version, state, executionState, lastChecked,
                lastMetCondition, protocolActionsStatus(), headers);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.watch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.parseDate;
import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.readOptionalDate;
import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.writeDate;
import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.writeOptionalDate;

public class WatchStatus implements ToXContentObject, Writeable {

    public static final String INCLUDE_STATE = "include_state";

    private State state;

    @Nullable
    private ExecutionState executionState;
    @Nullable
    private ZonedDateTime lastChecked;
    @Nullable
    private ZonedDateTime lastMetCondition;
    @Nullable
    private long version;
    @Nullable
    private Map<String, String> headers;
    private Map<String, ActionStatus> actions;

    public WatchStatus(StreamInput in) throws IOException {
        version = in.readLong();
        lastChecked = readOptionalDate(in);
        lastMetCondition = readOptionalDate(in);
        int count = in.readInt();
        Map<String, ActionStatus> actions = Maps.newMapWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            actions.put(in.readString(), ActionStatus.readFrom(in));
        }
        this.actions = unmodifiableMap(actions);
        state = new State(in.readBoolean(), Instant.ofEpochMilli(in.readLong()).atZone(ZoneOffset.UTC));
        boolean executionStateExists = in.readBoolean();
        if (executionStateExists) {
            executionState = ExecutionState.resolve(in.readString());
        }
        if (in.readBoolean()) {
            headers = in.readMap(StreamInput::readString);
        } else {
            headers = Collections.emptyMap();
        }
    }

    public WatchStatus(ZonedDateTime now, Map<String, ActionStatus> actions) {
        this(-1, new State(true, now), null, null, null, actions, Collections.emptyMap());
    }

    public WatchStatus(
        long version,
        State state,
        ExecutionState executionState,
        ZonedDateTime lastChecked,
        ZonedDateTime lastMetCondition,
        Map<String, ActionStatus> actions,
        Map<String, String> headers
    ) {
        this.version = version;
        this.lastChecked = lastChecked;
        this.lastMetCondition = lastMetCondition;
        this.actions = actions;
        this.state = state;
        this.executionState = executionState;
        this.headers = headers;
    }

    public State state() {
        return state;
    }

    public boolean checked() {
        return lastChecked != null;
    }

    public ZonedDateTime lastChecked() {
        return lastChecked;
    }

    public ZonedDateTime lastMetCondition() {
        return lastMetCondition;
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

        return Objects.equals(lastChecked, that.lastChecked)
            && Objects.equals(lastMetCondition, that.lastMetCondition)
            && Objects.equals(version, that.version)
            && Objects.equals(executionState, that.executionState)
            && Objects.equals(actions, that.actions)
            && Objects.equals(headers, that.headers);
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
    public void onCheck(boolean metCondition, ZonedDateTime timestamp) {
        lastChecked = timestamp;
        if (metCondition) {
            lastMetCondition = timestamp;
        } else {
            for (ActionStatus status : actions.values()) {
                status.resetAckStatus(timestamp);
            }
        }
    }

    public void onActionResult(String actionId, ZonedDateTime timestamp, Action.Result result) {
        ActionStatus status = actions.get(actionId);
        status.update(timestamp, result);
    }

    /**
     * Notifies this status that the givne actions were acked. If the current state of one of these actions is
     * {@link ActionStatus.AckStatus.State#ACKABLE ACKABLE},
     * then we'll it'll change to {@link ActionStatus.AckStatus.State#ACKED ACKED}
     * (when set to {@link ActionStatus.AckStatus.State#ACKED ACKED}, the AckThrottler
     * will throttle the execution of the action.
     *
     * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
     */
    boolean onAck(ZonedDateTime timestamp, String... actionIds) {
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

    boolean setActive(boolean active, ZonedDateTime now) {
        boolean change = this.state.active != active;
        if (change) {
            this.state = new State(active, now);
        }
        return change;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        writeOptionalDate(out, lastChecked);
        writeOptionalDate(out, lastMetCondition);
        out.writeInt(actions.size());
        for (Map.Entry<String, ActionStatus> entry : actions.entrySet()) {
            out.writeString(entry.getKey());
            ActionStatus.writeTo(entry.getValue(), out);
        }
        out.writeBoolean(state.active);
        writeDate(out, state.timestamp);
        out.writeBoolean(executionState != null);
        if (executionState != null) {
            out.writeString(executionState.id());
        }
        boolean statusHasHeaders = headers != null && headers.isEmpty() == false;
        out.writeBoolean(statusHasHeaders);
        if (statusHasHeaders) {
            out.writeMap(headers, StreamOutput::writeString);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(INCLUDE_STATE, true)) {
            builder.field(Field.STATE.getPreferredName(), state, params);
        }
        if (lastChecked != null) {
            writeDate(Field.LAST_CHECKED.getPreferredName(), builder, lastChecked);
        }
        if (lastMetCondition != null) {
            writeDate(Field.LAST_MET_CONDITION.getPreferredName(), builder, lastMetCondition);
        }
        if (actions != null) {
            builder.startObject(Field.ACTIONS.getPreferredName());
            for (Map.Entry<String, ActionStatus> entry : actions.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        if (executionState != null) {
            builder.field(Field.EXECUTION_STATE.getPreferredName(), executionState.id());
        }
        if (headers != null && headers.isEmpty() == false && WatcherParams.hideHeaders(params) == false) {
            builder.field(Field.HEADERS.getPreferredName(), headers);
        }
        builder.field(Field.VERSION.getPreferredName(), version);
        return builder.endObject();
    }

    public static WatchStatus parse(String watchId, WatcherXContentParser parser) throws IOException {
        State state = null;
        ExecutionState executionState = null;
        ZonedDateTime lastChecked = null;
        ZonedDateTime lastMetCondition = null;
        Map<String, ActionStatus> actions = null;
        long version = -1;
        Map<String, String> headers = Collections.emptyMap();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    state = State.parse(parser);
                } catch (ElasticsearchParseException e) {
                    throw new ElasticsearchParseException(
                        "could not parse watch status for [{}]. failed to parse field [{}]",
                        e,
                        watchId,
                        currentFieldName
                    );
                }
            } else if (Field.VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    version = parser.longValue();
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse watch status for [{}]. expecting field [{}] to hold a long " + "value, found [{}] instead",
                        watchId,
                        currentFieldName,
                        token
                    );
                }
            } else if (Field.LAST_CHECKED.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    lastChecked = parseDate(currentFieldName, parser, ZoneOffset.UTC);
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse watch status for [{}]. expecting field [{}] to hold a date " + "value, found [{}] instead",
                        watchId,
                        currentFieldName,
                        token
                    );
                }
            } else if (Field.LAST_MET_CONDITION.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    lastMetCondition = parseDate(currentFieldName, parser, ZoneOffset.UTC);
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse watch status for [{}]. expecting field [{}] to hold a date " + "value, found [{}] instead",
                        watchId,
                        currentFieldName,
                        token
                    );
                }
            } else if (Field.EXECUTION_STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    executionState = ExecutionState.resolve(parser.text());
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse watch status for [{}]. expecting field [{}] to hold a string " + "value, found [{}] instead",
                        watchId,
                        currentFieldName,
                        token
                    );
                }
            } else if (Field.ACTIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                actions = new HashMap<>();
                if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            ActionStatus actionStatus = ActionStatus.parse(watchId, currentFieldName, parser);
                            actions.put(currentFieldName, actionStatus);
                        }
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse watch status for [{}]. expecting field [{}] to be an object, " + "found [{}] instead",
                        watchId,
                        currentFieldName,
                        token
                    );
                }
            } else if (Field.HEADERS.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    headers = parser.mapStrings();
                }
            } else {
                parser.skipChildren();
            }
        }

        // if the watch status doesn't have a state, we assume active
        // this is to support old watches that weren't upgraded yet to
        // contain the state
        if (state == null) {
            state = new State(true, parser.getParseDateTime());
        }
        actions = actions == null ? emptyMap() : unmodifiableMap(actions);

        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actions, headers);
    }

    public static class State implements ToXContentObject {

        final boolean active;
        final ZonedDateTime timestamp;

        public State(boolean active, ZonedDateTime timestamp) {
            this.active = active;
            this.timestamp = timestamp;
        }

        public boolean isActive() {
            return active;
        }

        public ZonedDateTime getTimestamp() {
            return timestamp;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.ACTIVE.getPreferredName(), active);
            writeDate(Field.TIMESTAMP.getPreferredName(), builder, timestamp);
            return builder.endObject();
        }

        public static State parse(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("expected an object but found [{}] instead", parser.currentToken());
            }
            boolean active = true;
            ZonedDateTime timestamp = ZonedDateTime.now(ZoneOffset.UTC);
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.ACTIVE.match(currentFieldName, parser.getDeprecationHandler())) {
                    active = parser.booleanValue();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = parseDate(currentFieldName, parser, ZoneOffset.UTC);
                } else {
                    parser.skipChildren();
                }
            }
            return new State(active, timestamp);
        }
    }

    public interface Field {
        ParseField STATE = new ParseField("state");
        ParseField ACTIVE = new ParseField("active");
        ParseField TIMESTAMP = new ParseField("timestamp");
        ParseField LAST_CHECKED = new ParseField("last_checked");
        ParseField LAST_MET_CONDITION = new ParseField("last_met_condition");
        ParseField ACTIONS = new ParseField("actions");
        ParseField VERSION = new ParseField("version");
        ParseField EXECUTION_STATE = new ParseField("execution_state");
        ParseField HEADERS = new ParseField("headers");
    }
}

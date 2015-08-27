/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.actions.throttler.AckThrottler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.watcher.support.WatcherDateTimeUtils.*;

/**
 *
 */
public class WatchStatus implements ToXContent, Streamable {

    public static final String INCLUDE_VERSION_KEY = "include_version";

    private transient long version;

    private @Nullable DateTime lastChecked;
    private @Nullable DateTime lastMetCondition;
    private ImmutableMap<String, ActionStatus> actions;

    private volatile boolean dirty = false;

    // for serialization
    private WatchStatus() {
    }

    public WatchStatus(ImmutableMap<String, ActionStatus> actions) {
        this(-1, null, null, actions);
    }

    public WatchStatus(WatchStatus other) {
        this(other.version, other.lastChecked, other.lastMetCondition, other.actions);
    }

    private WatchStatus(long version, DateTime lastChecked, DateTime lastMetCondition, ImmutableMap<String, ActionStatus> actions) {
        this.version = version;
        this.lastChecked = lastChecked;
        this.lastMetCondition = lastMetCondition;
        this.actions = actions;
    }

    public long version() {
        return version;
    }

    public void version(long version) {
        this.version = version;
    }

    public boolean checked() {
        return lastChecked != null;
    }

    public DateTime lastChecked() {
        return lastChecked;
    }

    public boolean metCondition() {
        return lastMetCondition != null;
    }

    public DateTime lastMetCondition() {
        return lastMetCondition;
    }

    public ActionStatus actionStatus(String actionId) {
        return actions.get(actionId);
    }

    /**
     * marks this status as non-dirty. this should only be done when the current state of the status is in sync with
     * the persisted state.
     */
    public void resetDirty() {
        this.dirty = false;
    }

    /**
     * @return does this Watch.Status needs to be persisted to the index
     */
    public boolean dirty() {
        return dirty;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatchStatus that = (WatchStatus) o;

        if (version != that.version) return false;
        if (lastChecked != null ? !lastChecked.equals(that.lastChecked) : that.lastChecked != null) return false;
        if (lastMetCondition != null ? !lastMetCondition.equals(that.lastMetCondition) : that.lastMetCondition != null)
            return false;
        return !(actions != null ? !actions.equals(that.actions) : that.actions != null);
    }

    @Override
    public int hashCode() {
        int result = (int) (version ^ (version >>> 32));
        result = 31 * result + (lastChecked != null ? lastChecked.hashCode() : 0);
        result = 31 * result + (lastMetCondition != null ? lastMetCondition.hashCode() : 0);
        result = 31 * result + (actions != null ? actions.hashCode() : 0);
        return result;
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
            dirty = true;
        } else {
            for (ActionStatus status : actions.values()) {
                status.resetAckStatus(timestamp);
            }
        }
    }

    public void onActionResult(String actionId, DateTime timestamp, Action.Result result) {
        ActionStatus status = actions.get(actionId);
        status.update(timestamp, result);
        dirty = true;
    }

    /**
     * Notifies this status that the givne actions were acked. If the current state of one of these actions is {@link org.elasticsearch.watcher.actions.ActionStatus.AckStatus.State#ACKABLE ACKABLE},
     * then we'll it'll change to {@link org.elasticsearch.watcher.actions.ActionStatus.AckStatus.State#ACKED ACKED}
     * (when set to {@link org.elasticsearch.watcher.actions.ActionStatus.AckStatus.State#ACKED ACKED}, the {@link AckThrottler}
     * will throttle the execution of the action.
     *
     * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
     */
    boolean onAck(DateTime timestamp, String... actionIds) {
        boolean changed = false;
        boolean containsAll = false;
        for (String actionId : actionIds) {
            if (actionId.equals(Watch.ALL_ACTIONS_ID)) {
                containsAll = true;
                break;
            }
        }
        if (containsAll) {
            for (ActionStatus status : actions.values()) {
                changed |= status.onAck(timestamp);
            }
            dirty |= changed;
            return changed;
        }

        for (String actionId : actionIds) {
            ActionStatus status = actions.get(actionId);
            if (status != null) {
                changed |= status.onAck(timestamp);
            }
        }
        dirty |= changed;
        return changed;
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
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        version = in.readLong();
        lastChecked = readOptionalDate(in, DateTimeZone.UTC);
        lastMetCondition = readOptionalDate(in, DateTimeZone.UTC);
        ImmutableMap.Builder<String, ActionStatus> builder = ImmutableMap.builder();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            builder.put(in.readString(), ActionStatus.readFrom(in));
        }
        actions = builder.build();
    }

    public static WatchStatus read(StreamInput in) throws IOException {
        WatchStatus status = new WatchStatus();
        status.readFrom(in);
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(INCLUDE_VERSION_KEY, false)) {
            builder.field(Field.VERSION.getPreferredName(), version);
        }
        if (lastChecked != null) {
            builder.field(Field.LAST_CHECKED.getPreferredName(), lastChecked);
        }
        if (lastMetCondition != null) {
            builder.field(Field.LAST_MET_CONDITION.getPreferredName(), lastMetCondition);
        }
        if (actions != null) {
            builder.startObject(Field.ACTIONS.getPreferredName());
            for (Map.Entry<String, ActionStatus> entry : actions.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    public static WatchStatus parse(String watchId, XContentParser parser) throws IOException {

        DateTime lastChecked = null;
        DateTime lastMetCondition = null;
        ImmutableMap.Builder<String, ActionStatus> actions = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.LAST_CHECKED)) {
                if (token.isValue()) {
                    lastChecked = parseDate(currentFieldName, parser, DateTimeZone.UTC);
                } else {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to hold a date value, found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.LAST_MET_CONDITION)) {
                if (token.isValue()) {
                    lastMetCondition = parseDate(currentFieldName, parser, DateTimeZone.UTC);
                } else {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to hold a date value, found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.ACTIONS)) {
                actions = ImmutableMap.builder();
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
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to be an object, found [{}] instead", watchId, currentFieldName, token);
                }
            }
        }

        return new WatchStatus(-1, lastChecked, lastMetCondition, actions.build());
    }


    interface Field {
        ParseField VERSION = new ParseField("version");
        ParseField LAST_CHECKED = new ParseField("last_checked");
        ParseField LAST_MET_CONDITION = new ParseField("last_met_condition");
        ParseField ACTIONS = new ParseField("actions");
    }

}

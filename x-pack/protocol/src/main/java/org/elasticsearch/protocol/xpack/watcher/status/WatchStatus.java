/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.watcher.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.protocol.xpack.watcher.status.WatchStatusParams.hideHeaders;
import static org.elasticsearch.protocol.xpack.watcher.status.WatchStatusParams.includeState;
import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.parseDate;
import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.readDate;
import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.readOptionalDate;
import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.writeDate;
import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.writeOptionalDate;
import static org.joda.time.DateTimeZone.UTC;

public class WatchStatus implements ToXContentObject, Streamable {
    private WatchStatusState state;
    private Map<String, String> headers;
    private Map<String, ActionStatus> actions;
    private long version;

    @Nullable private ExecutionState executionState;
    @Nullable private DateTime lastChecked;
    @Nullable private DateTime lastMetCondition;

    // for serialization
    private WatchStatus() {
    }

    public WatchStatus(long version, WatchStatusState state, ExecutionState executionState, DateTime lastChecked,
                        DateTime lastMetCondition, Map<String, ActionStatus> actions, Map<String, String> headers) {
        this.version = version;
        this.lastChecked = lastChecked;
        this.lastMetCondition = lastMetCondition;
        this.actions = actions;
        this.state = state;
        this.executionState = executionState;
        this.headers = headers;
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

    public ExecutionState executionState() {
        return executionState;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public DateTime lastMetCondition() {
        return lastMetCondition;
    }

    public Map<String, ActionStatus> actions() {
        return actions;
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
        out.writeBoolean(state.isActive());
        writeDate(out, state.getTimestamp());
        out.writeBoolean(executionState != null);
        if (executionState != null) {
            out.writeString(executionState.id());
        }
        boolean statusHasHeaders = headers != null && headers.isEmpty() == false;
        out.writeBoolean(statusHasHeaders);
        if (statusHasHeaders) {
            out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        version = in.readLong();
        lastChecked = readOptionalDate(in, UTC);
        lastMetCondition = readOptionalDate(in, UTC);
        int count = in.readInt();
        Map<String, ActionStatus> actions = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            actions.put(in.readString(), ActionStatus.readFrom(in));
        }
        this.actions = unmodifiableMap(actions);
        state = new WatchStatusState(in.readBoolean(), readDate(in, UTC));
        boolean executionStateExists = in.readBoolean();
        if (executionStateExists) {
            executionState = ExecutionState.resolve(in.readString());
        }
        if (in.readBoolean()) {
            headers = in.readMap(StreamInput::readString, StreamInput::readString);
        }
    }

    public static WatchStatus read(StreamInput in) throws IOException {
        WatchStatus status = new WatchStatus();
        status.readFrom(in);
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (includeState(params)) {
            builder.field(WatchStatusField.STATE.getPreferredName(), state, params);
        }
        if (lastChecked != null) {
            builder.timeField(WatchStatusField.LAST_CHECKED.getPreferredName(), lastChecked);
        }
        if (lastMetCondition != null) {
            builder.timeField(WatchStatusField.LAST_MET_CONDITION.getPreferredName(), lastMetCondition);
        }
        if (actions != null) {
            builder.startObject(WatchStatusField.ACTIONS.getPreferredName());
            for (Map.Entry<String, ActionStatus> entry : actions.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        if (executionState != null) {
            builder.field(WatchStatusField.EXECUTION_STATE.getPreferredName(), executionState.id());
        }
        if (headers != null && headers.isEmpty() == false && hideHeaders(params) == false) {
            builder.field(WatchStatusField.HEADERS.getPreferredName(), headers);
        }
        builder.field(WatchStatusField.VERSION.getPreferredName(), version);
        return builder.endObject();
    }

    public static WatchStatus parse(String watchId, XContentParser parser) throws IOException {
        WatchStatusState state = null;
        ExecutionState executionState = null;
        DateTime lastChecked = null;
        DateTime lastMetCondition = null;
        Map<String, ActionStatus> actions = null;
        long version = -1;
        Map<String, String> headers = Collections.emptyMap();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (WatchStatusField.STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    state = WatchStatusState.parse(parser);
                } catch (ElasticsearchParseException e) {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. failed to parse field [{}]",
                            e, watchId, currentFieldName);
                }
            } else if (WatchStatusField.VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    version = parser.longValue();
                } else {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to hold a long " +
                            "value, found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (WatchStatusField.LAST_CHECKED.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    lastChecked = parseDate(currentFieldName, parser, UTC);
                } else {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to hold a date " +
                            "value, found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (WatchStatusField.LAST_MET_CONDITION.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    lastMetCondition = parseDate(currentFieldName, parser, UTC);
                } else {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to hold a date " +
                            "value, found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (WatchStatusField.EXECUTION_STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    executionState = ExecutionState.resolve(parser.text());
                } else {
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to hold a string " +
                            "value, found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (WatchStatusField.ACTIONS.match(currentFieldName, parser.getDeprecationHandler())) {
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
                    throw new ElasticsearchParseException("could not parse watch status for [{}]. expecting field [{}] to be an object, " +
                            "found [{}] instead", watchId, currentFieldName, token);
                }
            } else if (WatchStatusField.HEADERS.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    headers = parser.mapStrings();
                }
            }
        }

        actions = actions == null ? emptyMap() : unmodifiableMap(actions);

        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actions, headers);
    }

    @Override
    public String toString() {
        return "WatchStatus{" +
                "state=" + state +
                ", executionState=" + executionState +
                ", lastChecked=" + lastChecked +
                ", lastMetCondition=" + lastMetCondition +
                ", version=" + version +
                ", headers=" + headers +
                ", actions=" + actions +
                '}';
    }
}

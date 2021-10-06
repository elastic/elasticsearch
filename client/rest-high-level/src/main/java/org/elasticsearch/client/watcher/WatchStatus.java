/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.client.watcher.WatchStatusDateParser.parseDate;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class WatchStatus {

    private final State state;

    private final ExecutionState executionState;
    private final ZonedDateTime lastChecked;
    private final ZonedDateTime lastMetCondition;
    private final long version;
    private final Map<String, ActionStatus> actions;
    @Nullable private Map<String, String> headers;

    public WatchStatus(long version,
                       State state,
                       ExecutionState executionState,
                       ZonedDateTime lastChecked,
                       ZonedDateTime lastMetCondition,
                       Map<String, ActionStatus> actions,
                       Map<String, String> headers) {
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

    public Map<String, ActionStatus> getActions() {
        return actions;
    }

    public long version() {
        return version;
    }

    public ExecutionState getExecutionState() {
        return executionState;
    }

    public Map<String, String> getHeaders() {
        return headers;
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
        return Objects.hash(lastChecked, lastMetCondition, actions, version, executionState);
    }

    public static WatchStatus parse(XContentParser parser) throws IOException {
        State state = null;
        ExecutionState executionState = null;
        ZonedDateTime lastChecked = null;
        ZonedDateTime lastMetCondition = null;
        Map<String, ActionStatus> actions = null;
        Map<String, String> headers = Collections.emptyMap();
        long version = -1;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    state = State.parse(parser);
                } catch (ElasticsearchParseException e) {
                    throw new ElasticsearchParseException("could not parse watch status. failed to parse field [{}]",
                            e, currentFieldName);
                }
            } else if (Field.VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    version = parser.longValue();
                } else {
                    throw new ElasticsearchParseException("could not parse watch status. expecting field [{}] to hold a long " +
                            "value, found [{}] instead", currentFieldName, token);
                }
            } else if (Field.LAST_CHECKED.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    lastChecked = parseDate(currentFieldName, parser);
                } else {
                    throw new ElasticsearchParseException("could not parse watch status. expecting field [{}] to hold a date " +
                           "value, found [{}] instead", currentFieldName, token);
                }
            } else if (Field.LAST_MET_CONDITION.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    lastMetCondition = parseDate(currentFieldName, parser);
                } else {
                    throw new ElasticsearchParseException("could not parse watch status. expecting field [{}] to hold a date " +
                           "value, found [{}] instead", currentFieldName, token);
                }
            } else if (Field.EXECUTION_STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token.isValue()) {
                    executionState = ExecutionState.resolve(parser.text());
                } else {
                    throw new ElasticsearchParseException("could not parse watch status. expecting field [{}] to hold a string " +
                           "value, found [{}] instead", currentFieldName, token);
                }
            } else if (Field.ACTIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                actions = new HashMap<>();
                if (token == XContentParser.Token.START_OBJECT) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            ActionStatus actionStatus = ActionStatus.parse(currentFieldName, parser);
                            actions.put(currentFieldName, actionStatus);
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse watch status. expecting field [{}] to be an object, " +
                            "found [{}] instead", currentFieldName, token);
                }
            } else if (Field.HEADERS.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    headers = parser.mapStrings();
                }
            } else {
                parser.skipChildren();
            }
        }

        actions = actions == null ? emptyMap() : unmodifiableMap(actions);
        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actions, headers);
    }

    public static class State {

        private final boolean active;
        private final ZonedDateTime timestamp;

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
                    timestamp = parseDate(currentFieldName, parser);
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

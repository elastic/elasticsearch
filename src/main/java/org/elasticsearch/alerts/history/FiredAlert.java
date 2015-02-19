/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertExecution;
import org.elasticsearch.alerts.AlertsException;
import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionRegistry;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputRegistry;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FiredAlert implements ToXContent {

    private String id;
    private String name;
    private DateTime fireTime;
    private DateTime scheduledTime;
    private Input input;
    private Condition condition;
    private State state;
    private AlertExecution execution;

    private @Nullable String message;
    private @Nullable Map<String,Object> metadata;

    // Used for assertion purposes, so we can ensure/test what we have loaded in memory is the same as what is persisted.
    private transient long version;

    private final AtomicBoolean sealed = new AtomicBoolean(false);

    FiredAlert() {
    }

    public FiredAlert(Alert alert, DateTime scheduledTime, DateTime fireTime) {
        this.id = alert.name() + "#" + scheduledTime.toDateTimeISO();
        this.name = alert.name();
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.condition = alert.condition();
        this.input = alert.input();
        this.state = State.AWAITS_EXECUTION;
        this.metadata = alert.metadata();
        this.version = 1;
    }

    public String id() {
        return id;
    }

    public DateTime scheduledTime() {
        return scheduledTime;
    }

    public String name() {
        return name;
    }

    public DateTime fireTime() {
        return fireTime;
    }

    public Input input() { return input; }

    public Condition condition() {
        return condition;
    }

    public State state() {
        return state;
    }

    public String message(){
        return this.message;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public long version() {
        return version;
    }

    void version(long version) {
        this.version = version;
    }

    public void update(State state, @Nullable String message) {
        this.state = state;
        this.message = message;
    }

    public void update(AlertExecution execution) {
        assert sealed.compareAndSet(false, true) : "sealing an fired alert should only be done once";
        this.execution = execution;
        if (!execution.conditionResult().met()) {
            state = State.EXECUTION_NOT_NEEDED;
        } else {
            if (execution.throttleResult().throttle()) {
                state = State.THROTTLED;
            } else {
                state = State.EXECUTED;
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field(Parser.ALERT_NAME_FIELD.getPreferredName(), name);
        historyEntry.field(Parser.FIRE_TIME_FIELD.getPreferredName(), fireTime.toDateTimeISO());
        historyEntry.field(Parser.SCHEDULED_FIRE_TIME_FIELD.getPreferredName(), scheduledTime.toDateTimeISO());
        historyEntry.startObject(Alert.Parser.CONDITION_FIELD.getPreferredName()).field(condition.type(), condition, params).endObject();
        historyEntry.field(Parser.STATE_FIELD.getPreferredName(), state.id());

        if (message != null) {
            historyEntry.field(Parser.MESSAGE_FIELD.getPreferredName(), message);
        }
        if (metadata != null) {
            historyEntry.field(Parser.METADATA_FIELD.getPreferredName(), metadata);
        }

        if (execution != null) {
            historyEntry.field(Parser.ALERT_EXECUTION_FIELD.getPreferredName(), execution);
        }

        historyEntry.endObject();
        return historyEntry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FiredAlert entry = (FiredAlert) o;
        if (!id.equals(entry.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id;
    }

    public enum State {

        AWAITS_EXECUTION,
        CHECKING,
        EXECUTION_NOT_NEEDED,
        THROTTLED,
        EXECUTED,
        FAILED;

        public String id() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static State resolve(String id) {
            try {
                return valueOf(id.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                throw new AlertsSettingsException("unknown fired alert state [" + id + "]");
            }
        }

        @Override
        public String toString() {
            return id();
        }
    }

    public static class Parser extends AbstractComponent {

        public static final ParseField ALERT_NAME_FIELD = new ParseField("alert_name");
        public static final ParseField FIRE_TIME_FIELD = new ParseField("fire_time");
        public static final ParseField SCHEDULED_FIRE_TIME_FIELD = new ParseField("scheduled_fire_time");
        public static final ParseField MESSAGE_FIELD = new ParseField("message");
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField METADATA_FIELD = new ParseField("meta");
        public static final ParseField ALERT_EXECUTION_FIELD = new ParseField("alert_execution");

        private final ConditionRegistry conditionRegistry;
        private final ActionRegistry actionRegistry;
        private final InputRegistry inputRegistry;

        @Inject
        public Parser(Settings settings, ConditionRegistry conditionRegistry, ActionRegistry actionRegistry,
                      InputRegistry inputRegistry) {
            super(settings);
            this.conditionRegistry = conditionRegistry;
            this.actionRegistry = actionRegistry;
            this.inputRegistry = inputRegistry;
        }

        public FiredAlert parse(BytesReference source, String historyId, long version) {
            try (XContentParser parser = XContentHelper.createParser(source)) {
                return parse(parser, historyId, version);
            } catch (IOException e) {
                throw new ElasticsearchException("Error during parsing alert record", e);
            }
        }

        public FiredAlert parse(XContentParser parser, String id, long version) throws IOException {
            FiredAlert alert = new FiredAlert();
            alert.id = id;
            alert.version = version;

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Alert.Parser.INPUT_FIELD.match(currentFieldName)) {
                        alert.input = inputRegistry.parse(parser);
                    } else if (Alert.Parser.CONDITION_FIELD.match(currentFieldName)) {
                        alert.condition = conditionRegistry.parse(parser);
                    } else if (METADATA_FIELD.match(currentFieldName)) {
                        alert.metadata = parser.map();
                    } else if (ALERT_EXECUTION_FIELD.match(currentFieldName)) {
                        alert.execution = AlertExecution.Parser.parse(parser, conditionRegistry, actionRegistry,
                                inputRegistry);
                    } else {
                        throw new AlertsException("unable to parse fired alert. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if (ALERT_NAME_FIELD.match(currentFieldName)) {
                        alert.name = parser.text();
                    } else if (FIRE_TIME_FIELD.match(currentFieldName)) {
                        alert.fireTime = DateTime.parse(parser.text());
                    } else if (SCHEDULED_FIRE_TIME_FIELD.match(currentFieldName)) {
                        alert.scheduledTime = DateTime.parse(parser.text());
                    } else if (MESSAGE_FIELD.match(currentFieldName)) {
                        alert.message = parser.textOrNull();
                    } else if (STATE_FIELD.match(currentFieldName)) {
                        alert.state = State.resolve(parser.text());
                    } else {
                        throw new AlertsException("unable to parse fired alert. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new AlertsException("unable to parse fired alert. unexpected token [" + token + "] for [" + currentFieldName + "]");
                }
            }

            return alert;
        }
    }
}

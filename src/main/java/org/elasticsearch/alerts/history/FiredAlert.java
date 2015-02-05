/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.actions.AlertActions;
import org.elasticsearch.alerts.payload.Payload;
import org.elasticsearch.alerts.payload.PayloadRegistry;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.alerts.trigger.TriggerRegistry;
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
import java.util.Map;

public class FiredAlert implements ToXContent {

    private String id;
    private String name;
    private DateTime fireTime;
    private DateTime scheduledTime;
    private Trigger trigger;
    private AlertActions actions;
    private State state;

    /*Optional*/
    private Payload payload;
    private String errorMessage;
    private Map<String,Object> metadata;

    // During an fired alert execution we use this and then we store it with the history, after that we don't use it.
    // We store it because it may end up being useful for debug / history purposes
    private transient AlertsService.AlertRun alertRun;
    // Used for assertion purposes, so we can ensure/test what we have loaded in memory is the same as what is persisted.
    private transient long version;

    FiredAlert() {
    }

    public FiredAlert(Alert alert, DateTime scheduledTime, DateTime fireTime, State state) {
        this.id = alert.name() + "#" + scheduledTime.toDateTimeISO();
        this.name = alert.name();
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.trigger = alert.trigger();
        this.actions = alert.actions();
        this.state = state;
        this.metadata = alert.metadata();
        this.version = 1;
    }

    public String id() {
        return id;
    }

    public void id(String id) {
        this.id = id;
    }

    // TODO: Maybe pull into history service?
    public void update(Alert alert, AlertsService.AlertRun alertRun) {
        this.alertRun = alertRun;
        if (alertRun.triggerResult().triggered()) {
            if (alertRun.throttleResult().throttle()) {
                if (alert.status().state() != Alert.Status.State.NOT_EXECUTED) {
                    state = State.THROTTLED;
                }
            } else if (state != State.THROTTLED) {
                state = State.ACTION_PERFORMED;
            }
            payload = alert.payload();
        } else {
             state = State.NO_ACTION_NEEDED;
        }
    }

    public DateTime scheduledTime() {
        return scheduledTime;
    }

    public void scheduledTime(DateTime scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    public String name() {
        return name;
    }

    public void name(String name) {
        this.name = name;
    }

    public DateTime fireTime() {
        return fireTime;
    }

    public void fireTime(DateTime fireTime) {
        this.fireTime = fireTime;
    }

    public Trigger trigger() {
        return trigger;
    }

    public void trigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public AlertActions actions() {
        return actions;
    }

    public void actions(AlertActions actions) {
        this.actions = actions;
    }

    public State state() {
        return state;
    }

    public void state(State state) {
        this.state = state;
    }

    public long version() {
        return version;
    }

    public void version(long version) {
        this.version = version;
    }

    public String errorMessage(){
        return this.errorMessage;
    }

    public void errorMsg(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public void metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public Payload payload() {
        return payload;
    }

    public void payload(Payload payload) {
        this.payload = payload;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field(Parser.ALERT_NAME_FIELD.getPreferredName(), name);
        historyEntry.field(Parser.FIRE_TIME_FIELD.getPreferredName(), fireTime.toDateTimeISO());
        historyEntry.field(Parser.SCHEDULED_FIRE_TIME_FIELD.getPreferredName(), scheduledTime.toDateTimeISO());
        historyEntry.field(Parser.TRIGGER_FIELD.getPreferredName(), trigger, params);
        historyEntry.field(Parser.ACTIONS_FIELD.getPreferredName(), actions, params);
        historyEntry.field(Parser.STATE_FIELD.getPreferredName(), state.toString());

        if (payload != null) {
            historyEntry.field(Parser.PAYLOAD_FIELD.getPreferredName(), payload, params);
        }
        if (errorMessage != null) {
            historyEntry.field(Parser.ERROR_MESSAGE_FIELD.getPreferredName(), errorMessage);
        }
        if (metadata != null) {
            historyEntry.field(Parser.METADATA_FIELD.getPreferredName(), metadata);
        }
        // TODO: maybe let AlertRun implement ToXContent?
        if (alertRun != null) {
            historyEntry.field(Parser.TRIGGER_RESPONSE.getPreferredName(), alertRun.triggerResult().data());
            historyEntry.field(Parser.PAYLOAD_RESPONSE.getPreferredName(), alertRun.data());
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

        AWAITS_RUN,
        RUNNING,
        NO_ACTION_NEEDED,
        ACTION_PERFORMED,
        FAILED,
        THROTTLED;

        @Override
        public String toString(){
            switch (this) {
                case AWAITS_RUN:
                    return "AWAITS_RUN";
                case RUNNING:
                    return "RUNNING";
                case NO_ACTION_NEEDED:
                    return "NO_ACTION_NEEDED";
                case ACTION_PERFORMED:
                    return "ACTION_PERFORMED";
                case FAILED:
                    return "FAILED";
                case THROTTLED:
                    return "THROTTLED";
                default:
                    return "NO_ACTION_NEEDED";
            }
        }

        public static State fromString(String s) {
            switch(s.toUpperCase()) {
                case "AWAITS_RUN":
                    return AWAITS_RUN;
                case "RUNNING":
                    return RUNNING;
                case "NO_ACTION_NEEDED":
                    return NO_ACTION_NEEDED;
                case "ACTION_UNDERWAY":
                    return ACTION_PERFORMED;
                case "FAILED":
                    return FAILED;
                case "THROTTLED":
                    return THROTTLED;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown value [" + s + "] for AlertHistoryState" );
            }
        }

    }

    public static class Parser extends AbstractComponent {

        public static final ParseField ALERT_NAME_FIELD = new ParseField("alert_name");
        public static final ParseField FIRE_TIME_FIELD = new ParseField("fire_time");
        public static final ParseField SCHEDULED_FIRE_TIME_FIELD = new ParseField("scheduled_fire_time");
        public static final ParseField ERROR_MESSAGE_FIELD = new ParseField("error_msg");
        public static final ParseField TRIGGER_FIELD = new ParseField("trigger");
        public static final ParseField TRIGGER_RESPONSE = new ParseField("trigger_response");
        public static final ParseField PAYLOAD_FIELD = new ParseField("payload_request");
        public static final ParseField PAYLOAD_RESPONSE = new ParseField("payload_response");
        public static final ParseField ACTIONS_FIELD = new ParseField("actions");
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField METADATA_FIELD = new ParseField("meta");

        private final TriggerRegistry triggerRegistry;
        private final PayloadRegistry payloadRegistry;
        private final ActionRegistry actionRegistry;

        @Inject
        public Parser(Settings settings, TriggerRegistry triggerRegistry, PayloadRegistry payloadRegistry, ActionRegistry actionRegistry) {
            super(settings);
            this.triggerRegistry = triggerRegistry;
            this.payloadRegistry = payloadRegistry;
            this.actionRegistry = actionRegistry;
        }

        public FiredAlert parse(BytesReference source, String historyId, long version) {
            try (XContentParser parser = XContentHelper.createParser(source)) {
                return parse(parser, historyId, version);
            } catch (IOException e) {
                throw new ElasticsearchException("Error during parsing alert record", e);
            }
        }

        public FiredAlert parse(XContentParser parser, String id, long version) throws IOException {
            FiredAlert entry = new FiredAlert();
            entry.id(id);
            entry.version(version);

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (ACTIONS_FIELD.match(currentFieldName)) {
                        entry.actions(actionRegistry.parseActions(parser));
                    } else if (TRIGGER_FIELD.match(currentFieldName)) {
                        entry.trigger(triggerRegistry.parse(parser));
                    } else if (PAYLOAD_FIELD.match(currentFieldName)) {
                        entry.payload(payloadRegistry.parse(parser));
                    } else if (METADATA_FIELD.match(currentFieldName)) {
                        entry.metadata(parser.map());
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if (ALERT_NAME_FIELD.match(currentFieldName)) {
                        entry.name(parser.text());
                    } else if (FIRE_TIME_FIELD.match(currentFieldName)) {
                        entry.fireTime(DateTime.parse(parser.text()));
                    } else if (SCHEDULED_FIRE_TIME_FIELD.match(currentFieldName)) {
                        entry.scheduledTime(DateTime.parse(parser.text()));
                    } else if (ERROR_MESSAGE_FIELD.match(currentFieldName)) {
                        entry.errorMsg(parser.textOrNull());
                    } else if (STATE_FIELD.match(currentFieldName)) {
                        entry.state(State.fromString(parser.text()));
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "] for [" + currentFieldName + "]");
                }
            }

            return entry;
        }
    }
}

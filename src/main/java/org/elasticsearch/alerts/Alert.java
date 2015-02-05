/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.actions.AlertActions;
import org.elasticsearch.alerts.payload.Payload;
import org.elasticsearch.alerts.payload.PayloadRegistry;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
import org.elasticsearch.alerts.scheduler.schedule.ScheduleRegistry;
import org.elasticsearch.alerts.throttle.AckThrottler;
import org.elasticsearch.alerts.throttle.AlertThrottler;
import org.elasticsearch.alerts.throttle.PeriodThrottler;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.alerts.trigger.TriggerRegistry;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.alerts.support.AlertsDateUtils.parseDate;

public class Alert implements ToXContent {

    private final String name;
    private final Schedule schedule;
    private final Trigger trigger;
    private final AlertActions actions;
    private final Throttler throttler;
    private final Status status;
    private final TimeValue throttlePeriod;
    private final boolean ackable;

    @Nullable
    private final Map<String, Object> metadata;

    @Nullable
    private final Payload payload;

    public Alert(String name, Schedule schedule, Trigger trigger, Payload payload, TimeValue throttlePeriod, boolean ackable, AlertActions actions, Map<String, Object> metadata, Status status) {
        this.name = name;
        this.schedule = schedule;
        this.trigger = trigger;
        this.actions = actions;
        this.status = status != null ? status : new Status();
        this.throttlePeriod = throttlePeriod;
        this.ackable = ackable;

        this.metadata = metadata;
        this.payload = payload != null ? payload : Payload.NOOP;

        PeriodThrottler periodThrottler = throttlePeriod != null ? new PeriodThrottler(throttlePeriod) : null;
        AckThrottler ackThrottler = ackable ? new AckThrottler() : null;
        throttler = new AlertThrottler(periodThrottler, ackThrottler);
    }

    public String name() {
        return name;
    }

    public Schedule schedule() {
        return schedule;
    }

    public Trigger trigger() {
        return trigger;
    }

    public Payload payload() {
        return payload;
    }

    public Throttler throttler() {
        return throttler;
    }

    public AlertActions actions() {
        return actions;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public TimeValue throttlePeriod() {
        return throttlePeriod;
    }

    public boolean ackable() {
        return ackable;
    }

    public Status status() {
        return status;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alert alert = (Alert) o;
        return alert.name.equals(name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.SCHEDULE_FIELD.getPreferredName(), schedule);
        builder.field(Parser.TRIGGER_FIELD.getPreferredName(), trigger);
        if (payload != Payload.NOOP) {
            builder.field(Parser.PAYLOAD_FIELD.getPreferredName(), payload);
        }
        if (throttlePeriod != null) {
            builder.field(Parser.THROTTLE_PERIOD_FIELD.getPreferredName(), throttlePeriod.getMillis());
        }
        builder.field(Parser.ACKABLE_FIELD.getPreferredName(), ackable);
        builder.field(Parser.ACTIONS_FIELD.getPreferredName(), (ToXContent) actions);
        if (metadata != null) {
            builder.field(Parser.META_FIELD.getPreferredName(), metadata);
        }
        builder.field(Parser.STATUS_FIELD.getPreferredName(), status);
        builder.endObject();
        return builder;
    }

    public static class Parser extends AbstractComponent {

        public static final ParseField SCHEDULE_FIELD = new ParseField("schedule");
        public static final ParseField TRIGGER_FIELD = new ParseField("trigger");
        public static final ParseField ACTIONS_FIELD = new ParseField("actions");
        public static final ParseField PAYLOAD_FIELD = new ParseField("payload");
        public static final ParseField META_FIELD = new ParseField("meta");
        public static final ParseField STATUS_FIELD = new ParseField("status");
        public static final ParseField THROTTLE_PERIOD_FIELD = new ParseField("throttle_period");
        public static final ParseField ACKABLE_FIELD = new ParseField("ackable");

        private final TriggerRegistry triggerRegistry;
        private final ScheduleRegistry scheduleRegistry;
        private final PayloadRegistry payloadRegistry;
        private final ActionRegistry actionRegistry;

        @Inject
        public Parser(Settings settings, TriggerRegistry triggerRegistry, ScheduleRegistry scheduleRegistry,
                      PayloadRegistry payloadRegistry, ActionRegistry actionRegistry) {

            super(settings);
            this.triggerRegistry = triggerRegistry;
            this.scheduleRegistry = scheduleRegistry;
            this.payloadRegistry = payloadRegistry;
            this.actionRegistry = actionRegistry;
        }

        public Alert parse(String name, boolean includeStatus, BytesReference source) {
            try (XContentParser parser = XContentHelper.createParser(source)) {
                return parse(name, includeStatus, parser);
            } catch (IOException ioe) {
                throw new AlertsException("could not parse alert", ioe);
            }
        }

        public Alert parse(String name, boolean includeStatus, XContentParser parser) throws IOException {
            Schedule schedule = null;
            Trigger trigger = null;
            AlertActions actions = null;
            Payload payload = null;
            Map<String, Object> metatdata = null;
            Status status = null;
            boolean ackable = false;
            TimeValue throttlePeriod = null;

            String currentFieldName = null;
            XContentParser.Token token = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (SCHEDULE_FIELD.match(currentFieldName)) {
                    schedule = scheduleRegistry.parse(parser);
                } else if (TRIGGER_FIELD.match(currentFieldName)) {
                    trigger = triggerRegistry.parse(parser);
                } else if (ACTIONS_FIELD.match(currentFieldName)) {
                    actions = actionRegistry.parseActions(parser);
                } else if (PAYLOAD_FIELD.match(currentFieldName)) {
                    payload = payloadRegistry.parse(parser);
                } else if (META_FIELD.match(currentFieldName)) {
                    metatdata = parser.map();
                } else if (STATUS_FIELD.match(currentFieldName) && includeStatus) {
                    status = Status.fromXContent(parser);
                } else if (ACKABLE_FIELD.match(currentFieldName)) {
                    ackable = parser.booleanValue();
                } else if (THROTTLE_PERIOD_FIELD.match(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        throttlePeriod = TimeValue.parseTimeValue(parser.text(), null);
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        throttlePeriod = TimeValue.timeValueMillis(parser.longValue());
                    } else {
                        throw new AlertsSettingsException("could not parse alert throttle period. could not parse token [" + token + "] as time value (must either be string or number)");
                    }
                }
            }
            if (schedule == null) {
                throw new AlertsSettingsException("coult not parse alert[" + name + "]. missing alert schedule");
            }
            if (trigger == null) {
                throw new AlertsSettingsException("coult not parse alert[" + name + "]. missing alert trigger");
            }
            if (actions == null) {
                throw new AlertsSettingsException("coult not parse alert[" + name + "]. missing alert actions");
            }

            return new Alert(name, schedule, trigger, payload, throttlePeriod, ackable, actions, metatdata, status);
        }

    }

    public static class Status implements ToXContent {

        public enum State {
            NOT_EXECUTED,
            EXECUTED,
            ACKED
        }

        public static final ParseField LAST_RAN_FIELD = new ParseField("last_ran");
        public static final ParseField LAST_TRIGGERED_FIELD = new ParseField("last_triggered");
        public static final ParseField LAST_EXECUTED_FIELD = new ParseField("last_executed");
        public static final ParseField LAST_STATE_CHANGED_FIELD = new ParseField("last_state_changed");
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField LAST_THROTTLED_FIELD = new ParseField("last_throttled");
        public static final ParseField LAST_THROTTLE_REASON_FIELD = new ParseField("last_throttle_reason");

        private transient long version;
        private DateTime lastRan;
        private DateTime lastTriggered;
        private DateTime lastExecuted;
        private DateTime lastStateChanged;
        private State state;
        private DateTime lastThrottled;
        private String lastThrottleReason;

        public Status() {
            this(-1, null, null, null, State.NOT_EXECUTED, new DateTime(), null, null);
        }

        private Status(long version, DateTime lastRan, DateTime lastTriggered, DateTime lastExecuted,
                       State state, DateTime lastStateChanged, DateTime lastThrottled, String lastThrottleReason) {
            this.version = version;
            this.lastRan = lastRan;
            this.lastTriggered = lastTriggered;
            this.lastExecuted = lastExecuted;
            this.state = state;
            this.lastStateChanged = lastStateChanged;
            this.lastThrottled = lastThrottled;
            this.lastThrottleReason = lastThrottleReason;
        }

        public long version() {
            return version;
        }

        public void version(long version) {
            this.version = version;
        }

        public DateTime lastRan() {
            return lastRan;
        }

        public DateTime lastTriggered() {
            return lastTriggered;
        }

        public DateTime lastExecuted() {
            return lastExecuted;
        }

        public State state() {
            return state;
        }

        public DateTime lastStateChanged() {
            return lastStateChanged;
        }

        public DateTime lastThrottled() {
            return lastThrottled;
        }

        public String lastThrottledReason() {
            return lastThrottleReason;
        }

        /**
         * Called whenever an alert is ran
         */
        public void ran(DateTime timestamp) {
            lastRan = timestamp;
        }

        /**
         * Called whenever an alert run is throttled
         */
        public void throttled(DateTime timestamp, String reason) {
            lastThrottled = timestamp;
            lastThrottleReason = reason;
        }

        /**
         * Notifies this status about the triggered event of an alert run. The state will be updated accordingly -
         * if during a run, the alert was not triggered and the current state is ACKED, we the need to reset the
         * state to NOT_EXECUTED...
         */
        public void triggered(boolean triggered, DateTime timestamp) {
            if (triggered) {
                lastTriggered = timestamp;
            } else if (state == State.ACKED) {
                state = State.NOT_EXECUTED;
            }
        }

        /**
         * Notifies this status that the alert was acked. If the current state is EXECUTED, then we'll change it
         * to ACKED (when set to ACKED, the {@link org.elasticsearch.alerts.throttle.AckThrottler} will throttle the
         * execution.
         *
         * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
         */
        public boolean acked() {
            if (state == State.EXECUTED) {
                state = State.ACKED;
                return true;
            }
            return false;
        }

        /**
         * Notified this status that the alert was executed. If the current state is NOT_EXECUTED, it will change to
         * EXECUTED. When set to EXECUTED the alert can be acked (assuming the alert is ackable).
         */
        public void executed(DateTime timestamp) {
            lastExecuted = timestamp;
            if (state == State.NOT_EXECUTED) {
                state = State.EXECUTED;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LAST_RAN_FIELD.getPreferredName(), lastRan);
            builder.field(LAST_TRIGGERED_FIELD.getPreferredName(), lastTriggered);
            builder.field(LAST_EXECUTED_FIELD.getPreferredName(), lastExecuted);
            builder.field(LAST_STATE_CHANGED_FIELD.getPreferredName(), lastStateChanged);
            builder.field(STATE_FIELD.getPreferredName(), state.name().toLowerCase(Locale.ROOT));
            builder.field(LAST_THROTTLED_FIELD.getPreferredName(), lastThrottled);
            builder.field(LAST_THROTTLE_REASON_FIELD.getPreferredName(), lastThrottleReason);
            return builder.endObject();
        }

        public static Status fromXContent(XContentParser parser) throws IOException {

            DateTime lastRan = null;
            DateTime lastTriggered = null;
            DateTime lastExecuted = null;
            DateTime lastStateChanged = null;
            State state = null;
            DateTime lastThrottled = null;
            String lastThrottleReason = null;

            String currentFieldName = null;
            XContentParser.Token token = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (LAST_RAN_FIELD.match(currentFieldName)) {
                        lastRan = parseDate(currentFieldName, token, parser);
                    } else if (LAST_TRIGGERED_FIELD.match(currentFieldName)) {
                        lastTriggered = parseDate(currentFieldName, token, parser);
                    } else if (LAST_EXECUTED_FIELD.match(currentFieldName)) {
                        lastExecuted = parseDate(currentFieldName, token, parser);
                    } else if (LAST_STATE_CHANGED_FIELD.match(currentFieldName)) {
                        lastStateChanged = parseDate(currentFieldName, token, parser);
                    } else if (STATE_FIELD.match(currentFieldName)) {
                        state = State.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    } else if (LAST_THROTTLED_FIELD.match(currentFieldName)) {
                        lastThrottled = parseDate(currentFieldName, token, parser);
                    } else if (LAST_THROTTLE_REASON_FIELD.match(currentFieldName)) {
                        lastThrottleReason = parser.textOrNull();
                    }
                }
            }

            return new Status(-1, lastRan, lastTriggered, lastExecuted, state, lastStateChanged, lastThrottled, lastThrottleReason);
        }
    }
}

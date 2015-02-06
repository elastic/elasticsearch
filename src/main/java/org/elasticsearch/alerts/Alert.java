/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
import org.elasticsearch.alerts.scheduler.schedule.ScheduleRegistry;
import org.elasticsearch.alerts.throttle.AlertThrottler;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.alerts.transform.TransformRegistry;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.alerts.trigger.TriggerRegistry;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
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

import static org.elasticsearch.alerts.support.AlertsDateUtils.*;

public class Alert implements ToXContent {

    private final String name;
    private final Schedule schedule;
    private final Trigger trigger;
    private final Actions actions;
    private final Throttler throttler;
    private final Status status;
    private final TimeValue throttlePeriod;

    @Nullable
    private final Map<String, Object> metadata;

    @Nullable
    private final Transform transform;

    public Alert(String name, Schedule schedule, Trigger trigger, Transform transform, TimeValue throttlePeriod, Actions actions, Map<String, Object> metadata, Status status) {
        this.name = name;
        this.schedule = schedule;
        this.trigger = trigger;
        this.actions = actions;
        this.status = status != null ? status : new Status();
        this.throttlePeriod = throttlePeriod;
        this.metadata = metadata;
        this.transform = transform != null ? transform : Transform.NOOP;

        throttler = new AlertThrottler(throttlePeriod);
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

    public Transform transform() {
        return transform;
    }

    public Throttler throttler() {
        return throttler;
    }

    public Actions actions() {
        return actions;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public TimeValue throttlePeriod() {
        return throttlePeriod;
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
        builder.field(Parser.SCHEDULE_FIELD.getPreferredName()).startObject().field(schedule.type(), schedule).endObject();
        builder.field(Parser.TRIGGER_FIELD.getPreferredName()).startObject().field(trigger.type(), trigger).endObject();
        if (transform != Transform.NOOP) {
            builder.field(Parser.TRANSFORM_FIELD.getPreferredName()).startObject().field(transform.type(), transform).endObject();
        }
        if (throttlePeriod != null) {
            builder.field(Parser.THROTTLE_PERIOD_FIELD.getPreferredName(), throttlePeriod.getMillis());
        }
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
        public static final ParseField TRANSFORM_FIELD = new ParseField("transform");
        public static final ParseField META_FIELD = new ParseField("meta");
        public static final ParseField STATUS_FIELD = new ParseField("status");
        public static final ParseField THROTTLE_PERIOD_FIELD = new ParseField("throttle_period");

        private final TriggerRegistry triggerRegistry;
        private final ScheduleRegistry scheduleRegistry;
        private final TransformRegistry transformRegistry;
        private final ActionRegistry actionRegistry;

        @Inject
        public Parser(Settings settings, TriggerRegistry triggerRegistry, ScheduleRegistry scheduleRegistry,
                      TransformRegistry transformRegistry, ActionRegistry actionRegistry) {

            super(settings);
            this.triggerRegistry = triggerRegistry;
            this.scheduleRegistry = scheduleRegistry;
            this.transformRegistry = transformRegistry;
            this.actionRegistry = actionRegistry;
        }

        public Alert parse(String name, boolean includeStatus, BytesReference source) {
            if (logger.isTraceEnabled()) {
                logger.trace("parsing alert [{}] ", source.toUtf8());
            }
            try (XContentParser parser = XContentHelper.createParser(source)) {
                return parse(name, includeStatus, parser);
            } catch (IOException ioe) {
                throw new AlertsException("could not parse alert [" + name + "]", ioe);
            }
        }

        public Alert parse(String name, boolean includeStatus, XContentParser parser) throws IOException {
            Schedule schedule = null;
            Trigger trigger = null;
            Actions actions = null;
            Transform transform = null;
            Map<String, Object> metatdata = null;
            Status status = null;
            TimeValue throttlePeriod = null;

            String currentFieldName = null;
            XContentParser.Token token = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == null ){
                    throw new AlertsException("could not parse alert [" + name + "]. null token");
                } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) && currentFieldName !=null ) {
                    if (SCHEDULE_FIELD.match(currentFieldName)) {
                        schedule = scheduleRegistry.parse(parser);
                    } else if (TRIGGER_FIELD.match(currentFieldName)) {
                        trigger = triggerRegistry.parse(parser);
                    } else if (ACTIONS_FIELD.match(currentFieldName)) {
                        actions = actionRegistry.parseActions(parser);
                    } else if (TRANSFORM_FIELD.match(currentFieldName)) {
                        transform = transformRegistry.parse(parser);
                    } else if (META_FIELD.match(currentFieldName)) {
                        metatdata = parser.map();
                    } else if (STATUS_FIELD.match(currentFieldName) && includeStatus) {
                        status = Status.fromXContent(parser);
                    } else if (THROTTLE_PERIOD_FIELD.match(currentFieldName)) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            throttlePeriod = TimeValue.parseTimeValue(parser.text(), null);
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            throttlePeriod = TimeValue.timeValueMillis(parser.longValue());
                        } else {
                            throw new AlertsSettingsException("could not parse alert [" + name + "] throttle period. could not parse token [" + token + "] as time value (must either be string or number)");
                        }
                    }
                }
            }
            if (schedule == null) {
                throw new AlertsSettingsException("could not parse alert [" + name + "]. missing alert schedule");
            }
            if (trigger == null) {
                throw new AlertsSettingsException("could not parse alert [" + name + "]. missing alert trigger");
            }
            if (actions == null) {
                throw new AlertsSettingsException("could not parse alert [" + name + "]. missing alert actions");
            }

            return new Alert(name, schedule, trigger, transform, throttlePeriod, actions, metatdata, status);
        }

    }

    public static class Status implements ToXContent, Streamable {

        public static final ParseField TIMESTAMP_FIELD = new ParseField("last_throttled");
        public static final ParseField LAST_RAN_FIELD = new ParseField("last_ran");
        public static final ParseField LAST_TRIGGERED_FIELD = new ParseField("last_triggered");
        public static final ParseField LAST_EXECUTED_FIELD = new ParseField("last_executed");
        public static final ParseField ACK_FIELD = new ParseField("ack");
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField LAST_THROTTLE_FIELD = new ParseField("last_throttle");
        public static final ParseField REASON_FIELD = new ParseField("reason");

        private transient long version;
        private DateTime lastRan;
        private DateTime lastTriggered;
        private DateTime lastExecuted;
        private Ack ack;
        private Throttle lastThrottle;

        public Status() {
            this(-1, null, null, null, null, new Ack());
        }

        public Status(Status other) {
            this(other.version, other.lastRan, other.lastTriggered, other.lastExecuted, other.lastThrottle, other.ack);
        }

        private Status(long version, DateTime lastRan, DateTime lastTriggered, DateTime lastExecuted, Throttle lastThrottle, Ack ack) {
            this.version = version;
            this.lastRan = lastRan;
            this.lastTriggered = lastTriggered;
            this.lastExecuted = lastExecuted;
            this.lastThrottle = lastThrottle;
            this.ack = ack;
        }

        public long version() {
            return version;
        }

        public void version(long version) {
            this.version = version;
        }

        public boolean ran() {
            return lastRan != null;
        }

        public DateTime lastRan() {
            return lastRan;
        }

        public boolean triggered() {
            return lastTriggered != null;
        }

        public DateTime lastTriggered() {
            return lastTriggered;
        }

        public boolean executed() {
            return lastExecuted != null;
        }

        public DateTime lastExecuted() {
            return lastExecuted;
        }

        public Throttle lastThrottle() {
            return lastThrottle;
        }

        public Ack ack() {
            return ack;
        }

        public boolean acked() {
            return ack.state == Ack.State.ACKED;
        }

        /**
         * Called whenever an alert is ran
         */
        public void onRun(DateTime timestamp) {
            lastRan = timestamp;
        }

        /**
         * Called whenever an alert run is throttled
         */
        public void onThrottle(DateTime timestamp, String reason) {
            lastThrottle = new Throttle(timestamp, reason);
        }

        /**
         * Notifies this status about the triggered event of an alert run. The state will be updated accordingly -
         * if the alert is can be acked and during a run, the alert was not triggered and the current state is {@link Status.Ack.State#ACKED},
         * we then need to reset the state to {@link Status.Ack.State#AWAITS_EXECUTION}
         */
        public void onTrigger(boolean triggered, DateTime timestamp) {
            if (triggered) {
                lastTriggered = timestamp;
            } else if (ack.state == Ack.State.ACKED) {
                // didn't trigger now after it triggered in the past - we need to reset the ack state
                ack = new Ack(Ack.State.AWAITS_EXECUTION, timestamp);
            }
        }

        /**
         * Notifies this status that the alert was acked. If the current state is {@link Status.Ack.State#ACKABLE}, then we'll change it
         * to {@link Status.Ack.State#ACKED} (when set to {@link Status.Ack.State#ACKED}, the {@link org.elasticsearch.alerts.throttle.AckThrottler} will lastThrottle the
         * execution.
         *
         * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
         */
        public boolean onAck(DateTime timestamp) {
            if (ack.state == Ack.State.ACKABLE) {
                ack = new Ack(Ack.State.ACKED, timestamp);
                return true;
            }
            return false;
        }

        /**
         * Notified this status that the alert was executed. If the current state is {@link Status.Ack.State#AWAITS_EXECUTION}, it will change to
         * {@link Status.Ack.State#ACKABLE}.
         */
        public void onExecution(DateTime timestamp) {
            lastExecuted = timestamp;
            if (ack.state == Ack.State.AWAITS_EXECUTION) {
                ack = new Ack(Ack.State.ACKABLE, timestamp);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(version);
            writeOptionalDate(out, lastRan);
            writeOptionalDate(out, lastTriggered);
            writeOptionalDate(out, lastExecuted);
            if (lastThrottle == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                writeDate(out, lastThrottle.timestamp);
                out.writeString(lastThrottle.reason);
            }
            out.writeString(ack.state.name());
            writeDate(out, ack.timestamp);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            version = in.readLong();
            lastRan = readOptionalDate(in);
            lastTriggered = readOptionalDate(in);
            lastExecuted = readOptionalDate(in);
            lastThrottle = in.readBoolean() ? new Throttle(readDate(in), in.readString()) : null;
            ack = new Ack(Ack.State.valueOf(in.readString()), readDate(in));
        }

        public static Status read(StreamInput in) throws IOException {
            Alert.Status status = new Alert.Status();
            status.readFrom(in);
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (lastRan != null) {
                builder.field(LAST_RAN_FIELD.getPreferredName(), lastRan);
            }
            if (lastTriggered != null) {
                builder.field(LAST_TRIGGERED_FIELD.getPreferredName(), lastTriggered);
            }
            if (lastExecuted != null) {
                builder.field(LAST_EXECUTED_FIELD.getPreferredName(), lastExecuted);
            }
            builder.startObject(ACK_FIELD.getPreferredName())
                    .field(STATE_FIELD.getPreferredName(), ack.state.name().toLowerCase(Locale.ROOT))
                    .field(TIMESTAMP_FIELD.getPreferredName(), ack.timestamp)
                    .endObject();
            if (lastThrottle != null) {
                builder.startObject(LAST_THROTTLE_FIELD.getPreferredName())
                        .field(TIMESTAMP_FIELD.getPreferredName(), lastThrottle.timestamp)
                        .field(REASON_FIELD.getPreferredName(), lastThrottle.reason)
                        .endObject();
            }
            return builder.endObject();
        }

        public static Status fromXContent(XContentParser parser) throws IOException {

            DateTime lastRan = null;
            DateTime lastTriggered = null;
            DateTime lastExecuted = null;
            Throttle lastThrottle = null;
            Ack ack = null;

            String currentFieldName = null;
            XContentParser.Token token = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (LAST_RAN_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastRan = parseDate(currentFieldName, token, parser);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_TRIGGERED_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastTriggered = parseDate(currentFieldName, token, parser);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_EXECUTED_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastExecuted = parseDate(currentFieldName, token, parser);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_THROTTLE_FIELD.match(currentFieldName)) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        DateTime timestamp = null;
                        String reason = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if (TIMESTAMP_FIELD.match(currentFieldName)) {
                                    timestamp = parseDate(currentFieldName, token, parser);
                                } else if (REASON_FIELD.match(currentFieldName)) {
                                    reason = parser.text();
                                } else {
                                    throw new AlertsException("unknown filed [" + currentFieldName + "] in alert status throttle entry");
                                }
                            }
                        }
                        lastThrottle = new Throttle(timestamp, reason);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to be an object, found [" + token + "] instead");
                    }
                } else if (ACK_FIELD.match(currentFieldName)) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        Ack.State state = null;
                        DateTime timestamp = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if (TIMESTAMP_FIELD.match(currentFieldName)) {
                                    timestamp = parseDate(currentFieldName, token, parser);
                                } else if (STATE_FIELD.match(currentFieldName)) {
                                    state = Ack.State.valueOf(parser.text().toUpperCase(Locale.ROOT));
                                } else {
                                    throw new AlertsException("unknown filed [" + currentFieldName + "] in alert status throttle entry");
                                }
                            }
                        }
                        ack = new Ack(state, timestamp);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to be an object, found [" + token + "] instead");
                    }
                }
            }

            return new Status(-1, lastRan, lastTriggered, lastExecuted, lastThrottle, ack);
        }


        public static class Ack {

            public static enum State {
                AWAITS_EXECUTION,
                ACKABLE,
                ACKED
            }

            private final State state;
            private final DateTime timestamp;

            public Ack() {
                this(State.AWAITS_EXECUTION, new DateTime());
            }

            public Ack(State state, DateTime timestamp) {
                this.state = state;
                this.timestamp = timestamp;
            }

            public State state() {
                return state;
            }

            public DateTime timestamp() {
                return timestamp;
            }

        }

        public static class Throttle {

            private final DateTime timestamp;
            private final String reason;

            public Throttle(DateTime timestamp, String reason) {
                this.timestamp = timestamp;
                this.reason = reason;
            }

        }

    }
}

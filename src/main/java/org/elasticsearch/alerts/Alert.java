/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionRegistry;
import org.elasticsearch.alerts.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputRegistry;
import org.elasticsearch.alerts.input.NoneInput;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
import org.elasticsearch.alerts.scheduler.schedule.ScheduleRegistry;
import org.elasticsearch.alerts.support.clock.Clock;
import org.elasticsearch.alerts.throttle.AlertThrottler;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.alerts.transform.TransformRegistry;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
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

public class Alert implements Scheduler.Job, ToXContent {

    private final String name;
    private final Schedule schedule;
    private final Input input;
    private final Condition condition;
    private final Actions actions;
    private final Throttler throttler;
    private final Status status;
    private final TimeValue throttlePeriod;

    @Nullable
    private final Map<String, Object> metadata;

    @Nullable
    private final Transform transform;

    public Alert(String name, Clock clock, Schedule schedule, Input input, Condition condition, @Nullable Transform transform, Actions actions, @Nullable Map<String, Object> metadata, @Nullable TimeValue throttlePeriod, @Nullable Status status) {
        this.name = name;
        this.schedule = schedule;
        this.input = input;
        this.condition = condition;
        this.actions = actions;
        this.status = status != null ? status : new Status();
        this.throttlePeriod = throttlePeriod;
        this.metadata = metadata;
        this.transform = transform;
        throttler = new AlertThrottler(clock, throttlePeriod);
    }

    public String name() {
        return name;
    }

    public Schedule schedule() {
        return schedule;
    }

    public Input input() { return input;}

    public Condition condition() {
        return condition;
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

    /**
     * Acks this alert.
     *
     * @return  {@code true} if the status of this alert changed, {@code false} otherwise.
     */
    public boolean ack() {
        return status.onAck(new DateTime());
    }

    public boolean acked() {
        return status.ackStatus.state == Status.AckStatus.State.ACKED;
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
        builder.field(Parser.INPUT_FIELD.getPreferredName()).startObject().field(input.type(), input).endObject();
        builder.field(Parser.CONDITION_FIELD.getPreferredName()).startObject().field(condition.type(), condition).endObject();
        if (transform != null) {
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
        public static final ParseField INPUT_FIELD = new ParseField("input");
        public static final ParseField CONDITION_FIELD = new ParseField("condition");
        public static final ParseField ACTIONS_FIELD = new ParseField("actions");
        public static final ParseField TRANSFORM_FIELD = new ParseField("transform");
        public static final ParseField META_FIELD = new ParseField("meta");
        public static final ParseField STATUS_FIELD = new ParseField("status");
        public static final ParseField THROTTLE_PERIOD_FIELD = new ParseField("throttle_period");

        private final ConditionRegistry conditionRegistry;
        private final ScheduleRegistry scheduleRegistry;
        private final TransformRegistry transformRegistry;
        private final ActionRegistry actionRegistry;
        private final InputRegistry inputRegistry;
        private final Clock clock;

        private final Input defaultInput;
        private final Condition defaultCondition;

        @Inject
        public Parser(Settings settings, ConditionRegistry conditionRegistry, ScheduleRegistry scheduleRegistry,
                      TransformRegistry transformRegistry, ActionRegistry actionRegistry,
                      InputRegistry inputRegistry, Clock clock) {

            super(settings);
            this.conditionRegistry = conditionRegistry;
            this.scheduleRegistry = scheduleRegistry;
            this.transformRegistry = transformRegistry;
            this.actionRegistry = actionRegistry;
            this.inputRegistry = inputRegistry;
            this.clock = clock;

            this.defaultInput = new NoneInput(logger);
            this.defaultCondition = new AlwaysTrueCondition(logger);
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
            Input input = defaultInput;
            Condition condition = defaultCondition;
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
                    } else if (INPUT_FIELD.match(currentFieldName)) {
                        input = inputRegistry.parse(parser);
                    } else if (CONDITION_FIELD.match(currentFieldName)) {
                        condition = conditionRegistry.parse(parser);
                    } else if (ACTIONS_FIELD.match(currentFieldName)) {
                        actions = actionRegistry.parseActions(parser);
                    } else if (TRANSFORM_FIELD.match(currentFieldName)) {
                        transform = transformRegistry.parse(parser);
                    } else if (META_FIELD.match(currentFieldName)) {
                        metatdata = parser.map();
                    } else if (STATUS_FIELD.match(currentFieldName) && includeStatus) {
                        status = Status.parse(parser);
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
            if (actions == null) {
                throw new AlertsSettingsException("could not parse alert [" + name + "]. missing alert actions");
            }

            return new Alert(name, clock, schedule, input, condition, transform, actions, metatdata, throttlePeriod, status);
        }

    }

    public static class Status implements ToXContent, Streamable {

        public static final ParseField LAST_CHECKED_FIELD = new ParseField("last_checked");
        public static final ParseField LAST_MET_CONDITION_FIELD = new ParseField("last_met_condition");
        public static final ParseField LAST_THROTTLED_FIELD = new ParseField("last_throttled");
        public static final ParseField LAST_EXECUTED_FIELD = new ParseField("last_executed");
        public static final ParseField ACK_FIELD = new ParseField("ack");
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp");
        public static final ParseField REASON_FIELD = new ParseField("reason");

        private transient long version;

        private DateTime lastChecked;
        private DateTime lastMetCondition;
        private Throttle lastThrottle;
        private DateTime lastExecuted;
        private AckStatus ackStatus;

        public Status() {
            this(-1, null, null, null, null, new AckStatus());
        }

        public Status(Status other) {
            this(other.version, other.lastChecked, other.lastMetCondition, other.lastExecuted, other.lastThrottle, other.ackStatus);
        }

        private Status(long version, DateTime lastChecked, DateTime lastMetCondition, DateTime lastExecuted, Throttle lastThrottle, AckStatus ackStatus) {
            this.version = version;
            this.lastChecked = lastChecked;
            this.lastMetCondition = lastMetCondition;
            this.lastExecuted = lastExecuted;
            this.lastThrottle = lastThrottle;
            this.ackStatus = ackStatus;
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

        public boolean executed() {
            return lastExecuted != null;
        }

        public DateTime lastExecuted() {
            return lastExecuted;
        }

        public Throttle lastThrottle() {
            return lastThrottle;
        }

        public AckStatus ackStatus() {
            return ackStatus;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Status status = (Status) o;

            if (version != status.version) return false;
            if (!ackStatus.equals(status.ackStatus)) return false;
            if (lastChecked != null ? !lastChecked.equals(status.lastChecked) : status.lastChecked != null)
                return false;
            if (lastExecuted != null ? !lastExecuted.equals(status.lastExecuted) : status.lastExecuted != null)
                return false;
            if (lastMetCondition != null ? !lastMetCondition.equals(status.lastMetCondition) : status.lastMetCondition != null)
                return false;
            if (lastThrottle != null ? !lastThrottle.equals(status.lastThrottle) : status.lastThrottle != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (version ^ (version >>> 32));
            result = 31 * result + (lastChecked != null ? lastChecked.hashCode() : 0);
            result = 31 * result + (lastMetCondition != null ? lastMetCondition.hashCode() : 0);
            result = 31 * result + (lastThrottle != null ? lastThrottle.hashCode() : 0);
            result = 31 * result + (lastExecuted != null ? lastExecuted.hashCode() : 0);
            result = 31 * result + ackStatus.hashCode();
            return result;
        }

        /**
         * Called whenever an alert is checked, ie. the condition of the alert is evaluated to see if
         * the alert should be executed.
         *
         * @param metCondition  indicates whether the alert's condition was met.
         */
        public void onCheck(boolean metCondition, DateTime timestamp) {
            lastChecked = timestamp;
            if (metCondition) {
                lastMetCondition = timestamp;
            } else if (ackStatus.state == AckStatus.State.ACKED) {
                // didn't meet condition now after it met it in the past - we need to reset the ack state
                ackStatus = new AckStatus(AckStatus.State.AWAITS_EXECUTION, timestamp);
            }
        }

        /**
         * Called whenever an alert run is throttled
         */
        public void onThrottle(DateTime timestamp, String reason) {
            lastThrottle = new Throttle(timestamp, reason);
        }

        /**
         * Notified this status that the alert was executed. If the current state is {@link org.elasticsearch.alerts.Alert.Status.AckStatus.State#AWAITS_EXECUTION}, it will change to
         * {@link org.elasticsearch.alerts.Alert.Status.AckStatus.State#ACKABLE}.
         */
        public void onExecution(DateTime timestamp) {
            lastExecuted = timestamp;
            if (ackStatus.state == AckStatus.State.AWAITS_EXECUTION) {
                ackStatus = new AckStatus(AckStatus.State.ACKABLE, timestamp);
            }
        }

        /**
         * Notifies this status that the alert was acked. If the current state is {@link org.elasticsearch.alerts.Alert.Status.AckStatus.State#ACKABLE}, then we'll change it
         * to {@link org.elasticsearch.alerts.Alert.Status.AckStatus.State#ACKED} (when set to {@link org.elasticsearch.alerts.Alert.Status.AckStatus.State#ACKED}, the {@link org.elasticsearch.alerts.throttle.AckThrottler} will lastThrottle the
         * execution.
         *
         * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
         */
        boolean onAck(DateTime timestamp) {
            if (ackStatus.state == AckStatus.State.ACKABLE) {
                ackStatus = new AckStatus(AckStatus.State.ACKED, timestamp);
                return true;
            }
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(version);
            writeOptionalDate(out, lastChecked);
            writeOptionalDate(out, lastMetCondition);
            writeOptionalDate(out, lastExecuted);
            if (lastThrottle == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                writeDate(out, lastThrottle.timestamp);
                out.writeString(lastThrottle.reason);
            }
            out.writeString(ackStatus.state.name());
            writeDate(out, ackStatus.timestamp);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            version = in.readLong();
            lastChecked = readOptionalDate(in);
            lastMetCondition = readOptionalDate(in);
            lastExecuted = readOptionalDate(in);
            lastThrottle = in.readBoolean() ? new Throttle(readDate(in), in.readString()) : null;
            ackStatus = new AckStatus(AckStatus.State.valueOf(in.readString()), readDate(in));
        }

        public static Status read(StreamInput in) throws IOException {
            Alert.Status status = new Alert.Status();
            status.readFrom(in);
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (lastChecked != null) {
                builder.field(LAST_CHECKED_FIELD.getPreferredName(), lastChecked);
            }
            if (lastMetCondition != null) {
                builder.field(LAST_MET_CONDITION_FIELD.getPreferredName(), lastMetCondition);
            }
            if (lastExecuted != null) {
                builder.field(LAST_EXECUTED_FIELD.getPreferredName(), lastExecuted);
            }
            builder.startObject(ACK_FIELD.getPreferredName())
                    .field(STATE_FIELD.getPreferredName(), ackStatus.state.name().toLowerCase(Locale.ROOT))
                    .field(TIMESTAMP_FIELD.getPreferredName(), ackStatus.timestamp)
                    .endObject();
            if (lastThrottle != null) {
                builder.startObject(LAST_THROTTLED_FIELD.getPreferredName())
                        .field(TIMESTAMP_FIELD.getPreferredName(), lastThrottle.timestamp)
                        .field(REASON_FIELD.getPreferredName(), lastThrottle.reason)
                        .endObject();
            }
            return builder.endObject();
        }

        public static Status parse(XContentParser parser) throws IOException {

            DateTime lastChecked = null;
            DateTime lastMetCondition = null;
            Throttle lastThrottle = null;
            DateTime lastExecuted = null;
            AckStatus ackStatus = null;

            String currentFieldName = null;
            XContentParser.Token token = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (LAST_CHECKED_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastChecked = parseDate(currentFieldName, token, parser);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_MET_CONDITION_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastMetCondition = parseDate(currentFieldName, token, parser);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_EXECUTED_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastExecuted = parseDate(currentFieldName, token, parser);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_THROTTLED_FIELD.match(currentFieldName)) {
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
                        AckStatus.State state = null;
                        DateTime timestamp = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if (TIMESTAMP_FIELD.match(currentFieldName)) {
                                    timestamp = parseDate(currentFieldName, token, parser);
                                } else if (STATE_FIELD.match(currentFieldName)) {
                                    state = AckStatus.State.valueOf(parser.text().toUpperCase(Locale.ROOT));
                                } else {
                                    throw new AlertsException("unknown filed [" + currentFieldName + "] in alert status throttle entry");
                                }
                            }
                        }
                        ackStatus = new AckStatus(state, timestamp);
                    } else {
                        throw new AlertsException("expecting field [" + currentFieldName + "] to be an object, found [" + token + "] instead");
                    }
                }
            }

            return new Status(-1, lastChecked, lastMetCondition, lastExecuted, lastThrottle, ackStatus);
        }


        public static class AckStatus {

            public static enum State {
                AWAITS_EXECUTION,
                ACKABLE,
                ACKED
            }

            private final State state;
            private final DateTime timestamp;

            public AckStatus() {
                this(State.AWAITS_EXECUTION, new DateTime(DateTimeZone.UTC));
            }

            public AckStatus(State state, DateTime timestamp) {
                this.state = state;
                this.timestamp = timestamp;
            }

            public State state() {
                return state;
            }

            public DateTime timestamp() {
                return timestamp;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                AckStatus ackStatus = (AckStatus) o;

                if (state != ackStatus.state) return false;
                if (!timestamp.equals(ackStatus.timestamp)) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = state.hashCode();
                result = 31 * result + timestamp.hashCode();
                return result;
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

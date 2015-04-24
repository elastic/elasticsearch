/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

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
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.actions.ActionRegistry;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.ConditionRegistry;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.secret.SensitiveXContentParser;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.throttle.WatchThrottler;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerService;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.WatcherDateUtils.*;

public class Watch implements TriggerEngine.Job, ToXContent {

    private final static TimeValue DEFAULT_THROTTLE_PERIOD = new TimeValue(5, TimeUnit.SECONDS);
    private final static String DEFAULT_THROTTLE_PERIOD_SETTING = "watcher.throttle.period.default_period";

    private final String id;
    private final Trigger trigger;
    private final ExecutableInput input;
    private final ExecutableCondition condition;
    private final ExecutableActions actions;
    private final Throttler throttler;
    private final Status status;
    private final TimeValue throttlePeriod;

    @Nullable
    private final Map<String, Object> metadata;

    @Nullable
    private final ExecutableTransform transform;

    private final transient AtomicLong nonceCounter = new AtomicLong();

    public Watch(String id, Clock clock, LicenseService licenseService, Trigger trigger, ExecutableInput input, ExecutableCondition condition, @Nullable ExecutableTransform transform,
                 ExecutableActions actions, @Nullable Map<String, Object> metadata, @Nullable TimeValue throttlePeriod, @Nullable Status status) {
        this.id = id;
        this.trigger = trigger;
        this.input = input;
        this.condition = condition;
        this.actions = actions;
        this.status = status != null ? status : new Status();
        this.throttlePeriod = throttlePeriod;
        this.metadata = metadata;
        this.transform = transform;
        throttler = new WatchThrottler(clock, throttlePeriod, licenseService);
    }

    public String id() {
        return id;
    }

    public Trigger trigger() {
        return trigger;
    }

    public ExecutableInput input() { return input;}

    public ExecutableCondition condition() {
        return condition;
    }

    public ExecutableTransform transform() {
        return transform;
    }

    public Throttler throttler() {
        return throttler;
    }

    public ExecutableActions actions() {
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
     * Acks this watch.
     *
     * @return  {@code true} if the status of this watch changed, {@code false} otherwise.
     */
    public boolean ack() {
        return status.onAck(new DateTime(UTC));
    }

    public boolean acked() {
        return status.ackStatus.state == Status.AckStatus.State.ACKED;
    }

    public long nonce() {
        return nonceCounter.getAndIncrement();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Watch watch = (Watch) o;
        return watch.id.equals(id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.TRIGGER_FIELD.getPreferredName()).startObject().field(trigger.type(), trigger, params).endObject();
        builder.field(Parser.INPUT_FIELD.getPreferredName()).startObject().field(input.type(), input, params).endObject();
        builder.field(Parser.CONDITION_FIELD.getPreferredName()).startObject().field(condition.type(), condition, params).endObject();
        if (transform != null) {
            builder.field(Parser.TRANSFORM_FIELD.getPreferredName()).startObject().field(transform.type(), transform, params).endObject();
        }
        if (throttlePeriod != null) {
            builder.field(Parser.THROTTLE_PERIOD_FIELD.getPreferredName(), throttlePeriod.getMillis());
        }
        builder.field(Parser.ACTIONS_FIELD.getPreferredName(), actions, params);
        if (metadata != null) {
            builder.field(Parser.META_FIELD.getPreferredName(), metadata);
        }
        builder.field(Parser.STATUS_FIELD.getPreferredName(), status, params);
        builder.endObject();
        return builder;
    }

    public BytesReference getAsBytes() {
        // we don't want to cache this and instead rebuild it every time on demand. The watch is in
        // memory and we don't need this redundancy
        try {
            return toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS).bytes();
        } catch (IOException ioe) {
            throw new WatcherException("could not serialize watch [{}]", ioe, id);
        }
    }

    public static class Parser extends AbstractComponent {

        public static final ParseField TRIGGER_FIELD = new ParseField("trigger");
        public static final ParseField INPUT_FIELD = new ParseField("input");
        public static final ParseField CONDITION_FIELD = new ParseField("condition");
        public static final ParseField ACTIONS_FIELD = new ParseField("actions");
        public static final ParseField TRANSFORM_FIELD = new ParseField("transform");
        public static final ParseField META_FIELD = new ParseField("meta");
        public static final ParseField STATUS_FIELD = new ParseField("status");
        public static final ParseField THROTTLE_PERIOD_FIELD = new ParseField("throttle_period");

        private final LicenseService licenseService;
        private final ConditionRegistry conditionRegistry;
        private final TriggerService triggerService;
        private final TransformRegistry transformRegistry;
        private final ActionRegistry actionRegistry;
        private final InputRegistry inputRegistry;
        private final Clock clock;
        private final SecretService secretService;

        private final ExecutableInput defaultInput;
        private final ExecutableCondition defaultCondition;
        private final TimeValue defaultThrottleTimePeriod;

        @Inject
        public Parser(Settings settings, LicenseService licenseService, ConditionRegistry conditionRegistry, TriggerService triggerService,
                      TransformRegistry transformRegistry, ActionRegistry actionRegistry,
                      InputRegistry inputRegistry, Clock clock, SecretService secretService) {

            super(settings);
            this.licenseService = licenseService;
            this.conditionRegistry = conditionRegistry;
            this.transformRegistry = transformRegistry;
            this.triggerService = triggerService;
            this.actionRegistry = actionRegistry;
            this.inputRegistry = inputRegistry;
            this.clock = clock;
            this.secretService = secretService;

            this.defaultInput = new ExecutableNoneInput(logger);
            this.defaultCondition = new ExecutableAlwaysCondition(logger);
            this.defaultThrottleTimePeriod = settings.getAsTime(DEFAULT_THROTTLE_PERIOD_SETTING, DEFAULT_THROTTLE_PERIOD);
        }

        public Watch parse(String name, boolean includeStatus, BytesReference source) {
            return parse(name, includeStatus, false, source);
        }

        /**
         * Parses the watch represented by the given source. When parsing, any sensitive data that the
         * source might contain (e.g. passwords) will be converted to {@link org.elasticsearch.watcher.support.secret.Secret secrets}
         * Such that the returned watch will potentially hide this sensitive data behind a "secret". A secret
         * is an abstraction around sensitive data (text). There can be different implementations of how the
         * secret holds the data, depending on the wired up {@link SecretService}. When shield is installed, a
         * {@link org.elasticsearch.watcher.shield.ShieldSecretService} is used, that potentially encrypts the data
         * using Shield's configured system key.
         *
         * This method is only called once - when the user adds a new watch. From that moment on, all representations
         * of the watch in the system will be use secrets for sensitive data.
         *
         * @see org.elasticsearch.watcher.WatcherService#putWatch(String, BytesReference)
         */
        public Watch parseWithSecrets(String id, boolean includeStatus, BytesReference source) {
            return parse(id, includeStatus, true, source);
        }

        private Watch parse(String id, boolean includeStatus, boolean withSecrets, BytesReference source) {
            if (logger.isTraceEnabled()) {
                logger.trace("parsing watch [{}] ", source.toUtf8());
            }
            XContentParser parser = null;
            try {
                parser = XContentHelper.createParser(source);
                if (withSecrets) {
                    parser = new SensitiveXContentParser(parser, secretService);
                }
                return parse(id, includeStatus, parser);
            } catch (IOException ioe) {
                throw new WatcherException("could not parse watch [{}]", ioe, id);
            } finally {
                if (parser != null) {
                    parser.close();
                }
            }
        }

        public Watch parse(String id, boolean includeStatus, XContentParser parser) throws IOException {
            Trigger trigger = null;
            ExecutableInput input = defaultInput;
            ExecutableCondition condition = defaultCondition;
            ExecutableActions actions = null;
            ExecutableTransform transform = null;
            Map<String, Object> metatdata = null;
            Status status = null;
            TimeValue throttlePeriod = defaultThrottleTimePeriod;

            String currentFieldName = null;
            XContentParser.Token token = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == null ){
                    throw new WatcherException("could not parse watch [" + id + "]. null token");
                } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) && currentFieldName !=null ) {
                    if (TRIGGER_FIELD.match(currentFieldName)) {
                        trigger = triggerService.parseTrigger(id, parser);
                    } else if (INPUT_FIELD.match(currentFieldName)) {
                        input = inputRegistry.parse(id, parser);
                    } else if (CONDITION_FIELD.match(currentFieldName)) {
                        condition = conditionRegistry.parseExecutable(id, parser);
                    } else if (ACTIONS_FIELD.match(currentFieldName)) {
                        actions = actionRegistry.parseActions(id, parser);
                    } else if (TRANSFORM_FIELD.match(currentFieldName)) {
                        transform = transformRegistry.parse(id, parser);
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
                            throw new WatcherSettingsException("could not parse watch [" + id + "] throttle period. could not parse token [" + token + "] as time value (must either be string or number)");
                        }
                    }
                }
            }
            if (trigger == null) {
                throw new WatcherSettingsException("could not parse watch [" + id + "]. missing watch trigger");
            }
            if (actions == null) {
                throw new WatcherSettingsException("could not parse watch [" + id + "]. missing watch actions");
            }

            return new Watch(id, clock, licenseService, trigger, input, condition, transform, actions, metatdata, throttlePeriod, status);
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

        private volatile boolean dirty = false;

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

        /**
         * @param dirty if true this Watch.Status has been modified since it was read, if false we just wrote the updated watch
         */
        public void dirty(boolean dirty) {
            this.dirty = dirty;
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
         * Called whenever an watch is checked, ie. the condition of the watch is evaluated to see if
         * the watch should be executed.
         *
         * @param metCondition  indicates whether the watch's condition was met.
         */
        public void onCheck(boolean metCondition, DateTime timestamp) {
            lastChecked = timestamp;
            if (metCondition) {
                lastMetCondition = timestamp;
                dirty(true);
            } else if (ackStatus.state == AckStatus.State.ACKED) {
                // didn't meet condition now after it met it in the past - we need to reset the ack state
                ackStatus = new AckStatus(AckStatus.State.AWAITS_EXECUTION, timestamp);
                dirty(true);
            }
        }

        /**
         * Called whenever an watch run is throttled
         */
        public void onThrottle(DateTime timestamp, String reason) {
            lastThrottle = new Throttle(timestamp, reason);
            dirty(true);
        }

        /**
         * Notified this status that the watch was executed. If the current state is {@link Watch.Status.AckStatus.State#AWAITS_EXECUTION}, it will change to
         * {@link Watch.Status.AckStatus.State#ACKABLE}.
         * @return {@code true} if the state changed due to the execution {@code false} otherwise
         */
        public boolean onExecution(DateTime timestamp) {
            lastExecuted = timestamp;
            if (ackStatus.state == AckStatus.State.AWAITS_EXECUTION) {
                ackStatus = new AckStatus(AckStatus.State.ACKABLE, timestamp);
                dirty(true);
                return true;
            }
            return false;
        }

        /**
         * Notifies this status that the watch was acked. If the current state is {@link Watch.Status.AckStatus.State#ACKABLE}, then we'll change it
         * to {@link Watch.Status.AckStatus.State#ACKED} (when set to {@link Watch.Status.AckStatus.State#ACKED}, the {@link org.elasticsearch.watcher.throttle.AckThrottler} will lastThrottle the
         * execution.
         *
         * @return {@code true} if the state of changed due to the ack, {@code false} otherwise.
         */
        boolean onAck(DateTime timestamp) {
            if (ackStatus.state == AckStatus.State.ACKABLE) {
                ackStatus = new AckStatus(AckStatus.State.ACKED, timestamp);
                dirty(true);
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
            lastChecked = readOptionalDate(in, UTC);
            lastMetCondition = readOptionalDate(in, UTC);
            lastExecuted = readOptionalDate(in, UTC);
            lastThrottle = in.readBoolean() ? new Throttle(readDate(in, UTC), in.readString()) : null;
            ackStatus = new AckStatus(AckStatus.State.valueOf(in.readString()), readDate(in, UTC));
        }

        public static Status read(StreamInput in) throws IOException {
            Watch.Status status = new Watch.Status();
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
                        lastChecked = parseDate(currentFieldName, token, parser, UTC);
                    } else {
                        throw new WatcherException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_MET_CONDITION_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastMetCondition = parseDate(currentFieldName, token, parser, UTC);
                    } else {
                        throw new WatcherException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
                    }
                } else if (LAST_EXECUTED_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        lastExecuted = parseDate(currentFieldName, token, parser, UTC);
                    } else {
                        throw new WatcherException("expecting field [" + currentFieldName + "] to hold a date value, found [" + token + "] instead");
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
                                    timestamp = parseDate(currentFieldName, token, parser, UTC);
                                } else if (REASON_FIELD.match(currentFieldName)) {
                                    reason = parser.text();
                                } else {
                                    throw new WatcherException("unknown field [" + currentFieldName + "] in watch status throttle entry");
                                }
                            }
                        }
                        lastThrottle = new Throttle(timestamp, reason);
                    } else {
                        throw new WatcherException("expecting field [" + currentFieldName + "] to be an object, found [" + token + "] instead");
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
                                    timestamp = parseDate(currentFieldName, token, parser, UTC);
                                } else if (STATE_FIELD.match(currentFieldName)) {
                                    state = AckStatus.State.valueOf(parser.text().toUpperCase(Locale.ROOT));
                                } else {
                                    throw new WatcherException("unknown field [" + currentFieldName + "] in watch status throttle entry");
                                }
                            }
                        }
                        ackStatus = new AckStatus(state, timestamp);
                    } else {
                        throw new WatcherException("expecting field [" + currentFieldName + "] to be an object, found [" + token + "] instead");
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
                this(State.AWAITS_EXECUTION, new DateTime(UTC));
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

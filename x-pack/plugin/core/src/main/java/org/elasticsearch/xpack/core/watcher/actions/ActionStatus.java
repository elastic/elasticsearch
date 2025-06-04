/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;
import static org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils.dateTimeFormatter;

public class ActionStatus implements ToXContentObject {

    private AckStatus ackStatus;
    @Nullable
    private Execution lastExecution;
    @Nullable
    private Execution lastSuccessfulExecution;
    @Nullable
    private Throttle lastThrottle;

    public ActionStatus(ZonedDateTime now) {
        this(new AckStatus(now, AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION), null, null, null);
    }

    public ActionStatus(
        AckStatus ackStatus,
        @Nullable Execution lastExecution,
        @Nullable Execution lastSuccessfulExecution,
        @Nullable Throttle lastThrottle
    ) {
        this.ackStatus = ackStatus;
        this.lastExecution = lastExecution;
        this.lastSuccessfulExecution = lastSuccessfulExecution;
        this.lastThrottle = lastThrottle;
    }

    public AckStatus ackStatus() {
        return ackStatus;
    }

    public Execution lastExecution() {
        return lastExecution;
    }

    public Execution lastSuccessfulExecution() {
        return lastSuccessfulExecution;
    }

    public Throttle lastThrottle() {
        return lastThrottle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionStatus that = (ActionStatus) o;

        return Objects.equals(ackStatus, that.ackStatus)
            && Objects.equals(lastExecution, that.lastExecution)
            && Objects.equals(lastSuccessfulExecution, that.lastSuccessfulExecution)
            && Objects.equals(lastThrottle, that.lastThrottle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    public void update(ZonedDateTime timestamp, Action.Result result) {
        switch (result.status()) {

            case FAILURE:
                String reason = result instanceof Action.Result.Failure ? ((Action.Result.Failure) result).reason() : "";
                lastExecution = Execution.failure(timestamp, reason);
                return;

            case THROTTLED:
                reason = result instanceof Action.Result.Throttled ? ((Action.Result.Throttled) result).reason() : "";
                lastThrottle = new Throttle(timestamp, reason);
                return;

            case SUCCESS:
            case SIMULATED:
                lastExecution = Execution.successful(timestamp);
                lastSuccessfulExecution = lastExecution;
                if (ackStatus.state == AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION) {
                    ackStatus = new AckStatus(timestamp, AckStatus.State.ACKABLE);
                }
        }
    }

    public boolean onAck(ZonedDateTime timestamp) {
        if (ackStatus.state == AckStatus.State.ACKABLE) {
            ackStatus = new AckStatus(timestamp, AckStatus.State.ACKED);
            return true;
        }
        return false;
    }

    public boolean resetAckStatus(ZonedDateTime timestamp) {
        if (ackStatus.state != AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION) {
            ackStatus = new AckStatus(timestamp, AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION);
            return true;
        }
        return false;
    }

    public static void writeTo(ActionStatus status, StreamOutput out) throws IOException {
        AckStatus.writeTo(status.ackStatus, out);
        out.writeBoolean(status.lastExecution != null);
        if (status.lastExecution != null) {
            Execution.writeTo(status.lastExecution, out);
        }
        out.writeBoolean(status.lastSuccessfulExecution != null);
        if (status.lastSuccessfulExecution != null) {
            Execution.writeTo(status.lastSuccessfulExecution, out);
        }
        out.writeBoolean(status.lastThrottle != null);
        if (status.lastThrottle != null) {
            Throttle.writeTo(status.lastThrottle, out);
        }
    }

    public static ActionStatus readFrom(StreamInput in) throws IOException {
        AckStatus ackStatus = AckStatus.readFrom(in);
        Execution lastExecution = in.readBoolean() ? Execution.readFrom(in) : null;
        Execution lastSuccessfulExecution = in.readBoolean() ? Execution.readFrom(in) : null;
        Throttle lastThrottle = in.readBoolean() ? Throttle.readFrom(in) : null;
        return new ActionStatus(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.ACK_STATUS.getPreferredName(), ackStatus, params);
        if (lastExecution != null) {
            builder.field(Field.LAST_EXECUTION.getPreferredName(), lastExecution, params);
        }
        if (lastSuccessfulExecution != null) {
            builder.field(Field.LAST_SUCCESSFUL_EXECUTION.getPreferredName(), lastSuccessfulExecution, params);
        }
        if (lastThrottle != null) {
            builder.field(Field.LAST_THROTTLE.getPreferredName(), lastThrottle, params);
        }
        return builder.endObject();
    }

    public static ActionStatus parse(String watchId, String actionId, XContentParser parser) throws IOException {
        AckStatus ackStatus = null;
        Execution lastExecution = null;
        Execution lastSuccessfulExecution = null;
        Throttle lastThrottle = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.ACK_STATUS.match(currentFieldName, parser.getDeprecationHandler())) {
                ackStatus = AckStatus.parse(watchId, actionId, parser);
            } else if (Field.LAST_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                lastExecution = Execution.parse(watchId, actionId, parser);
            } else if (Field.LAST_SUCCESSFUL_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                lastSuccessfulExecution = Execution.parse(watchId, actionId, parser);
            } else if (Field.LAST_THROTTLE.match(currentFieldName, parser.getDeprecationHandler())) {
                lastThrottle = Throttle.parse(watchId, actionId, parser);
            } else {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. unexpected field [{}]",
                    watchId,
                    actionId,
                    currentFieldName
                );
            }
        }
        if (ackStatus == null) {
            throw new ElasticsearchParseException(
                "could not parse action status for [{}/{}]. missing required field [{}]",
                watchId,
                actionId,
                Field.ACK_STATUS.getPreferredName()
            );
        }
        return new ActionStatus(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    public static class AckStatus implements ToXContentObject {

        public enum State {
            AWAITS_SUCCESSFUL_EXECUTION((byte) 1),
            ACKABLE((byte) 2),
            ACKED((byte) 3);

            private byte value;

            State(byte value) {
                this.value = value;
            }

            static State resolve(byte value) {
                return switch (value) {
                    case 1 -> AWAITS_SUCCESSFUL_EXECUTION;
                    case 2 -> ACKABLE;
                    case 3 -> ACKED;
                    default -> throw illegalArgument("unknown action ack status state value [{}]", value);
                };
            }
        }

        private final ZonedDateTime timestamp;
        private final State state;

        public AckStatus(ZonedDateTime timestamp, State state) {
            this.timestamp = timestamp;
            this.state = state;
        }

        public ZonedDateTime timestamp() {
            return timestamp;
        }

        public State state() {
            return state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AckStatus ackStatus = (AckStatus) o;

            return Objects.equals(timestamp, ackStatus.timestamp) && Objects.equals(state, ackStatus.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, state);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(Field.TIMESTAMP.getPreferredName())
                .value(dateTimeFormatter.format(timestamp))
                .field(Field.ACK_STATUS_STATE.getPreferredName(), state.name().toLowerCase(Locale.ROOT))
                .endObject();
        }

        public static AckStatus parse(String watchId, String actionId, XContentParser parser) throws IOException {
            ZonedDateTime timestamp = null;
            State state = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = DateFormatters.from(dateTimeFormatter.parse(parser.text()));
                } else if (Field.ACK_STATUS_STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                    state = State.valueOf(parser.text().toUpperCase(Locale.ROOT));
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse action status for [{}/{}]. unexpected field [{}.{}]",
                        watchId,
                        actionId,
                        Field.ACK_STATUS.getPreferredName(),
                        currentFieldName
                    );
                }
            }
            if (timestamp == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId,
                    actionId,
                    Field.ACK_STATUS.getPreferredName(),
                    Field.TIMESTAMP.getPreferredName()
                );
            }
            if (state == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId,
                    actionId,
                    Field.ACK_STATUS.getPreferredName(),
                    Field.ACK_STATUS_STATE.getPreferredName()
                );
            }
            return new AckStatus(timestamp, state);
        }

        static void writeTo(AckStatus status, StreamOutput out) throws IOException {
            out.writeLong(status.timestamp.toInstant().toEpochMilli());
            out.writeByte(status.state.value);
        }

        static AckStatus readFrom(StreamInput in) throws IOException {
            ZonedDateTime timestamp = Instant.ofEpochMilli(in.readLong()).atZone(ZoneOffset.UTC);
            State state = State.resolve(in.readByte());
            return new AckStatus(timestamp, state);
        }
    }

    public record Execution(ZonedDateTime timestamp, boolean successful, String reason) implements ToXContentObject {

        public static Execution successful(ZonedDateTime timestamp) {
            return new Execution(timestamp, true, null);
        }

        public static Execution failure(ZonedDateTime timestamp, String reason) {
            return new Execution(timestamp, false, reason);
        }

        public Execution(ZonedDateTime timestamp, boolean successful, String reason) {
            this.timestamp = timestamp.withZoneSameInstant(ZoneOffset.UTC);
            this.successful = successful;
            this.reason = reason;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.TIMESTAMP.getPreferredName()).value(dateTimeFormatter.format(timestamp));
            builder.field(Field.EXECUTION_SUCCESSFUL.getPreferredName(), successful);
            if (reason != null) {
                builder.field(Field.REASON.getPreferredName(), reason);
            }
            return builder.endObject();
        }

        public static Execution parse(String watchId, String actionId, XContentParser parser) throws IOException {
            ZonedDateTime timestamp = null;
            Boolean successful = null;
            String reason = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = DateFormatters.from(dateTimeFormatter.parse(parser.text()));
                } else if (Field.EXECUTION_SUCCESSFUL.match(currentFieldName, parser.getDeprecationHandler())) {
                    successful = parser.booleanValue();
                } else if (Field.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse action status for [{}/{}]. unexpected field [{}.{}]",
                        watchId,
                        actionId,
                        Field.LAST_EXECUTION.getPreferredName(),
                        currentFieldName
                    );
                }
            }
            if (timestamp == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId,
                    actionId,
                    Field.LAST_EXECUTION.getPreferredName(),
                    Field.TIMESTAMP.getPreferredName()
                );
            }
            if (successful == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId,
                    actionId,
                    Field.LAST_EXECUTION.getPreferredName(),
                    Field.EXECUTION_SUCCESSFUL.getPreferredName()
                );
            }
            if (successful) {
                return successful(timestamp);
            }
            if (reason == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field for unsuccessful" + " execution [{}.{}]",
                    watchId,
                    actionId,
                    Field.LAST_EXECUTION.getPreferredName(),
                    Field.REASON.getPreferredName()
                );
            }
            return failure(timestamp, reason);
        }

        public static void writeTo(Execution execution, StreamOutput out) throws IOException {
            out.writeLong(execution.timestamp.toInstant().toEpochMilli());
            out.writeBoolean(execution.successful);
            if (execution.successful == false) {
                out.writeString(execution.reason);
            }
        }

        public static Execution readFrom(StreamInput in) throws IOException {
            ZonedDateTime timestamp = Instant.ofEpochMilli(in.readLong()).atZone(ZoneOffset.UTC);
            boolean successful = in.readBoolean();
            if (successful) {
                return successful(timestamp);
            }
            return failure(timestamp, in.readString());
        }
    }

    public record Throttle(ZonedDateTime timestamp, String reason) implements ToXContentObject {

        public Throttle(ZonedDateTime timestamp, String reason) {
            this.timestamp = timestamp.withZoneSameInstant(ZoneOffset.UTC);
            this.reason = reason;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(Field.TIMESTAMP.getPreferredName())
                .value(dateTimeFormatter.format(timestamp))
                .field(Field.REASON.getPreferredName(), reason)
                .endObject();
        }

        public static Throttle parse(String watchId, String actionId, XContentParser parser) throws IOException {
            ZonedDateTime timestamp = null;
            String reason = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = DateFormatters.from(dateTimeFormatter.parse(parser.text()));
                } else if (Field.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse action status for [{}/{}]. unexpected field [{}.{}]",
                        watchId,
                        actionId,
                        Field.LAST_THROTTLE.getPreferredName(),
                        currentFieldName
                    );
                }
            }
            if (timestamp == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId,
                    actionId,
                    Field.LAST_THROTTLE.getPreferredName(),
                    Field.TIMESTAMP.getPreferredName()
                );
            }
            if (reason == null) {
                throw new ElasticsearchParseException(
                    "could not parse action status for [{}/{}]. missing required field [{}.{}]",
                    watchId,
                    actionId,
                    Field.LAST_THROTTLE.getPreferredName(),
                    Field.REASON.getPreferredName()
                );
            }
            return new Throttle(timestamp, reason);
        }

        static void writeTo(Throttle throttle, StreamOutput out) throws IOException {
            out.writeLong(throttle.timestamp.toInstant().toEpochMilli());
            out.writeString(throttle.reason);
        }

        static Throttle readFrom(StreamInput in) throws IOException {
            ZonedDateTime timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.readLong()), ZoneOffset.UTC);
            return new Throttle(timestamp, in.readString());
        }
    }

    interface Field {
        ParseField ACK_STATUS = new ParseField("ack");
        ParseField ACK_STATUS_STATE = new ParseField("state");

        ParseField LAST_EXECUTION = new ParseField("last_execution");
        ParseField LAST_SUCCESSFUL_EXECUTION = new ParseField("last_successful_execution");
        ParseField EXECUTION_SUCCESSFUL = new ParseField("successful");

        ParseField LAST_THROTTLE = new ParseField("last_throttle");

        ParseField TIMESTAMP = new ParseField("timestamp");
        ParseField REASON = new ParseField("reason");
    }
}

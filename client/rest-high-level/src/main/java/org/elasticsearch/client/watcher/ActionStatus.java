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
import java.util.Locale;
import java.util.Objects;

public class ActionStatus {

    private final AckStatus ackStatus;
    @Nullable private final Execution lastExecution;
    @Nullable private final Execution lastSuccessfulExecution;
    @Nullable private final Throttle lastThrottle;

    public ActionStatus(AckStatus ackStatus,
                        @Nullable Execution lastExecution,
                        @Nullable Execution lastSuccessfulExecution,
                        @Nullable Throttle lastThrottle) {
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

        return Objects.equals(ackStatus, that.ackStatus) &&
                Objects.equals(lastExecution, that.lastExecution) &&
                Objects.equals(lastSuccessfulExecution, that.lastSuccessfulExecution) &&
                Objects.equals(lastThrottle, that.lastThrottle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    public static ActionStatus parse(String actionId, XContentParser parser) throws IOException {
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
                ackStatus = AckStatus.parse(actionId, parser);
            } else if (Field.LAST_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                lastExecution = Execution.parse(actionId, parser);
            } else if (Field.LAST_SUCCESSFUL_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                lastSuccessfulExecution = Execution.parse(actionId, parser);
            } else if (Field.LAST_THROTTLE.match(currentFieldName, parser.getDeprecationHandler())) {
                lastThrottle = Throttle.parse(actionId, parser);
            } else {
                parser.skipChildren();
            }
        }
        if (ackStatus == null) {
            throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}]",
                    actionId, Field.ACK_STATUS.getPreferredName());
        }
        return new ActionStatus(ackStatus, lastExecution, lastSuccessfulExecution, lastThrottle);
    }

    public static class AckStatus {

        public enum State {
            AWAITS_SUCCESSFUL_EXECUTION,
            ACKABLE,
            ACKED;
        }

        private final ZonedDateTime timestamp;
        private final State state;

        public AckStatus(ZonedDateTime timestamp, State state) {
            assert timestamp.getOffset() == ZoneOffset.UTC;
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

            return Objects.equals(timestamp, ackStatus.timestamp) &&  Objects.equals(state, ackStatus.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, state);
        }

        public static AckStatus parse(String actionId, XContentParser parser) throws IOException {
            ZonedDateTime timestamp = null;
            State state = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = WatchStatusDateParser.parseDate(parser.text());
                } else if (Field.ACK_STATUS_STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                    state = State.valueOf(parser.text().toUpperCase(Locale.ROOT));
                } else {
                    parser.skipChildren();
                }
            }
            if (timestamp == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}.{}]",
                        actionId, Field.ACK_STATUS.getPreferredName(), Field.TIMESTAMP.getPreferredName());
            }
            if (state == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}.{}]",
                        actionId, Field.ACK_STATUS.getPreferredName(), Field.ACK_STATUS_STATE.getPreferredName());
            }
            return new AckStatus(timestamp, state);
        }
    }

    public static class Execution {

        public static Execution successful(ZonedDateTime timestamp) {
            return new Execution(timestamp, true, null);
        }

        public static Execution failure(ZonedDateTime timestamp, String reason) {
            return new Execution(timestamp, false, reason);
        }

        private final ZonedDateTime timestamp;
        private final boolean successful;
        private final String reason;

        private Execution(ZonedDateTime timestamp, boolean successful, String reason) {
            this.timestamp = timestamp.withZoneSameInstant(ZoneOffset.UTC);
            this.successful = successful;
            this.reason = reason;
        }

        public ZonedDateTime timestamp() {
            return timestamp;
        }

        public boolean successful() {
            return successful;
        }

        public String reason() {
            return reason;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Execution execution = (Execution) o;

            return Objects.equals(successful, execution.successful) &&
                   Objects.equals(timestamp, execution.timestamp) &&
                   Objects.equals(reason, execution.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, successful, reason);
        }

        public static Execution parse(String actionId, XContentParser parser) throws IOException {
            ZonedDateTime timestamp = null;
            Boolean successful = null;
            String reason = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = WatchStatusDateParser.parseDate(parser.text());
                } else if (Field.EXECUTION_SUCCESSFUL.match(currentFieldName, parser.getDeprecationHandler())) {
                    successful = parser.booleanValue();
                } else if (Field.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
            if (timestamp == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}.{}]",
                        actionId, Field.LAST_EXECUTION.getPreferredName(), Field.TIMESTAMP.getPreferredName());
            }
            if (successful == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}.{}]",
                        actionId, Field.LAST_EXECUTION.getPreferredName(), Field.EXECUTION_SUCCESSFUL.getPreferredName());
            }
            if (successful) {
                return successful(timestamp);
            }
            if (reason == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field for unsuccessful" +
                        " execution [{}.{}]", actionId, Field.LAST_EXECUTION.getPreferredName(), Field.REASON.getPreferredName());
            }
            return failure(timestamp, reason);
        }
    }

    public static class Throttle {

        private final ZonedDateTime timestamp;
        private final String reason;

        public Throttle(ZonedDateTime timestamp, String reason) {
            this.timestamp = timestamp.withZoneSameInstant(ZoneOffset.UTC);
            this.reason = reason;
        }

        public ZonedDateTime timestamp() {
            return timestamp;
        }

        public String reason() {
            return reason;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Throttle throttle = (Throttle) o;
            return Objects.equals(timestamp, throttle.timestamp) && Objects.equals(reason, throttle.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, reason);
        }

        public static Throttle parse(String actionId, XContentParser parser) throws IOException {
            ZonedDateTime timestamp = null;
            String reason = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.TIMESTAMP.match(currentFieldName, parser.getDeprecationHandler())) {
                    timestamp = WatchStatusDateParser.parseDate(parser.text());
                } else if (Field.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
            if (timestamp == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}.{}]",
                        actionId, Field.LAST_THROTTLE.getPreferredName(), Field.TIMESTAMP.getPreferredName());
            }
            if (reason == null) {
                throw new ElasticsearchParseException("could not parse action status for [{}]. missing required field [{}.{}]",
                    actionId, Field.LAST_THROTTLE.getPreferredName(), Field.REASON.getPreferredName());
            }
            return new Throttle(timestamp, reason);
        }
    }

    private interface Field {
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

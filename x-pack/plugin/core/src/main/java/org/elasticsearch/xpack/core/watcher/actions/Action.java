/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

public interface Action extends ToXContentObject {

    String type();

    abstract class Result implements ToXContentFragment {

        public enum Status {
            SUCCESS,
            FAILURE,
            PARTIAL_FAILURE,
            ACKNOWLEDGED,
            THROTTLED,
            CONDITION_FAILED,
            SIMULATED;

            public String value() {
                return name().toLowerCase(Locale.ROOT);
            }
        }

        protected final String type;
        protected final Status status;

        protected Result(String type, Status status) {
            this.type = type;
            this.status = status;
        }

        public String type() {
            return type;
        }

        public Status status() {
            return status;
        }

        /**
         * {@code StoppedResult} is a {@link Result} with a {@link #reason()}.
         * <p>
         * Any {@code StoppedResult} should provide a reason <em>why</em> it is stopped.
         */
        public static class StoppedResult extends Result {

            private static ParseField REASON = new ParseField("reason");

            private final String reason;

            protected StoppedResult(String type, Status status, String reason, Object... args) {
                super(type, status);
                this.reason = LoggerMessageFormat.format(reason, args);
            }

            public String reason() {
                return reason;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.field(REASON.getPreferredName(), reason);
            }

        }

        /**
         * {@code Failure} is a {@link StoppedResult} with a status of {@link Status#FAILURE} for actiosn that have failed unexpectedly
         * (e.g., an exception was thrown in a place that wouldn't expect one, like transformation or an HTTP request).
         */
        public static class Failure extends StoppedResult {

            public Failure(String type, String reason, Object... args) {
                super(type, Status.FAILURE, reason, args);
            }
        }

        public static class FailureWithException extends Result {

            private final Exception exception;

            public FailureWithException(String type, Exception exception) {
                super(type, Status.FAILURE);
                this.exception = exception;
            }

            public Exception getException() {
                return exception;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                return builder;
            }
        }

        /**
         * {@code Throttled} is a {@link StoppedResult} with a status of {@link Status#THROTTLED} for actions that have been throttled.
         */
        public static class Throttled extends StoppedResult {

            public Throttled(String type, String reason) {
                super(type, Status.THROTTLED, reason);
            }

        }

        /**
         * {@code Acknowledged} is a {@link StoppedResult} with a status of {@link Status#ACKNOWLEDGED} for actions that
         * have been throttled.
         */
        public static class Acknowledged extends StoppedResult {

            public Acknowledged(String type, String reason) {
                super(type, Status.ACKNOWLEDGED, reason);
            }
        }

        /**
         * {@code ConditionFailed} is a {@link StoppedResult} with a status of {@link Status#FAILURE} for actions that have been skipped
         * because the action's condition failed (either expected or unexpected).
         */
        public static class ConditionFailed extends StoppedResult {

            public ConditionFailed(String type, String reason, Object... args) {
                super(type, Status.CONDITION_FAILED, reason, args);
            }

        }
    }

    interface Builder<A extends Action> {

        A build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public interface Action extends ToXContent {

    String type();

    abstract class Result implements ToXContent {

        public enum Status implements ToXContent {
            SUCCESS,
            FAILURE,
            PARTIAL_FAILURE,
            THROTTLED,
            SIMULATED;

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.value(name().toLowerCase(Locale.ROOT));
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

        public static class Failure extends Result {

            private final String reason;

            public Failure(String type, String reason, Object... args) {
                super(type, Status.FAILURE);
                this.reason = LoggerMessageFormat.format(reason, args);
            }

            public String reason() {
                return reason;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REASON.getPreferredName(), reason);
            }
        }

        public static class Throttled extends Result {

            private final String reason;

            public Throttled(String type, String reason) {
                super(type, Status.THROTTLED);
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REASON.getPreferredName(), reason);
            }
        }
    }

    interface Builder<A extends Action> {

        A build();
    }

    interface Field {
        ParseField REASON = new ParseField("reason");
    }
}

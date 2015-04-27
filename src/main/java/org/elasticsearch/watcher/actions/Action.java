/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
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

        public enum Status { SUCCESS, FAILURE, THROTTLED, SIMULATED }

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

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.STATUS.getPreferredName(), status.name().toLowerCase(Locale.ROOT));
            xContentBody(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException;

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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
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
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.REASON.getPreferredName(), reason);
            }
        }
    }

    interface Builder<A extends Action> {

        A build();
    }

    interface Field {
        ParseField STATUS = new ParseField("status");
        ParseField REASON = new ParseField("reason");
    }
}

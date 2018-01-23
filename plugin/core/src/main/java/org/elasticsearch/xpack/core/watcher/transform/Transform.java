/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transform;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.Locale;

public interface Transform extends ToXContentObject {

    ParseField TRANSFORM = new ParseField("transform");

    String type();

    abstract class Result implements ToXContentObject {

        private static final ParseField TYPE = new ParseField("type");
        private static final ParseField STATUS = new ParseField("status");
        private static final ParseField PAYLOAD = new ParseField("payload");
        private static final ParseField REASON = new ParseField("reason");

        public enum Status {
            SUCCESS, FAILURE
        }

        protected final String type;
        protected final Status status;
        @Nullable protected final Payload payload;
        @Nullable protected final String reason;
        @Nullable protected final Exception exception;

        public Result(String type, Payload payload) {
            this.type = type;
            this.status = Status.SUCCESS;
            this.payload = payload;
            this.reason = null;
            this.exception = null;
        }

        public Result(String type, String reason) {
            this.type = type;
            this.status = Status.FAILURE;
            this.reason = reason;
            this.payload = null;
            this.exception = null;
        }

        public Result(String type, Exception e) {
            this.type = type;
            this.status = Status.FAILURE;
            this.reason = e.getMessage();
            this.payload = null;
            this.exception = e;
        }

        public String type() {
            return type;
        }

        public Status status() {
            return status;
        }

        public Payload payload() {
            assert status == Status.SUCCESS;
            return payload;
        }

        public String reason() {
            assert status == Status.FAILURE;
            return reason;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE.getPreferredName(), type);
            builder.field(STATUS.getPreferredName(), status.name().toLowerCase(Locale.ROOT));
            switch (status) {
                case SUCCESS:
                    assert exception == null;
                    builder.field(PAYLOAD.getPreferredName(), payload, params);
                    break;
                case FAILURE:
                    assert payload == null;
                    builder.field(REASON.getPreferredName(), reason);
                    ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                    break;
                default:
                    assert false;
            }
            typeXContent(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException;

    }

    interface Builder<T extends Transform> {

        T build();
    }
}

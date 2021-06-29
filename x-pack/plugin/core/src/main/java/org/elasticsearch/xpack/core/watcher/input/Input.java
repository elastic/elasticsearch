/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.input;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.Locale;

public interface Input extends ToXContentObject {

    String type();

    abstract class Result implements ToXContentObject {

        private static final ParseField STATUS = new ParseField("status");
        private static final ParseField TYPE = new ParseField("type");
        private static final ParseField PAYLOAD = new ParseField("payload");

        public enum Status {
            SUCCESS, FAILURE
        }

        protected final String type;
        protected final Status status;
        private final Payload payload;
        @Nullable private final Exception exception;

        protected Result(String type, Payload payload) {
            this.status = Status.SUCCESS;
            this.type = type;
            this.payload = payload;
            this.exception = null;
        }

        protected Result(String type, Exception e) {
            this.status = Status.FAILURE;
            this.type = type;
            this.payload = Payload.EMPTY;
            this.exception = e;
        }

        public String type() {
            return type;
        }

        public Status status() {
            return status;
        }

        public Payload payload() {
            return payload;
        }

        public Exception getException() {
            assert status == Status.FAILURE;
            return exception;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE.getPreferredName(), type);
            builder.field(STATUS.getPreferredName(), status.name().toLowerCase(Locale.ROOT));
            switch (status) {
                case SUCCESS:
                    assert payload != null;
                    builder.field(PAYLOAD.getPreferredName(), payload, params);
                    break;
                case FAILURE:
                    assert exception != null;
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

    interface Builder<I extends Input> {
        I build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public interface Input extends ToXContent {

    String type();

    abstract class Result implements ToXContent {

        public enum Status {
            SUCCESS, FAILURE
        }

        protected final String type;
        protected final Status status;
        private final String reason;
        private final Payload payload;

        protected Result(String type, Payload payload) {
            this.status = Status.SUCCESS;
            this.type = type;
            this.payload = payload;
            this.reason = null;
        }

        protected Result(String type, Exception e) {
            this.status = Status.FAILURE;
            this.type = type;
            this.reason = ExceptionsHelper.detailedMessage(e);
            this.payload = null;
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.TYPE.getPreferredName(), type);
            builder.field(Field.STATUS.getPreferredName(), status.name().toLowerCase(Locale.ROOT));
            switch (status) {
                case SUCCESS:
                    assert payload != null;
                    builder.field(Field.PAYLOAD.getPreferredName(), payload, params);
                    break;
                case FAILURE:
                    assert reason != null;
                    builder.field(Field.REASON.getPreferredName(), reason);
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

    interface Field {
        ParseField STATUS = new ParseField("status");
        ParseField TYPE = new ParseField("type");
        ParseField PAYLOAD = new ParseField("payload");
        ParseField REASON = new ParseField("reason");
    }
}

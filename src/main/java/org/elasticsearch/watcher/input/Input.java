/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public interface Input extends ToXContent {

    String type();

    abstract class Result implements ToXContent {

        private final String type;
        private final Payload payload;

        public Result(String type, Payload payload) {
            this.type = type;
            this.payload = payload;
        }

        public String type() {
            return type;
        }


        public Payload payload() {
            return payload;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                    .field(Field.PAYLOAD.getPreferredName(), payload, params);
            toXContentBody(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException;
    }

    interface Builder<I extends Input> {

        I build();

    }

    interface Field {
        ParseField PAYLOAD = new ParseField("payload");
    }
}

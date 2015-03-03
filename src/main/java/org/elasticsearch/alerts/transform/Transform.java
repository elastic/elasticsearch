/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public abstract class Transform implements ToXContent {

    public static final Transform NOOP = new Transform() {
        @Override
        public String type() {
            return "noop";
        }

        @Override
        public Result apply(ExecutionContext context, Payload payload) throws IOException {
            return new Result("noop", payload);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    };

    public abstract String type();

    public abstract Result apply(ExecutionContext ctx, Payload payload) throws IOException;

    public static class Result implements ToXContent {

        public static final ParseField TYPE_FIELD = new ParseField("type");
        public static final ParseField PAYLOAD_FIELD = new ParseField("payload");

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
            return builder.startObject()
                    .field(TYPE_FIELD.getPreferredName(), type)
                    .field(PAYLOAD_FIELD.getPreferredName(), payload)
                    .endObject();
        }

        public static Result parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new AlertsSettingsException("could not parse transform result. expected an object, but found [" + token + "]");
            }

            String type = null;
            Payload payload = null;

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        if (TYPE_FIELD.match(currentFieldName)) {
                            type = parser.text();
                        } else {
                            throw new AlertsSettingsException("could not parse transform result. expected a string value for field [" + currentFieldName + "], found [" + token + "]");
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (PAYLOAD_FIELD.match(currentFieldName)) {
                            payload = new Payload.XContent(parser);
                        } else {
                            throw new AlertsSettingsException("could not parse transform result. expected an object for field [" + currentFieldName + "], found [" + token + "]");
                        }
                    } else {
                        throw new AlertsSettingsException("could not parse transform result. unexpected token [" + token + "]");
                    }
                }
            }
            if (type == null) {
                throw new AlertsSettingsException("could not parse transform result. missing [type] field");
            }
            if (payload == null) {
                throw new AlertsSettingsException("could not parse transform result. missing [payload] field");
            }
            return new Result(type, payload);
        }
    }

    public static interface Parser<T extends Transform> {

        public static final ParseField TRANSFORM_FIELD = new ParseField("transform");
        public static final ParseField TRANSFORM_RESULT_FIELD = new ParseField("transform_result");

        String type();

        T parse(XContentParser parser) throws IOException;

    }

    public static interface SourceBuilder extends ToXContent {

        String type();
    }

}

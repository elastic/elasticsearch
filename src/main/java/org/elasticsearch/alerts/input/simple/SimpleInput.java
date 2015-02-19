/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.input.simple;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * This class just defines a simple xcontent map as an input
 */
public class SimpleInput extends Input<SimpleInput.Result> {

    public static final String TYPE = "simple";
    private final Payload payload;

    public SimpleInput(ESLogger logger, Payload payload) {
        super(logger);
        this.payload = payload;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {
        return new Result(TYPE, payload);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Input.Result.PAYLOAD_FIELD.getPreferredName(), payload);
        return builder.endObject();
    }

    public static class Result extends Input.Result {

        public Result(String type, Payload payload) {
            super(type, payload);
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public static class Parser extends AbstractComponent implements Input.Parser<Result,SimpleInput> {

        @Inject
        public Parser(Settings settings) {
            super(settings);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public SimpleInput parse(XContentParser parser) throws IOException {
            Payload payload = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if (Input.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new InputException("unable to parse [" + TYPE + "] input. unexpected field [" + currentFieldName + "]");
                    }
                }
            }

            if (payload == null) {
                throw new InputException("unable to parse [" + TYPE + "] input [payload] is a required field");
            }

            return new SimpleInput(logger, payload);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Payload payload = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if (Input.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new InputException("unable to parse [" + TYPE + "] input result. unexpected field [" + currentFieldName + "]");
                    }
                }
            }

            if (payload == null) {
                throw new InputException("unable to parse [" + TYPE + "] input result [payload] is a required field");
            }

            return new Result(TYPE, payload);
        }
    }
}

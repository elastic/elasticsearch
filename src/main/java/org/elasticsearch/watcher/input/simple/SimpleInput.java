/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.simple;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class SimpleInput implements Input {

    public static final String TYPE = "simple";

    private final Payload payload;

    public SimpleInput(Payload payload) {
        this.payload = payload;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public Payload getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleInput that = (SimpleInput) o;

        return payload.equals(that.payload);
    }

    @Override
    public int hashCode() {
        return payload.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return payload.toXContent(builder, params);
    }

    public static SimpleInput parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new SimpleInputException("could not parse [{}] input for watch [{}]. expected an object but found [{}] instead", TYPE, watchId, parser.currentToken());
        }
        Payload payload = new Payload.Simple(parser.map());
        return new SimpleInput(payload);
    }

    public static Builder builder(Payload payload) {
        return new Builder(payload);
    }

    public static class Result extends Input.Result {

        public Result(Payload payload) {
            super(TYPE, payload);
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        public static Result parse(String watchId, XContentParser parser) throws IOException {
            Payload payload = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    if (Field.PAYLOAD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new SimpleInputException("could not parse [{}] input result for watch [{}]. unexpected field [{}]", TYPE, watchId, currentFieldName);
                    }
                }
            }

            if (payload == null) {
                throw new SimpleInputException("could not parse [{}] input result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.PAYLOAD.getPreferredName());
            }

            return new Result(payload);
        }
    }

    public static class Builder implements Input.Builder<SimpleInput> {

        private final Payload payload;

        private Builder(Payload payload) {
            this.payload = payload;
        }

        @Override
        public SimpleInput build() {
            return new SimpleInput(payload);
        }
    }
}

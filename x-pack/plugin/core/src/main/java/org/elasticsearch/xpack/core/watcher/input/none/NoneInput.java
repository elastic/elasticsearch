/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.input.none;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;

public class NoneInput implements Input {

    public static final String TYPE = "none";
    public static final NoneInput INSTANCE = new NoneInput();

    private NoneInput() {}

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    public static NoneInput parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            String formattedMessage = "could not parse [{}] input for watch [{}]. expected an empty object but found [{}] instead";
            throw new ElasticsearchParseException(formattedMessage, TYPE, watchId, parser.currentToken());
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String formattedMessage = "could not parse [{}] input for watch [{}]. expected an empty object but found [{}] instead";
            throw new ElasticsearchParseException(formattedMessage, TYPE, watchId, parser.currentToken());
        }
        return INSTANCE;
    }

    public static Builder builder() {
        return Builder.INSTANCE;
    }

    public static class Result extends Input.Result {

        public static final Result INSTANCE = new Result();

        private Result() {
            super(TYPE, Payload.EMPTY);
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public static class Builder implements Input.Builder<NoneInput> {

        private static final Builder INSTANCE = new Builder();

        private Builder() {}

        @Override
        public NoneInput build() {
            return NoneInput.INSTANCE;
        }
    }
}

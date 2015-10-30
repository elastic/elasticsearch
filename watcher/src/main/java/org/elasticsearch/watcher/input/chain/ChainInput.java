/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.chain;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChainInput implements Input {

    public static final String TYPE = "chain";
    private List<Tuple<String, Input>> inputs;

    public ChainInput(List<Tuple<String, Input>> inputs) {
        this.inputs = inputs;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("inputs");
        for (Tuple<String, Input> tuple : inputs) {
            builder.startObject().startObject(tuple.v1());
            builder.field(tuple.v2().type(), tuple.v2());
            builder.endObject().endObject();
        }
        builder.endArray();
        builder.endObject();

        return builder;
    }

    public List<Tuple<String, Input>> getInputs() {
        return inputs;
    }

    public static ChainInput parse(String watchId, XContentParser parser, InputRegistry inputRegistry) throws IOException {
        List<Tuple<String, Input>> inputs = new ArrayList<>();
        String currentFieldName;
        XContentParser.Token token;

        ParseField inputsField = new ParseField("inputs");

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (token == XContentParser.Token.START_ARRAY &&  inputsField.getPreferredName().equals(currentFieldName)) {
                    String currentInputFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentInputFieldName = parser.currentName();
                        } else if (currentInputFieldName != null && token == XContentParser.Token.START_OBJECT) {
                            inputs.add(new Tuple<>(currentInputFieldName, inputRegistry.parse(watchId, parser).input()));
                            currentInputFieldName = null;
                        }
                    }
                }
            }
        }

        return new ChainInput(inputs);
    }

    public static ChainInput.Builder builder() {
        return new Builder();
    }

    public static class Builder implements Input.Builder<ChainInput> {

        private List<Tuple<String, Input>> inputs;

        private Builder() {
            inputs = new ArrayList<>();
        }

        public Builder add(String name, Input.Builder input) {
            inputs.add(new Tuple<>(name, input.build()));
            return this;
        }

        @Override
        public ChainInput build() {
            return new ChainInput(inputs);
        }
    }

    public static class Result extends Input.Result {

        private List<Tuple<String, Input.Result>> results;

        protected Result(List<Tuple<String, Input.Result>> results, Payload payload) {
            super(TYPE, payload);
            this.results = results;
        }

        protected Result(Exception e) {
            super(TYPE, e);
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(type);
            for (Tuple<String, Input.Result> tuple : results) {
                builder.field(tuple.v1(), tuple.v2());
            }
            builder.endObject();

            return builder;
        }
    }
}

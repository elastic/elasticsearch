/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class TestGrokPatternAction extends ActionType<TestGrokPatternAction.Response> {

    public static final TestGrokPatternAction INSTANCE = new TestGrokPatternAction();
    public static final String NAME = "cluster:monitor/text_structure/testgrokpattern";

    private TestGrokPatternAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");
        public static final ParseField TEXTS = new ParseField("texts");

        private static final ObjectParser<Request.Builder, Void> PARSER = createParser();

        private static ObjectParser<Request.Builder, Void> createParser() {
            ObjectParser<Request.Builder, Void> parser = new ObjectParser<>("textstructure/testgrokpattern", false, Request.Builder::new);
            parser.declareString(Request.Builder::grokPattern, GROK_PATTERN);
            parser.declareStringArray(Request.Builder::texts, TEXTS);
            return parser;
        }

        public static class Builder {
            private String grokPattern;
            private List<String> texts;

            public void grokPattern(String grokPattern) {
                this.grokPattern = grokPattern;
            }

            public void texts(List<String> texts) {
                this.texts = texts;
            }

            public Request build() {
                return new Request(grokPattern, texts);
            }
        }

        private final String grokPattern;
        private final List<String> texts;

        private Request(String grokPattern, List<String> texts) {
            this.grokPattern = grokPattern;
            this.texts = texts;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            grokPattern = in.readString();
            texts = in.readStringCollectionAsList();
        }

        public static Request parseRequest(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null).build();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(grokPattern);
            out.writeStringCollection(texts);
        }

        public String getGrokPattern() {
            return grokPattern;
        }

        public List<String> getTexts() {
            return texts;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (grokPattern == null) {
                validationException = addValidationError("[" + GROK_PATTERN.getPreferredName() + "] missing.", validationException);
            }
            if (texts == null || texts.isEmpty()) {
                validationException = addValidationError("[" + TEXTS.getPreferredName() + "] missing or empty.", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(grokPattern, request.grokPattern) && Objects.equals(texts, request.texts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(grokPattern, texts);
        }

        @Override
        public String toString() {
            return "Request{" + "grokPattern='" + grokPattern + '\'' + ", texts=" + texts + '}';
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject, Writeable {

        Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}

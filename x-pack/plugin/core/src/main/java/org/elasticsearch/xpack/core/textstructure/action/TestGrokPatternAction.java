/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestGrokPatternAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("cluster:monitor/text_structure/test_grok_pattern");

    public static class Request extends LegacyActionRequest {

        public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");
        public static final ParseField TEXT = new ParseField("text");
        public static final ParseField ECS_COMPATIBILITY = new ParseField("ecs_compatibility");

        private static final ObjectParser<Builder, Void> PARSER = createParser();

        private static ObjectParser<Builder, Void> createParser() {
            ObjectParser<Builder, Void> parser = new ObjectParser<>("textstructure/testgrokpattern", false, Builder::new);
            parser.declareString(Builder::grokPattern, GROK_PATTERN);
            parser.declareStringArray(Builder::text, TEXT);
            return parser;
        }

        public static class Builder {
            private String grokPattern;
            private List<String> text;
            private String ecsCompatibility;

            public Builder grokPattern(String grokPattern) {
                this.grokPattern = grokPattern;
                return this;
            }

            public Builder text(List<String> text) {
                this.text = text;
                return this;
            }

            public Builder ecsCompatibility(String ecsCompatibility) {
                this.ecsCompatibility = Strings.isNullOrEmpty(ecsCompatibility) ? null : ecsCompatibility;
                return this;
            }

            public Request build() {
                return new Request(grokPattern, text, ecsCompatibility);
            }
        }

        private final String grokPattern;
        private final List<String> text;
        private final String ecsCompatibility;

        private Request(String grokPattern, List<String> text, String ecsCompatibility) {
            this.grokPattern = ExceptionsHelper.requireNonNull(grokPattern, GROK_PATTERN.getPreferredName());
            this.text = ExceptionsHelper.requireNonNull(text, TEXT.getPreferredName());
            this.ecsCompatibility = ecsCompatibility;
        }

        public static Request parseRequest(String ecsCompatibility, XContentParser parser) throws IOException {
            return PARSER.parse(parser, null).ecsCompatibility(ecsCompatibility).build();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        public String getGrokPattern() {
            return grokPattern;
        }

        public List<String> getText() {
            return text;
        }

        public String getEcsCompatibility() {
            return ecsCompatibility;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(grokPattern, request.grokPattern) && Objects.equals(text, request.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(grokPattern, text);
        }

        @Override
        public String toString() {
            return "Request{" + "grokPattern='" + grokPattern + '\'' + ", text=" + text + '}';
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject, Writeable {

        private final List<Map<String, Object>> ranges;

        public Response(List<Map<String, Object>> ranges) {
            this.ranges = ranges;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("matches");
            for (Map<String, Object> ranges : ranges) {
                builder.startObject();
                builder.field("matched", ranges != null);
                if (ranges != null) {
                    builder.startObject("fields");
                    for (Map.Entry<String, Object> rangeOrList : ranges.entrySet()) {
                        List<?> listOfRanges;
                        if (rangeOrList.getValue() instanceof List) {
                            listOfRanges = (List<?>) rangeOrList.getValue();
                        } else {
                            listOfRanges = List.of(rangeOrList.getValue());
                        }
                        builder.startArray(rangeOrList.getKey());
                        for (Object rangeObject : listOfRanges) {
                            GrokCaptureExtracter.Range range = (GrokCaptureExtracter.Range) rangeObject;
                            builder.startObject();
                            builder.field("match", range.match());
                            builder.field("offset", range.offset());
                            builder.field("length", range.length());
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AnalyzeResponse {

    private static final String TOKENS = "tokens";
    private static final String DETAIL = "detail";

    public static class AnalyzeToken {
        private String term;
        private int startOffset;
        private int endOffset;
        private int position;
        private int positionLength = 1;
        private String type;
        private final Map<String, Object> attributes = new HashMap<>();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnalyzeResponse.AnalyzeToken that = (AnalyzeResponse.AnalyzeToken) o;
            return startOffset == that.startOffset &&
                endOffset == that.endOffset &&
                position == that.position &&
                positionLength == that.positionLength &&
                Objects.equals(term, that.term) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, startOffset, endOffset, position, positionLength, attributes, type);
        }

        public String getTerm() {
            return this.term;
        }

        private void setTerm(String term) {
            this.term = term;
        }

        public int getStartOffset() {
            return this.startOffset;
        }

        private void setStartOffset(int startOffset) {
            this.startOffset = startOffset;
        }

        public int getEndOffset() {
            return this.endOffset;
        }

        private void setEndOffset(int endOffset) {
            this.endOffset = endOffset;
        }

        public int getPosition() {
            return this.position;
        }

        private void setPosition(int position) {
            this.position = position;
        }

        public int getPositionLength() {
            return this.positionLength;
        }

        private void setPositionLength(int positionLength) {
            this.positionLength = positionLength;
        }

        public String getType() {
            return this.type;
        }

        private void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getAttributes() {
            return this.attributes;
        }

        private void setAttribute(String key, Object value) {
            this.attributes.put(key, value);
        }

        private static final ObjectParser<AnalyzeToken, Void> PARSER
            = new ObjectParser<>("analyze_token", AnalyzeToken::setAttribute, AnalyzeToken::new);
        static {
            PARSER.declareString(AnalyzeToken::setTerm, new ParseField("token"));
            PARSER.declareString(AnalyzeToken::setType, new ParseField("type"));
            PARSER.declareInt(AnalyzeToken::setPosition, new ParseField("position"));
            PARSER.declareInt(AnalyzeToken::setStartOffset, new ParseField("start_offset"));
            PARSER.declareInt(AnalyzeToken::setEndOffset, new ParseField("end_offset"));
            PARSER.declareInt(AnalyzeToken::setPositionLength, new ParseField("positionLength"));
        }

        public static AnalyzeToken fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private final DetailAnalyzeResponse detail;
    private final List<AnalyzeResponse.AnalyzeToken> tokens;

    private AnalyzeResponse(List<AnalyzeResponse.AnalyzeToken> tokens, DetailAnalyzeResponse detail) {
        this.tokens = tokens;
        this.detail = detail;
    }

    public List<AnalyzeResponse.AnalyzeToken> getTokens() {
        return this.tokens;
    }

    public DetailAnalyzeResponse detail() {
        return this.detail;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AnalyzeResponse, Void> PARSER = new ConstructingObjectParser<>("analyze_response",
        true, args -> new AnalyzeResponse((List<AnalyzeResponse.AnalyzeToken>) args[0], (DetailAnalyzeResponse) args[1]));

    static {
        PARSER.declareObjectArray(optionalConstructorArg(), AnalyzeToken.PARSER, new ParseField(TOKENS));
        PARSER.declareObject(optionalConstructorArg(), DetailAnalyzeResponse.PARSER, new ParseField(DETAIL));
    }

    public static AnalyzeResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyzeResponse that = (AnalyzeResponse) o;
        return Objects.equals(detail, that.detail) &&
            Objects.equals(tokens, that.tokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(detail, tokens);
    }

}

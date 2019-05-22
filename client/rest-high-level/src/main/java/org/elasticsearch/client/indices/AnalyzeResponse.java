/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class AnalyzeResponse {

    private static final String TOKENS = "tokens";
    private static final String TOKEN = "token";
    private static final String START_OFFSET = "start_offset";
    private static final String END_OFFSET = "end_offset";
    private static final String TYPE = "type";
    private static final String POSITION = "position";
    private static final String POSITION_LENGTH = "positionLength";
    private static final String DETAIL = "detail";

    public static class AnalyzeToken {
        private final String term;
        private final int startOffset;
        private final int endOffset;
        private final int position;
        private final int positionLength;
        private final Map<String, Object> attributes;
        private final String type;

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

        AnalyzeToken(String term, int position, int startOffset, int endOffset, int positionLength,
                     String type, Map<String, Object> attributes) {
            this.term = term;
            this.position = position;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.positionLength = positionLength;
            this.type = type;
            this.attributes = attributes;
        }

        public String getTerm() {
            return this.term;
        }

        public int getStartOffset() {
            return this.startOffset;
        }

        public int getEndOffset() {
            return this.endOffset;
        }

        public int getPosition() {
            return this.position;
        }

        public int getPositionLength() {
            return this.positionLength;
        }

        public String getType() {
            return this.type;
        }

        public Map<String, Object> getAttributes() {
            return this.attributes;
        }

        // We can't use a ConstructingObjectParser here, because unknown fields are gathered
        // up into the attributes map, and there isn't a way of doing that in COP yet.
        public static AnalyzeResponse.AnalyzeToken fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            String field = null;
            String term = "";
            int position = -1;
            int startOffset = -1;
            int endOffset = -1;
            int positionLength = 1;
            String type = "";
            Map<String, Object> attributes = new HashMap<>();
            for (XContentParser.Token t = parser.nextToken(); t != XContentParser.Token.END_OBJECT; t = parser.nextToken()) {
                if (t == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                    continue;
                }
                if (TOKEN.equals(field)) {
                    term = parser.text();
                } else if (POSITION.equals(field)) {
                    position = parser.intValue();
                } else if (START_OFFSET.equals(field)) {
                    startOffset = parser.intValue();
                } else if (END_OFFSET.equals(field)) {
                    endOffset = parser.intValue();
                } else if (POSITION_LENGTH.equals(field)) {
                    positionLength = parser.intValue();
                } else if (TYPE.equals(field)) {
                    type = parser.text();
                } else {
                    if (t == XContentParser.Token.VALUE_STRING) {
                        attributes.put(field, parser.text());
                    } else if (t == XContentParser.Token.VALUE_NUMBER) {
                        attributes.put(field, parser.numberValue());
                    } else if (t == XContentParser.Token.VALUE_BOOLEAN) {
                        attributes.put(field, parser.booleanValue());
                    } else if (t == XContentParser.Token.START_OBJECT) {
                        attributes.put(field, parser.map());
                    } else if (t == XContentParser.Token.START_ARRAY) {
                        attributes.put(field, parser.list());
                    }
                }
            }
            return new AnalyzeResponse.AnalyzeToken(term, position, startOffset, endOffset, positionLength, type, attributes);
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
        PARSER.declareObjectArray(optionalConstructorArg(),
            (p, c) -> AnalyzeResponse.AnalyzeToken.fromXContent(p), new ParseField(TOKENS));
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

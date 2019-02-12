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
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class AnalyzeResponse extends ActionResponse implements Iterable<AnalyzeResponse.AnalyzeToken>, ToXContentObject {

    public static class AnalyzeToken implements Streamable, ToXContentObject {
        private String term;
        private int startOffset;
        private int endOffset;
        private int position;
        private int positionLength = 1;
        private Map<String, Object> attributes;
        private String type;

        AnalyzeToken() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnalyzeToken that = (AnalyzeToken) o;
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

        public AnalyzeToken(String term, int position, int startOffset, int endOffset, int positionLength,
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

        public Map<String, Object> getAttributes(){
            return this.attributes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.TOKEN, term);
            builder.field(Fields.START_OFFSET, startOffset);
            builder.field(Fields.END_OFFSET, endOffset);
            builder.field(Fields.TYPE, type);
            builder.field(Fields.POSITION, position);
            if (positionLength > 1) {
                builder.field(Fields.POSITION_LENGTH, positionLength);
            }
            if (attributes != null && !attributes.isEmpty()) {
                Map<String, Object> sortedAttributes = new TreeMap<>(attributes);
                for (Map.Entry<String, Object> entity : sortedAttributes.entrySet()) {
                    builder.field(entity.getKey(), entity.getValue());
                }
            }
            builder.endObject();
            return builder;
        }

        public static AnalyzeToken readAnalyzeToken(StreamInput in) throws IOException {
            AnalyzeToken analyzeToken = new AnalyzeToken();
            analyzeToken.readFrom(in);
            return analyzeToken;
        }

        public static AnalyzeToken fromXContent(XContentParser parser) throws IOException {
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
                if (Fields.TOKEN.equals(field)) {
                    term = parser.text();
                } else if (Fields.POSITION.equals(field)) {
                    position = parser.intValue();
                } else if (Fields.START_OFFSET.equals(field)) {
                    startOffset = parser.intValue();
                } else if (Fields.END_OFFSET.equals(field)) {
                    endOffset = parser.intValue();
                } else if (Fields.POSITION_LENGTH.equals(field)) {
                    positionLength = parser.intValue();
                } else if (Fields.TYPE.equals(field)) {
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
            return new AnalyzeToken(term, position, startOffset, endOffset, positionLength, type, attributes);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            term = in.readString();
            startOffset = in.readInt();
            endOffset = in.readInt();
            position = in.readVInt();
            Integer len = in.readOptionalVInt();
            if (len != null) {
                positionLength = len;
            } else {
                positionLength = 1;
            }
            type = in.readOptionalString();
            attributes = in.readMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(term);
            out.writeInt(startOffset);
            out.writeInt(endOffset);
            out.writeVInt(position);
            out.writeOptionalVInt(positionLength > 1 ? positionLength : null);
            out.writeOptionalString(type);
            out.writeMapWithConsistentOrder(attributes);
        }
    }

    private DetailAnalyzeResponse detail;

    private List<AnalyzeToken> tokens;

    AnalyzeResponse() {
    }

    public AnalyzeResponse(List<AnalyzeToken> tokens, DetailAnalyzeResponse detail) {
        this.tokens = tokens;
        this.detail = detail;
    }

    public List<AnalyzeToken> getTokens() {
        return this.tokens;
    }

    public DetailAnalyzeResponse detail() {
        return this.detail;
    }

    @Override
    public Iterator<AnalyzeToken> iterator() {
        return tokens.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (tokens != null) {
            builder.startArray(Fields.TOKENS);
            for (AnalyzeToken token : tokens) {
                token.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (detail != null) {
            builder.startObject(Fields.DETAIL);
            detail.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<AnalyzeResponse, Void> PARSER = new ConstructingObjectParser<>("analyze_response",
        true, args -> new AnalyzeResponse((List<AnalyzeToken>) args[0], (DetailAnalyzeResponse) args[1]));
    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> AnalyzeToken.fromXContent(p), new ParseField(Fields.TOKENS));
        PARSER.declareObject(optionalConstructorArg(), DetailAnalyzeResponse.PARSER, new ParseField(Fields.DETAIL));
    }

    public static AnalyzeResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        tokens = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            tokens.add(AnalyzeToken.readAnalyzeToken(in));
        }
        if (tokens.size() == 0) {
            tokens = null;
        }
        detail = in.readOptionalStreamable(DetailAnalyzeResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (tokens != null) {
            out.writeVInt(tokens.size());
            for (AnalyzeToken token : tokens) {
                token.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeOptionalStreamable(detail);
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

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    static final class Fields {
        static final String TOKENS = "tokens";
        static final String TOKEN = "token";
        static final String START_OFFSET = "start_offset";
        static final String END_OFFSET = "end_offset";
        static final String TYPE = "type";
        static final String POSITION = "position";
        static final String POSITION_LENGTH = "positionLength";
        static final String DETAIL = "detail";
    }
}

package org.elasticsearch.client.indices;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class AnalyzeResponse implements Iterable<AnalyzeResponse.AnalyzeToken>, ToXContentObject {

    public static class AnalyzeToken implements ToXContentObject {
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(AnalyzeResponse.Fields.TOKEN, term);
            builder.field(AnalyzeResponse.Fields.START_OFFSET, startOffset);
            builder.field(AnalyzeResponse.Fields.END_OFFSET, endOffset);
            builder.field(AnalyzeResponse.Fields.TYPE, type);
            builder.field(AnalyzeResponse.Fields.POSITION, position);
            if (positionLength > 1) {
                builder.field(AnalyzeResponse.Fields.POSITION_LENGTH, positionLength);
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
                if (AnalyzeResponse.Fields.TOKEN.equals(field)) {
                    term = parser.text();
                } else if (AnalyzeResponse.Fields.POSITION.equals(field)) {
                    position = parser.intValue();
                } else if (AnalyzeResponse.Fields.START_OFFSET.equals(field)) {
                    startOffset = parser.intValue();
                } else if (AnalyzeResponse.Fields.END_OFFSET.equals(field)) {
                    endOffset = parser.intValue();
                } else if (AnalyzeResponse.Fields.POSITION_LENGTH.equals(field)) {
                    positionLength = parser.intValue();
                } else if (AnalyzeResponse.Fields.TYPE.equals(field)) {
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

    public AnalyzeResponse(List<AnalyzeResponse.AnalyzeToken> tokens, DetailAnalyzeResponse detail) {
        this.tokens = tokens;
        this.detail = detail;
    }

    public List<AnalyzeResponse.AnalyzeToken> getTokens() {
        return this.tokens;
    }

    public DetailAnalyzeResponse detail() {
        return this.detail;
    }

    @Override
    public Iterator<AnalyzeResponse.AnalyzeToken> iterator() {
        return tokens.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (tokens != null) {
            builder.startArray(AnalyzeResponse.Fields.TOKENS);
            for (AnalyzeResponse.AnalyzeToken token : tokens) {
                token.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (detail != null) {
            builder.startObject(AnalyzeResponse.Fields.DETAIL);
            detail.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<AnalyzeResponse, Void> PARSER = new ConstructingObjectParser<>("analyze_response",
        true, args -> new AnalyzeResponse((List<AnalyzeResponse.AnalyzeToken>) args[0], (DetailAnalyzeResponse) args[1]));

    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> AnalyzeResponse.AnalyzeToken.fromXContent(p), new ParseField(AnalyzeResponse.Fields.TOKENS));
        PARSER.declareObject(optionalConstructorArg(), DetailAnalyzeResponse.PARSER, new ParseField(AnalyzeResponse.Fields.DETAIL));
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

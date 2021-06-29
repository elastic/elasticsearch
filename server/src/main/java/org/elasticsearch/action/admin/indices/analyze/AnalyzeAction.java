/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NameOrDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class AnalyzeAction extends ActionType<AnalyzeAction.Response> {

    public static final AnalyzeAction INSTANCE = new AnalyzeAction();
    public static final String NAME = "indices:admin/analyze";

    private AnalyzeAction() {
        super(NAME, AnalyzeAction.Response::new);
    }

    public static class Fields {
        public static final ParseField ANALYZER = new ParseField("analyzer");
        public static final ParseField TEXT = new ParseField("text");
        public static final ParseField FIELD = new ParseField("field");
        public static final ParseField TOKENIZER = new ParseField("tokenizer");
        public static final ParseField TOKEN_FILTERS = new ParseField("filter");
        public static final ParseField CHAR_FILTERS = new ParseField("char_filter");
        public static final ParseField EXPLAIN = new ParseField("explain");
        public static final ParseField ATTRIBUTES = new ParseField("attributes");
        public static final ParseField NORMALIZER = new ParseField("normalizer");
    }

    /**
     * A request to analyze a text associated with a specific index. Allow to provide
     * the actual analyzer name to perform the analysis with.
     */
    public static class Request extends SingleShardRequest<Request> {

        private String[] text;
        private String analyzer;
        private NameOrDefinition tokenizer;
        private final List<NameOrDefinition> tokenFilters = new ArrayList<>();
        private final List<NameOrDefinition> charFilters = new ArrayList<>();
        private String field;
        private boolean explain = false;
        private String[] attributes = Strings.EMPTY_ARRAY;
        private String normalizer;

        public Request() {
        }

        Request(StreamInput in) throws IOException {
            super(in);
            text = in.readStringArray();
            analyzer = in.readOptionalString();
            tokenizer = in.readOptionalWriteable(NameOrDefinition::new);
            tokenFilters.addAll(in.readList(NameOrDefinition::new));
            charFilters.addAll(in.readList(NameOrDefinition::new));
            field = in.readOptionalString();
            explain = in.readBoolean();
            attributes = in.readStringArray();
            normalizer = in.readOptionalString();
        }

        /**
         * Constructs a new analyzer request for the provided index.
         *
         * @param index The text to analyze
         */
        public Request(String index) {
            this.index(index);
        }

        public String[] text() {
            return this.text;
        }

        public Request text(String... text) {
            this.text = text;
            return this;
        }

        public Request text(List<String> text) {
            this.text = text.toArray(new String[]{});
            return this;
        }

        public Request analyzer(String analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public String analyzer() {
            return this.analyzer;
        }

        public Request tokenizer(String tokenizer) {
            this.tokenizer = new NameOrDefinition(tokenizer);
            return this;
        }

        public Request tokenizer(Map<String, ?> tokenizer) {
            this.tokenizer = new NameOrDefinition(tokenizer);
            return this;
        }

        public void tokenizer(NameOrDefinition tokenizer) {
            this.tokenizer = tokenizer;
        }

        public NameOrDefinition tokenizer() {
            return this.tokenizer;
        }

        public Request addTokenFilter(String tokenFilter) {
            this.tokenFilters.add(new NameOrDefinition(tokenFilter));
            return this;
        }

        public Request addTokenFilter(Map<String, ?> tokenFilter) {
            this.tokenFilters.add(new NameOrDefinition(tokenFilter));
            return this;
        }

        public void setTokenFilters(List<NameOrDefinition> tokenFilters) {
            this.tokenFilters.addAll(tokenFilters);
        }

        public List<NameOrDefinition> tokenFilters() {
            return this.tokenFilters;
        }

        public Request addCharFilter(Map<String, ?> charFilter) {
            this.charFilters.add(new NameOrDefinition(charFilter));
            return this;
        }

        public Request addCharFilter(String charFilter) {
            this.charFilters.add(new NameOrDefinition(charFilter));
            return this;
        }

        public void setCharFilters(List<NameOrDefinition> charFilters) {
            this.charFilters.addAll(charFilters);
        }

        public List<NameOrDefinition> charFilters() {
            return this.charFilters;
        }

        public Request field(String field) {
            this.field = field;
            return this;
        }

        public String field() {
            return this.field;
        }

        public Request explain(boolean explain) {
            this.explain = explain;
            return this;
        }

        public boolean explain() {
            return this.explain;
        }

        public Request attributes(String... attributes) {
            if (attributes == null) {
                throw new IllegalArgumentException("attributes must not be null");
            }
            this.attributes = attributes;
            return this;
        }

        public void attributes(List<String> attributes) {
            this.attributes = attributes.toArray(new String[]{});
        }

        public String[] attributes() {
            return this.attributes;
        }

        public String normalizer() {
            return this.normalizer;
        }

        public Request normalizer(String normalizer) {
            this.normalizer = normalizer;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (text == null || text.length == 0) {
                validationException = addValidationError("text is missing", validationException);
            }
            if ((index == null || index.length() == 0) && normalizer != null) {
                validationException = addValidationError("index is required if normalizer is specified", validationException);
            }
            if (normalizer != null && (tokenizer != null || analyzer != null)) {
                validationException
                    = addValidationError("tokenizer/analyze should be null if normalizer is specified", validationException);
            }
            if (analyzer != null && (tokenizer != null || charFilters.isEmpty() == false || tokenFilters.isEmpty() == false)) {
                validationException
                    = addValidationError("cannot define extra components on a named analyzer", validationException);
            }
            if (normalizer != null && (tokenizer != null || charFilters.isEmpty() == false || tokenFilters.isEmpty() == false)) {
                validationException
                    = addValidationError("cannot define extra components on a named normalizer", validationException);
            }
            if (field != null && (tokenizer != null || charFilters.isEmpty() == false || tokenFilters.isEmpty() == false)) {
                validationException
                    = addValidationError("cannot define extra components on a field-specific analyzer", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(text);
            out.writeOptionalString(analyzer);
            out.writeOptionalWriteable(tokenizer);
            out.writeList(tokenFilters);
            out.writeList(charFilters);
            out.writeOptionalString(field);
            out.writeBoolean(explain);
            out.writeStringArray(attributes);
            out.writeOptionalString(normalizer);
        }

        public static Request fromXContent(XContentParser parser, String index) throws IOException {
            Request request = new Request(index);
            PARSER.parse(parser, request, null);
            return request;
        }

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>("analyze_request");

        static {
            PARSER.declareStringArray(Request::text, Fields.TEXT);
            PARSER.declareString(Request::analyzer, Fields.ANALYZER);
            PARSER.declareField(Request::tokenizer, (p, c) -> NameOrDefinition.fromXContent(p),
                Fields.TOKENIZER, ObjectParser.ValueType.OBJECT_OR_STRING);
            PARSER.declareObjectArray(Request::setTokenFilters, (p, c) -> NameOrDefinition.fromXContent(p),
                Fields.TOKEN_FILTERS);
            PARSER.declareObjectArray(Request::setCharFilters, (p, c) -> NameOrDefinition.fromXContent(p),
                Fields.CHAR_FILTERS);
            PARSER.declareString(Request::field, Fields.FIELD);
            PARSER.declareBoolean(Request::explain, Fields.EXPLAIN);
            PARSER.declareStringArray(Request::attributes, Fields.ATTRIBUTES);
            PARSER.declareString(Request::normalizer, Fields.NORMALIZER);
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final DetailAnalyzeResponse detail;
        private final List<AnalyzeToken> tokens;

        public Response(List<AnalyzeToken> tokens, DetailAnalyzeResponse detail) {
            if (tokens == null && detail == null) {
                throw new IllegalArgumentException("Neither token nor detail set on AnalysisAction.Response");
            }
            this.tokens = tokens;
            this.detail = detail;
        }

        public Response(StreamInput in) throws IOException {
            AnalyzeToken[] tokenArray = in.readOptionalArray(AnalyzeToken::new, AnalyzeToken[]::new);
            tokens = tokenArray != null ? Arrays.asList(tokenArray) : null;
            detail = in.readOptionalWriteable(DetailAnalyzeResponse::new);
        }

        public List<AnalyzeToken> getTokens() {
            return this.tokens;
        }

        public DetailAnalyzeResponse detail() {
            return this.detail;
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            AnalyzeToken[] tokenArray = null;
            if (tokens != null) {
                tokenArray = tokens.toArray(new AnalyzeToken[0]);
            }
            out.writeOptionalArray(tokenArray);
            out.writeOptionalWriteable(detail);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response that = (Response) o;
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

            static final String DETAIL = "detail";
        }
    }

    public static class AnalyzeToken implements Writeable, ToXContentObject {
        private final String term;
        private final int startOffset;
        private final int endOffset;
        private final int position;
        private final int positionLength;
        private final Map<String, Object> attributes;
        private final String type;

        static final String TOKEN = "token";
        static final String START_OFFSET = "start_offset";
        static final String END_OFFSET = "end_offset";
        static final String TYPE = "type";
        static final String POSITION = "position";
        static final String POSITION_LENGTH = "positionLength";

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
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

        AnalyzeToken(StreamInput in) throws IOException {
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
            builder.field(TOKEN, term);
            builder.field(START_OFFSET, startOffset);
            builder.field(END_OFFSET, endOffset);
            builder.field(TYPE, type);
            builder.field(POSITION, position);
            if (positionLength > 1) {
                builder.field(POSITION_LENGTH, positionLength);
            }
            if (attributes != null && attributes.isEmpty() == false) {
                Map<String, Object> sortedAttributes = new TreeMap<>(attributes);
                for (Map.Entry<String, Object> entity : sortedAttributes.entrySet()) {
                    builder.field(entity.getKey(), entity.getValue());
                }
            }
            builder.endObject();
            return builder;
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

    public static class DetailAnalyzeResponse implements Writeable, ToXContentFragment {

        private final boolean customAnalyzer;
        private final AnalyzeTokenList analyzer;
        private final CharFilteredText[] charfilters;
        private final AnalyzeTokenList tokenizer;
        private final AnalyzeTokenList[] tokenfilters;

        public DetailAnalyzeResponse(AnalyzeTokenList analyzer) {
            this(false, analyzer, null, null, null);
        }

        public DetailAnalyzeResponse(CharFilteredText[] charfilters, AnalyzeTokenList tokenizer, AnalyzeTokenList[] tokenfilters) {
            this(true, null, charfilters, tokenizer, tokenfilters);
        }

        DetailAnalyzeResponse(boolean customAnalyzer,
                                     AnalyzeTokenList analyzer,
                                     CharFilteredText[] charfilters,
                                     AnalyzeTokenList tokenizer,
                                     AnalyzeTokenList[] tokenfilters) {
            this.customAnalyzer = customAnalyzer;
            this.analyzer = analyzer;
            this.charfilters = charfilters;
            this.tokenizer = tokenizer;
            this.tokenfilters = tokenfilters;
        }

        DetailAnalyzeResponse(StreamInput in) throws IOException {
            this.customAnalyzer = in.readBoolean();
            if (customAnalyzer) {
                tokenizer = new AnalyzeTokenList(in);
                int size = in.readVInt();
                if (size > 0) {
                    charfilters = new CharFilteredText[size];
                    for (int i = 0; i < size; i++) {
                        charfilters[i] = new CharFilteredText(in);
                    }
                } else {
                    charfilters = null;
                }
                size = in.readVInt();
                if (size > 0) {
                    tokenfilters = new AnalyzeTokenList[size];
                    for (int i = 0; i < size; i++) {
                        tokenfilters[i] = new AnalyzeTokenList(in);
                    }
                } else {
                    tokenfilters = null;
                }
                analyzer = null;
            } else {
                analyzer = new AnalyzeTokenList(in);
                tokenfilters = null;
                tokenizer = null;
                charfilters = null;
            }
        }

        public AnalyzeTokenList analyzer() {
            return this.analyzer;
        }

        public CharFilteredText[] charfilters() {
            return this.charfilters;
        }

        public AnalyzeTokenList tokenizer() {
            return tokenizer;
        }

        public AnalyzeTokenList[] tokenfilters() {
            return tokenfilters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DetailAnalyzeResponse that = (DetailAnalyzeResponse) o;
            return customAnalyzer == that.customAnalyzer &&
                Objects.equals(analyzer, that.analyzer) &&
                Arrays.equals(charfilters, that.charfilters) &&
                Objects.equals(tokenizer, that.tokenizer) &&
                Arrays.equals(tokenfilters, that.tokenfilters);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(customAnalyzer, analyzer, tokenizer);
            result = 31 * result + Arrays.hashCode(charfilters);
            result = 31 * result + Arrays.hashCode(tokenfilters);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("custom_analyzer", customAnalyzer);

            if (analyzer != null) {
                builder.startObject("analyzer");
                analyzer.toXContentWithoutObject(builder, params);
                builder.endObject();
            }

            if (charfilters != null) {
                builder.startArray("charfilters");
                for (CharFilteredText charfilter : charfilters) {
                    charfilter.toXContent(builder, params);
                }
                builder.endArray();
            }

            if (tokenizer != null) {
                builder.startObject("tokenizer");
                tokenizer.toXContentWithoutObject(builder, params);
                builder.endObject();
            }

            if (tokenfilters != null) {
                builder.startArray("tokenfilters");
                for (AnalyzeTokenList tokenfilter : tokenfilters) {
                    tokenfilter.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(customAnalyzer);
            if (customAnalyzer) {
                tokenizer.writeTo(out);
                if (charfilters != null) {
                    out.writeVInt(charfilters.length);
                    for (CharFilteredText charfilter : charfilters) {
                        charfilter.writeTo(out);
                    }
                } else {
                    out.writeVInt(0);
                }
                if (tokenfilters != null) {
                    out.writeVInt(tokenfilters.length);
                    for (AnalyzeTokenList tokenfilter : tokenfilters) {
                        tokenfilter.writeTo(out);
                    }
                } else {
                    out.writeVInt(0);
                }
            } else {
                analyzer.writeTo(out);
            }
        }
    }

    public static class AnalyzeTokenList implements Writeable, ToXContentObject {
        private final String name;
        private final AnalyzeToken[] tokens;

        static final String NAME = "name";

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AnalyzeTokenList that = (AnalyzeTokenList) o;
            return Objects.equals(name, that.name) &&
                Arrays.equals(tokens, that.tokens);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(tokens);
            return result;
        }

        public AnalyzeTokenList(String name, AnalyzeToken[] tokens) {
            this.name = name;
            this.tokens = tokens;
        }

        AnalyzeTokenList(StreamInput in) throws IOException {
            name = in.readString();
            tokens = in.readOptionalArray(AnalyzeToken::new, AnalyzeToken[]::new);
        }

        public String getName() {
            return name;
        }

        public AnalyzeToken[] getTokens() {
            return tokens;
        }

        void toXContentWithoutObject(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME, this.name);
            builder.startArray(Response.Fields.TOKENS);
            if (tokens != null) {
                for (AnalyzeToken token : tokens) {
                    token.toXContent(builder, params);
                }
            }
            builder.endArray();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentWithoutObject(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeOptionalArray(tokens);
        }
    }

    public static class CharFilteredText implements Writeable, ToXContentObject {
        private final String name;
        private final String[] texts;

        static final String NAME = "name";
        static final String FILTERED_TEXT = "filtered_text";

        public CharFilteredText(String name, String[] texts) {
            this.name = name;
            if (texts != null) {
                this.texts = texts;
            } else {
                this.texts = Strings.EMPTY_ARRAY;
            }
        }

        CharFilteredText(StreamInput in) throws IOException {
            name = in.readString();
            texts = in.readStringArray();
        }

        public String getName() {
            return name;
        }

        public String[] getTexts() {
            return texts;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME, name);
            builder.array(FILTERED_TEXT, texts);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeStringArray(texts);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CharFilteredText that = (CharFilteredText) o;
            return Objects.equals(name, that.name) &&
                Arrays.equals(texts, that.texts);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(texts);
            return result;
        }
    }

}

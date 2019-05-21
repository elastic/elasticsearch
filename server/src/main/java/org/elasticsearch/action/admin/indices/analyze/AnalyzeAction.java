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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class AnalyzeAction extends Action<AnalyzeResponse> {

    public static final AnalyzeAction INSTANCE = new AnalyzeAction();
    public static final String NAME = "indices:admin/analyze";

    private AnalyzeAction() {
        super(NAME);
    }

    @Override
    public Writeable.Reader<AnalyzeResponse> getResponseReader() {
        return AnalyzeResponse::new;
    }

    @Override
    public AnalyzeResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    /**
     * A request to analyze a text associated with a specific index. Allow to provide
     * the actual analyzer name to perform the analysis with.
     */
    public static class Request extends SingleShardRequest<Request> implements ToXContentObject {

        private String[] text;

        private String analyzer;

        private NameOrDefinition tokenizer;

        private final List<NameOrDefinition> tokenFilters = new ArrayList<>();

        private final List<NameOrDefinition> charFilters = new ArrayList<>();

        private String field;

        private boolean explain = false;

        private String[] attributes = Strings.EMPTY_ARRAY;

        private String normalizer;

        public static class NameOrDefinition implements Writeable, ToXContentFragment {
            // exactly one of these two members is not null
            public final String name;
            public final Settings definition;

            NameOrDefinition(String name) {
                this.name = Objects.requireNonNull(name);
                this.definition = null;
            }

            NameOrDefinition(Map<String, ?> definition) {
                this.name = null;
                Objects.requireNonNull(definition);
                try {
                    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                    builder.map(definition);
                    this.definition = Settings.builder().loadFromSource(Strings.toString(builder), builder.contentType()).build();
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse [" + definition + "]", e);
                }
            }

            NameOrDefinition(StreamInput in) throws IOException {
                name = in.readOptionalString();
                if (in.readBoolean()) {
                    definition = Settings.readSettingsFromStream(in);
                } else {
                    definition = null;
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalString(name);
                boolean isNotNullDefinition = this.definition != null;
                out.writeBoolean(isNotNullDefinition);
                if (isNotNullDefinition) {
                    Settings.writeSettingsToStream(definition, out);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                if (definition == null) {
                    return builder.value(name);
                }
                return definition.toXContent(builder, params);
            }

            public static NameOrDefinition fromXContent(XContentParser parser) throws IOException {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return new NameOrDefinition(parser.text());
                }
                if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    return new NameOrDefinition(parser.map());
                }
                throw new XContentParseException(parser.getTokenLocation(),
                    "Expected [VALUE_STRING] or [START_OBJECT], got " + parser.currentToken());
            }

        }

        public Request() {
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
                validationException = addValidationError("tokenizer/analyze should be null if normalizer is specified", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("text", text);
            if (Strings.isNullOrEmpty(analyzer) == false) {
                builder.field("analyzer", analyzer);
            }
            if (tokenizer != null) {
                tokenizer.toXContent(builder, params);
            }
            if (tokenFilters.size() > 0) {
                builder.field("filter", tokenFilters);
            }
            if (charFilters.size() > 0) {
                builder.field("char_filter", charFilters);
            }
            if (Strings.isNullOrEmpty(field) == false) {
                builder.field("field", field);
            }
            if (explain) {
                builder.field("explain", true);
            }
            if (attributes.length > 0) {
                builder.field("attributes", attributes);
            }
            if (Strings.isNullOrEmpty(normalizer) == false) {
                builder.field("normalizer", normalizer);
            }
            return builder.endObject();
        }

        public static Request fromXContent(XContentParser parser, String index) throws IOException {
            Request request = new Request(index);
            PARSER.parse(parser, request, null);
            return request;
        }

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>("analyze_request", null);
        static {
            PARSER.declareStringArray(Request::text, new ParseField("text"));
            PARSER.declareString(Request::analyzer, new ParseField("analyzer"));
            PARSER.declareField(Request::tokenizer, (p, c) -> NameOrDefinition.fromXContent(p),
                new ParseField("tokenizer"), ObjectParser.ValueType.OBJECT_OR_STRING);
            PARSER.declareObjectArray(Request::setTokenFilters, (p, c) -> NameOrDefinition.fromXContent(p),
                new ParseField("filter"));
            PARSER.declareObjectArray(Request::setCharFilters, (p, c) -> NameOrDefinition.fromXContent(p),
                new ParseField("char_filter"));
            PARSER.declareString(Request::field, new ParseField("field"));
            PARSER.declareBoolean(Request::explain, new ParseField("explain"));
            PARSER.declareStringArray(Request::attributes, new ParseField("attributes"));
            PARSER.declareString(Request::normalizer, new ParseField("normalizer"));
        }

    }
}

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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.admin.indices.RestAnalyzeAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for the categorization analyzer.
 *
 * The syntax is a subset of what can be supplied to the {@linkplain RestAnalyzeAction <code>_analyze</code> endpoint}.
 * To summarize, the first option is to specify the name of an out-of-the-box analyzer:
 * <code>
 *     "categorization_analyzer" : "standard"
 * </code>
 *
 * The second option is to specify a custom analyzer by combining the <code>char_filters</code>, <code>tokenizer</code>
 * and <code>token_filters</code> fields.  In turn, each of these can be specified as the name of an out-of-the-box
 * one or as an object defining a custom one.  For example:
 * <code>
 *     "char_filters" : [
 *         "html_strip",
 *         { "type" : "pattern_replace", "pattern": "SQL: .*" }
 *     ],
 *     "tokenizer" : "thai",
 *     "token_filters" : [
 *         "lowercase",
 *         { "type" : "pattern_replace", "pattern": "^[0-9].*" }
 *     ]
 * </code>
 */
public class CategorizationAnalyzerConfig implements ToXContentFragment {

    public static final ParseField CATEGORIZATION_ANALYZER = new ParseField("categorization_analyzer");
    private static final ParseField TOKENIZER = RestAnalyzeAction.Fields.TOKENIZER;
    private static final ParseField TOKEN_FILTERS = RestAnalyzeAction.Fields.TOKEN_FILTERS;
    private static final ParseField CHAR_FILTERS = RestAnalyzeAction.Fields.CHAR_FILTERS;

    /**
     * This method is only used in the unit tests - in production code this config is always parsed as a fragment.
     */
    static CategorizationAnalyzerConfig buildFromXContentObject(XContentParser parser) throws IOException {

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected start object but got [" + parser.currentToken() + "]");
        }
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("Expected field name but got [" + parser.currentToken() + "]");
        }
        parser.nextToken();
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = buildFromXContentFragment(parser);
        parser.nextToken();
        return categorizationAnalyzerConfig;
    }

    /**
     * Parse a <code>categorization_analyzer</code> configuration.  A custom parser is needed due to the
     * complexity of the format, with many elements able to be specified as either the name of a built-in
     * element or an object containing a custom definition.
     */
    static CategorizationAnalyzerConfig buildFromXContentFragment(XContentParser parser) throws IOException {

        CategorizationAnalyzerConfig.Builder builder = new CategorizationAnalyzerConfig.Builder();

        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            builder.setAnalyzer(parser.text());
        } else if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("[" + CATEGORIZATION_ANALYZER + "] should be analyzer's name or settings [" + token + "]");
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (CHAR_FILTERS.match(currentFieldName, parser.getDeprecationHandler())
                        && token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            builder.addCharFilter(parser.text());
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            builder.addCharFilter(parser.map());
                        } else {
                            throw new IllegalArgumentException("[" + currentFieldName + "] in [" + CATEGORIZATION_ANALYZER +
                                    "] array element should contain char_filter's name or settings [" + token + "]");
                        }
                    }
                } else if (TOKENIZER.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        builder.setTokenizer(parser.text());
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        builder.setTokenizer(parser.map());
                    } else {
                        throw new IllegalArgumentException("[" + currentFieldName + "] in [" + CATEGORIZATION_ANALYZER +
                                "] should be tokenizer's name or settings [" + token + "]");
                    }
                } else if (TOKEN_FILTERS.match(currentFieldName, parser.getDeprecationHandler())
                        && token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            builder.addTokenFilter(parser.text());
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            builder.addTokenFilter(parser.map());
                        } else {
                            throw new IllegalArgumentException("[" + currentFieldName + "] in [" + CATEGORIZATION_ANALYZER +
                                    "] array element should contain token_filter's name or settings [" + token + "]");
                        }
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Simple store of either a name of a built-in analyzer element or a custom definition.
     */
    public static final class NameOrDefinition implements ToXContentFragment {

        // Exactly one of these two members is not null
        public final String name;
        public final Settings definition;

        NameOrDefinition(String name) {
            this.name = Objects.requireNonNull(name);
            this.definition = null;
        }

        NameOrDefinition(ParseField field, Map<String, Object> definition) {
            this.name = null;
            Objects.requireNonNull(definition);
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(definition);
                this.definition = Settings.builder().loadFromSource(Strings.toString(builder), builder.contentType()).build();
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to parse [" + definition + "] in [" + field.getPreferredName() + "]", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (definition == null) {
                builder.value(name);
            } else {
                builder.startObject();
                definition.toXContent(builder, params);
                builder.endObject();
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NameOrDefinition that = (NameOrDefinition) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(definition, that.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, definition);
        }

        @Override
        public String toString() {
            if (definition == null) {
                return name;
            } else {
                return definition.toDelimitedString(';');
            }
        }
    }

    private final String analyzer;
    private final List<NameOrDefinition> charFilters;
    private final NameOrDefinition tokenizer;
    private final List<NameOrDefinition> tokenFilters;

    private CategorizationAnalyzerConfig(String analyzer, List<NameOrDefinition> charFilters, NameOrDefinition tokenizer,
                                         List<NameOrDefinition> tokenFilters) {
        this.analyzer = analyzer;
        this.charFilters = Collections.unmodifiableList(charFilters);
        this.tokenizer = tokenizer;
        this.tokenFilters = Collections.unmodifiableList(tokenFilters);
    }

    public String getAnalyzer() {
        return analyzer;
    }

    public List<NameOrDefinition> getCharFilters() {
        return charFilters;
    }

    public NameOrDefinition getTokenizer() {
        return tokenizer;
    }

    public List<NameOrDefinition> getTokenFilters() {
        return tokenFilters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (analyzer != null) {
            builder.field(CATEGORIZATION_ANALYZER.getPreferredName(), analyzer);
        } else {
            builder.startObject(CATEGORIZATION_ANALYZER.getPreferredName());
            if (charFilters.isEmpty() == false) {
                builder.startArray(CHAR_FILTERS.getPreferredName());
                for (NameOrDefinition charFilter : charFilters) {
                    charFilter.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (tokenizer != null) {
                builder.field(TOKENIZER.getPreferredName(), tokenizer);
            }
            if (tokenFilters.isEmpty() == false) {
                builder.startArray(TOKEN_FILTERS.getPreferredName());
                for (NameOrDefinition tokenFilter : tokenFilters) {
                    tokenFilter.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategorizationAnalyzerConfig that = (CategorizationAnalyzerConfig) o;
        return Objects.equals(analyzer, that.analyzer) &&
                Objects.equals(charFilters, that.charFilters) &&
                Objects.equals(tokenizer, that.tokenizer) &&
                Objects.equals(tokenFilters, that.tokenFilters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyzer, charFilters, tokenizer, tokenFilters);
    }

    public static class Builder {

        private String analyzer;
        private List<NameOrDefinition> charFilters = new ArrayList<>();
        private NameOrDefinition tokenizer;
        private List<NameOrDefinition> tokenFilters = new ArrayList<>();

        public Builder() {
        }

        public Builder(CategorizationAnalyzerConfig categorizationAnalyzerConfig) {
            this.analyzer = categorizationAnalyzerConfig.analyzer;
            this.charFilters = new ArrayList<>(categorizationAnalyzerConfig.charFilters);
            this.tokenizer = categorizationAnalyzerConfig.tokenizer;
            this.tokenFilters = new ArrayList<>(categorizationAnalyzerConfig.tokenFilters);
        }

        public Builder setAnalyzer(String analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public Builder addCharFilter(String charFilter) {
            this.charFilters.add(new NameOrDefinition(charFilter));
            return this;
        }

        public Builder addCharFilter(Map<String, Object> charFilter) {
            this.charFilters.add(new NameOrDefinition(CHAR_FILTERS, charFilter));
            return this;
        }

        public Builder setTokenizer(String tokenizer) {
            this.tokenizer = new NameOrDefinition(tokenizer);
            return this;
        }

        public Builder setTokenizer(Map<String, Object> tokenizer) {
            this.tokenizer = new NameOrDefinition(TOKENIZER, tokenizer);
            return this;
        }

        public Builder addTokenFilter(String tokenFilter) {
            this.tokenFilters.add(new NameOrDefinition(tokenFilter));
            return this;
        }

        public Builder addTokenFilter(Map<String, Object> tokenFilter) {
            this.tokenFilters.add(new NameOrDefinition(TOKEN_FILTERS, tokenFilter));
            return this;
        }

        /**
         * Create a config
         */
        public CategorizationAnalyzerConfig build() {
            return new CategorizationAnalyzerConfig(analyzer, charFilters, tokenizer, tokenFilters);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.rest.action.admin.indices.RestAnalyzeAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
public class CategorizationAnalyzerConfig implements ToXContentFragment, Writeable {

    public static final ParseField CATEGORIZATION_ANALYZER = new ParseField("categorization_analyzer");
    public static final ParseField TOKENIZER = AnalyzeAction.Fields.TOKENIZER;
    public static final ParseField TOKEN_FILTERS = AnalyzeAction.Fields.TOKEN_FILTERS;
    public static final ParseField CHAR_FILTERS = AnalyzeAction.Fields.CHAR_FILTERS;

    /**
     * This method is only used in the unit tests - in production code this config is always parsed as a fragment.
     */
    public static CategorizationAnalyzerConfig buildFromXContentObject(XContentParser parser, boolean ignoreUnknownFields)
        throws IOException {

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected start object but got [" + parser.currentToken() + "]");
        }
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME
                || CATEGORIZATION_ANALYZER.match(parser.currentName(), parser.getDeprecationHandler()) == false) {
            throw new IllegalArgumentException("Expected [" + CATEGORIZATION_ANALYZER + "] field but got [" + parser.currentToken() + "]");
        }
        parser.nextToken();
        CategorizationAnalyzerConfig categorizationAnalyzerConfig = buildFromXContentFragment(parser, ignoreUnknownFields);
        parser.nextToken();
        return categorizationAnalyzerConfig;
    }

    /**
     * Parse a <code>categorization_analyzer</code> from configuration or cluster state.  A custom parser is needed
     * due to the complexity of the format, with many elements able to be specified as either the name of a built-in
     * element or an object containing a custom definition.
     *
     * The parser is strict when parsing config and lenient when parsing cluster state.
     */
    static CategorizationAnalyzerConfig buildFromXContentFragment(XContentParser parser, boolean ignoreUnknownFields) throws IOException {

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
                // Be lenient when parsing cluster state - assume unknown fields are from future versions
                } else if (ignoreUnknownFields == false) {
                    throw new IllegalArgumentException("Parameter [" + currentFieldName + "] in [" + CATEGORIZATION_ANALYZER +
                            "] is unknown or of the wrong type [" + token + "]");
                }
            }
        }

        return builder.build();
    }

    /**
     * Create a <code>categorization_analyzer</code> that mimics what the tokenizer and filters built into the original ML
     * C++ code do.  This is the default analyzer for categorization to ensure that people upgrading from old versions
     * get the same behaviour from their categorization jobs before and after upgrade.
     * @param categorizationFilters Categorization filters (if any) from the <code>analysis_config</code>.
     * @return The default categorization analyzer.
     */
    public static CategorizationAnalyzerConfig buildDefaultCategorizationAnalyzer(List<String> categorizationFilters) {

        return new CategorizationAnalyzerConfig.Builder()
            .addCategorizationFilters(categorizationFilters)
            .setTokenizer("ml_classic")
            .addDateWordsTokenFilter()
            .build();
    }

    /**
     * Create a <code>categorization_analyzer</code> that will be used for newly created jobs where no categorization
     * analyzer is explicitly provided.  This analyzer differs from the default one in that it uses the <code>ml_standard</code>
     * tokenizer instead of the <code>ml_classic</code> tokenizer, and it only considers the first non-blank line of each message.
     * This analyzer is <em>not</em> used for jobs that specify no categorization analyzer, as that would break jobs that were
     * originally run in older versions.  Instead, this analyzer is explicitly added to newly created jobs once the entire cluster
     * is upgraded to version 7.14 or above.
     * @param categorizationFilters Categorization filters (if any) from the <code>analysis_config</code>.
     * @return The standard categorization analyzer.
     */
    public static CategorizationAnalyzerConfig buildStandardCategorizationAnalyzer(List<String> categorizationFilters) {

        return new CategorizationAnalyzerConfig.Builder()
            .addCharFilter("first_non_blank_line")
            .addCategorizationFilters(categorizationFilters)
            .setTokenizer("ml_standard")
            .addDateWordsTokenFilter()
            .build();
    }

    private final String analyzer;
    private final List<NameOrDefinition> charFilters;
    private final NameOrDefinition tokenizer;
    private final List<NameOrDefinition> tokenFilters;

    private CategorizationAnalyzerConfig(String analyzer, List<NameOrDefinition> charFilters, NameOrDefinition tokenizer,
                                         List<NameOrDefinition> tokenFilters) {
        this.analyzer = analyzer;
        this.charFilters = Objects.requireNonNull(charFilters);
        this.tokenizer = tokenizer;
        this.tokenFilters = Objects.requireNonNull(tokenFilters);
    }

    public CategorizationAnalyzerConfig(StreamInput in) throws IOException {
        analyzer = in.readOptionalString();
        charFilters = in.readList(NameOrDefinition::new);
        tokenizer = in.readOptionalWriteable(NameOrDefinition::new);
        tokenFilters = in.readList(NameOrDefinition::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(analyzer);
        out.writeList(charFilters);
        out.writeOptionalWriteable(tokenizer);
        out.writeList(tokenFilters);
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

    /**
     * Get the categorization analyzer structured as a generic map.
     * This can be used to provide the structure that the XContent serialization but as a Java map rather than text.
     * Since it is created by round-tripping through text it is not particularly efficient and is expected to be
     * used only rarely.
     */
    public Map<String, Object> asMap(NamedXContentRegistry xContentRegistry) throws IOException {
        String strRep = Strings.toString(this);
        XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, strRep);
        return parser.mapOrdered();
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
            this.charFilters.add(new NameOrDefinition(charFilter));
            return this;
        }

        public Builder addCategorizationFilters(List<String> categorizationFilters) {
            if (categorizationFilters != null) {
                for (String categorizationFilter : categorizationFilters) {
                    Map<String, Object> charFilter = new HashMap<>();
                    charFilter.put("type", "pattern_replace");
                    charFilter.put("pattern", categorizationFilter);
                    addCharFilter(charFilter);
                }
            }
            return this;
        }

        public Builder setTokenizer(String tokenizer) {
            this.tokenizer = new NameOrDefinition(tokenizer);
            return this;
        }

        public Builder setTokenizer(Map<String, Object> tokenizer) {
            this.tokenizer = new NameOrDefinition(tokenizer);
            return this;
        }

        public Builder addTokenFilter(String tokenFilter) {
            this.tokenFilters.add(new NameOrDefinition(tokenFilter));
            return this;
        }

        public Builder addTokenFilter(Map<String, Object> tokenFilter) {
            this.tokenFilters.add(new NameOrDefinition(tokenFilter));
            return this;
        }

        Builder addDateWordsTokenFilter() {
            Map<String, Object> tokenFilter = new HashMap<>();
            tokenFilter.put("type", "stop");
            tokenFilter.put("stopwords", Arrays.asList(
                "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
                "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun",
                "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December",
                "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                "GMT", "UTC"));
            addTokenFilter(tokenFilter);
            return this;
        }

        /**
         * Create a config validating only structure, not exact analyzer/tokenizer/filter names
         */
        public CategorizationAnalyzerConfig build() {
            if (analyzer == null && tokenizer == null) {
                throw new IllegalArgumentException(CATEGORIZATION_ANALYZER + " that is not a global analyzer must specify a ["
                        + TOKENIZER + "] field");
            }
            if (analyzer != null && charFilters.isEmpty() == false) {
                throw new IllegalArgumentException(CATEGORIZATION_ANALYZER + " that is a global analyzer cannot also specify a ["
                        + CHAR_FILTERS + "] field");
            }
            if (analyzer != null && tokenizer != null) {
                throw new IllegalArgumentException(CATEGORIZATION_ANALYZER + " that is a global analyzer cannot also specify a ["
                        + TOKENIZER + "] field");
            }
            if (analyzer != null && tokenFilters.isEmpty() == false) {
                throw new IllegalArgumentException(CATEGORIZATION_ANALYZER + " that is a global analyzer cannot also specify a ["
                        + TOKEN_FILTERS + "] field");
            }
            return new CategorizationAnalyzerConfig(analyzer, charFilters, tokenizer, tokenFilters);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A request to analyze text
 */
public class AnalyzeRequest implements Validatable, ToXContentObject {

    private String index;

    private String[] text;

    private String analyzer;

    private NameOrDefinition tokenizer;

    private final List<NameOrDefinition> tokenFilters = new ArrayList<>();

    private final List<NameOrDefinition> charFilters = new ArrayList<>();

    private String field;

    private boolean explain = false;

    private String[] attributes = Strings.EMPTY_ARRAY;

    private String normalizer;

    /**
     * Analyzes text using a global analyzer
     */
    public static AnalyzeRequest withGlobalAnalyzer(String analyzer, String... text) {
        return new AnalyzeRequest(null, analyzer, null, null, text);
    }

    /**
     * Analyzes text using a custom analyzer built from global components
     */
    public static CustomAnalyzerBuilder buildCustomAnalyzer(String tokenizer) {
        return new CustomAnalyzerBuilder(null, new NameOrDefinition(tokenizer));
    }

    /**
     * Analyzes text using a custom analyzer built from global components
     */
    public static CustomAnalyzerBuilder buildCustomAnalyzer(Map<String, Object> tokenizerSettings) {
        return new CustomAnalyzerBuilder(null, new NameOrDefinition(tokenizerSettings));
    }

    /**
     * Analyzes text using a custom analyzer built from components defined on an index
     */
    public static CustomAnalyzerBuilder buildCustomAnalyzer(String index, String tokenizer) {
        return new CustomAnalyzerBuilder(index, new NameOrDefinition(tokenizer));
    }

    /**
     * Analyzes text using a custom analyzer built from components defined on an index
     */
    public static CustomAnalyzerBuilder buildCustomAnalyzer(String index, Map<String, Object> tokenizerSettings) {
        return new CustomAnalyzerBuilder(index, new NameOrDefinition(tokenizerSettings));
    }

    /**
     * Analyzes text using a named analyzer on an index
     */
    public static AnalyzeRequest withIndexAnalyzer(String index, String analyzer, String... text) {
        return new AnalyzeRequest(index, analyzer, null, null, text);
    }

    /**
     * Analyzes text using the analyzer defined on a specific field within an index
     */
    public static AnalyzeRequest withField(String index, String field, String... text) {
        return new AnalyzeRequest(index, null, null, field, text);
    }

    /**
     * Analyzes text using a named normalizer on an index
     */
    public static AnalyzeRequest withNormalizer(String index, String normalizer, String... text) {
        return new AnalyzeRequest(index, null, normalizer, null, text);
    }

    /**
     * Analyzes text using a custom normalizer built from global components
     */
    public static CustomAnalyzerBuilder buildCustomNormalizer() {
        return new CustomAnalyzerBuilder(null, null);
    }

    /**
     * Analyzes text using a custom normalizer built from components defined on an index
     */
    public static CustomAnalyzerBuilder buildCustomNormalizer(String index) {
        return new CustomAnalyzerBuilder(index, null);
    }

    /**
     * Helper class to build custom analyzer definitions
     */
    public static class CustomAnalyzerBuilder {

        final NameOrDefinition tokenizer;
        final String index;
        List<NameOrDefinition> charFilters = new ArrayList<>();
        List<NameOrDefinition> tokenFilters = new ArrayList<>();

        CustomAnalyzerBuilder(String index, NameOrDefinition tokenizer) {
            this.tokenizer = tokenizer;
            this.index = index;
        }

        public CustomAnalyzerBuilder addCharFilter(String name) {
            charFilters.add(new NameOrDefinition(name));
            return this;
        }

        public CustomAnalyzerBuilder addCharFilter(Map<String, Object> settings) {
            charFilters.add(new NameOrDefinition(settings));
            return this;
        }

        public CustomAnalyzerBuilder addTokenFilter(String name) {
            tokenFilters.add(new NameOrDefinition(name));
            return this;
        }

        public CustomAnalyzerBuilder addTokenFilter(Map<String, Object> settings) {
            tokenFilters.add(new NameOrDefinition(settings));
            return this;
        }

        public AnalyzeRequest build(String... text) {
            return new AnalyzeRequest(index, tokenizer, charFilters, tokenFilters, text);
        }
    }

    private AnalyzeRequest(String index, String analyzer, String normalizer, String field, String... text) {
        this.index = index;
        this.analyzer = analyzer;
        this.normalizer = normalizer;
        this.field = field;
        this.text = text;
    }

    private AnalyzeRequest(
        String index,
        NameOrDefinition tokenizer,
        List<NameOrDefinition> charFilters,
        List<NameOrDefinition> tokenFilters,
        String... text
    ) {
        this.index = index;
        this.analyzer = null;
        this.normalizer = null;
        this.field = null;
        this.tokenizer = tokenizer;
        this.charFilters.addAll(charFilters);
        this.tokenFilters.addAll(tokenFilters);
        this.text = text;
    }

    static class NameOrDefinition implements ToXContentFragment {
        // exactly one of these two members is not null
        public final String name;
        public final Settings definition;

        NameOrDefinition(String name) {
            this.name = Objects.requireNonNull(name);
            this.definition = null;
        }

        NameOrDefinition(Settings settings) {
            this.name = null;
            this.definition = Objects.requireNonNull(settings);
        }

        NameOrDefinition(Map<String, ?> definition) {
            this.name = null;
            Objects.requireNonNull(definition);
            this.definition = Settings.builder().loadFromMap(definition).build();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (definition == null) {
                return builder.value(name);
            }
            builder.startObject();
            definition.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

    }

    /**
     * Returns the index that the request should be executed against, or {@code null} if
     * no index is specified
     */
    public String index() {
        return this.index;
    }

    /**
     * Returns the text to be analyzed
     */
    public String[] text() {
        return this.text;
    }

    /**
     * Returns the named analyzer used for analysis, if defined
     */
    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Returns the named tokenizer used for analysis, if defined
     */
    public String normalizer() {
        return this.normalizer;
    }

    /**
     * Returns a custom Tokenizer used for analysis, if defined
     */
    public NameOrDefinition tokenizer() {
        return this.tokenizer;
    }

    /**
     * Returns the custom token filters used for analysis, if defined
     */
    public List<NameOrDefinition> tokenFilters() {
        return this.tokenFilters;
    }

    /**
     * Returns the custom character filters used for analysis, if defined
     */
    public List<NameOrDefinition> charFilters() {
        return this.charFilters;
    }

    /**
     * Returns the field to take an Analyzer from, if defined
     */
    public String field() {
        return this.field;
    }

    /**
     * Set whether or not detailed explanations of analysis should be returned
     */
    public AnalyzeRequest explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public boolean explain() {
        return this.explain;
    }

    public AnalyzeRequest attributes(String... attributes) {
        if (attributes == null) {
            throw new IllegalArgumentException("attributes must not be null");
        }
        this.attributes = attributes;
        return this;
    }

    public String[] attributes() {
        return this.attributes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("text", text);
        if (Strings.isNullOrEmpty(analyzer) == false) {
            builder.field("analyzer", analyzer);
        }
        if (tokenizer != null) {
            builder.field("tokenizer", tokenizer);
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
}

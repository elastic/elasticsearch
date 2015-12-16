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

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SimpleQueryParser.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * SimpleQuery is a query parser that acts similar to a query_string query, but
 * won't throw exceptions for any weird string syntax.
 *
 * For more detailed explanation of the query string syntax see also the <a
 * href=
 * "https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html"
 * > online documentation</a>.
 */
public class SimpleQueryStringBuilder extends AbstractQueryBuilder<SimpleQueryStringBuilder> {
    /** Default locale used for parsing.*/
    public static final Locale DEFAULT_LOCALE = Locale.ROOT;
    /** Default for lowercasing parsed terms.*/
    public static final boolean DEFAULT_LOWERCASE_EXPANDED_TERMS = true;
    /** Default for using lenient query parsing.*/
    public static final boolean DEFAULT_LENIENT = false;
    /** Default for wildcard analysis.*/
    public static final boolean DEFAULT_ANALYZE_WILDCARD = false;
    /** Default for default operator to use for linking boolean clauses.*/
    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    /** Default for search flags to use. */
    public static final int DEFAULT_FLAGS = SimpleQueryStringFlag.ALL.value;
    /** Name for (de-)serialization. */
    public static final String NAME = "simple_query_string";

    static final SimpleQueryStringBuilder PROTOTYPE = new SimpleQueryStringBuilder("");

    /** Query text to parse. */
    private final String queryText;
    /**
     * Fields to query against. If left empty will query default field,
     * currently _ALL. Uses a TreeMap to hold the fields so boolean clauses are
     * always sorted in same order for generated Lucene query for easier
     * testing.
     *
     * Can be changed back to HashMap once https://issues.apache.org/jira/browse/LUCENE-6305 is fixed.
     */
    private final Map<String, Float> fieldsAndWeights = new TreeMap<>();
    /** If specified, analyzer to use to parse the query text, defaults to registered default in toQuery. */
    private String analyzer;
    /** Default operator to use for linking boolean clauses. Defaults to OR according to docs. */
    private Operator defaultOperator = DEFAULT_OPERATOR;
    /** If result is a boolean query, minimumShouldMatch parameter to apply. Ignored otherwise. */
    private String minimumShouldMatch;
    /** Any search flags to be used, ALL by default. */
    private int flags = DEFAULT_FLAGS;

    /** Further search settings needed by the ES specific query string parser only. */
    private Settings settings = new Settings();

    /** Construct a new simple query with this query string. */
    public SimpleQueryStringBuilder(String queryText) {
        if (queryText == null) {
            throw new IllegalArgumentException("query text missing");
        }
        this.queryText = queryText;
    }

    /** Returns the text to parse the query from. */
    public String value() {
        return this.queryText;
    }

    /** Add a field to run the query against. */
    public SimpleQueryStringBuilder field(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        this.fieldsAndWeights.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
        return this;
    }

    /** Add a field to run the query against with a specific boost. */
    public SimpleQueryStringBuilder field(String field, float boost) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        this.fieldsAndWeights.put(field, boost);
        return this;
    }

    /** Add several fields to run the query against with a specific boost. */
    public SimpleQueryStringBuilder fields(Map<String, Float> fields) {
        Objects.requireNonNull(fields, "fields cannot be null");
        this.fieldsAndWeights.putAll(fields);
        return this;
    }

    /** Returns the fields including their respective boosts to run the query against. */
    public Map<String, Float> fields() {
        return this.fieldsAndWeights;
    }

    /** Specify an analyzer to use for the query. */
    public SimpleQueryStringBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Returns the analyzer to use for the query. */
    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Specify the default operator for the query. Defaults to "OR" if no
     * operator is specified.
     */
    public SimpleQueryStringBuilder defaultOperator(Operator defaultOperator) {
        this.defaultOperator = (defaultOperator != null) ? defaultOperator : DEFAULT_OPERATOR;
        return this;
    }

    /** Returns the default operator for the query. */
    public Operator defaultOperator() {
        return this.defaultOperator;
    }

    /**
     * Specify the enabled features of the SimpleQueryString. Defaults to ALL if
     * none are specified.
     */
    public SimpleQueryStringBuilder flags(SimpleQueryStringFlag... flags) {
        if (flags != null && flags.length > 0) {
            int value = 0;
            for (SimpleQueryStringFlag flag : flags) {
                value |= flag.value;
            }
            this.flags = value;
        } else {
            this.flags = DEFAULT_FLAGS;
        }

        return this;
    }

    /** For testing and serialisation only. */
    SimpleQueryStringBuilder flags(int flags) {
        this.flags = flags;
        return this;
    }

    /** For testing only: Return the flags set for this query. */
    int flags() {
        return this.flags;
    }

    /**
     * Specifies whether parsed terms for this query should be lower-cased.
     * Defaults to true if not set.
     */
    public SimpleQueryStringBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.settings.lowercaseExpandedTerms(lowercaseExpandedTerms);
        return this;
    }

    /** Returns whether parsed terms should be lower cased for this query. */
    public boolean lowercaseExpandedTerms() {
        return this.settings.lowercaseExpandedTerms();
    }

    /** Specifies the locale for parsing terms. Defaults to ROOT if none is set. */
    public SimpleQueryStringBuilder locale(Locale locale) {
        this.settings.locale(locale);
        return this;
    }

    /** Returns the locale for parsing terms for this query. */
    public Locale locale() {
        return this.settings.locale();
    }

    /** Specifies whether query parsing should be lenient. Defaults to false. */
    public SimpleQueryStringBuilder lenient(boolean lenient) {
        this.settings.lenient(lenient);
        return this;
    }

    /** Returns whether query parsing should be lenient. */
    public boolean lenient() {
        return this.settings.lenient();
    }

    /** Specifies whether wildcards should be analyzed. Defaults to false. */
    public SimpleQueryStringBuilder analyzeWildcard(boolean analyzeWildcard) {
        this.settings.analyzeWildcard(analyzeWildcard);
        return this;
    }

    /** Returns whether wildcards should by analyzed. */
    public boolean analyzeWildcard() {
        return this.settings.analyzeWildcard();
    }

    /**
     * Specifies the minimumShouldMatch to apply to the resulting query should
     * that be a Boolean query.
     */
    public SimpleQueryStringBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Returns the minimumShouldMatch to apply to the resulting query should
     * that be a Boolean query.
     */
    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // field names in builder can have wildcards etc, need to resolve them here
        Map<String, Float> resolvedFieldsAndWeights = new TreeMap<>();
        // Use the default field if no fields specified
        if (fieldsAndWeights.isEmpty()) {
            resolvedFieldsAndWeights.put(resolveIndexName(context.defaultField(), context), AbstractQueryBuilder.DEFAULT_BOOST);
        } else {
            for (Map.Entry<String, Float> fieldEntry : fieldsAndWeights.entrySet()) {
                if (Regex.isSimpleMatchPattern(fieldEntry.getKey())) {
                    for (String fieldName : context.getMapperService().simpleMatchToIndexNames(fieldEntry.getKey())) {
                        resolvedFieldsAndWeights.put(fieldName, fieldEntry.getValue());
                    }
                } else {
                    resolvedFieldsAndWeights.put(resolveIndexName(fieldEntry.getKey(), context), fieldEntry.getValue());
                }
            }
        }

        // Use standard analyzer by default if none specified
        Analyzer luceneAnalyzer;
        if (analyzer == null) {
            luceneAnalyzer = context.getMapperService().searchAnalyzer();
        } else {
            luceneAnalyzer = context.getAnalysisService().analyzer(analyzer);
            if (luceneAnalyzer == null) {
                throw new QueryShardException(context, "[" + SimpleQueryStringBuilder.NAME + "] analyzer [" + analyzer
                        + "] not found");
            }

        }

        SimpleQueryParser sqp = new SimpleQueryParser(luceneAnalyzer, resolvedFieldsAndWeights, flags, settings);
        sqp.setDefaultOperator(defaultOperator.toBooleanClauseOccur());

        Query query = sqp.parse(queryText);
        if (minimumShouldMatch != null && query instanceof BooleanQuery) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        return query;
    }

    private static String resolveIndexName(String fieldName, QueryShardContext context) {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            return fieldType.name();
        }
        return fieldName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(SimpleQueryStringParser.QUERY_FIELD.getPreferredName(), queryText);

        if (fieldsAndWeights.size() > 0) {
            builder.startArray(SimpleQueryStringParser.FIELDS_FIELD.getPreferredName());
            for (Map.Entry<String, Float> entry : fieldsAndWeights.entrySet()) {
                builder.value(entry.getKey() + "^" + entry.getValue());
            }
            builder.endArray();
        }

        if (analyzer != null) {
            builder.field(SimpleQueryStringParser.ANALYZER_FIELD.getPreferredName(), analyzer);
        }

        builder.field(SimpleQueryStringParser.FLAGS_FIELD.getPreferredName(), flags);
        builder.field(SimpleQueryStringParser.DEFAULT_OPERATOR_FIELD.getPreferredName(), defaultOperator.name().toLowerCase(Locale.ROOT));
        builder.field(SimpleQueryStringParser.LOWERCASE_EXPANDED_TERMS_FIELD.getPreferredName(), settings.lowercaseExpandedTerms());
        builder.field(SimpleQueryStringParser.LENIENT_FIELD.getPreferredName(), settings.lenient());
        builder.field(SimpleQueryStringParser.ANALYZE_WILDCARD_FIELD.getPreferredName(), settings.analyzeWildcard());
        builder.field(SimpleQueryStringParser.LOCALE_FIELD.getPreferredName(), (settings.locale().toLanguageTag()));

        if (minimumShouldMatch != null) {
            builder.field(SimpleQueryStringParser.MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected SimpleQueryStringBuilder doReadFrom(StreamInput in) throws IOException {
        SimpleQueryStringBuilder result = new SimpleQueryStringBuilder(in.readString());
        int size = in.readInt();
        Map<String, Float> fields = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            Float weight = in.readFloat();
            fields.put(field, weight);
        }
        result.fieldsAndWeights.putAll(fields);
        result.flags = in.readInt();
        result.analyzer = in.readOptionalString();
        result.defaultOperator = Operator.readOperatorFrom(in);
        result.settings.lowercaseExpandedTerms(in.readBoolean());
        result.settings.lenient(in.readBoolean());
        result.settings.analyzeWildcard(in.readBoolean());
        String localeStr = in.readString();
        result.settings.locale(Locale.forLanguageTag(localeStr));
        result.minimumShouldMatch = in.readOptionalString();
        return result;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(queryText);
        out.writeInt(fieldsAndWeights.size());
        for (Map.Entry<String, Float> entry : fieldsAndWeights.entrySet()) {
            out.writeString(entry.getKey());
            out.writeFloat(entry.getValue());
        }
        out.writeInt(flags);
        out.writeOptionalString(analyzer);
        defaultOperator.writeTo(out);
        out.writeBoolean(settings.lowercaseExpandedTerms());
        out.writeBoolean(settings.lenient());
        out.writeBoolean(settings.analyzeWildcard());
        out.writeString(settings.locale().toLanguageTag());
        out.writeOptionalString(minimumShouldMatch);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldsAndWeights, analyzer, defaultOperator, queryText, minimumShouldMatch, settings, flags);
    }

    @Override
    protected boolean doEquals(SimpleQueryStringBuilder other) {
        return Objects.equals(fieldsAndWeights, other.fieldsAndWeights) && Objects.equals(analyzer, other.analyzer)
                && Objects.equals(defaultOperator, other.defaultOperator) && Objects.equals(queryText, other.queryText)
                && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
                && Objects.equals(settings, other.settings) && (flags == other.flags);
    }
}


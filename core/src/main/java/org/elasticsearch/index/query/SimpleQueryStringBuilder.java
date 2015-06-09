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
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
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
public class SimpleQueryStringBuilder extends AbstractQueryBuilder<SimpleQueryStringBuilder> implements BoostableQueryBuilder<SimpleQueryStringBuilder> {
    /** Default locale used for parsing.*/
    public static final Locale DEFAULT_LOCALE = Locale.ROOT;
    /** Default for lowercasing parsed terms.*/
    public static final boolean DEFAULT_LOWERCASE_EXPANDED_TERMS = true;
    /** Default for using lenient query parsing.*/
    public static final boolean DEFAULT_LENIENT = false;
    /** Default for wildcard analysis.*/
    public static final boolean DEFAULT_ANALYZE_WILDCARD = false;
    /** Default for boost to apply to resulting Lucene query. Defaults to 1.0*/
    public static final float DEFAULT_BOOST = 1.0f; 
    /** Default for default operator to use for linking boolean clauses.*/
    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    /** Default for search flags to use. */
    public static final int DEFAULT_FLAGS = SimpleQueryStringFlag.ALL.value;
    /** Name for (de-)serialization. */
    public static final String NAME = "simple_query_string";
    /** Query text to parse. */
    private final String queryText;
    /** Boost to apply to resulting Lucene query. Defaults to 1.0*/
    private float boost = DEFAULT_BOOST;
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
    /** Name of the query. Optional.*/
    private String queryName;
    /** Default operator to use for linking boolean clauses. Defaults to OR according to docs. */
    private Operator defaultOperator = DEFAULT_OPERATOR;
    /** If result is a boolean query, minimumShouldMatch parameter to apply. Ignored otherwise. */
    private String minimumShouldMatch;
    /** Any search flags to be used, ALL by default. */
    private int flags = DEFAULT_FLAGS;

    /** Further search settings needed by the ES specific query string parser only. */
    private Settings settings = new Settings();

    static final SimpleQueryStringBuilder PROTOTYPE = new SimpleQueryStringBuilder(null);

    /** Operators available for linking boolean clauses. */
    // Move out after #11345 is in.
    public static enum Operator {
        AND,
        OR;

        public static Operator parseFromInt(int ordinal) {
            switch (ordinal) {
                case 0:
                    return AND;
                case 1:
                    return OR;
                default:
                    throw new IllegalArgumentException("cannot parse Operator from ordinal " + ordinal);
            }

        }
    }

    /** Construct a new simple query with this query string. */
    public SimpleQueryStringBuilder(String queryText) {
        this.queryText = queryText;
    }

    @Override
    public SimpleQueryStringBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /** Returns the boost to apply to resulting Lucene query.*/
    public float boost() {
        return this.boost;
    }
    /** Returns the text to parse the query from. */
    public String text() {
        return this.queryText;
    }

    /** Add a field to run the query against. */
    public SimpleQueryStringBuilder field(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        this.fieldsAndWeights.put(field, 1.0f);
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

    /** Set the name for this query. */
    public SimpleQueryStringBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /** Returns the name for this query. */
    public String queryName() {
        return queryName;
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
        this.settings.analyzeWildcard(DEFAULT_ANALYZE_WILDCARD);
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

    /** 
     * {@inheritDoc} 
     * 
     * Checks that mandatory queryText is neither null nor empty.
     * */
    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;

        // Query text is required
        if (queryText == null) {
            validationException = QueryValidationException.addValidationError("[" + SimpleQueryStringBuilder.NAME + "] query text missing",
                    validationException);
        }

        return validationException;
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) {
        // Use the default field (_all) if no fields specified
        if (fieldsAndWeights.isEmpty()) {
            String field = parseContext.defaultField();
            fieldsAndWeights.put(field, 1.0F);
        }

        // Use standard analyzer by default if none specified
        Analyzer luceneAnalyzer;
        if (analyzer == null) {
            luceneAnalyzer = parseContext.mapperService().searchAnalyzer();
        } else {
            luceneAnalyzer = parseContext.analysisService().analyzer(analyzer);
            if (luceneAnalyzer == null) {
                throw new QueryParsingException(parseContext, "[" + SimpleQueryStringBuilder.NAME + "] analyzer [" + analyzer
                        + "] not found");
            }

        }
        SimpleQueryParser sqp = new SimpleQueryParser(luceneAnalyzer, fieldsAndWeights, flags, settings);

        if (defaultOperator != null) {
            switch (defaultOperator) {
                case OR:
                    sqp.setDefaultOperator(Occur.SHOULD);
                    break;
                case AND:
                    sqp.setDefaultOperator(Occur.MUST);
                    break;
            }
        }

        Query query = sqp.parse(queryText);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }

        if (minimumShouldMatch != null && query instanceof BooleanQuery) {
            Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }

        // safety check - https://github.com/elastic/elasticsearch/pull/11696#discussion-diff-32532468 
        if (query != null) {
            query.setBoost(boost);
        }
        return query;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field("query", queryText);

        if (fieldsAndWeights.size() > 0) {
            builder.startArray("fields");
            for (Map.Entry<String, Float> entry : fieldsAndWeights.entrySet()) {
                String field = entry.getKey();
                Float boost = entry.getValue();
                if (boost != null) {
                    builder.value(field + "^" + boost);
                } else {
                    builder.value(field);
                }
            }
            builder.endArray();
        }

        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }

        builder.field("flags", flags);
        builder.field("default_operator", defaultOperator.name().toLowerCase(Locale.ROOT));
        builder.field("lowercase_expanded_terms", settings.lowercaseExpandedTerms());
        builder.field("lenient", settings.lenient());
        builder.field("analyze_wildcard", settings.analyzeWildcard());
        builder.field("locale", (settings.locale().toLanguageTag()));

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }

        if (boost != -1.0f) {
            builder.field("boost", boost);
        }

        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public SimpleQueryStringBuilder readFrom(StreamInput in) throws IOException {
        SimpleQueryStringBuilder result = new SimpleQueryStringBuilder(in.readString());
        result.boost = in.readFloat();
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

        result.defaultOperator = Operator.parseFromInt(in.readInt());
        result.settings.lowercaseExpandedTerms(in.readBoolean());
        result.settings.lenient(in.readBoolean());
        result.settings.analyzeWildcard(in.readBoolean());

        String localeStr = in.readString();
        result.settings.locale(Locale.forLanguageTag(localeStr));

        result.queryName = in.readOptionalString();
        result.minimumShouldMatch = in.readOptionalString();

        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryText);
        out.writeFloat(boost);
        out.writeInt(fieldsAndWeights.size());
        for (Map.Entry<String, Float> entry : fieldsAndWeights.entrySet()) {
            out.writeString(entry.getKey());
            out.writeFloat(entry.getValue());
        }
        out.writeInt(flags);
        out.writeOptionalString(analyzer);
        out.writeInt(defaultOperator.ordinal());
        out.writeBoolean(settings.lowercaseExpandedTerms());
        out.writeBoolean(settings.lenient());
        out.writeBoolean(settings.analyzeWildcard());
        out.writeString(settings.locale().toLanguageTag());

        out.writeOptionalString(queryName);
        out.writeOptionalString(minimumShouldMatch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldsAndWeights, analyzer, defaultOperator, queryText, queryName, minimumShouldMatch, settings);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimpleQueryStringBuilder other = (SimpleQueryStringBuilder) obj;
        return Objects.equals(fieldsAndWeights, other.fieldsAndWeights) && Objects.equals(analyzer, other.analyzer)
                && Objects.equals(defaultOperator, other.defaultOperator) && Objects.equals(queryText, other.queryText)
                && Objects.equals(queryName, other.queryName) && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
                && Objects.equals(settings, other.settings);
    }
}


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

import com.carrotsearch.hppc.ObjectFloatHashMap;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A query that parses a query string and runs it. There are two modes that this operates. The first,
 * when no field is added (using {@link #field(String)}, will run the query once and non prefixed fields
 * will use the {@link #defaultField(String)} set. The second, when one or more fields are added
 * (using {@link #field(String)}), will run the parsed query against the provided fields, and combine
 * them either using DisMax or a plain boolean query (see {@link #useDisMax(boolean)}).
 * <p/>
 */
public class QueryStringQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<QueryStringQueryBuilder> {

    public enum Operator {
        OR,
        AND
    }

    private final String queryString;

    private String defaultField;

    private Operator defaultOperator;

    private String analyzer;
    private String quoteAnalyzer;

    private String quoteFieldSuffix;

    private Boolean autoGeneratePhraseQueries;

    private Boolean allowLeadingWildcard;

    private Boolean lowercaseExpandedTerms;

    private Boolean enablePositionIncrements;

    private Boolean analyzeWildcard;

    private Locale locale;

    private float boost = -1;

    private Fuzziness fuzziness;
    private int fuzzyPrefixLength = -1;
    private int fuzzyMaxExpansions = -1;
    private String fuzzyRewrite;

    private int phraseSlop = -1;

    private List<String> fields;

    private ObjectFloatHashMap<String> fieldsBoosts;

    private Boolean useDisMax;

    private float tieBreaker = -1;

    private String rewrite = null;

    private String minimumShouldMatch;

    private Boolean lenient;

    private String queryName;

    private String timeZone;

    /** To limit effort spent determinizing regexp queries. */
    private Integer maxDeterminizedStates;

    private Boolean escape;

    public QueryStringQueryBuilder(String queryString) {
        this.queryString = queryString;
    }

    /**
     * The default field to run against when no prefix field is specified. Only relevant when
     * not explicitly adding fields the query string will run against.
     */
    public QueryStringQueryBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    /**
     * Adds a field to run the query string against.
     */
    public QueryStringQueryBuilder field(String field) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(field);
        return this;
    }

    /**
     * Adds a field to run the query string against with a specific boost.
     */
    public QueryStringQueryBuilder field(String field, float boost) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(field);
        if (fieldsBoosts == null) {
            fieldsBoosts = new ObjectFloatHashMap<>();
        }
        fieldsBoosts.put(field, boost);
        return this;
    }

    /**
     * When more than one field is used with the query string, should queries be combined using
     * dis max, or boolean query. Defaults to dis max (<tt>true</tt>).
     */
    public QueryStringQueryBuilder useDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
        return this;
    }

    /**
     * When more than one field is used with the query string, and combined queries are using
     * dis max, control the tie breaker for it.
     */
    public QueryStringQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * Sets the boolean operator of the query parser used to parse the query string.
     * <p/>
     * <p>In default mode ({@link Operator#OR}) terms without any modifiers
     * are considered optional: for example <code>capital of Hungary</code> is equal to
     * <code>capital OR of OR Hungary</code>.
     * <p/>
     * <p>In {@link Operator#AND} mode terms are considered to be in conjunction: the
     * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
     */
    public QueryStringQueryBuilder defaultOperator(Operator defaultOperator) {
        this.defaultOperator = defaultOperator;
        return this;
    }

    /**
     * The optional analyzer used to analyze the query string. Note, if a field has search analyzer
     * defined for it, then it will be used automatically. Defaults to the smart search analyzer.
     */
    public QueryStringQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * The optional analyzer used to analyze the query string for phrase searches. Note, if a field has search (quote) analyzer
     * defined for it, then it will be used automatically. Defaults to the smart search analyzer.
     */
    public QueryStringQueryBuilder quoteAnalyzer(String analyzer) {
        this.quoteAnalyzer = analyzer;
        return this;
    }


    /**
     * Set to true if phrase queries will be automatically generated
     * when the analyzer returns more than one term from whitespace
     * delimited text.
     * NOTE: this behavior may not be suitable for all languages.
     * <p/>
     * Set to false if phrase queries should only be generated when
     * surrounded by double quotes.
     */
    public QueryStringQueryBuilder autoGeneratePhraseQueries(boolean autoGeneratePhraseQueries) {
        this.autoGeneratePhraseQueries = autoGeneratePhraseQueries;
        return this;
    }

    /**
     * Protects against too-difficult regular expression queries.
     */
    public QueryStringQueryBuilder maxDeterminizedStates(int maxDeterminizedStates) {
        this.maxDeterminizedStates = maxDeterminizedStates;
        return this;
    }

    /**
     * Should leading wildcards be allowed or not. Defaults to <tt>true</tt>.
     */
    public QueryStringQueryBuilder allowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
        return this;
    }

    /**
     * Whether terms of wildcard, prefix, fuzzy and range queries are to be automatically
     * lower-cased or not.  Default is <tt>true</tt>.
     */
    public QueryStringQueryBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    /**
     * Set to <tt>true</tt> to enable position increments in result query. Defaults to
     * <tt>true</tt>.
     * <p/>
     * <p>When set, result phrase and multi-phrase queries will be aware of position increments.
     * Useful when e.g. a StopFilter increases the position increment of the token that follows an omitted token.
     */
    public QueryStringQueryBuilder enablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
        return this;
    }

    /**
     * Set the edit distance for fuzzy queries. Default is "AUTO".
     */
    public QueryStringQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    /**
     * Set the minimum prefix length for fuzzy queries. Default is 1.
     */
    public QueryStringQueryBuilder fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        return this;
    }

    public QueryStringQueryBuilder fuzzyMaxExpansions(int fuzzyMaxExpansions) {
        this.fuzzyMaxExpansions = fuzzyMaxExpansions;
        return this;
    }

    public QueryStringQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    /**
     * Sets the default slop for phrases.  If zero, then exact phrase matches
     * are required. Default value is zero.
     */
    public QueryStringQueryBuilder phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
        return this;
    }

    /**
     * Set to <tt>true</tt> to enable analysis on wildcard and prefix queries.
     */
    public QueryStringQueryBuilder analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        return this;
    }

    public QueryStringQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public QueryStringQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public QueryStringQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * An optional field name suffix to automatically try and add to the field searched when using quoted text.
     */
    public QueryStringQueryBuilder quoteFieldSuffix(String quoteFieldSuffix) {
        this.quoteFieldSuffix = quoteFieldSuffix;
        return this;
    }

    /**
     * Sets the query string parser to be lenient when parsing field values, defaults to the index
     * setting and if not set, defaults to false.
     */
    public QueryStringQueryBuilder lenient(Boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public QueryStringQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    public QueryStringQueryBuilder locale(Locale locale) {
        this.locale = locale;
        return this;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public QueryStringQueryBuilder timeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Set to <tt>true</tt> to enable escaping of the query string
     */
    public QueryStringQueryBuilder escape(boolean escape) {
        this.escape = escape;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(QueryStringQueryParser.NAME);
        builder.field("query", queryString);
        if (defaultField != null) {
            builder.field("default_field", defaultField);
        }
        if (fields != null) {
            builder.startArray("fields");
            for (String field : fields) {
                if (fieldsBoosts != null && fieldsBoosts.containsKey(field)) {
                    field += "^" + fieldsBoosts.get(field);
                }
                builder.value(field);
            }
            builder.endArray();
        }
        if (useDisMax != null) {
            builder.field("use_dis_max", useDisMax);
        }
        if (tieBreaker != -1) {
            builder.field("tie_breaker", tieBreaker);
        }
        if (defaultOperator != null) {
            builder.field("default_operator", defaultOperator.name().toLowerCase(Locale.ROOT));
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (quoteAnalyzer != null) {
            builder.field("quote_analyzer", quoteAnalyzer);
        }
        if (autoGeneratePhraseQueries != null) {
            builder.field("auto_generate_phrase_queries", autoGeneratePhraseQueries);
        }
        if (maxDeterminizedStates != null) {
            builder.field("max_determinized_states", maxDeterminizedStates);
        }
        if (allowLeadingWildcard != null) {
            builder.field("allow_leading_wildcard", allowLeadingWildcard);
        }
        if (lowercaseExpandedTerms != null) {
            builder.field("lowercase_expanded_terms", lowercaseExpandedTerms);
        }
        if (enablePositionIncrements != null) {
            builder.field("enable_position_increments", enablePositionIncrements);
        }
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (fuzzyPrefixLength != -1) {
            builder.field("fuzzy_prefix_length", fuzzyPrefixLength);
        }
        if (fuzzyMaxExpansions != -1) {
            builder.field("fuzzy_max_expansions", fuzzyMaxExpansions);
        }
        if (fuzzyRewrite != null) {
            builder.field("fuzzy_rewrite", fuzzyRewrite);
        }
        if (phraseSlop != -1) {
            builder.field("phrase_slop", phraseSlop);
        }
        if (analyzeWildcard != null) {
            builder.field("analyze_wildcard", analyzeWildcard);
        }
        if (rewrite != null) {
            builder.field("rewrite", rewrite);
        }
        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (quoteFieldSuffix != null) {
            builder.field("quote_field_suffix", quoteFieldSuffix);
        }
        if (lenient != null) {
            builder.field("lenient", lenient);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (locale != null) {
            builder.field("locale", locale.toString());
        }
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
        if (escape != null) {
            builder.field("escape", escape);
        }
        builder.endObject();
    }
}

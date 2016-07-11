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

import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.support.QueryParsers;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A query that parses a query string and runs it. There are two modes that this operates. The first,
 * when no field is added (using {@link #field(String)}, will run the query once and non prefixed fields
 * will use the {@link #defaultField(String)} set. The second, when one or more fields are added
 * (using {@link #field(String)}), will run the parsed query against the provided fields, and combine
 * them either using DisMax or a plain boolean query (see {@link #useDisMax(boolean)}).
 */
public class QueryStringQueryBuilder extends AbstractQueryBuilder<QueryStringQueryBuilder> {

    public static final String NAME = "query_string";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    public static final boolean DEFAULT_AUTO_GENERATE_PHRASE_QUERIES = false;
    public static final int DEFAULT_MAX_DETERMINED_STATES = Operations.DEFAULT_MAX_DETERMINIZED_STATES;
    public static final boolean DEFAULT_LOWERCASE_EXPANDED_TERMS = true;
    public static final boolean DEFAULT_ENABLE_POSITION_INCREMENTS = true;
    public static final boolean DEFAULT_ESCAPE = false;
    public static final boolean DEFAULT_USE_DIS_MAX = true;
    public static final int DEFAULT_FUZZY_PREFIX_LENGTH = FuzzyQuery.defaultPrefixLength;
    public static final int DEFAULT_FUZZY_MAX_EXPANSIONS = FuzzyQuery.defaultMaxExpansions;
    public static final int DEFAULT_PHRASE_SLOP = 0;
    public static final float DEFAULT_TIE_BREAKER = 0.0f;
    public static final Fuzziness DEFAULT_FUZZINESS = Fuzziness.AUTO;
    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    public static final Locale DEFAULT_LOCALE = Locale.ROOT;

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField DEFAULT_FIELD_FIELD = new ParseField("default_field");
    private static final ParseField DEFAULT_OPERATOR_FIELD = new ParseField("default_operator");
    private static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static final ParseField QUOTE_ANALYZER_FIELD = new ParseField("quote_analyzer");
    private static final ParseField ALLOW_LEADING_WILDCARD_FIELD = new ParseField("allow_leading_wildcard");
    private static final ParseField AUTO_GENERATED_PHRASE_QUERIES_FIELD = new ParseField("auto_generated_phrase_queries");
    private static final ParseField MAX_DETERMINED_STATES_FIELD = new ParseField("max_determined_states");
    private static final ParseField LOWERCASE_EXPANDED_TERMS_FIELD = new ParseField("lowercase_expanded_terms");
    private static final ParseField ENABLE_POSITION_INCREMENTS_FIELD = new ParseField("enable_position_increment");
    private static final ParseField ESCAPE_FIELD = new ParseField("escape");
    private static final ParseField USE_DIS_MAX_FIELD = new ParseField("use_dis_max");
    private static final ParseField FUZZY_PREFIX_LENGTH_FIELD = new ParseField("fuzzy_prefix_length");
    private static final ParseField FUZZY_MAX_EXPANSIONS_FIELD = new ParseField("fuzzy_max_expansions");
    private static final ParseField FUZZY_REWRITE_FIELD = new ParseField("fuzzy_rewrite");
    private static final ParseField PHRASE_SLOP_FIELD = new ParseField("phrase_slop");
    private static final ParseField TIE_BREAKER_FIELD = new ParseField("tie_breaker");
    private static final ParseField ANALYZE_WILDCARD_FIELD = new ParseField("analyze_wildcard");
    private static final ParseField REWRITE_FIELD = new ParseField("rewrite");
    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    private static final ParseField QUOTE_FIELD_SUFFIX_FIELD = new ParseField("quote_field_suffix");
    private static final ParseField LENIENT_FIELD = new ParseField("lenient");
    private static final ParseField LOCALE_FIELD = new ParseField("locale");
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");


    private final String queryString;

    private String defaultField;
    /**
     * Fields to query against. If left empty will query default field,
     * currently _ALL. Uses a TreeMap to hold the fields so boolean clauses are
     * always sorted in same order for generated Lucene query for easier
     * testing.
     *
     * Can be changed back to HashMap once https://issues.apache.org/jira/browse/LUCENE-6305 is fixed.
     */
    private final Map<String, Float> fieldsAndWeights = new TreeMap<>();

    private Operator defaultOperator = DEFAULT_OPERATOR;

    private String analyzer;
    private String quoteAnalyzer;

    private String quoteFieldSuffix;

    private boolean autoGeneratePhraseQueries = DEFAULT_AUTO_GENERATE_PHRASE_QUERIES;

    private Boolean allowLeadingWildcard;

    private Boolean analyzeWildcard;

    private boolean lowercaseExpandedTerms = DEFAULT_LOWERCASE_EXPANDED_TERMS;

    private boolean enablePositionIncrements = DEFAULT_ENABLE_POSITION_INCREMENTS;

    private Locale locale = DEFAULT_LOCALE;

    private Fuzziness fuzziness = DEFAULT_FUZZINESS;

    private int fuzzyPrefixLength = DEFAULT_FUZZY_PREFIX_LENGTH;

    private int fuzzyMaxExpansions = DEFAULT_FUZZY_MAX_EXPANSIONS;

    private String rewrite;

    private String fuzzyRewrite;

    private boolean escape = DEFAULT_ESCAPE;

    private int phraseSlop = DEFAULT_PHRASE_SLOP;

    private boolean useDisMax = DEFAULT_USE_DIS_MAX;

    private float tieBreaker = DEFAULT_TIE_BREAKER;

    private String minimumShouldMatch;

    private Boolean lenient;

    private DateTimeZone timeZone;

    /** To limit effort spent determinizing regexp queries. */
    private int maxDeterminizedStates = DEFAULT_MAX_DETERMINED_STATES;

    public QueryStringQueryBuilder(String queryString) {
        if (queryString == null) {
            throw new IllegalArgumentException("query text missing");
        }
        this.queryString = queryString;
    }

    /**
     * Read from a stream.
     */
    public QueryStringQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryString = in.readString();
        defaultField = in.readOptionalString();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            fieldsAndWeights.put(in.readString(), in.readFloat());
        }
        defaultOperator = Operator.readFromStream(in);
        analyzer = in.readOptionalString();
        quoteAnalyzer = in.readOptionalString();
        quoteFieldSuffix = in.readOptionalString();
        autoGeneratePhraseQueries = in.readBoolean();
        allowLeadingWildcard = in.readOptionalBoolean();
        analyzeWildcard = in.readOptionalBoolean();
        lowercaseExpandedTerms = in.readBoolean();
        enablePositionIncrements = in.readBoolean();
        locale = Locale.forLanguageTag(in.readString());
        fuzziness = new Fuzziness(in);
        fuzzyPrefixLength = in.readVInt();
        fuzzyMaxExpansions = in.readVInt();
        fuzzyRewrite = in.readOptionalString();
        phraseSlop = in.readVInt();
        useDisMax = in.readBoolean();
        tieBreaker = in.readFloat();
        rewrite = in.readOptionalString();
        minimumShouldMatch = in.readOptionalString();
        lenient = in.readOptionalBoolean();
        timeZone = in.readOptionalTimeZone();
        escape = in.readBoolean();
        maxDeterminizedStates = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.queryString);
        out.writeOptionalString(this.defaultField);
        out.writeVInt(this.fieldsAndWeights.size());
        for (Map.Entry<String, Float> fieldsEntry : this.fieldsAndWeights.entrySet()) {
            out.writeString(fieldsEntry.getKey());
            out.writeFloat(fieldsEntry.getValue());
        }
        this.defaultOperator.writeTo(out);
        out.writeOptionalString(this.analyzer);
        out.writeOptionalString(this.quoteAnalyzer);
        out.writeOptionalString(this.quoteFieldSuffix);
        out.writeBoolean(this.autoGeneratePhraseQueries);
        out.writeOptionalBoolean(this.allowLeadingWildcard);
        out.writeOptionalBoolean(this.analyzeWildcard);
        out.writeBoolean(this.lowercaseExpandedTerms);
        out.writeBoolean(this.enablePositionIncrements);
        out.writeString(this.locale.toLanguageTag());
        this.fuzziness.writeTo(out);
        out.writeVInt(this.fuzzyPrefixLength);
        out.writeVInt(this.fuzzyMaxExpansions);
        out.writeOptionalString(this.fuzzyRewrite);
        out.writeVInt(this.phraseSlop);
        out.writeBoolean(this.useDisMax);
        out.writeFloat(this.tieBreaker);
        out.writeOptionalString(this.rewrite);
        out.writeOptionalString(this.minimumShouldMatch);
        out.writeOptionalBoolean(this.lenient);
        out.writeOptionalTimeZone(timeZone);
        out.writeBoolean(this.escape);
        out.writeVInt(this.maxDeterminizedStates);
    }

    public String queryString() {
        return this.queryString;
    }

    /**
     * The default field to run against when no prefix field is specified. Only relevant when
     * not explicitly adding fields the query string will run against.
     */
    public QueryStringQueryBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    public String defaultField() {
        return this.defaultField;
    }

    /**
     * Adds a field to run the query string against. The field will be associated with the
     * default boost of {@link AbstractQueryBuilder#DEFAULT_BOOST}.
     * Use {@link #field(String, float)} to set a specific boost for the field.
     */
    public QueryStringQueryBuilder field(String field) {
        this.fieldsAndWeights.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
        return this;
    }

    /**
     * Adds a field to run the query string against with a specific boost.
     */
    public QueryStringQueryBuilder field(String field, float boost) {
        this.fieldsAndWeights.put(field, boost);
        return this;
    }

    /**
     * Add several fields to run the query against with a specific boost.
     */
    public QueryStringQueryBuilder fields(Map<String, Float> fields) {
        this.fieldsAndWeights.putAll(fields);
        return this;
    }

    /** Returns the fields including their respective boosts to run the query against. */
    public Map<String, Float> fields() {
        return this.fieldsAndWeights;
    }

    /**
     * When more than one field is used with the query string, should queries be combined using
     * dis max, or boolean query. Defaults to dis max (<tt>true</tt>).
     */
    public QueryStringQueryBuilder useDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
        return this;
    }

    public boolean useDisMax() {
        return this.useDisMax;
    }

    /**
     * When more than one field is used with the query string, and combined queries are using
     * dis max, control the tie breaker for it.
     */
    public QueryStringQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    public float tieBreaker() {
        return this.tieBreaker;
    }

    /**
     * Sets the boolean operator of the query parser used to parse the query string.
     * <p>
     * In default mode ({@link Operator#OR}) terms without any modifiers
     * are considered optional: for example <code>capital of Hungary</code> is equal to
     * <code>capital OR of OR Hungary</code>.
     * <p>
     * In {@link Operator#AND} mode terms are considered to be in conjunction: the
     * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
     */
    public QueryStringQueryBuilder defaultOperator(Operator defaultOperator) {
        this.defaultOperator = defaultOperator == null ? DEFAULT_OPERATOR : defaultOperator;
        return this;
    }

    public Operator defaultOperator() {
        return this.defaultOperator;
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
    public QueryStringQueryBuilder quoteAnalyzer(String quoteAnalyzer) {
        this.quoteAnalyzer = quoteAnalyzer;
        return this;
    }

    /**
     * Set to true if phrase queries will be automatically generated
     * when the analyzer returns more than one term from whitespace
     * delimited text.
     * NOTE: this behavior may not be suitable for all languages.
     * <p>
     * Set to false if phrase queries should only be generated when
     * surrounded by double quotes.
     */
    public QueryStringQueryBuilder autoGeneratePhraseQueries(boolean autoGeneratePhraseQueries) {
        this.autoGeneratePhraseQueries = autoGeneratePhraseQueries;
        return this;
    }

    public boolean autoGeneratePhraseQueries() {
        return this.autoGeneratePhraseQueries;
    }

    /**
     * Protects against too-difficult regular expression queries.
     */
    public QueryStringQueryBuilder maxDeterminizedStates(int maxDeterminizedStates) {
        this.maxDeterminizedStates = maxDeterminizedStates;
        return this;
    }

    public int maxDeterminizedStates() {
        return this.maxDeterminizedStates;
    }

    /**
     * Should leading wildcards be allowed or not. Defaults to <tt>true</tt>.
     */
    public QueryStringQueryBuilder allowLeadingWildcard(Boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
        return this;
    }

    public Boolean allowLeadingWildcard() {
        return this.allowLeadingWildcard;
    }

    /**
     * Whether terms of wildcard, prefix, fuzzy and range queries are to be automatically
     * lower-cased or not.  Default is <tt>true</tt>.
     */
    public QueryStringQueryBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    public boolean lowercaseExpandedTerms() {
        return this.lowercaseExpandedTerms;
    }

    /**
     * Set to <tt>true</tt> to enable position increments in result query. Defaults to
     * <tt>true</tt>.
     * <p>
     * When set, result phrase and multi-phrase queries will be aware of position increments.
     * Useful when e.g. a StopFilter increases the position increment of the token that follows an omitted token.
     */
    public QueryStringQueryBuilder enablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
        return this;
    }

    public boolean enablePositionIncrements() {
        return this.enablePositionIncrements;
    }

    /**
     * Set the edit distance for fuzzy queries. Default is "AUTO".
     */
    public QueryStringQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness == null ? DEFAULT_FUZZINESS : fuzziness;
        return this;
    }

    public Fuzziness fuzziness() {
        return this.fuzziness;
    }

    /**
     * Set the minimum prefix length for fuzzy queries. Default is 1.
     */
    public QueryStringQueryBuilder fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        return this;
    }

    public int fuzzyPrefixLength() {
        return fuzzyPrefixLength;
    }

    public QueryStringQueryBuilder fuzzyMaxExpansions(int fuzzyMaxExpansions) {
        this.fuzzyMaxExpansions = fuzzyMaxExpansions;
        return this;
    }

    public int fuzzyMaxExpansions() {
        return fuzzyMaxExpansions;
    }

    public QueryStringQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    public String fuzzyRewrite() {
        return fuzzyRewrite;
    }

    /**
     * Sets the default slop for phrases.  If zero, then exact phrase matches
     * are required. Default value is zero.
     */
    public QueryStringQueryBuilder phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
        return this;
    }

    public int phraseSlop() {
        return phraseSlop;
    }

    /**
     * Set to <tt>true</tt> to enable analysis on wildcard and prefix queries.
     */
    public QueryStringQueryBuilder analyzeWildcard(Boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        return this;
    }

    public Boolean analyzeWildcard() {
        return this.analyzeWildcard;
    }

    public QueryStringQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public String rewrite() {
        return this.rewrite;
    }

    public QueryStringQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    /**
     * An optional field name suffix to automatically try and add to the field searched when using quoted text.
     */
    public QueryStringQueryBuilder quoteFieldSuffix(String quoteFieldSuffix) {
        this.quoteFieldSuffix = quoteFieldSuffix;
        return this;
    }

    public String quoteFieldSuffix() {
        return this.quoteFieldSuffix;
    }

    /**
     * Sets the query string parser to be lenient when parsing field values, defaults to the index
     * setting and if not set, defaults to false.
     */
    public QueryStringQueryBuilder lenient(Boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    public Boolean lenient() {
        return this.lenient;
    }

    public QueryStringQueryBuilder locale(Locale locale) {
        this.locale = locale == null ? DEFAULT_LOCALE : locale;
        return this;
    }

    public Locale locale() {
        return this.locale;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public QueryStringQueryBuilder timeZone(String timeZone) {
        if (timeZone != null) {
            this.timeZone = DateTimeZone.forID(timeZone);
        } else {
            this.timeZone = null;
        }
        return this;
    }

    public QueryStringQueryBuilder timeZone(DateTimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public DateTimeZone timeZone() {
        return this.timeZone;
    }

    /**
     * Set to <tt>true</tt> to enable escaping of the query string
     */
    public QueryStringQueryBuilder escape(boolean escape) {
        this.escape = escape;
        return this;
    }

    public boolean escape() {
        return this.escape;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), this.queryString);
        if (this.defaultField != null) {
            builder.field(DEFAULT_FIELD_FIELD.getPreferredName(), this.defaultField);
        }
        builder.startArray(FIELDS_FIELD.getPreferredName());
        for (Map.Entry<String, Float> fieldEntry : this.fieldsAndWeights.entrySet()) {
            builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
        }
        builder.endArray();
        builder.field(USE_DIS_MAX_FIELD.getPreferredName(), this.useDisMax);
        builder.field(TIE_BREAKER_FIELD.getPreferredName(), this.tieBreaker);
        builder.field(DEFAULT_OPERATOR_FIELD.getPreferredName(),
                this.defaultOperator.name().toLowerCase(Locale.ROOT));
        if (this.analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), this.analyzer);
        }
        if (this.quoteAnalyzer != null) {
            builder.field(QUOTE_ANALYZER_FIELD.getPreferredName(), this.quoteAnalyzer);
        }
        builder.field(AUTO_GENERATED_PHRASE_QUERIES_FIELD.getPreferredName(), this.autoGeneratePhraseQueries);
        builder.field(MAX_DETERMINED_STATES_FIELD.getPreferredName(), this.maxDeterminizedStates);
        if (this.allowLeadingWildcard != null) {
            builder.field(ALLOW_LEADING_WILDCARD_FIELD.getPreferredName(), this.allowLeadingWildcard);
        }
        builder.field(LOWERCASE_EXPANDED_TERMS_FIELD.getPreferredName(), this.lowercaseExpandedTerms);
        builder.field(ENABLE_POSITION_INCREMENTS_FIELD.getPreferredName(), this.enablePositionIncrements);
        this.fuzziness.toXContent(builder, params);
        builder.field(FUZZY_PREFIX_LENGTH_FIELD.getPreferredName(), this.fuzzyPrefixLength);
        builder.field(FUZZY_MAX_EXPANSIONS_FIELD.getPreferredName(), this.fuzzyMaxExpansions);
        if (this.fuzzyRewrite != null) {
            builder.field(FUZZY_REWRITE_FIELD.getPreferredName(), this.fuzzyRewrite);
        }
        builder.field(PHRASE_SLOP_FIELD.getPreferredName(), this.phraseSlop);
        if (this.analyzeWildcard != null) {
            builder.field(ANALYZE_WILDCARD_FIELD.getPreferredName(), this.analyzeWildcard);
        }
        if (this.rewrite != null) {
            builder.field(REWRITE_FIELD.getPreferredName(), this.rewrite);
        }
        if (this.minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), this.minimumShouldMatch);
        }
        if (this.quoteFieldSuffix != null) {
            builder.field(QUOTE_FIELD_SUFFIX_FIELD.getPreferredName(), this.quoteFieldSuffix);
        }
        if (this.lenient != null) {
            builder.field(LENIENT_FIELD.getPreferredName(), this.lenient);
        }
        builder.field(LOCALE_FIELD.getPreferredName(), this.locale.toLanguageTag());
        if (this.timeZone != null) {
            builder.field(TIME_ZONE_FIELD.getPreferredName(), this.timeZone.getID());
        }
        builder.field(ESCAPE_FIELD.getPreferredName(), this.escape);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<QueryStringQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        String currentFieldName = null;
        XContentParser.Token token;
        String queryString = null;
        String defaultField = null;
        String analyzer = null;
        String quoteAnalyzer = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean autoGeneratePhraseQueries = QueryStringQueryBuilder.DEFAULT_AUTO_GENERATE_PHRASE_QUERIES;
        int maxDeterminizedStates = QueryStringQueryBuilder.DEFAULT_MAX_DETERMINED_STATES;
        boolean lowercaseExpandedTerms = QueryStringQueryBuilder.DEFAULT_LOWERCASE_EXPANDED_TERMS;
        boolean enablePositionIncrements = QueryStringQueryBuilder.DEFAULT_ENABLE_POSITION_INCREMENTS;
        boolean escape = QueryStringQueryBuilder.DEFAULT_ESCAPE;
        boolean useDisMax = QueryStringQueryBuilder.DEFAULT_USE_DIS_MAX;
        int fuzzyPrefixLength = QueryStringQueryBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        int fuzzyMaxExpansions = QueryStringQueryBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        int phraseSlop = QueryStringQueryBuilder.DEFAULT_PHRASE_SLOP;
        float tieBreaker = QueryStringQueryBuilder.DEFAULT_TIE_BREAKER;
        Boolean analyzeWildcard = null;
        Boolean allowLeadingWildcard = null;
        String minimumShouldMatch = null;
        String quoteFieldSuffix = null;
        Boolean lenient = null;
        Operator defaultOperator = QueryStringQueryBuilder.DEFAULT_OPERATOR;
        String timeZone = null;
        Locale locale = QueryStringQueryBuilder.DEFAULT_LOCALE;
        Fuzziness fuzziness = QueryStringQueryBuilder.DEFAULT_FUZZINESS;
        String fuzzyRewrite = null;
        String rewrite = null;
        Map<String, Float> fieldsAndWeights = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String fField = null;
                        float fBoost = AbstractQueryBuilder.DEFAULT_BOOST;
                        char[] text = parser.textCharacters();
                        int end = parser.textOffset() + parser.textLength();
                        for (int i = parser.textOffset(); i < end; i++) {
                            if (text[i] == '^') {
                                int relativeLocation = i - parser.textOffset();
                                fField = new String(text, parser.textOffset(), relativeLocation);
                                fBoost = Float.parseFloat(new String(text, i + 1, parser.textLength() - relativeLocation - 1));
                                break;
                            }
                        }
                        if (fField == null) {
                            fField = parser.text();
                        }
                        fieldsAndWeights.put(fField, fBoost);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    queryString = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, DEFAULT_FIELD_FIELD)) {
                    defaultField = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, DEFAULT_OPERATOR_FIELD)) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, ANALYZER_FIELD)) {
                    analyzer = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, QUOTE_ANALYZER_FIELD)) {
                    quoteAnalyzer = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, ALLOW_LEADING_WILDCARD_FIELD)) {
                    allowLeadingWildcard = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AUTO_GENERATED_PHRASE_QUERIES_FIELD)) {
                    autoGeneratePhraseQueries = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, MAX_DETERMINED_STATES_FIELD)) {
                    maxDeterminizedStates = parser.intValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LOWERCASE_EXPANDED_TERMS_FIELD)) {
                    lowercaseExpandedTerms = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, ENABLE_POSITION_INCREMENTS_FIELD)) {
                    enablePositionIncrements = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, ESCAPE_FIELD)) {
                    escape = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, USE_DIS_MAX_FIELD)) {
                    useDisMax = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FUZZY_PREFIX_LENGTH_FIELD)) {
                    fuzzyPrefixLength = parser.intValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FUZZY_MAX_EXPANSIONS_FIELD)) {
                    fuzzyMaxExpansions = parser.intValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, FUZZY_REWRITE_FIELD)) {
                    fuzzyRewrite = parser.textOrNull();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, PHRASE_SLOP_FIELD)) {
                    phraseSlop = parser.intValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                    fuzziness = Fuzziness.parse(parser);
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, TIE_BREAKER_FIELD)) {
                    tieBreaker = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, ANALYZE_WILDCARD_FIELD)) {
                    analyzeWildcard = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, REWRITE_FIELD)) {
                    rewrite = parser.textOrNull();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, QUOTE_FIELD_SUFFIX_FIELD)) {
                    quoteFieldSuffix = parser.textOrNull();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LENIENT_FIELD)) {
                    lenient = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, LOCALE_FIELD)) {
                    String localeStr = parser.text();
                    locale = Locale.forLanguageTag(localeStr);
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, TIME_ZONE_FIELD)) {
                    try {
                        timeZone = parser.text();
                    } catch (IllegalArgumentException e) {
                        throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME +
                                "] time_zone [" + parser.text() + "] is unknown");
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME +
                        "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }
        if (queryString == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] must be provided with a [query]");
        }

        QueryStringQueryBuilder queryStringQuery = new QueryStringQueryBuilder(queryString);
        queryStringQuery.fields(fieldsAndWeights);
        queryStringQuery.defaultField(defaultField);
        queryStringQuery.defaultOperator(defaultOperator);
        queryStringQuery.analyzer(analyzer);
        queryStringQuery.quoteAnalyzer(quoteAnalyzer);
        queryStringQuery.allowLeadingWildcard(allowLeadingWildcard);
        queryStringQuery.autoGeneratePhraseQueries(autoGeneratePhraseQueries);
        queryStringQuery.maxDeterminizedStates(maxDeterminizedStates);
        queryStringQuery.lowercaseExpandedTerms(lowercaseExpandedTerms);
        queryStringQuery.enablePositionIncrements(enablePositionIncrements);
        queryStringQuery.escape(escape);
        queryStringQuery.useDisMax(useDisMax);
        queryStringQuery.fuzzyPrefixLength(fuzzyPrefixLength);
        queryStringQuery.fuzzyMaxExpansions(fuzzyMaxExpansions);
        queryStringQuery.fuzzyRewrite(fuzzyRewrite);
        queryStringQuery.phraseSlop(phraseSlop);
        queryStringQuery.fuzziness(fuzziness);
        queryStringQuery.tieBreaker(tieBreaker);
        queryStringQuery.analyzeWildcard(analyzeWildcard);
        queryStringQuery.rewrite(rewrite);
        queryStringQuery.minimumShouldMatch(minimumShouldMatch);
        queryStringQuery.quoteFieldSuffix(quoteFieldSuffix);
        queryStringQuery.lenient(lenient);
        queryStringQuery.timeZone(timeZone);
        queryStringQuery.locale(locale);
        queryStringQuery.boost(boost);
        queryStringQuery.queryName(queryName);
        return Optional.of(queryStringQuery);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(QueryStringQueryBuilder other) {
        return Objects.equals(queryString, other.queryString) &&
                Objects.equals(defaultField, other.defaultField) &&
                Objects.equals(fieldsAndWeights, other.fieldsAndWeights) &&
                Objects.equals(defaultOperator, other.defaultOperator) &&
                Objects.equals(analyzer, other.analyzer) &&
                Objects.equals(quoteAnalyzer, other.quoteAnalyzer) &&
                Objects.equals(quoteFieldSuffix, other.quoteFieldSuffix) &&
                Objects.equals(autoGeneratePhraseQueries, other.autoGeneratePhraseQueries) &&
                Objects.equals(allowLeadingWildcard, other.allowLeadingWildcard) &&
                Objects.equals(lowercaseExpandedTerms, other.lowercaseExpandedTerms) &&
                Objects.equals(enablePositionIncrements, other.enablePositionIncrements) &&
                Objects.equals(analyzeWildcard, other.analyzeWildcard) &&
                Objects.equals(locale.toLanguageTag(), other.locale.toLanguageTag()) &&
                Objects.equals(fuzziness, other.fuzziness) &&
                Objects.equals(fuzzyPrefixLength, other.fuzzyPrefixLength) &&
                Objects.equals(fuzzyMaxExpansions, other.fuzzyMaxExpansions) &&
                Objects.equals(fuzzyRewrite, other.fuzzyRewrite) &&
                Objects.equals(phraseSlop, other.phraseSlop) &&
                Objects.equals(useDisMax, other.useDisMax) &&
                Objects.equals(tieBreaker, other.tieBreaker) &&
                Objects.equals(rewrite, other.rewrite) &&
                Objects.equals(minimumShouldMatch, other.minimumShouldMatch) &&
                Objects.equals(lenient, other.lenient) &&
                timeZone == null ? other.timeZone == null : other.timeZone != null &&
                Objects.equals(timeZone.getID(), other.timeZone.getID()) &&
                Objects.equals(escape, other.escape) &&
                Objects.equals(maxDeterminizedStates, other.maxDeterminizedStates);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queryString, defaultField, fieldsAndWeights, defaultOperator, analyzer, quoteAnalyzer,
                quoteFieldSuffix, autoGeneratePhraseQueries, allowLeadingWildcard, lowercaseExpandedTerms,
                enablePositionIncrements, analyzeWildcard, locale.toLanguageTag(), fuzziness, fuzzyPrefixLength,
                fuzzyMaxExpansions, fuzzyRewrite, phraseSlop, useDisMax, tieBreaker, rewrite, minimumShouldMatch, lenient,
                timeZone == null ? 0 : timeZone.getID(), escape, maxDeterminizedStates);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        //TODO would be nice to have all the settings in one place: some change though at query execution time
        //e.g. field names get expanded to concrete names, defaults get resolved sometimes to settings values etc.
        QueryParserSettings qpSettings;
        if (this.escape) {
            qpSettings = new QueryParserSettings(org.apache.lucene.queryparser.classic.QueryParser.escape(this.queryString));
        } else {
            qpSettings = new QueryParserSettings(this.queryString);
        }
        qpSettings.defaultField(this.defaultField == null ? context.defaultField() : this.defaultField);
        Map<String, Float> resolvedFields = new TreeMap<>();
        for (Map.Entry<String, Float> fieldsEntry : fieldsAndWeights.entrySet()) {
            String fieldName = fieldsEntry.getKey();
            Float weight = fieldsEntry.getValue();
            if (Regex.isSimpleMatchPattern(fieldName)) {
                for (String resolvedFieldName : context.getMapperService().simpleMatchToIndexNames(fieldName)) {
                    resolvedFields.put(resolvedFieldName, weight);
                }
            } else {
                resolvedFields.put(fieldName, weight);
            }
        }
        qpSettings.fieldsAndWeights(resolvedFields);
        qpSettings.defaultOperator(defaultOperator.toQueryParserOperator());

        if (analyzer == null) {
            qpSettings.defaultAnalyzer(context.getMapperService().searchAnalyzer());
        } else {
            NamedAnalyzer namedAnalyzer = context.getAnalysisService().analyzer(analyzer);
            if (namedAnalyzer == null) {
                throw new QueryShardException(context, "[query_string] analyzer [" + analyzer + "] not found");
            }
            qpSettings.forceAnalyzer(namedAnalyzer);
        }
        if (quoteAnalyzer != null) {
            NamedAnalyzer namedAnalyzer = context.getAnalysisService().analyzer(quoteAnalyzer);
            if (namedAnalyzer == null) {
                throw new QueryShardException(context, "[query_string] quote_analyzer [" + quoteAnalyzer + "] not found");
            }
            qpSettings.forceQuoteAnalyzer(namedAnalyzer);
        } else if (analyzer != null) {
            qpSettings.forceQuoteAnalyzer(qpSettings.analyzer());
        } else {
            qpSettings.defaultQuoteAnalyzer(context.getMapperService().searchQuoteAnalyzer());
        }

        qpSettings.quoteFieldSuffix(quoteFieldSuffix);
        qpSettings.autoGeneratePhraseQueries(autoGeneratePhraseQueries);
        qpSettings.allowLeadingWildcard(allowLeadingWildcard == null ? context.queryStringAllowLeadingWildcard() : allowLeadingWildcard);
        qpSettings.analyzeWildcard(analyzeWildcard == null ? context.queryStringAnalyzeWildcard() : analyzeWildcard);
        qpSettings.lowercaseExpandedTerms(lowercaseExpandedTerms);
        qpSettings.enablePositionIncrements(enablePositionIncrements);
        qpSettings.locale(locale);
        qpSettings.fuzziness(fuzziness);
        qpSettings.fuzzyPrefixLength(fuzzyPrefixLength);
        qpSettings.fuzzyMaxExpansions(fuzzyMaxExpansions);
        qpSettings.fuzzyRewriteMethod(QueryParsers.parseRewriteMethod(context.getParseFieldMatcher(), this.fuzzyRewrite));
        qpSettings.phraseSlop(phraseSlop);
        qpSettings.useDisMax(useDisMax);
        qpSettings.tieBreaker(tieBreaker);
        qpSettings.rewriteMethod(QueryParsers.parseRewriteMethod(context.getParseFieldMatcher(), this.rewrite));
        qpSettings.lenient(lenient == null ? context.queryStringLenient() : lenient);
        qpSettings.timeZone(timeZone);
        qpSettings.maxDeterminizedStates(maxDeterminizedStates);

        MapperQueryParser queryParser = context.queryParser(qpSettings);
        Query query;
        try {
            query = queryParser.parse(queryString);
        } catch (org.apache.lucene.queryparser.classic.ParseException e) {
            throw new QueryShardException(context, "Failed to parse query [" + this.queryString + "]", e);
        }

        if (query == null) {
            return null;
        }

        //save the BoostQuery wrapped structure if present
        List<Float> boosts = new ArrayList<>();
        while(query instanceof BoostQuery) {
            BoostQuery boostQuery = (BoostQuery) query;
            boosts.add(boostQuery.getBoost());
            query = boostQuery.getQuery();
        }

        query = Queries.fixNegativeQueryIfNeeded(query);
        // If the coordination factor is disabled on a boolean query we don't apply the minimum should match.
        // This is done to make sure that the minimum_should_match doesn't get applied when there is only one word
        // and multiple variations of the same word in the query (synonyms for instance).
        if (query instanceof BooleanQuery && !((BooleanQuery) query).isCoordDisabled()) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, this.minimumShouldMatch());
        }

        //restore the previous BoostQuery wrapping
        for (int i = boosts.size() - 1; i >= 0; i--) {
            query = new BoostQuery(query, boosts.get(i));
        }

        return query;
    }
}

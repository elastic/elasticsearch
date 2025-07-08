/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.index.search.QueryStringQueryParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A query that parses a query string and runs it. There are two modes that this operates. The first,
 * when no field is added (using {@link #field(String)}, will run the query once and non prefixed fields
 * will use the {@link #defaultField(String)} set. The second, when one or more fields are added
 * (using {@link #field(String)}), will run the parsed query against the provided fields, and combine
 * them using Dismax.
 */
public final class QueryStringQueryBuilder extends AbstractQueryBuilder<QueryStringQueryBuilder> {

    public static final String NAME = "query_string";

    public static final int DEFAULT_MAX_DETERMINED_STATES = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;
    public static final boolean DEFAULT_ENABLE_POSITION_INCREMENTS = true;
    public static final boolean DEFAULT_ESCAPE = false;
    public static final int DEFAULT_FUZZY_PREFIX_LENGTH = FuzzyQuery.defaultPrefixLength;
    public static final int DEFAULT_FUZZY_MAX_EXPANSIONS = FuzzyQuery.defaultMaxExpansions;
    public static final int DEFAULT_PHRASE_SLOP = 0;
    public static final Fuzziness DEFAULT_FUZZINESS = Fuzziness.AUTO;
    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    public static final MultiMatchQueryBuilder.Type DEFAULT_TYPE = MultiMatchQueryBuilder.Type.BEST_FIELDS;
    public static final boolean DEFAULT_FUZZY_TRANSPOSITIONS = FuzzyQuery.defaultTranspositions;

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField BOOST_FIELD = new ParseField("boost");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField DEFAULT_FIELD_FIELD = new ParseField("default_field");
    public static final ParseField DEFAULT_OPERATOR_FIELD = new ParseField("default_operator");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField QUOTE_ANALYZER_FIELD = new ParseField("quote_analyzer");
    public static final ParseField ALLOW_LEADING_WILDCARD_FIELD = new ParseField("allow_leading_wildcard");
    public static final ParseField MAX_DETERMINIZED_STATES_FIELD = new ParseField("max_determinized_states");
    public static final ParseField ENABLE_POSITION_INCREMENTS_FIELD = new ParseField("enable_position_increments");
    public static final ParseField ESCAPE_FIELD = new ParseField("escape");
    public static final ParseField FUZZINESS_FIELD = new ParseField("fuzziness");
    public static final ParseField FUZZY_PREFIX_LENGTH_FIELD = new ParseField("fuzzy_prefix_length");
    public static final ParseField FUZZY_MAX_EXPANSIONS_FIELD = new ParseField("fuzzy_max_expansions");
    public static final ParseField FUZZY_REWRITE_FIELD = new ParseField("fuzzy_rewrite");
    public static final ParseField PHRASE_SLOP_FIELD = new ParseField("phrase_slop");
    public static final ParseField TIE_BREAKER_FIELD = new ParseField("tie_breaker");
    public static final ParseField ANALYZE_WILDCARD_FIELD = new ParseField("analyze_wildcard");
    public static final ParseField REWRITE_FIELD = new ParseField("rewrite");
    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField QUOTE_FIELD_SUFFIX_FIELD = new ParseField("quote_field_suffix");
    public static final ParseField LENIENT_FIELD = new ParseField("lenient");
    public static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField GENERATE_SYNONYMS_PHRASE_QUERY = new ParseField("auto_generate_synonyms_phrase_query");
    public static final ParseField FUZZY_TRANSPOSITIONS_FIELD = new ParseField("fuzzy_transpositions");

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

    private Boolean allowLeadingWildcard;

    private Boolean analyzeWildcard;

    private boolean enablePositionIncrements = DEFAULT_ENABLE_POSITION_INCREMENTS;

    private Fuzziness fuzziness = DEFAULT_FUZZINESS;

    private int fuzzyPrefixLength = DEFAULT_FUZZY_PREFIX_LENGTH;

    private int fuzzyMaxExpansions = DEFAULT_FUZZY_MAX_EXPANSIONS;

    private String rewrite;

    private String fuzzyRewrite;

    private boolean escape = DEFAULT_ESCAPE;

    private int phraseSlop = DEFAULT_PHRASE_SLOP;

    private MultiMatchQueryBuilder.Type type = DEFAULT_TYPE;

    private Float tieBreaker;

    private String minimumShouldMatch;

    private Boolean lenient;

    private ZoneId timeZone;

    /** To limit effort spent determinizing regexp queries. */
    private int maxDeterminizedStates = DEFAULT_MAX_DETERMINED_STATES;

    private boolean autoGenerateSynonymsPhraseQuery = true;

    private boolean fuzzyTranspositions = DEFAULT_FUZZY_TRANSPOSITIONS;

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
            String field = in.readString();
            Float weight = in.readFloat();
            checkNegativeBoost(weight);
            fieldsAndWeights.put(field, weight);
        }
        defaultOperator = Operator.readFromStream(in);
        analyzer = in.readOptionalString();
        quoteAnalyzer = in.readOptionalString();
        quoteFieldSuffix = in.readOptionalString();
        allowLeadingWildcard = in.readOptionalBoolean();
        analyzeWildcard = in.readOptionalBoolean();
        enablePositionIncrements = in.readBoolean();
        fuzziness = new Fuzziness(in);
        fuzzyPrefixLength = in.readVInt();
        fuzzyMaxExpansions = in.readVInt();
        fuzzyRewrite = in.readOptionalString();
        phraseSlop = in.readVInt();
        type = MultiMatchQueryBuilder.Type.readFromStream(in);
        tieBreaker = in.readOptionalFloat();

        rewrite = in.readOptionalString();
        minimumShouldMatch = in.readOptionalString();
        lenient = in.readOptionalBoolean();
        timeZone = in.readOptionalZoneId();
        escape = in.readBoolean();
        maxDeterminizedStates = in.readVInt();
        autoGenerateSynonymsPhraseQuery = in.readBoolean();
        fuzzyTranspositions = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.queryString);
        out.writeOptionalString(this.defaultField);
        out.writeMap(this.fieldsAndWeights, StreamOutput::writeFloat);
        this.defaultOperator.writeTo(out);
        out.writeOptionalString(this.analyzer);
        out.writeOptionalString(this.quoteAnalyzer);
        out.writeOptionalString(this.quoteFieldSuffix);
        out.writeOptionalBoolean(this.allowLeadingWildcard);
        out.writeOptionalBoolean(this.analyzeWildcard);
        out.writeBoolean(this.enablePositionIncrements);
        this.fuzziness.writeTo(out);
        out.writeVInt(this.fuzzyPrefixLength);
        out.writeVInt(this.fuzzyMaxExpansions);
        out.writeOptionalString(this.fuzzyRewrite);
        out.writeVInt(this.phraseSlop);
        type.writeTo(out);
        out.writeOptionalFloat(tieBreaker);
        out.writeOptionalString(this.rewrite);
        out.writeOptionalString(this.minimumShouldMatch);
        out.writeOptionalBoolean(this.lenient);
        out.writeOptionalZoneId(timeZone);
        out.writeBoolean(this.escape);
        out.writeVInt(this.maxDeterminizedStates);
        out.writeBoolean(autoGenerateSynonymsPhraseQuery);
        out.writeBoolean(fuzzyTranspositions);
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
        checkNegativeBoost(boost);
        this.fieldsAndWeights.put(field, boost);
        return this;
    }

    /**
     * Add several fields to run the query against with a specific boost.
     */
    public QueryStringQueryBuilder fields(Map<String, Float> fields) {
        for (float fieldBoost : fields.values()) {
            checkNegativeBoost(fieldBoost);
        }
        this.fieldsAndWeights.putAll(fields);
        return this;
    }

    /** Returns the fields including their respective boosts to run the query against. */
    public Map<String, Float> fields() {
        return this.fieldsAndWeights;
    }

    /**
     * @param type Sets how multiple fields should be combined to build textual part queries.
     */
    public QueryStringQueryBuilder type(MultiMatchQueryBuilder.Type type) {
        this.type = type;
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

    public Float tieBreaker() {
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
     * The optional analyzer used to analyze the query string. Note, if a field has search analyzer
     * defined for it, then it will be used automatically. Defaults to the smart search analyzer.
     */
    public String analyzer() {
        return analyzer;
    }

    /**
     * The optional analyzer used to analyze the query string for phrase searches. Note, if a field has search (quote) analyzer
     * defined for it, then it will be used automatically. Defaults to the smart search analyzer.
     */
    public String quoteAnalyzer() {
        return quoteAnalyzer;
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
     * Should leading wildcards be allowed or not. Defaults to {@code true}.
     */
    public QueryStringQueryBuilder allowLeadingWildcard(Boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
        return this;
    }

    public Boolean allowLeadingWildcard() {
        return this.allowLeadingWildcard;
    }

    /**
     * Set to {@code true} to enable position increments in result query. Defaults to
     * {@code true}.
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

    public QueryStringQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Set to {@code true} to enable analysis on wildcard and prefix queries.
     */
    public QueryStringQueryBuilder analyzeWildcard(Boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        return this;
    }

    public Boolean analyzeWildcard() {
        return this.analyzeWildcard;
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

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public QueryStringQueryBuilder timeZone(String timeZone) {
        if (timeZone != null) {
            this.timeZone = ZoneId.of(timeZone);
        } else {
            this.timeZone = null;
        }
        return this;
    }

    public QueryStringQueryBuilder timeZone(ZoneId timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public ZoneId timeZone() {
        return this.timeZone;
    }

    /**
     * Set to {@code true} to enable escaping of the query string
     */
    public QueryStringQueryBuilder escape(boolean escape) {
        this.escape = escape;
        return this;
    }

    public boolean escape() {
        return this.escape;
    }

    public QueryStringQueryBuilder autoGenerateSynonymsPhraseQuery(boolean value) {
        this.autoGenerateSynonymsPhraseQuery = value;
        return this;
    }

    /**
     * Whether phrase queries should be automatically generated for multi terms synonyms.
     * Defaults to {@code true}.
     */
    public boolean autoGenerateSynonymsPhraseQuery() {
        return autoGenerateSynonymsPhraseQuery;
    }

    public boolean fuzzyTranspositions() {
        return fuzzyTranspositions;
    }

    /**
     * Sets whether transpositions are supported in fuzzy queries.<p>
     * The default metric used by fuzzy queries to determine a match is the Damerau-Levenshtein
     * distance formula which supports transpositions. Setting transposition to false will
     * switch to classic Levenshtein distance.<br>
     * If not set, Damerau-Levenshtein distance metric will be used.
     */
    public QueryStringQueryBuilder fuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
        return this;
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
        if (this.type != DEFAULT_TYPE) {
            builder.field(TYPE_FIELD.getPreferredName(), type.toString().toLowerCase(Locale.ENGLISH));
        }
        if (tieBreaker != null) {
            builder.field(TIE_BREAKER_FIELD.getPreferredName(), this.tieBreaker);
        }
        if (defaultOperator != DEFAULT_OPERATOR) {
            builder.field(DEFAULT_OPERATOR_FIELD.getPreferredName(), this.defaultOperator.name().toLowerCase(Locale.ROOT));
        }
        if (this.analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), this.analyzer);
        }
        if (this.quoteAnalyzer != null) {
            builder.field(QUOTE_ANALYZER_FIELD.getPreferredName(), this.quoteAnalyzer);
        }
        if (this.maxDeterminizedStates != DEFAULT_MAX_DETERMINED_STATES) {
            builder.field(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), this.maxDeterminizedStates);
        }
        if (this.allowLeadingWildcard != null) {
            builder.field(ALLOW_LEADING_WILDCARD_FIELD.getPreferredName(), this.allowLeadingWildcard);
        }
        if (this.enablePositionIncrements != DEFAULT_ENABLE_POSITION_INCREMENTS) {
            builder.field(ENABLE_POSITION_INCREMENTS_FIELD.getPreferredName(), this.enablePositionIncrements);
        }
        if (this.fuzziness != DEFAULT_FUZZINESS) {
            this.fuzziness.toXContent(builder, params);
        }
        if (this.fuzzyPrefixLength != DEFAULT_FUZZY_PREFIX_LENGTH) {
            builder.field(FUZZY_PREFIX_LENGTH_FIELD.getPreferredName(), this.fuzzyPrefixLength);
        }
        if (this.fuzzyMaxExpansions != DEFAULT_FUZZY_MAX_EXPANSIONS) {
            builder.field(FUZZY_MAX_EXPANSIONS_FIELD.getPreferredName(), this.fuzzyMaxExpansions);
        }
        if (this.fuzzyRewrite != null) {
            builder.field(FUZZY_REWRITE_FIELD.getPreferredName(), this.fuzzyRewrite);
        }
        if (this.phraseSlop != DEFAULT_PHRASE_SLOP) {
            builder.field(PHRASE_SLOP_FIELD.getPreferredName(), this.phraseSlop);
        }
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
        if (this.timeZone != null) {
            builder.field(TIME_ZONE_FIELD.getPreferredName(), this.timeZone.getId());
        }
        if (this.escape != DEFAULT_ESCAPE) {
            builder.field(ESCAPE_FIELD.getPreferredName(), this.escape);
        }
        if (this.autoGenerateSynonymsPhraseQuery != true) {
            builder.field(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), autoGenerateSynonymsPhraseQuery);
        }
        if (this.fuzzyTranspositions != DEFAULT_FUZZY_TRANSPOSITIONS) {
            builder.field(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), fuzzyTranspositions);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    public static QueryStringQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        String queryString = null;
        String defaultField = null;
        String analyzer = null;
        String quoteAnalyzer = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        int maxDeterminizedStates = QueryStringQueryBuilder.DEFAULT_MAX_DETERMINED_STATES;
        boolean enablePositionIncrements = QueryStringQueryBuilder.DEFAULT_ENABLE_POSITION_INCREMENTS;
        boolean escape = QueryStringQueryBuilder.DEFAULT_ESCAPE;
        int fuzzyPrefixLength = QueryStringQueryBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        int fuzzyMaxExpansions = QueryStringQueryBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        int phraseSlop = QueryStringQueryBuilder.DEFAULT_PHRASE_SLOP;
        MultiMatchQueryBuilder.Type type = DEFAULT_TYPE;
        Float tieBreaker = null;
        Boolean analyzeWildcard = null;
        Boolean allowLeadingWildcard = null;
        String minimumShouldMatch = null;
        String quoteFieldSuffix = null;
        Boolean lenient = null;
        Operator defaultOperator = QueryStringQueryBuilder.DEFAULT_OPERATOR;
        String timeZone = null;
        Fuzziness fuzziness = QueryStringQueryBuilder.DEFAULT_FUZZINESS;
        String fuzzyRewrite = null;
        String rewrite = null;
        Map<String, Float> fieldsAndWeights = null;
        boolean autoGenerateSynonymsPhraseQuery = true;
        boolean fuzzyTranspositions = DEFAULT_FUZZY_TRANSPOSITIONS;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<String> fields = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
                    }
                    fieldsAndWeights = QueryParserHelper.parseFieldsAndWeights(fields);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + QueryStringQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryString = parser.text();
                } else if (DEFAULT_FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    defaultField = parser.text();
                } else if (DEFAULT_OPERATOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if (ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzer = parser.text();
                } else if (QUOTE_ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    quoteAnalyzer = parser.text();
                } else if (ALLOW_LEADING_WILDCARD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    allowLeadingWildcard = parser.booleanValue();
                } else if (MAX_DETERMINIZED_STATES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxDeterminizedStates = parser.intValue();
                } else if (ENABLE_POSITION_INCREMENTS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    enablePositionIncrements = parser.booleanValue();
                } else if (ESCAPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    escape = parser.booleanValue();
                } else if (FUZZY_PREFIX_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyPrefixLength = parser.intValue();
                } else if (FUZZY_MAX_EXPANSIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyMaxExpansions = parser.intValue();
                } else if (FUZZY_REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyRewrite = parser.textOrNull();
                } else if (PHRASE_SLOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    phraseSlop = parser.intValue();
                } else if (Fuzziness.FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzziness = Fuzziness.parse(parser);
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    type = MultiMatchQueryBuilder.Type.parse(parser.text(), parser.getDeprecationHandler());
                } else if (TIE_BREAKER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tieBreaker = parser.floatValue();
                } else if (ANALYZE_WILDCARD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzeWildcard = parser.booleanValue();
                } else if (REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    rewrite = parser.textOrNull();
                } else if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (QUOTE_FIELD_SUFFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    quoteFieldSuffix = parser.textOrNull();
                } else if (LENIENT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    lenient = parser.booleanValue();
                } else if (MAX_DETERMINIZED_STATES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxDeterminizedStates = parser.intValue();
                } else if (TIME_ZONE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        timeZone = parser.text();
                    } catch (IllegalArgumentException e) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + QueryStringQueryBuilder.NAME + "] time_zone [" + parser.text() + "] is unknown"
                        );
                    }
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (GENERATE_SYNONYMS_PHRASE_QUERY.match(currentFieldName, parser.getDeprecationHandler())) {
                    autoGenerateSynonymsPhraseQuery = parser.booleanValue();
                } else if (FUZZY_TRANSPOSITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyTranspositions = parser.booleanValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + QueryStringQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + QueryStringQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
        if (queryString == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] must be provided with a [query]");
        }

        QueryStringQueryBuilder queryStringQuery = new QueryStringQueryBuilder(queryString);
        if (fieldsAndWeights != null) {
            queryStringQuery.fields(fieldsAndWeights);
        }
        queryStringQuery.defaultField(defaultField);
        queryStringQuery.defaultOperator(defaultOperator);
        queryStringQuery.analyzer(analyzer);
        queryStringQuery.quoteAnalyzer(quoteAnalyzer);
        queryStringQuery.allowLeadingWildcard(allowLeadingWildcard);
        queryStringQuery.maxDeterminizedStates(maxDeterminizedStates);
        queryStringQuery.enablePositionIncrements(enablePositionIncrements);
        queryStringQuery.escape(escape);
        queryStringQuery.fuzzyPrefixLength(fuzzyPrefixLength);
        queryStringQuery.fuzzyMaxExpansions(fuzzyMaxExpansions);
        queryStringQuery.fuzzyRewrite(fuzzyRewrite);
        queryStringQuery.phraseSlop(phraseSlop);
        queryStringQuery.fuzziness(fuzziness);
        queryStringQuery.type(type);
        if (tieBreaker != null) {
            queryStringQuery.tieBreaker(tieBreaker);
        }
        queryStringQuery.analyzeWildcard(analyzeWildcard);
        queryStringQuery.rewrite(rewrite);
        queryStringQuery.minimumShouldMatch(minimumShouldMatch);
        queryStringQuery.quoteFieldSuffix(quoteFieldSuffix);
        queryStringQuery.lenient(lenient);
        queryStringQuery.timeZone(timeZone);
        queryStringQuery.boost(boost);
        queryStringQuery.queryName(queryName);
        queryStringQuery.autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        queryStringQuery.fuzzyTranspositions(fuzzyTranspositions);
        return queryStringQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(QueryStringQueryBuilder other) {
        return Objects.equals(queryString, other.queryString)
            && Objects.equals(defaultField, other.defaultField)
            && Objects.equals(fieldsAndWeights, other.fieldsAndWeights)
            && Objects.equals(defaultOperator, other.defaultOperator)
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(quoteAnalyzer, other.quoteAnalyzer)
            && Objects.equals(quoteFieldSuffix, other.quoteFieldSuffix)
            && Objects.equals(allowLeadingWildcard, other.allowLeadingWildcard)
            && Objects.equals(enablePositionIncrements, other.enablePositionIncrements)
            && Objects.equals(analyzeWildcard, other.analyzeWildcard)
            && Objects.equals(fuzziness, other.fuzziness)
            && Objects.equals(fuzzyPrefixLength, other.fuzzyPrefixLength)
            && Objects.equals(fuzzyMaxExpansions, other.fuzzyMaxExpansions)
            && Objects.equals(fuzzyRewrite, other.fuzzyRewrite)
            && Objects.equals(phraseSlop, other.phraseSlop)
            && Objects.equals(type, other.type)
            && Objects.equals(tieBreaker, other.tieBreaker)
            && Objects.equals(rewrite, other.rewrite)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(lenient, other.lenient)
            && Objects.equals(timeZone == null ? null : timeZone.getId(), other.timeZone == null ? null : other.timeZone.getId())
            && Objects.equals(escape, other.escape)
            && Objects.equals(maxDeterminizedStates, other.maxDeterminizedStates)
            && Objects.equals(autoGenerateSynonymsPhraseQuery, other.autoGenerateSynonymsPhraseQuery)
            && Objects.equals(fuzzyTranspositions, other.fuzzyTranspositions);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            queryString,
            defaultField,
            fieldsAndWeights,
            defaultOperator,
            analyzer,
            quoteAnalyzer,
            quoteFieldSuffix,
            allowLeadingWildcard,
            analyzeWildcard,
            enablePositionIncrements,
            fuzziness,
            fuzzyPrefixLength,
            fuzzyMaxExpansions,
            fuzzyRewrite,
            phraseSlop,
            type,
            tieBreaker,
            rewrite,
            minimumShouldMatch,
            lenient,
            timeZone == null ? 0 : timeZone.getId(),
            escape,
            maxDeterminizedStates,
            autoGenerateSynonymsPhraseQuery,
            fuzzyTranspositions
        );
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        String rewrittenQueryString = escape ? org.apache.lucene.queryparser.classic.QueryParser.escape(this.queryString) : queryString;
        if (fieldsAndWeights.size() > 0 && this.defaultField != null) {
            throw addValidationError("cannot use [fields] parameter in conjunction with [default_field]", null);
        }

        QueryStringQueryParser queryParser;
        boolean isLenient = lenient == null ? context.queryStringLenient() : lenient;
        if (defaultField != null) {
            if (Regex.isMatchAllPattern(defaultField)) {
                queryParser = new QueryStringQueryParser(context, lenient == null ? true : lenient);
            } else {
                queryParser = new QueryStringQueryParser(context, defaultField, isLenient);
            }
        } else if (fieldsAndWeights.size() > 0) {
            final Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(context, fieldsAndWeights);
            if (QueryParserHelper.hasAllFieldsWildcard(fieldsAndWeights.keySet())) {
                queryParser = new QueryStringQueryParser(context, resolvedFields, lenient == null ? true : lenient);
            } else {
                queryParser = new QueryStringQueryParser(context, resolvedFields, isLenient);
            }
        } else {
            List<String> defaultFields = context.defaultFields();
            if (QueryParserHelper.hasAllFieldsWildcard(defaultFields)) {
                queryParser = new QueryStringQueryParser(context, lenient == null ? true : lenient);
            } else {
                final Map<String, Float> resolvedFields = QueryParserHelper.resolveMappingFields(
                    context,
                    QueryParserHelper.parseFieldsAndWeights(defaultFields)
                );
                queryParser = new QueryStringQueryParser(context, resolvedFields, isLenient);
            }
        }

        if (analyzer != null) {
            NamedAnalyzer namedAnalyzer = context.getIndexAnalyzers().get(analyzer);
            if (namedAnalyzer == null) {
                throw new QueryShardException(context, "[query_string] analyzer [" + analyzer + "] not found");
            }
            queryParser.setForceAnalyzer(namedAnalyzer);
        }

        if (quoteAnalyzer != null) {
            NamedAnalyzer forceQuoteAnalyzer = context.getIndexAnalyzers().get(quoteAnalyzer);
            if (forceQuoteAnalyzer == null) {
                throw new QueryShardException(context, "[query_string] quote_analyzer [" + quoteAnalyzer + "] not found");
            }
            queryParser.setForceQuoteAnalyzer(forceQuoteAnalyzer);
        }

        queryParser.setDefaultOperator(defaultOperator.toQueryParserOperator());
        queryParser.setType(type);
        if (tieBreaker != null) {
            queryParser.setGroupTieBreaker(tieBreaker);
        } else {
            queryParser.setGroupTieBreaker(type.tieBreaker());
        }
        queryParser.setPhraseSlop(phraseSlop);
        queryParser.setQuoteFieldSuffix(quoteFieldSuffix);
        queryParser.setAllowLeadingWildcard(
            allowLeadingWildcard == null ? context.queryStringAllowLeadingWildcard() : allowLeadingWildcard
        );
        queryParser.setAnalyzeWildcard(analyzeWildcard == null ? context.queryStringAnalyzeWildcard() : analyzeWildcard);
        queryParser.setEnablePositionIncrements(enablePositionIncrements);
        queryParser.setFuzziness(fuzziness);
        queryParser.setFuzzyPrefixLength(fuzzyPrefixLength);
        queryParser.setFuzzyMaxExpansions(fuzzyMaxExpansions);
        queryParser.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(this.fuzzyRewrite, LoggingDeprecationHandler.INSTANCE));
        queryParser.setMultiTermRewriteMethod(QueryParsers.parseRewriteMethod(this.rewrite, LoggingDeprecationHandler.INSTANCE));
        queryParser.setTimeZone(timeZone);
        queryParser.setDeterminizeWorkLimit(maxDeterminizedStates);
        queryParser.setAutoGenerateMultiTermSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        queryParser.setFuzzyTranspositions(fuzzyTranspositions);

        Query query;
        try {
            query = queryParser.parse(rewrittenQueryString);
        } catch (org.apache.lucene.queryparser.classic.ParseException e) {
            throw new QueryShardException(context, "Failed to parse query [" + this.queryString + "]", e);
        }

        if (query == null) {
            return null;
        }

        // save the BoostQuery wrapped structure if present
        List<Float> boosts = new ArrayList<>();
        while (query instanceof BoostQuery boostQuery) {
            boosts.add(boostQuery.getBoost());
            query = boostQuery.getQuery();
        }

        query = Queries.fixNegativeQueryIfNeeded(query);
        query = Queries.maybeApplyMinimumShouldMatch(query, this.minimumShouldMatch);

        // restore the previous BoostQuery wrapping
        for (int i = boosts.size() - 1; i >= 0; i--) {
            query = new BoostQuery(query, boosts.get(i));
        }

        return query;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}

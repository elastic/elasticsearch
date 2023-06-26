/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.index.search.SimpleQueryStringQueryParser;
import org.elasticsearch.index.search.SimpleQueryStringQueryParser.Settings;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * SimpleQuery is a query parser that acts similar to a query_string query, but
 * won't throw exceptions for any weird string syntax. It supports
 * the following:
 * <ul>
 * <li>'{@code +}' specifies {@code AND} operation: {@code token1+token2}
 * <li>'{@code |}' specifies {@code OR} operation: {@code token1|token2}
 * <li>'{@code -}' negates a single token: {@code -token0}
 * <li>'{@code "}' creates phrases of terms: {@code "term1 term2 ..."}
 * <li>'{@code *}' at the end of terms specifies prefix query: {@code term*}
 * <li>'{@code (}' and '{@code)}' specifies precedence: {@code token1 + (token2 | token3)}
 * <li>'{@code ~}N' at the end of terms specifies fuzzy query: {@code term~1}
 * <li>'{@code ~}N' at the end of phrases specifies near/slop query: {@code "term1 term2"~5}
 * </ul>
 * <p>
 * See: {@link SimpleQueryStringQueryParser} for more information.
 * <p>
 * This query supports these options:
 * <p>
 * Required:
 * {@code query} - query text to be converted into other queries
 * <p>
 * Optional:
 * {@code analyzer} - anaylzer to be used for analyzing tokens to determine
 * which kind of query they should be converted into, defaults to "standard"
 * {@code default_operator} - default operator for boolean queries, defaults
 * to OR
 * {@code fields} - fields to search, defaults to _all if not set, allows
 * boosting a field with ^n
 *
 * For more detailed explanation of the query string syntax see also the <a
 * href=
 * "https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html"
 * > online documentation</a>.
 */
public class SimpleQueryStringBuilder extends AbstractQueryBuilder<SimpleQueryStringBuilder> {

    /** Default for using lenient query parsing.*/
    public static final boolean DEFAULT_LENIENT = false;
    /** Default for wildcard analysis.*/
    public static final boolean DEFAULT_ANALYZE_WILDCARD = false;
    /** Default for default operator to use for linking boolean clauses.*/
    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    /** Default for search flags to use. */
    public static final int DEFAULT_FLAGS = SimpleQueryStringFlag.ALL.value;
    /** Default for prefix length in fuzzy queries.*/
    public static final int DEFAULT_FUZZY_PREFIX_LENGTH = FuzzyQuery.defaultPrefixLength;
    /** Default number of terms fuzzy queries will expand to.*/
    public static final int DEFAULT_FUZZY_MAX_EXPANSIONS = FuzzyQuery.defaultMaxExpansions;
    /** Default for using transpositions in fuzzy queries.*/
    public static final boolean DEFAULT_FUZZY_TRANSPOSITIONS = FuzzyQuery.defaultTranspositions;

    /** Name for (de-)serialization. */
    public static final String NAME = "simple_query_string";

    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    private static final ParseField ANALYZE_WILDCARD_FIELD = new ParseField("analyze_wildcard");
    private static final ParseField LENIENT_FIELD = new ParseField("lenient");
    private static final ParseField FLAGS_FIELD = new ParseField("flags");
    private static final ParseField DEFAULT_OPERATOR_FIELD = new ParseField("default_operator");
    private static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField QUOTE_FIELD_SUFFIX_FIELD = new ParseField("quote_field_suffix");
    private static final ParseField GENERATE_SYNONYMS_PHRASE_QUERY = new ParseField("auto_generate_synonyms_phrase_query");
    private static final ParseField FUZZY_PREFIX_LENGTH_FIELD = new ParseField("fuzzy_prefix_length");
    private static final ParseField FUZZY_MAX_EXPANSIONS_FIELD = new ParseField("fuzzy_max_expansions");
    private static final ParseField FUZZY_TRANSPOSITIONS_FIELD = new ParseField("fuzzy_transpositions");

    /** Query text to parse. */
    private final String queryText;
    /**
     * Fields to query against. If left empty will query default field,
     * currently _ALL.
     */
    private Map<String, Float> fieldsAndWeights = new HashMap<>();
    /** If specified, analyzer to use to parse the query text, defaults to registered default in toQuery. */
    private String analyzer;
    /** Default operator to use for linking boolean clauses. Defaults to OR according to docs. */
    private Operator defaultOperator = DEFAULT_OPERATOR;
    /** If result is a boolean query, minimumShouldMatch parameter to apply. Ignored otherwise. */
    private String minimumShouldMatch;
    /** Any search flags to be used, ALL by default. */
    private int flags = DEFAULT_FLAGS;
    /** Whether or not the lenient flag has been set or not */
    private boolean lenientSet = false;

    /** Further search settings needed by the ES specific query string parser only. */
    private Settings settings = new Settings();

    /** Construct a new simple query with this query string. */
    public SimpleQueryStringBuilder(String queryText) {
        if (queryText == null) {
            throw new IllegalArgumentException("query text missing");
        }
        this.queryText = queryText;
    }

    /**
     * Read from a stream.
     */
    public SimpleQueryStringBuilder(StreamInput in) throws IOException {
        super(in);
        queryText = in.readString();
        int size = in.readInt();
        Map<String, Float> fields = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            Float weight = in.readFloat();
            checkNegativeBoost(weight);
            fields.put(field, weight);
        }
        fieldsAndWeights.putAll(fields);
        flags = in.readInt();
        analyzer = in.readOptionalString();
        defaultOperator = Operator.readFromStream(in);
        settings.lenient(in.readBoolean());
        this.lenientSet = in.readBoolean();
        settings.analyzeWildcard(in.readBoolean());
        minimumShouldMatch = in.readOptionalString();
        settings.quoteFieldSuffix(in.readOptionalString());
        settings.autoGenerateSynonymsPhraseQuery(in.readBoolean());
        settings.fuzzyPrefixLength(in.readVInt());
        settings.fuzzyMaxExpansions(in.readVInt());
        settings.fuzzyTranspositions(in.readBoolean());
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
        out.writeBoolean(settings.lenient());
        out.writeBoolean(lenientSet);
        out.writeBoolean(settings.analyzeWildcard());
        out.writeOptionalString(minimumShouldMatch);
        out.writeOptionalString(settings.quoteFieldSuffix());
        out.writeBoolean(settings.autoGenerateSynonymsPhraseQuery());
        out.writeVInt(settings.fuzzyPrefixLength());
        out.writeVInt(settings.fuzzyMaxExpansions());
        out.writeBoolean(settings.fuzzyTranspositions());
    }

    /** Returns the text to parse the query from. */
    public String value() {
        return this.queryText;
    }

    /** Add a field to run the query against. */
    public SimpleQueryStringBuilder field(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty");
        }
        this.fieldsAndWeights.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
        return this;
    }

    /** Add a field to run the query against with a specific boost. */
    public SimpleQueryStringBuilder field(String field, float boost) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty");
        }
        checkNegativeBoost(boost);
        this.fieldsAndWeights.put(field, boost);
        return this;
    }

    /** Add several fields to run the query against with a specific boost. */
    public SimpleQueryStringBuilder fields(Map<String, Float> fields) {
        Objects.requireNonNull(fields, "fields cannot be null");
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
        if (CollectionUtils.isEmpty(flags) == false) {
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
     * Set the suffix to append to field names for phrase matching.
     */
    public SimpleQueryStringBuilder quoteFieldSuffix(String suffix) {
        settings.quoteFieldSuffix(suffix);
        return this;
    }

    /**
     * Return the suffix to append to field names for phrase matching.
     */
    public String quoteFieldSuffix() {
        return settings.quoteFieldSuffix();
    }

    /** Specifies whether query parsing should be lenient. Defaults to false. */
    public SimpleQueryStringBuilder lenient(boolean lenient) {
        this.settings.lenient(lenient);
        this.lenientSet = true;
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

    public SimpleQueryStringBuilder autoGenerateSynonymsPhraseQuery(boolean value) {
        this.settings.autoGenerateSynonymsPhraseQuery(value);
        return this;
    }

    /**
     * Whether phrase queries should be automatically generated for multi terms synonyms.
     * Defaults to {@code true}.
     */
    public boolean autoGenerateSynonymsPhraseQuery() {
        return settings.autoGenerateSynonymsPhraseQuery();
    }

    public SimpleQueryStringBuilder fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.settings.fuzzyPrefixLength(fuzzyPrefixLength);
        return this;
    }

    public int fuzzyPrefixLength() {
        return settings.fuzzyPrefixLength();
    }

    public SimpleQueryStringBuilder fuzzyMaxExpansions(int fuzzyMaxExpansions) {
        this.settings.fuzzyMaxExpansions(fuzzyMaxExpansions);
        return this;
    }

    public int fuzzyMaxExpansions() {
        return settings.fuzzyMaxExpansions();
    }

    public boolean fuzzyTranspositions() {
        return settings.fuzzyTranspositions();
    }

    /**
     * Sets whether transpositions are supported in fuzzy queries.<p>
     * The default metric used by fuzzy queries to determine a match is the Damerau-Levenshtein
     * distance formula which supports transpositions. Setting transposition to false will
     * switch to classic Levenshtein distance.<br>
     * If not set, Damerau-Levenshtein distance metric will be used.
     */
    public SimpleQueryStringBuilder fuzzyTranspositions(boolean fuzzyTranspositions) {
        this.settings.fuzzyTranspositions(fuzzyTranspositions);
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        Settings newSettings = new Settings(settings);
        final Map<String, Float> resolvedFieldsAndWeights;
        boolean isAllField;
        if (fieldsAndWeights.isEmpty() == false) {
            resolvedFieldsAndWeights = QueryParserHelper.resolveMappingFields(context, fieldsAndWeights);
            isAllField = QueryParserHelper.hasAllFieldsWildcard(fieldsAndWeights.keySet());
        } else {
            List<String> defaultFields = context.defaultFields();
            resolvedFieldsAndWeights = QueryParserHelper.resolveMappingFields(
                context,
                QueryParserHelper.parseFieldsAndWeights(defaultFields)
            );
            isAllField = QueryParserHelper.hasAllFieldsWildcard(defaultFields);
        }

        if (isAllField) {
            newSettings.lenient(lenientSet ? settings.lenient() : true);
        }

        final SimpleQueryStringQueryParser sqp;
        if (analyzer == null) {
            sqp = new SimpleQueryStringQueryParser(resolvedFieldsAndWeights, flags, newSettings, context);
        } else {
            Analyzer luceneAnalyzer = context.getIndexAnalyzers().get(analyzer);
            if (luceneAnalyzer == null) {
                throw new QueryShardException(context, "[" + SimpleQueryStringBuilder.NAME + "] analyzer [" + analyzer + "] not found");
            }
            sqp = new SimpleQueryStringQueryParser(luceneAnalyzer, resolvedFieldsAndWeights, flags, newSettings, context);
        }
        sqp.setDefaultOperator(defaultOperator.toBooleanClauseOccur());
        Query query = sqp.parse(queryText);
        return Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(QUERY_FIELD.getPreferredName(), queryText);

        if (fieldsAndWeights.size() > 0) {
            builder.startArray(FIELDS_FIELD.getPreferredName());
            for (Map.Entry<String, Float> entry : fieldsAndWeights.entrySet()) {
                builder.value(entry.getKey() + "^" + entry.getValue());
            }
            builder.endArray();
        }

        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }

        if (flags != DEFAULT_FLAGS) {
            builder.field(FLAGS_FIELD.getPreferredName(), flags);
        }
        if (defaultOperator != DEFAULT_OPERATOR) {
            builder.field(DEFAULT_OPERATOR_FIELD.getPreferredName(), defaultOperator.name().toLowerCase(Locale.ROOT));
        }
        if (lenientSet) {
            builder.field(LENIENT_FIELD.getPreferredName(), settings.lenient());
        }
        if (settings.analyzeWildcard() != DEFAULT_ANALYZE_WILDCARD) {
            builder.field(ANALYZE_WILDCARD_FIELD.getPreferredName(), settings.analyzeWildcard());
        }
        if (settings.quoteFieldSuffix() != null) {
            builder.field(QUOTE_FIELD_SUFFIX_FIELD.getPreferredName(), settings.quoteFieldSuffix());
        }

        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        if (settings.autoGenerateSynonymsPhraseQuery() != true) {
            builder.field(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), settings.autoGenerateSynonymsPhraseQuery());
        }
        if (settings.fuzzyPrefixLength() != DEFAULT_FUZZY_PREFIX_LENGTH) {
            builder.field(FUZZY_PREFIX_LENGTH_FIELD.getPreferredName(), settings.fuzzyPrefixLength());
        }
        if (settings.fuzzyMaxExpansions() != DEFAULT_FUZZY_MAX_EXPANSIONS) {
            builder.field(FUZZY_MAX_EXPANSIONS_FIELD.getPreferredName(), settings.fuzzyMaxExpansions());
        }
        if (settings.fuzzyTranspositions() != DEFAULT_FUZZY_TRANSPOSITIONS) {
            builder.field(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), settings.fuzzyTranspositions());
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    public static SimpleQueryStringBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String queryBody = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String minimumShouldMatch = null;
        Map<String, Float> fieldsAndWeights = null;
        Operator defaultOperator = null;
        String analyzerName = null;
        int flags = SimpleQueryStringFlag.ALL.value();
        Boolean lenient = null;
        boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;
        String quoteFieldSuffix = null;
        boolean autoGenerateSynonymsPhraseQuery = true;
        int fuzzyPrefixLenght = SimpleQueryStringBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        int fuzzyMaxExpansions = SimpleQueryStringBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        boolean fuzzyTranspositions = SimpleQueryStringBuilder.DEFAULT_FUZZY_TRANSPOSITIONS;

        XContentParser.Token token;
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
                        "[" + SimpleQueryStringBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryBody = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzerName = parser.text();
                } else if (DEFAULT_OPERATOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if (FLAGS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                        // Possible options are:
                        // ALL, NONE, AND, OR, PREFIX, PHRASE, PRECEDENCE, ESCAPE, WHITESPACE, FUZZY, NEAR, SLOP
                        flags = SimpleQueryStringFlag.resolveFlags(parser.text());
                    } else {
                        flags = parser.intValue();
                        if (flags < 0) {
                            flags = SimpleQueryStringFlag.ALL.value();
                        }
                    }
                } else if (LENIENT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    lenient = parser.booleanValue();
                } else if (ANALYZE_WILDCARD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzeWildcard = parser.booleanValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (QUOTE_FIELD_SUFFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    quoteFieldSuffix = parser.textOrNull();
                } else if (GENERATE_SYNONYMS_PHRASE_QUERY.match(currentFieldName, parser.getDeprecationHandler())) {
                    autoGenerateSynonymsPhraseQuery = parser.booleanValue();
                } else if (FUZZY_PREFIX_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyPrefixLenght = parser.intValue();
                } else if (FUZZY_MAX_EXPANSIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyMaxExpansions = parser.intValue();
                } else if (FUZZY_TRANSPOSITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyTranspositions = parser.booleanValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + SimpleQueryStringBuilder.NAME + "] unsupported field [" + parser.currentName() + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + SimpleQueryStringBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        // Query text is required
        if (queryBody == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME + "] query text missing");
        }

        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder(queryBody);
        if (fieldsAndWeights != null) {
            qb.fields(fieldsAndWeights);
        }
        qb.boost(boost).analyzer(analyzerName).queryName(queryName).minimumShouldMatch(minimumShouldMatch);
        qb.flags(flags).defaultOperator(defaultOperator);
        if (lenient != null) {
            qb.lenient(lenient);
        }
        qb.analyzeWildcard(analyzeWildcard).boost(boost).quoteFieldSuffix(quoteFieldSuffix);
        qb.autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        qb.fuzzyPrefixLength(fuzzyPrefixLenght);
        qb.fuzzyMaxExpansions(fuzzyMaxExpansions);
        qb.fuzzyTranspositions(fuzzyTranspositions);
        return qb;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldsAndWeights, analyzer, defaultOperator, queryText, minimumShouldMatch, settings, flags);
    }

    @Override
    protected boolean doEquals(SimpleQueryStringBuilder other) {
        return Objects.equals(fieldsAndWeights, other.fieldsAndWeights)
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(defaultOperator, other.defaultOperator)
            && Objects.equals(queryText, other.queryText)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(settings, other.settings)
            && (flags == other.flags);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }

    @Override
    public Query toHighlightQuery(String fieldName) {
        return new WildcardQuery(new Term(fieldName, queryText));
    }
}

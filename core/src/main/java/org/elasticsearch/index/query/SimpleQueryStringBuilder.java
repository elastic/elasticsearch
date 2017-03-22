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
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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
 * won't throw exceptions for any weird string syntax. It supports
 * the following:
 * <ul>
 * <li>'{@code +}' specifies {@code AND} operation: <tt>token1+token2</tt>
 * <li>'{@code |}' specifies {@code OR} operation: <tt>token1|token2</tt>
 * <li>'{@code -}' negates a single token: <tt>-token0</tt>
 * <li>'{@code "}' creates phrases of terms: <tt>"term1 term2 ..."</tt>
 * <li>'{@code *}' at the end of terms specifies prefix query: <tt>term*</tt>
 * <li>'{@code (}' and '{@code)}' specifies precedence: <tt>token1 + (token2 | token3)</tt>
 * <li>'{@code ~}N' at the end of terms specifies fuzzy query: <tt>term~1</tt>
 * <li>'{@code ~}N' at the end of phrases specifies near/slop query: <tt>"term1 term2"~5</tt>
 * </ul>
 * <p>
 * See: {@link SimpleQueryParser} for more information.
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

    /** Name for (de-)serialization. */
    public static final String NAME = "simple_query_string";

    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    private static final ParseField ANALYZE_WILDCARD_FIELD = new ParseField("analyze_wildcard");
    private static final ParseField LENIENT_FIELD = new ParseField("lenient");
    private static final ParseField LOWERCASE_EXPANDED_TERMS_FIELD = new ParseField("lowercase_expanded_terms")
            .withAllDeprecated("Decision is now made by the analyzer");
    private static final ParseField LOCALE_FIELD = new ParseField("locale")
            .withAllDeprecated("Decision is now made by the analyzer");
    private static final ParseField FLAGS_FIELD = new ParseField("flags");
    private static final ParseField DEFAULT_OPERATOR_FIELD = new ParseField("default_operator");
    private static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField QUOTE_FIELD_SUFFIX_FIELD = new ParseField("quote_field_suffix");
    private static final ParseField ALL_FIELDS_FIELD = new ParseField("all_fields");

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
    /** Flag specifying whether query should be forced to expand to all searchable fields */
    private Boolean useAllFields;
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
            fields.put(field, weight);
        }
        fieldsAndWeights.putAll(fields);
        flags = in.readInt();
        analyzer = in.readOptionalString();
        defaultOperator = Operator.readFromStream(in);
        if (in.getVersion().before(Version.V_5_1_1_UNRELEASED)) {
            in.readBoolean(); // lowercase_expanded_terms
        }
        settings.lenient(in.readBoolean());
        if (in.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            this.lenientSet = in.readBoolean();
        }
        settings.analyzeWildcard(in.readBoolean());
        if (in.getVersion().before(Version.V_5_1_1_UNRELEASED)) {
            in.readString(); // locale
        }
        minimumShouldMatch = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            settings.quoteFieldSuffix(in.readOptionalString());
            useAllFields = in.readOptionalBoolean();
        }
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
        if (out.getVersion().before(Version.V_5_1_1_UNRELEASED)) {
            out.writeBoolean(true); // lowercase_expanded_terms
        }
        out.writeBoolean(settings.lenient());
        if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            out.writeBoolean(lenientSet);
        }
        out.writeBoolean(settings.analyzeWildcard());
        if (out.getVersion().before(Version.V_5_1_1_UNRELEASED)) {
            out.writeString(Locale.ROOT.toLanguageTag()); // locale
        }
        out.writeOptionalString(minimumShouldMatch);
        if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            out.writeOptionalString(settings.quoteFieldSuffix());
            out.writeOptionalBoolean(useAllFields);
        }
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

    public Boolean useAllFields() {
        return useAllFields;
    }

    public SimpleQueryStringBuilder useAllFields(Boolean useAllFields) {
        this.useAllFields = useAllFields;
        return this;
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

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // field names in builder can have wildcards etc, need to resolve them here
        Map<String, Float> resolvedFieldsAndWeights = new TreeMap<>();

        if ((useAllFields != null && useAllFields) && (fieldsAndWeights.size() != 0)) {
            throw addValidationError("cannot use [all_fields] parameter in conjunction with [fields]", null);
        }

        // If explicitly required to use all fields, use all fields, OR:
        // Automatically determine the fields (to replace the _all field) if all of the following are true:
        // - The _all field is disabled,
        // - and the default_field has not been changed in the settings
        // - and no fields are specified in the request
        Settings newSettings = new Settings(settings);
        if ((this.useAllFields != null && this.useAllFields) ||
                (context.getMapperService().allEnabled() == false &&
                        "_all".equals(context.defaultField()) &&
                        this.fieldsAndWeights.isEmpty())) {
            resolvedFieldsAndWeights = QueryStringQueryBuilder.allQueryableDefaultFields(context);
            // Need to use lenient mode when using "all-mode" so exceptions aren't thrown due to mismatched types
            newSettings.lenient(lenientSet ? settings.lenient() : true);
        } else {
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
        }

        // Use standard analyzer by default if none specified
        Analyzer luceneAnalyzer;
        if (analyzer == null) {
            luceneAnalyzer = context.getMapperService().searchAnalyzer();
        } else {
            luceneAnalyzer = context.getIndexAnalyzers().get(analyzer);
            if (luceneAnalyzer == null) {
                throw new QueryShardException(context, "[" + SimpleQueryStringBuilder.NAME + "] analyzer [" + analyzer
                        + "] not found");
            }

        }

        SimpleQueryParser sqp = new SimpleQueryParser(luceneAnalyzer, resolvedFieldsAndWeights, flags, newSettings, context);
        sqp.setDefaultOperator(defaultOperator.toBooleanClauseOccur());
        Query query = sqp.parse(queryText);
        return Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
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

        builder.field(FLAGS_FIELD.getPreferredName(), flags);
        builder.field(DEFAULT_OPERATOR_FIELD.getPreferredName(), defaultOperator.name().toLowerCase(Locale.ROOT));
        if (lenientSet) {
            builder.field(LENIENT_FIELD.getPreferredName(), settings.lenient());
        }
        builder.field(ANALYZE_WILDCARD_FIELD.getPreferredName(), settings.analyzeWildcard());
        if (settings.quoteFieldSuffix() != null) {
            builder.field(QUOTE_FIELD_SUFFIX_FIELD.getPreferredName(), settings.quoteFieldSuffix());
        }

        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        if (useAllFields != null) {
            builder.field(ALL_FIELDS_FIELD.getPreferredName(), useAllFields);
        }

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static SimpleQueryStringBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        String queryBody = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String minimumShouldMatch = null;
        Map<String, Float> fieldsAndWeights = new HashMap<>();
        Operator defaultOperator = null;
        String analyzerName = null;
        int flags = SimpleQueryStringFlag.ALL.value();
        Boolean lenient = null;
        boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;
        String quoteFieldSuffix = null;
        Boolean useAllFields = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (FIELDS_FIELD.match(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String fField = null;
                        float fBoost = 1;
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
                    throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (QUERY_FIELD.match(currentFieldName)) {
                    queryBody = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (ANALYZER_FIELD.match(currentFieldName)) {
                    analyzerName = parser.text();
                } else if (DEFAULT_OPERATOR_FIELD.match(currentFieldName)) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if (FLAGS_FIELD.match(currentFieldName)) {
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
                } else if (LOCALE_FIELD.match(currentFieldName)) {
                    // ignore, deprecated setting
                } else if (LOWERCASE_EXPANDED_TERMS_FIELD.match(currentFieldName)) {
                    // ignore, deprecated setting
                } else if (LENIENT_FIELD.match(currentFieldName)) {
                    lenient = parser.booleanValue();
                } else if (ANALYZE_WILDCARD_FIELD.match(currentFieldName)) {
                    analyzeWildcard = parser.booleanValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (QUOTE_FIELD_SUFFIX_FIELD.match(currentFieldName)) {
                    quoteFieldSuffix = parser.textOrNull();
                } else if (ALL_FIELDS_FIELD.match(currentFieldName)) {
                    useAllFields = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME +
                            "] unsupported field [" + parser.currentName() + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME +
                        "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        // Query text is required
        if (queryBody == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME + "] query text missing");
        }

        if ((useAllFields != null && useAllFields) && (fieldsAndWeights.size() != 0)) {
            throw new ParsingException(parser.getTokenLocation(),
                    "cannot use [all_fields] parameter in conjunction with [fields]");
        }

        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder(queryBody);
        qb.boost(boost).fields(fieldsAndWeights).analyzer(analyzerName).queryName(queryName).minimumShouldMatch(minimumShouldMatch);
        qb.flags(flags).defaultOperator(defaultOperator);
        if (lenient != null) {
            qb.lenient(lenient);
        }
        qb.analyzeWildcard(analyzeWildcard).boost(boost).quoteFieldSuffix(quoteFieldSuffix);
        qb.useAllFields(useAllFields);
        return qb;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldsAndWeights, analyzer, defaultOperator, queryText, minimumShouldMatch, settings, flags, useAllFields);
    }

    @Override
    protected boolean doEquals(SimpleQueryStringBuilder other) {
        return Objects.equals(fieldsAndWeights, other.fieldsAndWeights) && Objects.equals(analyzer, other.analyzer)
                && Objects.equals(defaultOperator, other.defaultOperator) && Objects.equals(queryText, other.queryText)
                && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
                && Objects.equals(settings, other.settings)
                && (flags == other.flags)
                && (useAllFields == other.useAllFields);
    }

}

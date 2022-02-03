/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQueryParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.PREFIX_LENGTH_FIELD;

/**
 * The boolean prefix query analyzes the input text and creates a boolean query containing a Term query for each term, except
 * for the last term, which is used to create a prefix query
 */
public class MatchBoolPrefixQueryBuilder extends AbstractQueryBuilder<MatchBoolPrefixQueryBuilder> {

    public static final String NAME = "match_bool_prefix";

    private static final Operator DEFAULT_OPERATOR = Operator.OR;

    private final String fieldName;

    private final Object value;

    private String analyzer;

    private Operator operator = DEFAULT_OPERATOR;

    private String minimumShouldMatch;

    private Fuzziness fuzziness;

    private int prefixLength = FuzzyQuery.defaultPrefixLength;

    private int maxExpansions = FuzzyQuery.defaultMaxExpansions;

    private boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;

    private String fuzzyRewrite;

    public MatchBoolPrefixQueryBuilder(String fieldName, Object value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    public MatchBoolPrefixQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        analyzer = in.readOptionalString();
        operator = Operator.readFromStream(in);
        minimumShouldMatch = in.readOptionalString();
        fuzziness = in.readOptionalWriteable(Fuzziness::new);
        prefixLength = in.readVInt();
        maxExpansions = in.readVInt();
        fuzzyTranspositions = in.readBoolean();
        fuzzyRewrite = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeOptionalString(analyzer);
        operator.writeTo(out);
        out.writeOptionalString(minimumShouldMatch);
        out.writeOptionalWriteable(fuzziness);
        out.writeVInt(prefixLength);
        out.writeVInt(maxExpansions);
        out.writeBoolean(fuzzyTranspositions);
        out.writeOptionalString(fuzzyRewrite);
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    /** Get the analyzer to use, if previously set, otherwise {@code null} */
    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public MatchBoolPrefixQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Sets the operator to use when using a boolean query. Defaults to {@code OR}. */
    public MatchBoolPrefixQueryBuilder operator(Operator operator) {
        if (operator == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires operator to be non-null");
        }
        this.operator = operator;
        return this;
    }

    /** Returns the operator to use in a boolean query.*/
    public Operator operator() {
        return this.operator;
    }

    /** Sets optional minimumShouldMatch value to apply to the query */
    public MatchBoolPrefixQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /** Gets the minimumShouldMatch value */
    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    /** Sets the fuzziness used when evaluated to a fuzzy query type. Defaults to "AUTO". */
    public MatchBoolPrefixQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    /**  Gets the fuzziness used when evaluated to a fuzzy query type. */
    public Fuzziness fuzziness() {
        return this.fuzziness;
    }

    /**
     * Sets the length of a length of common (non-fuzzy) prefix for fuzzy match queries
     * @param prefixLength non-negative length of prefix
     * @throws IllegalArgumentException in case the prefix is negative
     */
    public MatchBoolPrefixQueryBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires prefix length to be non-negative.");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Gets the length of a length of common (non-fuzzy) prefix for fuzzy match queries
     */
    public int prefixLength() {
        return this.prefixLength;
    }

    /**
     * When using fuzzy or prefix type query, the number of term expansions to use.
     */
    public MatchBoolPrefixQueryBuilder maxExpansions(int maxExpansions) {
        if (maxExpansions <= 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires maxExpansions to be positive.");
        }
        this.maxExpansions = maxExpansions;
        return this;
    }

    /**
     * Get the (optional) number of term expansions when using fuzzy or prefix type query.
     */
    public int maxExpansions() {
        return this.maxExpansions;
    }

    /**
     * Sets whether transpositions are supported in fuzzy queries.<p>
     * The default metric used by fuzzy queries to determine a match is the Damerau-Levenshtein
     * distance formula which supports transpositions. Setting transposition to false will
     * switch to classic Levenshtein distance.<br>
     * If not set, Damerau-Levenshtein distance metric will be used.
     */
    public MatchBoolPrefixQueryBuilder fuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
        return this;
    }

    /** Gets the fuzzy query transposition setting. */
    public boolean fuzzyTranspositions() {
        return this.fuzzyTranspositions;
    }

    /** Sets the fuzzy_rewrite parameter controlling how the fuzzy query will get rewritten */
    public MatchBoolPrefixQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    /**
     * Get the fuzzy_rewrite parameter
     * @see #fuzzyRewrite(String)
     */
    public String fuzzyRewrite() {
        return this.fuzzyRewrite;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MatchQueryBuilder.QUERY_FIELD.getPreferredName(), value);
        if (analyzer != null) {
            builder.field(MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(OPERATOR_FIELD.getPreferredName(), operator.toString());
        if (minimumShouldMatch != null) {
            builder.field(MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        builder.field(PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
        builder.field(MAX_EXPANSIONS_FIELD.getPreferredName(), maxExpansions);
        builder.field(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), fuzzyTranspositions);
        if (fuzzyRewrite != null) {
            builder.field(FUZZY_REWRITE_FIELD.getPreferredName(), fuzzyRewrite);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static MatchBoolPrefixQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        Operator operator = DEFAULT_OPERATOR;
        String minimumShouldMatch = null;
        Fuzziness fuzziness = null;
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansion = FuzzyQuery.defaultMaxExpansions;
        boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;
        String fuzzyRewrite = null;
        String queryName = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (MatchQueryBuilder.QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.objectText();
                        } else if (MatchQueryBuilder.ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            analyzer = parser.text();
                        } else if (OPERATOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            operator = Operator.fromString(parser.text());
                        } else if (MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            minimumShouldMatch = parser.textOrNull();
                        } else if (Fuzziness.FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            fuzziness = Fuzziness.parse(parser);
                        } else if (PREFIX_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            prefixLength = parser.intValue();
                        } else if (MAX_EXPANSIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            maxExpansion = parser.intValue();
                        } else if (FUZZY_TRANSPOSITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            fuzzyTranspositions = parser.booleanValue();
                        } else if (FUZZY_REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            fuzzyRewrite = parser.textOrNull();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }

        MatchBoolPrefixQueryBuilder queryBuilder = new MatchBoolPrefixQueryBuilder(fieldName, value);
        queryBuilder.analyzer(analyzer);
        queryBuilder.operator(operator);
        queryBuilder.minimumShouldMatch(minimumShouldMatch);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        if (fuzziness != null) {
            queryBuilder.fuzziness(fuzziness);
        }
        queryBuilder.prefixLength(prefixLength);
        queryBuilder.maxExpansions(maxExpansion);
        queryBuilder.fuzzyTranspositions(fuzzyTranspositions);
        queryBuilder.fuzzyRewrite(fuzzyRewrite);
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (analyzer != null && context.getIndexAnalyzers().get(analyzer) == null) {
            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
        }

        final MatchQueryParser queryParser = new MatchQueryParser(context);
        if (analyzer != null) {
            queryParser.setAnalyzer(analyzer);
        }
        queryParser.setOccur(operator.toBooleanClauseOccur());
        queryParser.setFuzziness(fuzziness);
        queryParser.setFuzzyPrefixLength(prefixLength);
        queryParser.setMaxExpansions(maxExpansions);
        queryParser.setTranspositions(fuzzyTranspositions);
        queryParser.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(fuzzyRewrite, null, LoggingDeprecationHandler.INSTANCE));

        final Query query = queryParser.parse(MatchQueryParser.Type.BOOLEAN_PREFIX, fieldName, value);
        return Queries.maybeApplyMinimumShouldMatch(query, minimumShouldMatch);
    }

    @Override
    protected boolean doEquals(MatchBoolPrefixQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(value, other.value)
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(operator, other.operator)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(fuzziness, other.fuzziness)
            && Objects.equals(prefixLength, other.prefixLength)
            && Objects.equals(maxExpansions, other.maxExpansions)
            && Objects.equals(fuzzyTranspositions, other.fuzzyTranspositions)
            && Objects.equals(fuzzyRewrite, other.fuzzyRewrite);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            fieldName,
            value,
            analyzer,
            operator,
            minimumShouldMatch,
            fuzziness,
            prefixLength,
            maxExpansions,
            fuzzyTranspositions,
            fuzzyRewrite
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_2_0;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ALLOW_LEADING_WILDCARD_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ANALYZE_WILDCARD_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.DEFAULT_FIELD_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.DEFAULT_OPERATOR_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.ENABLE_POSITION_INCREMENTS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZINESS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_PREFIX_LENGTH_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.MAX_DETERMINIZED_STATES_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.PHRASE_SLOP_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.QUOTE_ANALYZER_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.QUOTE_FIELD_SUFFIX_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.REWRITE_FIELD;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.TIME_ZONE_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class QueryString extends FullTextFunction implements OptionalArgument {

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(ALLOW_LEADING_WILDCARD_FIELD.getPreferredName(), BOOLEAN),
        entry(ANALYZE_WILDCARD_FIELD.getPreferredName(), BOOLEAN),
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), BOOLEAN),
        entry(DEFAULT_FIELD_FIELD.getPreferredName(), KEYWORD),
        entry(DEFAULT_OPERATOR_FIELD.getPreferredName(), KEYWORD),
        entry(ENABLE_POSITION_INCREMENTS_FIELD.getPreferredName(), BOOLEAN),
        entry(FUZZINESS_FIELD.getPreferredName(), KEYWORD),
        entry(FUZZY_MAX_EXPANSIONS_FIELD.getPreferredName(), INTEGER),
        entry(FUZZY_PREFIX_LENGTH_FIELD.getPreferredName(), INTEGER),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), BOOLEAN),
        entry(LENIENT_FIELD.getPreferredName(), BOOLEAN),
        entry(MAX_DETERMINIZED_STATES_FIELD.getPreferredName(), INTEGER),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), KEYWORD),
        entry(QUOTE_ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(QUOTE_FIELD_SUFFIX_FIELD.getPreferredName(), KEYWORD),
        entry(PHRASE_SLOP_FIELD.getPreferredName(), INTEGER),
        entry(REWRITE_FIELD.getPreferredName(), KEYWORD),
        entry(TIME_ZONE_FIELD.getPreferredName(), KEYWORD)
    );

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "QStr",
        QueryString::readFrom
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a <<query-dsl-query-string-query,query string query>>. "
            + "Returns true if the provided query string matches the row.",
        examples = {
            @Example(file = "qstr-function", tag = "qstr-with-field"),
            @Example(file = "qstr-function", tag = "qstr-with-options") }
    )
    public QueryString(
        Source source,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Query string in Lucene query string format."
        ) Expression queryString,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "default_field",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Default field to search if no field is provided in the query string. Supports wildcards (*)."
                ),
                @MapParam.MapParamEntry(
                    name = "allow_leading_wildcard",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, the wildcard characters * and ? are allowed as the first character of the query string. "
                        + "Defaults to true."
                ),
                @MapParam.MapParamEntry(
                    name = "allow_wildcard",
                    type = "boolean",
                    valueHint = { "false", "true" },
                    description = "If true, the query attempts to analyze wildcard terms in the query string. Defaults to false."
                ),
                @MapParam.MapParamEntry(
                    name = "analyzer",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Analyzer used to convert the text in the query value into token. "
                        + "Defaults to the index-time analyzer mapped for the default_field."
                ),
                @MapParam.MapParamEntry(
                    name = "auto_generate_synonyms_phrase_query",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, match phrase queries are automatically created for multi-term synonyms. Defaults to true."
                ),
                @MapParam.MapParamEntry(
                    name = "fuzziness",
                    type = "keyword",
                    valueHint = { "AUTO", "1", "2" },
                    description = "Maximum edit distance allowed for matching."
                ),
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query."
                ),
                @MapParam.MapParamEntry(
                    name = "default_operator",
                    type = "keyword",
                    valueHint = { "OR", "AND" },
                    description = "Default boolean logic used to interpret text in the query string if no operators are specified."
                ),
                @MapParam.MapParamEntry(
                    name = "enable_position_increments",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, enable position increments in queries constructed from a query_string search. Defaults to true."
                ),
                @MapParam.MapParamEntry(
                    name = "fields",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Array of fields to search. Supports wildcards (*)."
                ),
                @MapParam.MapParamEntry(
                    name = "fuzzy_max_expansions",
                    type = "integer",
                    valueHint = { "50" },
                    description = "Maximum number of terms to which the query expands for fuzzy matching. Defaults to 50."
                ),
                @MapParam.MapParamEntry(
                    name = "fuzzy_prefix_length",
                    type = "integer",
                    valueHint = { "0" },
                    description = "Number of beginning characters left unchanged for fuzzy matching. Defaults to 0."
                ),
                @MapParam.MapParamEntry(
                    name = "fuzzy_transpositions",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). "
                        + "Defaults to true."
                ),
                @MapParam.MapParamEntry(
                    name = "lenient",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If false, format-based errors, such as providing a text query value for a numeric field, are returned. "
                        + "Defaults to false."
                ),
                @MapParam.MapParamEntry(
                    name = "max_determinized_states",
                    type = "integer",
                    valueHint = { "10000" },
                    description = "Maximum number of automaton states required for the query. Default is 10000."
                ),
                @MapParam.MapParamEntry(
                    name = "minimum_should_match",
                    type = "string",
                    valueHint = { "standard" },
                    description = "Minimum number of clauses that must match for a document to be returned."
                ),
                @MapParam.MapParamEntry(
                    name = "quote_analyzer",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Analyzer used to convert quoted text in the query string into tokens. "
                        + "Defaults to the search_quote_analyzer mapped for the default_field."
                ),
                @MapParam.MapParamEntry(
                    name = "phrase_slop",
                    type = "integer",
                    valueHint = { "0" },
                    description = "Maximum number of positions allowed between matching tokens for phrases. "
                        + "Defaults to 0 (which means exact matches are required)."
                ),
                @MapParam.MapParamEntry(
                    name = "quote_field_suffix",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Suffix appended to quoted text in the query string."
                ),
                @MapParam.MapParamEntry(
                    name = "rewrite",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Method used to rewrite the query."
                ),
                @MapParam.MapParamEntry(
                    name = "time_zone",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Coordinated Universal Time (UTC) offset or IANA time zone used to convert date values in the "
                        + "query string to UTC."
                ), },
            description = "(Optional) Additional options for Query String as <<esql-function-named-params,function named parameters>>."
                + " See <<query-dsl-query-string-query,query string query>> for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, queryString, options, null);
    }

    // Options for QueryString. They don't need to be serialized as the data nodes will retrieve them from the query builder.
    private final transient Expression options;

    public QueryString(Source source, Expression queryString, Expression options, QueryBuilder queryBuilder) {
        super(source, queryString, options == null ? List.of(queryString) : List.of(queryString, options), queryBuilder);
        this.options = options;
    }

    private static QueryString readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        }
        return new QueryString(source, query, null, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(queryBuilder());
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String functionName() {
        return "QSTR";
    }

    public Expression options() {
        return options;
    }

    public static final Set<DataType> QUERY_DATA_TYPES = Set.of(KEYWORD, TEXT, SEMANTIC_TEXT);

    private TypeResolution resolveQuery() {
        return isType(query(), QUERY_DATA_TYPES::contains, sourceText(), FIRST, "keyword, text, semantic_text").and(
            isNotNullAndFoldable(query(), sourceText(), FIRST)
        );
    }

    private Map<String, Object> queryStringOptions() throws InvalidArgumentException {
        if (options() == null) {
            return null;
        }

        Map<String, Object> matchOptions = new HashMap<>();
        for (EntryExpression entry : ((MapExpression) options()).entryExpressions()) {
            Expression optionExpr = entry.key();
            Expression valueExpr = entry.value();
            TypeResolution resolution = isFoldable(optionExpr, sourceText(), SECOND).and(isFoldable(valueExpr, sourceText(), SECOND));
            if (resolution.unresolved()) {
                throw new InvalidArgumentException(resolution.message());
            }
            Object optionExprLiteral = ((Literal) optionExpr).value();
            Object valueExprLiteral = ((Literal) valueExpr).value();
            String optionName = optionExprLiteral instanceof BytesRef br ? br.utf8ToString() : optionExprLiteral.toString();
            String optionValue = valueExprLiteral instanceof BytesRef br ? br.utf8ToString() : valueExprLiteral.toString();
            // validate the optionExpr is supported
            DataType dataType = ALLOWED_OPTIONS.get(optionName);
            if (dataType == null) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], expected one of {}", optionName, sourceText(), ALLOWED_OPTIONS.keySet())
                );
            }
            try {
                matchOptions.put(optionName, DataTypeConverter.convert(optionValue, dataType));
            } catch (InvalidArgumentException e) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], {}", optionName, sourceText(), e.getMessage())
                );
            }
        }

        return matchOptions;
    }

    private TypeResolution resolveOptions() {
        if (options() != null) {
            TypeResolution resolution = isNotNull(options(), sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }
            // MapExpression does not have a DataType associated with it
            resolution = isMapExpression(options(), sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }

            try {
                queryStringOptions();
            } catch (InvalidArgumentException e) {
                return new TypeResolution(e.getMessage());
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveQuery().and(resolveOptions());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new QueryString(source(), newChildren.get(0), newChildren.size() == 1 ? null : newChildren.get(1), queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, QueryString::new, query(), options(), queryBuilder());
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        return new QueryStringQuery(source(), Objects.toString(queryAsObject()), Map.of(), queryStringOptions());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new QueryString(source(), query(), options(), queryBuilder);
    }

    @Override
    public boolean equals(Object o) {
        // QueryString does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing.
        if (o == null || getClass() != o.getClass()) return false;
        var qstr = (QueryString) o;
        return Objects.equals(query(), qstr.query()) && Objects.equals(queryBuilder(), qstr.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(query(), queryBuilder());
    }
}

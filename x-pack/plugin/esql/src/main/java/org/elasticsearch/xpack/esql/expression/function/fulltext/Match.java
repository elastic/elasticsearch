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
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MatchQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.PREFIX_LENGTH_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.ZERO_TERMS_QUERY_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.formatIncompatibleTypesMessage;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchQuery} .
 */
public class Match extends FullTextFunction implements OptionalArgument, PostOptimizationVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);
    public static final Set<DataType> FIELD_DATA_TYPES = Set.of(
        KEYWORD,
        TEXT,
        SEMANTIC_TEXT,
        BOOLEAN,
        DATETIME,
        DATE_NANOS,
        DOUBLE,
        INTEGER,
        IP,
        LONG,
        UNSIGNED_LONG,
        VERSION
    );
    public static final Set<DataType> QUERY_DATA_TYPES = Set.of(
        KEYWORD,
        BOOLEAN,
        DATETIME,
        DATE_NANOS,
        DOUBLE,
        INTEGER,
        IP,
        LONG,
        UNSIGNED_LONG,
        VERSION
    );

    protected final Expression field;

    // Options for match function. They don't need to be serialized as the data nodes will retrieve them from the query builder
    private final transient Expression options;

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), BOOLEAN),
        entry(Fuzziness.FIELD.getPreferredName(), KEYWORD),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), BOOLEAN),
        entry(FUZZY_REWRITE_FIELD.getPreferredName(), KEYWORD),
        entry(LENIENT_FIELD.getPreferredName(), BOOLEAN),
        entry(MAX_EXPANSIONS_FIELD.getPreferredName(), INTEGER),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), KEYWORD),
        entry(OPERATOR_FIELD.getPreferredName(), KEYWORD),
        entry(PREFIX_LENGTH_FIELD.getPreferredName(), INTEGER),
        entry(ZERO_TERMS_QUERY_FIELD.getPreferredName(), KEYWORD)
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = """
            Use `MATCH` to perform a <<query-dsl-match-query,match query>> on the specified field.
            Using `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.

            Match can be used on fields from the text family like <<text, text>> and <<semantic-text, semantic_text>>,
            as well as other field types like keyword, boolean, dates, and numeric types.

            Match can use <<esql-function-named-params,function named parameters>> to specify additional options for the match query.
            All <<match-field-params,match query parameters>> are supported.

            For a simplified syntax, you can use the <<esql-search-operators,match operator>> `:` operator instead of `MATCH`.

            `MATCH` returns true if the provided query matches the row.""",
        examples = {
            @Example(file = "match-function", tag = "match-with-field"),
            @Example(file = "match-function", tag = "match-with-named-function-params") }
    )
    public Match(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Field that the query will target."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Value to find in the provided field."
        ) Expression matchQuery,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "analyzer",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Analyzer used to convert the text in the query value into token."
                ),
                @MapParam.MapParamEntry(
                    name = "auto_generate_synonyms_phrase_query",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, match phrase queries are automatically created for multi-term synonyms."
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
                    name = "fuzzy_transpositions",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba)."
                ),
                @MapParam.MapParamEntry(
                    name = "fuzzy_rewrite",
                    type = "keyword",
                    valueHint = {
                        "constant_score_blended",
                        "constant_score",
                        "constant_score_boolean",
                        "top_terms_blended_freqs_N",
                        "top_terms_boost_N",
                        "top_terms_N" },
                    description = "Method used to rewrite the query. See the rewrite parameter for valid values and more information."
                ),
                @MapParam.MapParamEntry(
                    name = "lenient",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If false, format-based errors, such as providing a text query value for a numeric field, are returned."
                ),
                @MapParam.MapParamEntry(
                    name = "max_expansions",
                    type = "integer",
                    valueHint = { "50" },
                    description = "Maximum number of terms to which the query will expand."
                ),
                @MapParam.MapParamEntry(
                    name = "minimum_should_match",
                    type = "integer",
                    valueHint = { "2" },
                    description = "Minimum number of clauses that must match for a document to be returned."
                ),
                @MapParam.MapParamEntry(
                    name = "operator",
                    type = "keyword",
                    valueHint = { "AND", "OR" },
                    description = "Boolean logic used to interpret text in the query value."
                ),
                @MapParam.MapParamEntry(
                    name = "prefix_length",
                    type = "integer",
                    valueHint = { "1" },
                    description = "Number of beginning characters left unchanged for fuzzy matching."
                ),
                @MapParam.MapParamEntry(
                    name = "zero_terms_query",
                    type = "keyword",
                    valueHint = { "none", "all" },
                    description = "Number of beginning characters left unchanged for fuzzy matching."
                ) },
            description = "Match additional options as <<esql-function-named-params,function named parameters>>."
                + " See <<query-dsl-match-query,match query>> for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, field, matchQuery, options, null);
    }

    public Match(Source source, Expression field, Expression matchQuery, Expression options, QueryBuilder queryBuilder) {
        super(source, matchQuery, options == null ? List.of(field, matchQuery) : List.of(field, matchQuery, options), queryBuilder);
        this.field = field;
        this.options = options;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        }
        return new Match(source, field, query, null, queryBuilder);
    }

    // This is not meant to be overriden by MatchOperator - MatchOperator should be serialized to Match
    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(queryBuilder());
        }
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveField().and(resolveQuery()).and(resolveOptions()).and(checkParamCompatibility());
    }

    private TypeResolution resolveField() {
        return isNotNull(field, sourceText(), FIRST).and(
            isType(
                field,
                FIELD_DATA_TYPES::contains,
                sourceText(),
                FIRST,
                "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
            )
        );
    }

    private TypeResolution resolveQuery() {
        return isType(
            query(),
            QUERY_DATA_TYPES::contains,
            sourceText(),
            SECOND,
            "keyword, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
        ).and(isNotNullAndFoldable(query(), sourceText(), SECOND));
    }

    private TypeResolution checkParamCompatibility() {
        DataType fieldType = field().dataType();
        DataType queryType = query().dataType();

        // Field and query types should match. If the query is a string, then it can match any field type.
        if ((fieldType == queryType) || (queryType == KEYWORD)) {
            return TypeResolution.TYPE_RESOLVED;
        }

        if (fieldType.isNumeric() && queryType.isNumeric()) {
            // When doing an unsigned long query, field must be an unsigned long
            if ((queryType == UNSIGNED_LONG && fieldType != UNSIGNED_LONG) == false) {
                return TypeResolution.TYPE_RESOLVED;
            }
        }

        return new TypeResolution(formatIncompatibleTypesMessage(fieldType, queryType, sourceText()));
    }

    private TypeResolution resolveOptions() {
        if (options() != null) {
            TypeResolution resolution = isNotNull(options(), sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }
            // MapExpression does not have a DataType associated with it
            resolution = isMapExpression(options(), sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }

            try {
                matchQueryOptions();
            } catch (InvalidArgumentException e) {
                return new TypeResolution(e.getMessage());
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    private Map<String, Object> matchQueryOptions() throws InvalidArgumentException {

        if (options() == null) {
            return Map.of(LENIENT_FIELD.getPreferredName(), true);
        }

        Map<String, Object> matchOptions = new HashMap<>();
        // Match is lenient by default to avoid failing on incompatible types
        matchOptions.put(LENIENT_FIELD.getPreferredName(), true);

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

    public Expression field() {
        return field;
    }

    public Expression options() {
        return options;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field(), query(), options(), queryBuilder());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Match(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            queryBuilder()
        );
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Match(source(), field, query(), options(), queryBuilder);
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        Expression fieldExpression = field();
        // Field may be converted to other data type (field_name :: data_type), so we need to check the original field
        if (fieldExpression instanceof AbstractConvertFunction convertFunction) {
            fieldExpression = convertFunction.field();
        }
        if (fieldExpression instanceof FieldAttribute == false) {
            failures.add(
                Failure.fail(
                    field,
                    "[{}] {} cannot operate on [{}], which is not a field from an index mapping",
                    functionName(),
                    functionType(),
                    field.sourceText()
                )
            );
        }
    }

    @Override
    public Object queryAsObject() {
        Object queryAsObject = query().fold(FoldContext.small() /* TODO remove me */);

        // Convert BytesRef to string for string-based values
        if (queryAsObject instanceof BytesRef bytesRef) {
            return switch (query().dataType()) {
                case IP -> EsqlDataTypeConverter.ipToString(bytesRef);
                case VERSION -> EsqlDataTypeConverter.versionToString(bytesRef);
                default -> bytesRef.utf8ToString();
            };
        }

        // Converts specific types to the correct type for the query
        if (query().dataType() == DataType.UNSIGNED_LONG) {
            return NumericUtils.unsignedLongAsBigInteger((Long) queryAsObject);
        } else if (query().dataType() == DataType.DATETIME && queryAsObject instanceof Long) {
            // When casting to date and datetime, we get a long back. But Match query needs a date string
            return EsqlDataTypeConverter.dateTimeToString((Long) queryAsObject);
        } else if (query().dataType() == DATE_NANOS && queryAsObject instanceof Long) {
            return EsqlDataTypeConverter.nanoTimeToString((Long) queryAsObject);
        }

        return queryAsObject;
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        Expression fieldExpression = field;
        // Field may be converted to other data type (field_name :: data_type), so we need to check the original field
        if (fieldExpression instanceof AbstractConvertFunction convertFunction) {
            fieldExpression = convertFunction.field();
        }
        if (fieldExpression instanceof FieldAttribute fieldAttribute) {
            String fieldName = fieldAttribute.name();
            if (fieldAttribute.field() instanceof MultiTypeEsField multiTypeEsField) {
                // If we have multiple field types, we allow the query to be done, but getting the underlying field name
                fieldName = multiTypeEsField.getName();
            }
            // Make query lenient so mixed field types can be queried when a field type is incompatible with the value provided
            return new MatchQuery(source(), fieldName, queryAsObject(), matchQueryOptions());
        }

        throw new IllegalArgumentException("Match must have a field attribute as the first argument");
    }

    @Override
    public boolean equals(Object o) {
        // Match does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing two Match functions
        if (o == null || getClass() != o.getClass()) return false;
        Match match = (Match) o;
        return Objects.equals(field(), match.field())
            && Objects.equals(query(), match.query())
            && Objects.equals(queryBuilder(), match.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), query(), queryBuilder());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.MatchQueryBuilder.ZERO_TERMS_QUERY_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZINESS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.PREFIX_LENGTH_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.SLOP_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.TIE_BREAKER_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.TYPE_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
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
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.formatIncompatibleTypesMessage;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchQuery} .
 */
public class Match extends FullTextFunction implements OptionalArgument, PostAnalysisPlanVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);
    public static final Set<DataType> FIELD_DATA_TYPES = Set.of(
        KEYWORD,
        TEXT,
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
    public static final Map<String, DataType> OPTIONS = Map.ofEntries(
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(SLOP_FIELD.getPreferredName(), INTEGER),
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), BOOLEAN),
        entry(FUZZINESS_FIELD.getPreferredName(), KEYWORD),
        entry(FUZZY_REWRITE_FIELD.getPreferredName(), KEYWORD),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), BOOLEAN),
        entry(LENIENT_FIELD.getPreferredName(), BOOLEAN),
        entry(MAX_EXPANSIONS_FIELD.getPreferredName(), INTEGER),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), KEYWORD),
        entry(OPERATOR_FIELD.getPreferredName(), KEYWORD),
        entry(PREFIX_LENGTH_FIELD.getPreferredName(), INTEGER),
        entry(TIE_BREAKER_FIELD.getPreferredName(), FLOAT),
        entry(TYPE_FIELD.getPreferredName(), KEYWORD),
        entry(ZERO_TERMS_QUERY_FIELD.getPreferredName(), KEYWORD)
    );

    private static final Set<String> MULTIMATCH_SPECIFIC_OPTIONS = Set.of(
        SLOP_FIELD.getPreferredName(),
        TIE_BREAKER_FIELD.getPreferredName(),
        TYPE_FIELD.getPreferredName()
    );

    // TODO: update descriptions and comments.
    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = """
            Use `MATCH` to perform a <<query-dsl-match-query,match query>> on the specified field.
            Using `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.

            Match can be used on fields from the text family like <<text, text>> and <<semantic-text, semantic_text>>,
            as well as other field types like keyword, boolean, dates, and numeric types.
            When Match is used on a <<semantic-text, semantic_text>> field, it will perform a semantic query on the field.

            Match can use <<esql-function-named-params,function named parameters>> to specify additional options for the match query.
            All <<match-field-params,match query parameters>> are supported.

            For a simplified syntax, you can use the <<esql-match-operator,match operator>> `:` operator instead of `MATCH`.

            `MATCH` returns true if the provided query matches the row.""",
        examples = {
            @Example(file = "multi-match-function", tag = "multi-match-with-field"),
            @Example(file = "multi-match-function", tag = "multi-match-with-named-function-params") },
        appliesTo = {
            @FunctionAppliesTo(
                lifeCycle = FunctionAppliesToLifecycle.COMING,
                version = "9.1.0",
                description = "Support for optional named parameters is only available from 9.1.0"
            ) }
    )
    public Match(
        Source source,
        @Param(
            name = "fields",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "text", "unsigned_long", "version" },
            description = "Fields to use for matching"
        ) List<Expression> fields,
        @Param(
            name = "query",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Value to find in the provided fields."
        ) Expression query,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "analyzer",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Analyzer used to convert the text in the query value into token. Defaults to the index-time analyzer"
                        + " mapped for the field. If no analyzer is mapped, the index’s default analyzer is used."
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
                    description = "Floating point number used to decrease or increase the relevance scores of the query. Defaults to 1.0."
                ),
                @MapParam.MapParamEntry(
                    name = "fuzzy_transpositions",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). "
                        + "Defaults to true."
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
                    description = "Method used to rewrite the query. See the rewrite parameter for valid values and more information. "
                        + "If the fuzziness parameter is not 0, the match query uses a fuzzy_rewrite method of "
                        + "top_terms_blended_freqs_${max_expansions} by default."
                ),
                @MapParam.MapParamEntry(
                    name = "lenient",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If false, format-based errors, such as providing a text query value for a numeric field, are returned. "
                        + "Defaults to true."
                ),
                @MapParam.MapParamEntry(
                    name = "max_expansions",
                    type = "integer",
                    valueHint = { "50" },
                    description = "Maximum number of terms to which the query will expand. Defaults to 50."
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
                    description = "Boolean logic used to interpret text in the query value. Defaults to OR."
                ),
                @MapParam.MapParamEntry(
                    name = "prefix_length",
                    type = "integer",
                    valueHint = { "1" },
                    description = "Number of beginning characters left unchanged for fuzzy matching. Defaults to 0."
                ),
                @MapParam.MapParamEntry(
                    name = "tie_breaker",
                    type = "float",
                    valueHint = { "0" },
                    description = "Controls how score is blended together between field groups. Defaults to 0 (best score from each group)."
                ),
                @MapParam.MapParamEntry(
                    name = "type",
                    type = "object",
                    valueHint = { "'best_fields'" },
                    description = "Controls the way multi_match is executed internally. Can be one of `best_fields`, `most_fields`, "
                        + "`cross_fields`, `phrase`, `phrase_prefix` or `bool_prefix`. Defaults to 'best_fields'. "
                        + "See <<multi-match-types,multi_match types>>."
                ),
                @MapParam.MapParamEntry(
                    name = "zero_terms_query",
                    type = "keyword",
                    valueHint = { "none", "all" },
                    description = "Indicates whether all documents or none are returned if the analyzer removes all tokens, such as "
                        + "when using a stop filter. Defaults to none."
                ) },
            description = "(Optional) Additional options for MultiMatch, "
                + "passed as <<esql-function-named-params,function named parameters>>.\"\n"
                + " See <<query-dsl-multi-match-query,multi-match query>> for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, fields, query, options, null);
    }

    // Due to current limitations, the options field may contain a field, in which case treat it as a field, and use "null" for actual
    // options. We also remember the originally supplied arguments in order to make tests happy.
    private final transient List<Expression> fields;
    private final transient List<Expression> fieldsOriginal;
    private final transient Expression options;
    private final transient Expression optionsOriginal;

    private static List<Expression> initChildren(Expression query, List<Expression> fields, Expression options) {
        Stream<Expression> fieldsAndQuery = Stream.concat(Stream.of(query), fields.stream());
        return (options == null ? fieldsAndQuery : Stream.concat(fieldsAndQuery, Stream.of(options))).toList();
    }

    protected Match(Source source, List<Expression> fields, Expression query, Expression options, QueryBuilder queryBuilder) {
        super(source, query, initChildren(query, fields, options), queryBuilder);
        this.fieldsOriginal = fields;
        this.optionsOriginal = options;

        if (options == null || options instanceof MapExpression) {
            this.fields = fields;
            this.options = options;
        } else {
            this.fields = Stream.concat(fields.stream(), Stream.of(options)).toList();
            this.options = null;
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readNamedWriteable(Expression.class);
        List<Expression> fields = in.readNamedWriteableCollectionAsList(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        return new Match(source, fields, query, null, queryBuilder);
    }

    // This is not meant to be overriden by MatchOperator - MatchOperator should be serialized to Match
    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query());
        out.writeNamedWriteableCollection(fields);
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveFields().and(resolveQuery()).and(resolveOptions(options(), THIRD)).and(checkParamCompatibility());
    }

    private TypeResolution resolveFields() {
        return fields.stream()
            .map(
                (Expression field) -> isNotNull(field, sourceText(), FIRST).and(
                    isType(
                        field,
                        FIELD_DATA_TYPES::contains,
                        sourceText(),
                        FIRST,
                        "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
                    )
                )
            )
            .reduce(TypeResolution::and)
            .orElse(null);
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
        DataType queryType = query().dataType();

        return fields.stream().map((Expression field) -> {
            DataType fieldType = field.dataType();

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
        }).reduce(TypeResolution::and).orElse(null);
    }

    @Override
    public String functionName() {
        return ENTRY.name;
    }

    @Override
    protected Map<String, Object> resolvedOptions() {
        return matchQueryOptions();
    }

    private Map<String, Object> matchQueryOptions() throws InvalidArgumentException {
        Map<String, Object> options = new HashMap<>();
        options.put(MatchQueryBuilder.LENIENT_FIELD.getPreferredName(), true);
        if (options() == null) {
            return options;
        }

        Match.populateOptionsMap((MapExpression) options(), options, THIRD, sourceText(), OPTIONS);
        return options;
    }

    public List<Expression> fields() {
        return fields;
    }

    public Expression options() {
        return options;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        // Specifically create new instance with original arguments.
        return NodeInfo.create(this, Match::new, fieldsOriginal, query(), optionsOriginal);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        // "query" is the first child.
        if (newChildren.getLast() instanceof MapExpression || newChildren.size() == children().size()) {
            // if the last child is a MapExpression, it is the options map
            return new Match(
                source(),
                newChildren.subList(1, newChildren.size() - 1),
                newChildren.getFirst(),
                newChildren.getLast(),
                queryBuilder()
            );
        }

        return new Match(source(), newChildren.subList(1, newChildren.size()), newChildren.getFirst(), null, queryBuilder());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        // Specifically create new instance with original arguments.
        return new Match(source(), fieldsOriginal, query(), optionsOriginal, queryBuilder);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (plan, failures) -> {
            super.postAnalysisPlanVerification().accept(plan, failures);
            plan.forEachExpression(Match.class, match -> {
                for (Expression field : fields) {
                    if (fieldAsFieldAttribute(field) == null) {
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
            });
        };
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
        Map<String, Float> fieldsWithBoost = new HashMap<>();
        for (Expression field : fields) {
            var fieldAttribute = fieldAsFieldAttribute(field);
            Check.notNull(fieldAttribute, "Match must have field attributes as arguments #1 to #N-1.");
            String fieldName = getNameFromFieldAttribute(fieldAttribute);
            fieldsWithBoost.put(fieldName, 1.0f);
        }

        var options = matchQueryOptions();
        if (fieldsWithBoost.size() != 1 || options.keySet().stream().anyMatch(MULTIMATCH_SPECIFIC_OPTIONS::contains)) {
            // For 0 or 2+ fields, or with multimatch-specific options, translate to multi_match.
            return new MultiMatchQuery(source(), Objects.toString(queryAsObject()), fieldsWithBoost, options);
        } else {
            // Translate to Match when having exactly one field.
            return new MatchQuery(source(), fieldsWithBoost.keySet().stream().findFirst().get(), queryAsObject(), options);
        }
    }

    @Override
    public boolean equals(Object o) {
        // Match does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing two Match functions
        if (o == null || getClass() != o.getClass()) return false;
        Match match = (Match) o;
        return Objects.equals(fields(), match.fields())
            && Objects.equals(query(), match.query())
            && Objects.equals(queryBuilder(), match.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields(), query(), queryBuilder());
    }
}

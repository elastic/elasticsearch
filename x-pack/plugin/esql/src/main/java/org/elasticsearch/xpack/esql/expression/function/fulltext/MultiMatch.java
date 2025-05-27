/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

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
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
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
import org.elasticsearch.xpack.esql.querydsl.query.MultiMatchQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Map.entry;
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
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MultiMatchQuery} .
 */
public class MultiMatch extends FullTextFunction implements OptionalArgument, PostAnalysisPlanVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MultiMatch",
        MultiMatch::readFrom
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

    public static final Map<String, DataType> OPTIONS = Map.ofEntries(
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(SLOP_FIELD.getPreferredName(), INTEGER),
        // TODO: add "zero_terms_query"
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
        entry(TYPE_FIELD.getPreferredName(), KEYWORD)
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = """
            Use `MULTI_MATCH` to perform a <<query-dsl-multi-match-query,multi-match query>> on the specified field.
            The multi_match query builds on the match query to allow multi-field queries.""",
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
    public MultiMatch(
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
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query."
                ),
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
                ), },
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

    private MultiMatch(Source source, List<Expression> fields, Expression query, Expression options, QueryBuilder queryBuilder) {
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

    private static MultiMatch readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readNamedWriteable(Expression.class);
        List<Expression> fields = in.readNamedWriteableCollectionAsList(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        return new MultiMatch(source, fields, query, null, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query());
        out.writeNamedWriteableCollection(fields);
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        // "query" is the first child.
        if (newChildren.getLast() instanceof MapExpression || newChildren.size() == children().size()) {
            // if the last child is a MapExpression, it is the options map
            return new MultiMatch(
                source(),
                newChildren.subList(1, newChildren.size() - 1),
                newChildren.getFirst(),
                newChildren.getLast(),
                queryBuilder()
            );
        }

        return new MultiMatch(source(), newChildren.subList(1, newChildren.size()), newChildren.getFirst(), null, queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        // Specifically create new instance with original arguments.
        return NodeInfo.create(this, MultiMatch::new, fieldsOriginal, query(), optionsOriginal);
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        Map<String, Float> fieldsWithBoost = new HashMap<>();
        for (Expression field : fields) {
            var fieldAttribute = Match.fieldAsFieldAttribute(field);
            Check.notNull(fieldAttribute, "MultiMatch must have field attributes as arguments #2 to #N-1.");
            String fieldName = Match.getNameFromFieldAttribute(fieldAttribute);
            fieldsWithBoost.put(fieldName, 1.0f);
        }
        return new MultiMatchQuery(source(), Objects.toString(queryAsObject()), fieldsWithBoost, getOptions());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        // Specifically create new instance with original arguments.
        return new MultiMatch(source(), fieldsOriginal, query(), optionsOriginal, queryBuilder);
    }

    public List<Expression> fields() {
        return fields;
    }

    public Expression options() {
        return options;
    }

    private Map<String, Object> getOptions() throws InvalidArgumentException {
        Map<String, Object> options = new HashMap<>();
        options.put(MatchQueryBuilder.LENIENT_FIELD.getPreferredName(), true);
        if (options() == null) {
            return options;
        }

        Match.populateOptionsMap((MapExpression) options(), options, THIRD, sourceText(), OPTIONS);
        return options;
    }

    @Override
    public String functionName() {
        return ENTRY.name;
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
                getOptions();
            } catch (InvalidArgumentException e) {
                return new TypeResolution(e.getMessage());
            }
        }
        return TypeResolution.TYPE_RESOLVED;
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

    @Override
    protected TypeResolution resolveParams() {
        return resolveFields().and(resolveQuery()).and(resolveOptions());
    }

    @Override
    public boolean equals(Object o) {
        // MultiMatch does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing two MultiMatch functions
        if (o == null || getClass() != o.getClass()) return false;
        MultiMatch mm = (MultiMatch) o;
        return Objects.equals(fields(), mm.fields())
            && Objects.equals(query(), mm.query())
            && Objects.equals(queryBuilder(), mm.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields(), query(), queryBuilder());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (plan, failures) -> {
            super.postAnalysisPlanVerification().accept(plan, failures);
            plan.forEachExpression(MultiMatch.class, mm -> {
                for (Expression field : fields) {
                    if (Match.fieldAsFieldAttribute(field) == null) {
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
}

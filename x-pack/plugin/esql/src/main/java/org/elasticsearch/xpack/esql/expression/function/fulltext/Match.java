/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
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
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.formatIncompatibleTypesMessage;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchQuery} .
 */
public class Match extends SingleFieldFullTextFunction implements OptionalArgument, ConfigurationFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Match.class)
        .ternaryConfig(Match::new)
        .capabilities("runtime_filter")
        .name("match");
    public static final Set<DataType> FIELD_DATA_TYPES = Set.of(
        NULL,
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
    private static final String CONTENT_FIELD = "content_field";

    private final Configuration configuration;

    @FunctionInfo(
        returnType = "boolean",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1.0") },
        briefSummary = "Performs a match query on the specified field or expression.",
        description = """
            Use `MATCH` to perform a <<query-dsl-match-query,match query>> on the specified field or expression.
            Using `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.""",
        detailedDescription = """
            Match can be used on fields from the text family like <<text, text>> and <<semantic-text, semantic_text>>,
            as well as other field types like keyword, boolean, dates, and numeric types.
            When Match is used on a <<semantic-text, semantic_text>> field, it will perform a semantic query on the field.

            {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`
            `MATCH` can also search expressions that are not backed by an index, such as
            computed columns produced by `EVAL`, `STATS`, or other commands.
            When the target is not an indexed field, the search evaluates by scanning
            values row by row, which may be slower on large datasets.
            When searching expressions, <<esql-function-named-params,function named parameters>>
            (match query options) are not supported.
            Additionally, `MATCH` on an expression does not contribute to the relevance score
            when using `METADATA _score`.

            Match can use <<esql-function-named-params,function named parameters>> to specify additional options
            for the match query.
            All <<match-field-params,match query parameters>> are supported.

            For a simplified syntax, you can use the <<esql-match-operator,match operator>> `:` operator instead of `MATCH`.

            `MATCH` returns true if the provided query matches the row.

            :::{tip}
            Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
            :::
            """,
        examples = {
            @Example(file = "match-function", tag = "match-with-field"),
            @Example(file = "match-function", tag = "match-with-named-function-params") }
    )
    public Match(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Field or expression that the query will target."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Value to find in the provided field or expression."
        ) Expression matchQuery,
        @MapParam(
            name = "options",
            description = "(Optional) Match additional options as <<esql-function-named-params,function named parameters>>.",
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
                        + "Defaults to false."
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
                    name = "zero_terms_query",
                    type = "keyword",
                    valueHint = { "none", "all" },
                    description = "Indicates whether all documents or none are returned if the analyzer removes all tokens, such as "
                        + "when using a stop filter. Defaults to none."
                ) },
            optional = true
        ) Expression options,
        Configuration configuration
    ) {
        this(source, field, matchQuery, options, null, configuration);
    }

    public Match(
        Source source,
        Expression field,
        Expression matchQuery,
        Expression options,
        QueryBuilder queryBuilder,
        Configuration configuration
    ) {
        super(
            source,
            field,
            matchQuery,
            options,
            options == null ? List.of(field, matchQuery) : List.of(field, matchQuery, options),
            queryBuilder
        );
        this.configuration = configuration;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        Configuration configuration = ((PlanStreamInput) in).configuration();
        return new Match(source, field, query, null, queryBuilder, configuration);
    }

    // This is not meant to be overriden by MatchOperator - MatchOperator should be serialized to Match
    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    protected TypeResolution resolveParams() {
        return super.resolveParams().and(checkParamCompatibility());
    }

    private TypeResolution checkParamCompatibility() {
        DataType fieldType = field().dataType();
        DataType queryType = query().dataType();

        // Field and query types should match. If the query is a string, then it can match any field type.
        // If the field is null, it will be folded to null.
        if ((fieldType == queryType) || (queryType == KEYWORD) || fieldType == NULL) {
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

    @Override
    protected Set<DataType> getFieldDataTypes() {
        return FIELD_DATA_TYPES;
    }

    @Override
    protected Set<DataType> getQueryDataTypes() {
        return QUERY_DATA_TYPES;
    }

    @Override
    protected Map<String, DataType> getAllowedOptions() {
        return ALLOWED_OPTIONS;
    }

    protected Configuration configuration() {
        return configuration;
    }

    private Map<String, Object> matchQueryOptions() throws InvalidArgumentException {
        if (options() == null) {
            return Map.of(LENIENT_FIELD.getPreferredName(), true);
        }

        Map<String, Object> matchOptions = new HashMap<>();
        // Match is lenient by default to avoid failing on incompatible types
        matchOptions.put(LENIENT_FIELD.getPreferredName(), true);

        Options.populateMap((MapExpression) options(), matchOptions, source(), SECOND, ALLOWED_OPTIONS);
        return matchOptions;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field(), query(), options(), queryBuilder(), configuration);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Match(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            queryBuilder(),
            configuration
        );
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Match(source(), field, query(), options(), queryBuilder, configuration);
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fieldAttribute = fieldAsFieldAttribute();
        Check.notNull(fieldAttribute, "Match must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        // Make query lenient so mixed field types can be queried when a field type is incompatible with the value provided
        return new MatchQuery(source(), fieldName, queryAsObject(), matchQueryOptions());
    }

    @Override
    protected boolean isRuntimeSearch() {
        if (false == configuration.pragmas().runtimeLexicalSearch()) {
            // Runtime search is disabled.
            return false;
        }
        if (fieldAsFieldAttribute() == null) {
            // This *isn't* a field in the index OR a pushed block loader
            return true;
        }
        if (fieldAsFieldAttribute().field() instanceof FunctionEsField functionEsField) {
            // This is a pushed block loader.
            // We can only support FIELD_EXTRACT(flattened, "constant"), here named EXTRACT_FLATTENED_SUBFIELD
            return functionEsField.functionConfig().function() == BlockLoaderFunctionConfig.Function.EXTRACT_FLATTENED_SUBFIELD;
        }
        return false;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (fieldAsFieldAttribute() == null) {
            return Translatable.NO;
        }
        return super.translatable(pushdownPredicates);
    }

    @Override
    protected void fieldVerifier(LogicalPlan plan, FullTextFunction function, Expression field, Failures failures) {
        super.fieldVerifier(plan, function, field, failures);
        if (isRuntimeSearch() == false) {
            return;
        }
        if (options() != null) {
            failures.add(
                Failure.fail(
                    field,
                    "Options are not supported for [MATCH] function call on non-index-mapped field [" + field.sourceText() + "]"
                )
            );
        }
        // The query value can only be converted to the field's runtime type once it has been folded down to a
        // Literal; if it hasn't yet (e.g. pre-optimization), this check is skipped here and retried once
        // postOptimizationPlanVerification runs.
        if (query() instanceof Literal) {
            try {
                verifyRuntimeQueryValue();
            } catch (InvalidArgumentException | IllegalArgumentException e) {
                failures.add(
                    Failure.fail(
                        query(),
                        "[MATCH] query value [{}] does not match the type ([{}]) of non-index-mapped field [{}]",
                        query().sourceText(),
                        field.dataType().typeName(),
                        field.sourceText()
                    )
                );
            }
        }
    }

    /**
     * Verifies that the (foldable) query value can be converted to the runtime field's type, throwing if not.
     * Only used for {@link #isRuntimeSearch()}. The converted value itself is discarded here; it's recomputed
     * (cheaply, since the query value is a constant) by {@link #queryAsRuntimeSearchValue} when building the evaluator.
     */
    private void verifyRuntimeQueryValue() {
        if (field.dataType() == TEXT || field.dataType() == NULL) {
            return;
        }
        queryAsRuntimeSearchValue(field.dataType(), query().dataType(), Foldables.queryAsObject(query(), sourceText()));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (false == isRuntimeSearch()) {
            // we push down match to the shards as a Lucene query.
            return super.toEvaluator(toEvaluator);
        }

        // Text fields keep analyzer-based matching; every other type compares the query value against the field block directly.
        if (field.dataType() == TEXT) {
            return new MatchTextEvaluator.Factory(source(), toEvaluator.apply(field()), queryAsObject().toString(), new StandardAnalyzer());
        }

        Object queryValue = queryAsRuntimeSearchValue(field.dataType(), query().dataType(), Foldables.queryAsObject(query(), sourceText()));
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case BYTES_REF -> new MatchBytesRefEvaluator.Factory(
                source(),
                toEvaluator.apply(field()),
                (BytesRef) queryValue,
                context -> new BytesRef()
            );
            case BOOLEAN -> new MatchBooleanEvaluator.Factory(source(), toEvaluator.apply(field()), (Boolean) queryValue);
            case DOUBLE -> new MatchDoubleEvaluator.Factory(source(), toEvaluator.apply(field()), (Double) queryValue);
            case LONG -> new MatchLongEvaluator.Factory(source(), toEvaluator.apply(field()), (Long) queryValue);
            case INT -> new MatchIntegerEvaluator.Factory(source(), toEvaluator.apply(field()), (Integer) queryValue);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    /**
     * Converts the folded query value into the typed value used by the runtime-search evaluators, in the same
     * representation that the field's block stores (encoded {@code ip}/{@code version}/{@code unsigned_long},
     * epoch millis for {@code datetime}, epoch nanos for {@code date_nanos}, ...). String queries are parsed with
     * the strict {@link EsqlDataTypeConverter} converters; numeric queries are coerced to the field's element type.
     * <p>
     * Not used for {@link DataType#TEXT}, which is matched through an analyzer rather than by value equality.
     * Kept package-private and static so the conversion matrix can be exercised directly in unit tests.
     *
     * @param fieldType  the data type of the field being matched
     * @param queryType  the data type of the query expression
     * @param queryValue the folded query value, as returned by {@link Foldables#queryAsObject}
     */
    static Object queryAsRuntimeSearchValue(DataType fieldType, DataType queryType, Object queryValue) {
        String queryString = queryValue instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : null;
        return switch (PlannerUtils.toElementType(fieldType)) {
            case BYTES_REF -> {
                assert queryValue instanceof BytesRef;
                if (fieldType == IP && DataType.isString(queryType)) {
                    yield EsqlDataTypeConverter.stringToIP(queryString);
                }
                if (fieldType == VERSION && DataType.isString(queryType)) {
                    yield EsqlDataTypeConverter.stringToVersion(queryString);
                }
                yield queryValue;
            }
            case BOOLEAN -> queryString != null ? EsqlDataTypeConverter.stringToBoolean(queryString) : (Boolean) queryValue;
            case DOUBLE -> queryString != null ? EsqlDataTypeConverter.stringToDouble(queryString) : ((Number) queryValue).doubleValue();
            case LONG -> {
                Object value;
                if (fieldType == UNSIGNED_LONG) {
                    if (queryString != null) {
                        value = EsqlDataTypeConverter.stringToUnsignedLong(queryString);
                    } else if (queryType == UNSIGNED_LONG) {
                        value = ((Number) queryValue).longValue();
                    } else {
                        value = EsqlDataTypeConverter.longToUnsignedLong(((Number) queryValue).longValue(), true);
                    }
                } else if (fieldType == DATETIME) {
                    value = queryString != null ? EsqlDataTypeConverter.dateTimeToLong(queryString) : ((Number) queryValue).longValue();
                } else if (fieldType == DATE_NANOS) {
                    value = queryString != null ? EsqlDataTypeConverter.dateNanosToLong(queryString) : ((Number) queryValue).longValue();
                } else if (fieldType.isNumeric()) {
                    value = queryString != null ? EsqlDataTypeConverter.stringToLong(queryString) : ((Number) queryValue).longValue();
                } else {
                    value = queryValue;
                }
                if (false == value instanceof Long) {
                    throw EsqlIllegalArgumentException.illegalDataType(queryType);
                }
                yield value;
            }
            case INT -> queryString != null ? EsqlDataTypeConverter.stringToInt(queryString) : ((Number) queryValue).intValue();
            default -> throw EsqlIllegalArgumentException.illegalDataType(fieldType);
        };
    }

    @Evaluator(extraName = "Text", warnExceptions = { IOException.class }, allNullsIsNull = false)
    static boolean processText(@Position int position, BytesRefBlock fieldBlock, @Fixed String queryString, @Fixed Analyzer analyzer)
        throws IOException {
        if (fieldBlock == null) {
            return false;
        }

        final var valueCount = fieldBlock.getValueCount(position);
        final var startIndex = fieldBlock.getFirstValueIndex(position);
        var value = new BytesRef();

        for (int valueIndex = startIndex; valueIndex < startIndex + valueCount; valueIndex++) {
            // TODO: See if we really need a memory index, we could match the terms directly from the analyzer token stream.
            MemoryIndex index = new MemoryIndex();
            value = fieldBlock.getBytesRef(valueIndex, value);
            index.addField(CONTENT_FIELD, value.utf8ToString(), analyzer);
            IndexSearcher searcher = index.createSearcher();

            org.apache.lucene.util.QueryBuilder queryBuilder = new org.apache.lucene.util.QueryBuilder(analyzer);
            // TODO: Use the operator specified in the query options instead of `BooleanClause.Occur.SHOULD`.
            org.apache.lucene.search.Query query = queryBuilder.createBooleanQuery(CONTENT_FIELD, queryString, BooleanClause.Occur.SHOULD);

            TopDocs topDocs = searcher.search(query, 1);
            if (topDocs.scoreDocs.length > 0) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean processBytesRef(
        @Position int position,
        BytesRefBlock fieldBlock,
        @Fixed BytesRef queryStringBytesRef,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        if (fieldBlock == null) {
            return false;
        }

        return fieldBlock.hasValue(position, queryStringBytesRef, scratch);
    }

    @Evaluator(extraName = "Boolean", allNullsIsNull = false)
    static boolean processBoolean(@Position int position, BooleanBlock fieldBlock, @Fixed Boolean query) {
        if (fieldBlock == null) {
            return false;
        }
        return fieldBlock.hasValue(position, query);
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean processDouble(@Position int position, DoubleBlock fieldBlock, @Fixed Double query) {
        if (fieldBlock == null) {
            return false;
        }

        return fieldBlock.hasValue(position, query);
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean processLong(@Position int position, LongBlock fieldBlock, @Fixed Long query) {
        if (fieldBlock == null) {
            return false;
        }
        return fieldBlock.hasValue(position, query);
    }

    @Evaluator(extraName = "Integer", allNullsIsNull = false)
    static boolean processInteger(@Position int position, IntBlock fieldBlock, @Fixed Integer query) {
        if (fieldBlock == null) {
            return false;
        }
        return fieldBlock.hasValue(position, query);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) {
            return false;
        }

        Match other = (Match) o;
        return configuration.equals(other.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), configuration);
    }

    @Override
    public boolean contributesToScore() {
        if (isRuntimeSearch()) {
            return false;
        }

        return super.contributesToScore();
    }
}

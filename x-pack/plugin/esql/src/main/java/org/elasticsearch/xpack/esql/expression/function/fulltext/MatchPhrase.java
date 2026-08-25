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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.Foldables;
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
import org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.index.query.MatchPhraseQueryBuilder.SLOP_FIELD;
import static org.elasticsearch.index.query.MatchPhraseQueryBuilder.ZERO_TERMS_QUERY_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchPhraseQuery} .
 */
public class MatchPhrase extends SingleFieldFullTextFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MatchPhrase",
        MatchPhrase::readFrom
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MatchPhrase.class)
        .ternary(MatchPhrase::new)
        .capabilities("runtime_filter", "unmapped_fields_pushdown_fix", "runtime_options", "runtime_analyzer", "runtime_score")
        .name("match_phrase");
    public static final Set<DataType> FIELD_DATA_TYPES = Set.of(KEYWORD, TEXT, NULL);
    public static final Set<DataType> QUERY_DATA_TYPES = Set.of(KEYWORD, TEXT);

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(SLOP_FIELD.getPreferredName(), INTEGER),
        entry(ZERO_TERMS_QUERY_FIELD.getPreferredName(), KEYWORD)
    );

    @FunctionInfo(
        returnType = "boolean",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1.0") },
        briefSummary = "Performs a match_phrase query on the specified field or expression.",
        description = """
            Use `MATCH_PHRASE` to perform a [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) on the
            specified field or expression.
            Using `MATCH_PHRASE` is equivalent to using the `match_phrase` query in the Elasticsearch Query DSL.""",
        detailedDescription = """
            MatchPhrase can be used on <<text, text>> and keyword fields.
            MatchPhrase is not supported for other field types, like <<semantic-text, semantic_text>>, boolean, date, or numeric types.

            MatchPhrase can use <<esql-function-named-params,function named parameters>> to specify additional options for the
            match_phrase query.
            All [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) query parameters are supported.

            `MATCH_PHRASE` returns true if the provided query matches the row.

            **`MATCH_PHRASE` on expressions**

            {applies_to}`stack: preview 9.6` {applies_to}`serverless: preview`
            `MATCH_PHRASE` can also search `text` and `keyword` expressions that are not backed by an index,
            such as computed columns produced by `EVAL`, `STATS`, or other commands.
            When the target is not an indexed field, the search evaluates by scanning
            values row by row, which may be slower on large datasets.
            On a `keyword` expression the whole query string must equal a value exactly, matching
            the term query semantics of `match_phrase` on an indexed keyword field.
            When using `METADATA _score`, `MATCH_PHRASE` on an expression contributes to the relevance
            score: a matching row scores the `boost` option (1.0 by default). Unlike indexed fields,
            expressions are not scored with BM25, as there are no index statistics for an expression.

            When searching `text` expressions, <<esql-function-named-params,function named parameters>>
            (match_phrase query options) are supported. The `analyzer` option must name a registered
            analyzer (prebuilt or plugin-contributed); per-index custom analyzers cannot be used because
            the expression is not backed by an index. Unlike on an indexed field, the analyzer is applied
            to both the query and the expression values; when no analyzer is specified, the `standard`
            analyzer is used. On `keyword` expressions options are not supported.

            :::{tip}
            Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
            :::
            """,
        examples = { @Example(file = "match-phrase-function", tag = "match-phrase-with-field", applies_to = "stack: ga 9.1.0") }
    )
    public MatchPhrase(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "Field or expression that the query will target."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Value to find in the provided field or expression."
        ) Expression matchPhraseQuery,
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
                    name = "slop",
                    type = "integer",
                    valueHint = { "1" },
                    description = "Maximum number of positions allowed between matching tokens. Defaults to 0."
                        + " Transposed terms have a slop of 2."
                ),
                @MapParam.MapParamEntry(
                    name = "zero_terms_query",
                    type = "keyword",
                    valueHint = { "none", "all" },
                    description = "Indicates whether all documents or none are returned if the analyzer removes all tokens, such as "
                        + "when using a stop filter. Defaults to none."
                ),
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query. Defaults to 1.0."
                ) },
            description = "(Optional) MatchPhrase additional options as <<esql-function-named-params,function named parameters>>."
                + " See [`match_phrase`](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, field, matchPhraseQuery, options, null);
    }

    public MatchPhrase(Source source, Expression field, Expression matchPhraseQuery, Expression options, QueryBuilder queryBuilder) {
        super(
            source,
            field,
            matchPhraseQuery,
            options,
            options == null ? List.of(field, matchPhraseQuery) : List.of(field, matchPhraseQuery, options),
            queryBuilder
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String functionName() {
        return ENTRY.name;
    }

    private static MatchPhrase readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        Expression options = in.getTransportVersion().supports(ESQL_OPTIONS_FOR_SEARCH_FUNCTIONS)
            ? in.readOptionalNamedWriteable(Expression.class)
            : null;
        return new MatchPhrase(source, field, query, options, queryBuilder);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());

        if (out.getTransportVersion().supports(ESQL_OPTIONS_FOR_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(options());
        }
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

    private Map<String, Object> matchPhraseQueryOptions() throws InvalidArgumentException {
        if (options() == null) {
            return Map.of();
        }

        Map<String, Object> matchPhraseOptions = new HashMap<>();
        Options.populateMap((MapExpression) options(), matchPhraseOptions, source(), SECOND, ALLOWED_OPTIONS);
        return matchPhraseOptions;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MatchPhrase::new, field(), query(), options(), queryBuilder());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MatchPhrase(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            queryBuilder()
        );
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new MatchPhrase(source(), field, query(), options(), queryBuilder);
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fieldAttribute = fieldAsFieldAttribute();
        Check.notNull(fieldAttribute, "MatchPhrase must have a field attribute as the first argument");
        return matchPhraseQuery(getNameFromFieldAttribute(fieldAttribute));
    }

    /**
     * The same query as {@link #translate}, but targeting {@code fieldName} instead of the mapped index field, so it
     * matches text lexically wherever it runs. HIGHLIGHT uses this to run the query against a per-row MemoryIndex keyed
     * by the ON column names, where the index mapping (e.g. {@code semantic_text} inference) must not participate.
     */
    public QueryBuilder asLexicalQueryBuilder(String fieldName) {
        return matchPhraseQuery(fieldName).toQueryBuilder();
    }

    private MatchPhraseQuery matchPhraseQuery(String fieldName) {
        return new MatchPhraseQuery(source(), fieldName, queryAsObject(), matchPhraseQueryOptions());
    }

    @Override
    public boolean isRuntimeSearch() {
        FieldAttribute fieldAttribute = fieldAsFieldAttribute();
        if (fieldAttribute == null) {
            // This isn't a field in the index, so the expression is evaluated at runtime, row by row.
            return true;
        }
        if (fieldAttribute.isPotentiallyUnmapped()) {
            // A potentially unmapped field cannot be pushed down: the Lucene query would silently miss the rows of the
            // indices where the field is unmapped, so it is matched at runtime instead.
            return true;
        }
        if (fieldAttribute.field() instanceof FunctionEsField functionEsField) {
            // This is a pushed block loader. There is no indexed Lucene field behind it, so the match must run at
            // runtime. We can only support FIELD_EXTRACT(flattened, "constant"), here named EXTRACT_FLATTENED_SUBFIELD.
            return functionEsField.functionConfig().function() == BlockLoaderFunctionConfig.Function.EXTRACT_FLATTENED_SUBFIELD;
        }
        return false;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        FieldAttribute fieldAttribute = fieldAsFieldAttribute();

        if (fieldAttribute == null) {
            return Translatable.NO;
        }

        if (fieldAttribute.isPotentiallyUnmapped()) {
            return Translatable.NO;
        }

        return super.translatable(pushdownPredicates);
    }

    @Override
    protected void fieldVerifier(
        LogicalPlan plan,
        FullTextFunction function,
        Expression field,
        @Nullable AnalysisRegistry analysisRegistry,
        Failures failures
    ) {
        super.fieldVerifier(plan, function, field, analysisRegistry, failures);
        if (isRuntimeSearch() == false) {
            return;
        }
        if (options() != null && field().dataType() == TEXT) {
            verifyRuntimeOptions(function, field, analysisRegistry, failures);
        } else if (options() != null) {
            failures.add(
                Failure.fail(
                    field,
                    "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped, non-TEXT field ["
                        + field.sourceText()
                        + "]"
                )
            );
        }
    }

    /**
     * Validates the options for a runtime-search {@code match_phrase} on a {@code text} field. Checks that the
     * {@code analyzer} option (if present) names a registered analyzer and that the options produce a valid
     * {@code MatchPhraseQueryBuilder}.
     */
    private void verifyRuntimeOptions(
        FullTextFunction function,
        Expression field,
        @Nullable AnalysisRegistry analysisRegistry,
        Failures failures
    ) {
        Map<String, Object> opts = matchPhraseQueryOptions();
        // The registry is only available in the post-analysis pass; analyzer names cannot change during
        // optimization, so the post-optimization pass runs with a null registry and skips this check.
        if (analysisRegistry != null && opts.containsKey(ANALYZER_FIELD.getPreferredName())) {
            try {
                PlannerUtils.resolveAnalyzer(BytesRefs.toString(opts.get(ANALYZER_FIELD.getPreferredName())), analysisRegistry);
            } catch (InvalidArgumentException e) {
                failures.add(Failure.fail(function, "{}", e.getMessage()));
                return;
            }
        }
        if (query() instanceof Literal) {
            // Validate that the options produce a valid MatchPhraseQueryBuilder at plan-verification time rather than at execution time.
            try {
                new MatchPhraseQuery(source(), RuntimeSearch.CONTENT_FIELD, queryAsObject(), opts).toQueryBuilder();
            } catch (IllegalArgumentException e) {
                failures.add(
                    Failure.fail(
                        function,
                        "[MATCH_PHRASE] function failed to build query for non-index-mapped field [{}]: {}",
                        field.sourceText(),
                        e.getMessage()
                    )
                );
            }
        }
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (false == isRuntimeSearch()) {
            // we push down match_phrase to the shards as a Lucene query.
            return super.toEvaluator(toEvaluator);
        }

        if (field.dataType() == TEXT && options() == null) {
            return runtimeTextEvaluator(toEvaluator, RuntimeSearch.PhraseMatcher::new);
        }
        // When options are used, we build a Lucene query
        if (field.dataType() == TEXT) {
            Map<String, Object> opts = matchPhraseQueryOptions();
            return textEvaluatorForQueryWithOptions(
                new MatchPhraseQuery(source(), RuntimeSearch.CONTENT_FIELD, queryAsObject(), opts),
                opts,
                toEvaluator
            );
        }
        // Guard against a field type that resolveField() accepts but this method was not taught to evaluate:
        // falling through to exact matching would silently give it the wrong semantics. NULL fields never get
        // here because the function folds to null.
        if (field.dataType() != KEYWORD) {
            throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        }
        // A pushed-down match_phrase on a keyword field rewrites to a term query, so the runtime path preserves
        // that: exact, unanalyzed equality with the query string. Query types are strings only, so no conversion
        // of the folded value is needed.
        return new RuntimeSearchBytesRefEvaluator.Factory(
            source(),
            toEvaluator.apply(field()),
            (BytesRef) Foldables.queryAsObject(query(), sourceText()),
            context -> new BytesRef()
        );
    }

    @Override
    public boolean contributesToScore() {
        return true;
    }

    /**
     * Scores runtime phrase matches with {@link RuntimeSearch}'s boolean-similarity semantics — there are no corpus
     * statistics to feed BM25 (for now ...) — so a matched phrase scores its boost, 1.0 by default. Keyword exact matches
     * score 1.0.
     */
    @Override
    public ExpressionEvaluator.Factory toScorer(ToScorer toScorer) {
        if (false == isRuntimeSearch()) {
            // Pushed-down match_phrase is scored by running the Lucene query on the shard.
            return super.toScorer(toScorer);
        }

        // With options, score through the same Lucene query the boolean evaluator runs.
        if (field.dataType() == TEXT && options() != null) {
            Map<String, Object> opts = matchPhraseQueryOptions();
            return textScoreEvaluatorForQueryWithOptions(
                new MatchPhraseQuery(source(), RuntimeSearch.CONTENT_FIELD, queryAsObject(), opts),
                opts,
                toScorer.toEvaluator()
            );
        }
        // No options, so both the text phrase matcher and the keyword exact matcher both score 1.0 on hits.
        return new RuntimeSearchScoreFromBooleanEvaluator.Factory(source(), toEvaluator(toScorer.toEvaluator()));
    }
}

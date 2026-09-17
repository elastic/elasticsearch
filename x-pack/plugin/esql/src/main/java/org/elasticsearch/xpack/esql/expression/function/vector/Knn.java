/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
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
import org.elasticsearch.xpack.esql.core.util.Check;
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
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.KnnQuery;
import org.elasticsearch.xpack.esql.score.ExpressionScoreMapper;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.K_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VECTOR_SIMILARITY_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VISIT_PERCENTAGE_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityMetric.DOT_PRODUCT;

public class Knn extends SingleFieldFullTextFunction
    implements
        OptionalArgument,
        VectorFunction,
        PostOptimizationVerificationAware,
        ConfigurationFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Knn", Knn::readFrom);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Knn.class)
        .ternaryConfig(Knn::new)
        // Snapshot-only, matching the pragma that enables KNN's runtime search in the first place.
        .snapshotCapabilities("runtime_anywhere")
        .snapshotCapabilities("runtime_similarity_function")
        .name("knn");

    private final Integer implicitK;
    // Expressions to be used as prefilters in knn query
    private final List<Expression> filterExpressions;
    private final Configuration configuration;
    private float[] cachedQuery;

    public static final String MIN_CANDIDATES_OPTION = "min_candidates";

    /**
     * Names the {@link VectorSimilarityMetric} to compare the vectors with, e.g. {@code "l2_norm"}. Only accepted
     * when knn runs over a runtime expression - an indexed field takes its metric from the mapping instead. Not
     * documented as a function named parameter yet because runtime knn, the only path that accepts it, is snapshot
     * only; add the {@code @MapParam.MapParamEntry} for it when that path is released.
     */
    public static final String SIMILARITY_FUNCTION_OPTION = "similarity_function";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(K_FIELD.getPreferredName(), INTEGER),
        entry(MIN_CANDIDATES_OPTION, INTEGER),
        entry(VECTOR_SIMILARITY_FIELD.getPreferredName(), FLOAT),
        entry(SIMILARITY_FUNCTION_OPTION, KEYWORD),
        entry(VISIT_PERCENTAGE_FIELD.getPreferredName(), FLOAT),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(KnnQuery.RESCORE_OVERSAMPLE_FIELD, FLOAT)
    );

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Finds the k nearest vectors to a query vector using a similarity metric.",
        description = "Finds the k nearest vectors to a query vector, as measured by a similarity metric. "
            + "knn function finds nearest vectors through approximate search on indexed dense_vectors or semantic_text fields.",
        examples = { @Example(file = "knn-function", tag = "knn-function") },
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") }
    )
    public Knn(
        Source source,
        @Param(
            name = "field",
            type = { "dense_vector", "text" },
            description = "Field that the query will target. "
                + "knn function can be used with dense_vector or semantic_text fields. Other text fields are not allowed"
        ) Expression field,
        @Param(
            name = "query",
            type = { "dense_vector" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Vector value to find top nearest neighbours for."
        ) Expression query,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "k",
                    type = "integer",
                    valueHint = { "10" },
                    description = "The number of nearest neighbors to return from each shard. "
                        + "Elasticsearch collects k results from each shard, then merges them to find the global top results. "
                        + "This value must be less than or equal to num_candidates. "
                        + "This value is automatically set with any LIMIT applied to the function."
                ),
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query."
                        + "Defaults to 1.0."
                ),
                @MapParam.MapParamEntry(
                    name = "min_candidates",
                    type = "integer",
                    valueHint = { "10" },
                    description = "The minimum number of nearest neighbor candidates to consider per shard while doing knn search. "
                        + " KNN may use a higher number of candidates in case the query can't use a approximate results. "
                        + "Cannot exceed 10,000. Increasing min_candidates tends to improve the accuracy of the final results. "
                        + "Defaults to 1.5 * k (or LIMIT) used for the query."
                ),
                @MapParam.MapParamEntry(
                    name = "visit_percentage",
                    type = "float",
                    valueHint = { "10" },
                    description = "The percentage of vectors to explore per shard while doing knn search with bbq_disk. "
                        + "Must be between 0 and 100. 0 will default to using num_candidates for calculating the percent visited. "
                        + "Increasing visit_percentage tends to improve the accuracy of the final results. "
                        + "If visit_percentage is set for bbq_disk, num_candidates is ignored. "
                        + "Defaults to ~1% per shard for every 1 million vectors"
                ),
                @MapParam.MapParamEntry(
                    name = "similarity",
                    type = "double",
                    valueHint = { "0.01" },
                    description = "The minimum similarity required for a document to be considered a match. "
                        + "The similarity value calculated relates to the raw similarity used, not the document score."
                ),
                @MapParam.MapParamEntry(
                    name = "rescore_oversample",
                    type = "double",
                    valueHint = { "3.5" },
                    description = "Applies the specified oversampling for rescoring quantized vectors. "
                        + "See [oversampling and rescoring quantized vectors]"
                        + "(docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details."
                ), },
            description = "(Optional) kNN additional options as <<esql-function-named-params,function named parameters>>."
                + " See [knn query](/reference/query-languages/query-dsl/query-dsl-knn-query.md) for more information.",
            optional = true
        ) Expression options,
        Configuration configuration
    ) {
        this(source, field, query, options, null, null, List.of(), configuration);
    }

    public Knn(
        Source source,
        Expression field,
        Expression query,
        Expression options,
        Integer implicitK,
        QueryBuilder queryBuilder,
        List<Expression> filterExpressions,
        Configuration configuration
    ) {
        super(source, field, query, options, expressionList(field, query, options), queryBuilder);
        this.implicitK = implicitK;
        this.filterExpressions = filterExpressions;
        this.configuration = configuration;
    }

    private static List<Expression> expressionList(Expression field, Expression query, Expression options) {
        List<Expression> result = new ArrayList<>();
        result.add(field);
        result.add(query);
        if (options != null) {
            result.add(options);
        }
        return result;
    }

    public Integer implicitK() {
        return implicitK;
    }

    public List<Expression> filterExpressions() {
        return filterExpressions;
    }

    public Configuration configuration() {
        return configuration;
    }

    public Knn withImplicitK(Integer k) {
        Check.notNull(k, "k must not be null");
        return new Knn(source(), field(), query(), options(), k, queryBuilder(), filterExpressions(), configuration());
    }

    public List<Number> queryAsObject() {
        // we need to check that we got a list and every element in the list is a number
        Expression query = query();
        if (query instanceof Literal literal) {
            @SuppressWarnings("unchecked")
            List<Number> result = ((List<Number>) literal.value());
            return result;
        }
        throw new EsqlIllegalArgumentException(format(null, "Query value must be a list of numbers in [{}], found [{}]", source(), query));
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Knn(source(), field(), query(), options(), implicitK(), queryBuilder, filterExpressions(), configuration);
    }

    /** Unlike the lexical search functions, KNN's runtime search is still gated behind a pragma. */
    @Override
    public boolean supportsRuntimeSearch() {
        return Build.current().isSnapshot() && configuration.pragmas().knnRuntimeField();
    }

    @Override
    public boolean isRuntimeSearch() {
        if (supportsRuntimeSearch() == false) {
            return false;
        }
        FieldAttribute fieldAttribute = fieldAsFieldAttribute();
        if (fieldAttribute == null) {
            // This isn't a field in the index OR a pushed block loader
            return true;
        }

        if (fieldAttribute.isPotentiallyUnmapped()) {
            // A potentially unmapped field cannot be pushed down: the Lucene query would silently miss the rows of the
            // indices where the field is unmapped, so it is matched at runtime instead.
            return true;
        }
        return false;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (isRuntimeSearch()) {
            return Translatable.NO;
        }
        Translatable translatable = super.translatable(pushdownPredicates);
        // We need to check whether filter expressions are translatable as well
        for (Expression filterExpression : filterExpressions()) {
            translatable = translatable.merge(TranslationAware.translatable(filterExpression, pushdownPredicates));
        }

        return translatable;
    }

    /**
     * Beyond the type checks every option gets, the {@code similarity_function} value has to name one of the
     * {@link VectorSimilarityMetric}s. Whether the option is allowed here at all depends on the plan rather than on
     * the option itself, so that part is checked in {@link #fieldVerifier}.
     */
    @Override
    protected TypeResolution resolveOptions() {
        if (options() == null) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return Options.resolve(options(), source(), THIRD, getAllowedOptions(), opts -> {
            String metric = BytesRefs.toString(opts.get(SIMILARITY_FUNCTION_OPTION));
            if (metric != null && VectorSimilarityMetric.fromOptionValue(metric) == null) {
                throw new InvalidArgumentException(
                    format(
                        null,
                        "Invalid option [{}] in [{}], expected one of {}",
                        SIMILARITY_FUNCTION_OPTION,
                        sourceText(),
                        VectorSimilarityMetric.optionValues()
                    )
                );
            }
        });
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
        if (false == isRuntimeSearch()) {
            if (options() != null && queryOptions().get(SIMILARITY_FUNCTION_OPTION) != null) {
                failures.add(
                    Failure.fail(
                        options(),
                        "[KNN] option [{}] is only supported when [{}] is a non-index-mapped field or expression; "
                            + "an indexed field is compared with the similarity declared in its mapping",
                        SIMILARITY_FUNCTION_OPTION,
                        field.sourceText()
                    )
                );
            }
            return;
        }

        if (false == getRuntimeFieldDataTypes().contains(field.dataType())) {
            failures.add(
                Failure.fail(
                    query(),
                    "[KNN] cannot operate on [{}] of type [{}]; a non-index-mapped field/expression must resolve to dense_vector type",
                    field.sourceText(),
                    field.dataType().typeName()
                )
            );
        }
        // The query value can only be converted to the field's runtime type once it has been folded down to a
        // Literal; if it hasn't yet (e.g. pre-optimization), this check is skipped here and retried once
        // postOptimizationPlanVerification runs.
        if (query() instanceof Literal) {
            if (query().dataType() != DENSE_VECTOR) {
                failures.add(
                    Failure.fail(
                        query(),
                        "[KNN] cannot operate on [{}] of type [{}]; a non-index-mapped field must be a dense_vector expression",
                        query().sourceText(),
                        query().dataType().typeName()
                    )
                );
            }
            validateQueryVector(failures);
        }
    }

    private void validateQueryVector(Failures failures) {
        float[] vector = queryAsFloats();
        float squaredMagnitude = VectorUtil.dotProduct(vector, vector);
        if (Float.isNaN(squaredMagnitude) || Float.isInfinite(squaredMagnitude)) {
            failures.add(
                Failure.fail(query(), "[KNN] cannot operate on [{}]; query vector values are too large or too small.", query().sourceText())
            );
        }
        // Only cosine divides by the magnitude; the other metrics are well defined for a zero vector.
        if (squaredMagnitude == 0.0f && similarityMetric() == VectorSimilarityMetric.COSINE) {
            failures.add(
                Failure.fail(
                    query(),
                    "[KNN] cannot operate on [{}]; Cosine similarity does not support (query) vectors with zero magnitude.",
                    query().sourceText()
                )
            );
        }
        if (similarityMetric() == DOT_PRODUCT && VectorUtil.isUnitVector(vector) == false) {
            failures.add(
                Failure.fail(
                    query(),
                    "[KNN] dot_product requires unit-length vectors; query vector [{}] has magnitude [{}]",
                    query().sourceText(),
                    Math.sqrt(squaredMagnitude)
                )
            );
        }
    }

    @Override
    public boolean contributesToScore() {
        return true;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (false == isRuntimeSearch()) {
            return super.toEvaluator(toEvaluator);
        }
        return evaluatorForRuntimeSearch(toEvaluator);
    }

    private ExpressionEvaluator.Factory evaluatorForRuntimeSearch(ToEvaluator toEvaluator) {
        float[] queryVector = queryAsFloats();
        Float similarityThreshold = similarityThresholdOption();
        VectorSimilarityMetric metric = similarityMetric();
        if (metric == DOT_PRODUCT) {
            return new KnnRuntimeFilterForDotProductEvaluator.Factory(
                source(),
                toEvaluator.apply(field()),
                queryVector,
                similarityThreshold,
                // Allocate a scratch buffer whenever we will actually read the field vector: either to compare against
                // the threshold or to validate unit length for DOT_PRODUCT.
                context -> new float[queryVector.length]
            );
        } else {
            return new KnnRuntimeFilterEvaluator.Factory(
                source(),
                toEvaluator.apply(field()),
                queryVector,
                metric,
                similarityThreshold,
                // Allocate a scratch buffer whenever we will actually read the field vector: either to compare against
                // the threshold or to validate unit length for DOT_PRODUCT.
                context -> similarityThreshold == null ? null : new float[queryVector.length]
            );
        }
    }

    @Override
    public ExpressionEvaluator.Factory toScorer(ExpressionScoreMapper.ToScorer toScorer) {
        if (false == isRuntimeSearch()) {
            return super.toScorer(toScorer);
        }
        return scorerForRuntimeSearch(toScorer);
    }

    private ExpressionEvaluator.Factory scorerForRuntimeSearch(ExpressionScoreMapper.ToScorer toScorer) {
        float[] queryVector = queryAsFloats();
        float boost = getBoost();
        return new KnnRuntimeScoreEvaluator.Factory(
            source(),
            toScorer.toEvaluator().apply(field()),
            queryVector,
            similarityMetric(),
            boost,
            context -> new float[queryVector.length]
        );
    }

    @Nullable
    private Float similarityThresholdOption() {
        if (options() == null) {
            return null;
        }
        Map<String, Object> opts = queryOptions();
        return (Float) opts.get(VECTOR_SIMILARITY_FIELD.getPreferredName());
    }

    /**
     * The vector similarity metric used for runtime search path, defaulting to cosine when the
     * {@code similarity_function} option is absent.
     */
    private VectorSimilarityMetric similarityMetric() {
        if (options() == null) {
            return VectorSimilarityMetric.COSINE;
        }
        String metric = BytesRefs.toString(queryOptions().get(SIMILARITY_FUNCTION_OPTION));
        return metric == null ? VectorSimilarityMetric.COSINE : VectorSimilarityMetric.fromOptionValue(metric);
    }

    private float getBoost() {
        if (options() == null) {
            return 1.0f;
        }
        Map<String, Object> opts = queryOptions();
        return (Float) opts.getOrDefault(BOOST_FIELD.getPreferredName(), 1.0f);
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        assert implicitK() != null : "Knn function must have a k value set before translation";
        var fieldAttribute = fieldAsFieldAttribute(field());

        Check.notNull(fieldAttribute, "Knn must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        float[] queryAsFloats = queryAsFloats();

        List<QueryBuilder> filterQueries = new ArrayList<>();
        for (Expression filterExpression : filterExpressions()) {
            if (filterExpression instanceof TranslationAware translationAware) {
                // We can only translate filter expressions that are translatable. In case any is not translatable,
                // Knn won't be pushed down so it's safe not to translate all filters and check them when creating an evaluator
                // for the non-pushed down query
                if (translationAware.translatable(pushdownPredicates) == Translatable.YES) {
                    filterQueries.add(handler.asQuery(pushdownPredicates, filterExpression).toQueryBuilder());
                }
            }
        }

        Map<String, Object> options = queryOptions();
        Integer explicitK = (Integer) options.get(K_FIELD.getPreferredName());

        return new KnnQuery(source(), fieldName, queryAsFloats, explicitK != null ? explicitK : implicitK(), options, filterQueries);
    }

    private float[] queryAsFloats() {
        if (cachedQuery == null) {
            List<Number> queryFolded = queryAsObject();
            cachedQuery = new float[queryFolded.size()];
            for (int i = 0; i < queryFolded.size(); i++) {
                cachedQuery[i] = queryFolded.get(i).floatValue();
            }
        }
        return cachedQuery;
    }

    public Expression withFilters(List<Expression> filterExpressions) {
        return new Knn(source(), field(), query(), options(), implicitK(), queryBuilder(), filterExpressions, configuration());
    }

    private Map<String, Object> queryOptions() throws InvalidArgumentException {
        Map<String, Object> options = new HashMap<>();
        if (options() != null) {
            Options.populateMap((MapExpression) options(), options, source(), FOURTH, ALLOWED_OPTIONS);
        }
        return options;
    }

    protected QueryBuilder evaluatorQueryBuilder() {
        // Either we couldn't push down due to non-pushable filters, or because it's part of a disjuncion.
        // Uses a nearest neighbors exact query instead of an approximate one
        var fieldAttribute = fieldAsFieldAttribute(field());
        Check.notNull(fieldAttribute, "Knn must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        Map<String, Object> opts = queryOptions();

        return new ExactKnnQueryBuilder(VectorData.fromFloats(queryAsFloats()), fieldName, (Float) opts.get(VECTOR_SIMILARITY_FIELD));
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        // Check that a k has been set
        if (implicitK() == null) {
            failures.add(
                Failure.fail(this, "Knn function must be used with a LIMIT clause after it to set the number of nearest neighbors to find")
            );
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Knn(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            implicitK(),
            queryBuilder(),
            filterExpressions(),
            configuration()
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            Knn::new,
            field(),
            query(),
            options(),
            implicitK(),
            queryBuilder(),
            filterExpressions(),
            configuration()
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static Knn readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        List<Expression> filterExpressions = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression options = in.getTransportVersion().supports(ESQL_OPTIONS_FOR_SEARCH_FUNCTIONS)
            ? in.readOptionalNamedWriteable(Expression.class)
            : null;
        Integer implicitK = in.getTransportVersion().supports(ESQL_OPTIONS_FOR_SEARCH_FUNCTIONS) ? in.readOptionalInt() : null;
        Configuration configuration = ((PlanStreamInput) in).configuration();
        return new Knn(source, field, query, options, implicitK, queryBuilder, filterExpressions, configuration);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());
        out.writeNamedWriteableCollection(filterExpressions());

        if (out.getTransportVersion().supports(ESQL_OPTIONS_FOR_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(options());
            out.writeOptionalInt(implicitK());
        }
    }

    @Override
    protected Set<DataType> getFieldDataTypes() {
        if (false == isRuntimeSearch()) {
            // Knn accepts DENSE_VECTOR or TEXT (for semantic_text), plus NULL for missing fields
            return Set.of(DENSE_VECTOR, TEXT, NULL);
        } else {
            return getRuntimeFieldDataTypes();
        }
    }

    private Set<DataType> getRuntimeFieldDataTypes() {
        // Knn on runtime field accepts DENSE_VECTOR or NULL
        return Set.of(DENSE_VECTOR, NULL);
    }

    @Override
    protected Set<DataType> getQueryDataTypes() {
        return Set.of(DENSE_VECTOR);
    }

    @Override
    protected Map<String, DataType> getAllowedOptions() {
        return ALLOWED_OPTIONS;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Knn knn = (Knn) o;
        return super.equals(knn)
            && Objects.equals(implicitK(), knn.implicitK())
            && Objects.equals(filterExpressions(), knn.filterExpressions());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), query(), queryBuilder(), implicitK(), filterExpressions(), options());
    }

    private static void requireUnitLength(float[] vector) {
        if (false == VectorUtil.isUnitVector(vector)) {
            throw new IllegalArgumentException(
                format(
                    null,
                    "dot_product requires unit-length vectors but encountered magnitude [{}]",
                    Math.sqrt(VectorUtil.dotProduct(vector, vector))
                )
            );
        }
    }

    /**
     * Evaluator factory for runtime KNN filter (boolean result): returns true for rows whose field vector is at
     * least as similar to the query vector as the threshold (or always true when no threshold is set), false otherwise.
     * <p>
     * Both sides are compared as normalized scores rather than as raw similarities, which is what makes the
     * threshold mean "at least this similar" for every metric. This is the same normalize-then-compare check
     * the knn query does.
     */
    @Evaluator(extraName = "RuntimeFilter", allNullsIsNull = false, warnExceptions = { IllegalArgumentException.class })
    static boolean runtimeFilter(
        @Position int position,
        FloatBlock fieldBlock,
        @Fixed float[] queryVector,
        @Fixed VectorSimilarityMetric similarityMetric,
        @Fixed @Nullable Float similarityThreshold,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) float[] scratchVector
    ) {
        if (fieldBlock.isNull(position)) {
            return false;
        }
        int dimensions = fieldBlock.getValueCount(position);
        if (dimensions != queryVector.length) {
            throw new IllegalArgumentException("dense_vector dimensions do not match");
        }
        // With no threshold, every row with a non-null vector passes. No need to read the vector.
        if (similarityThreshold == null) {
            return true;
        }
        int first = fieldBlock.getFirstValueIndex(position);
        for (int i = 0; i < dimensions; i++) {
            scratchVector[i] = fieldBlock.getFloat(first + i);
        }

        float similarity = similarityMetric.calculateSimilarity(scratchVector, queryVector);
        return similarityMetric.normalizeToRelevanceScore(similarity) >= similarityMetric.normalizeToRelevanceScore(similarityThreshold);
    }

    /**
     * Same as {@link #runtimeFilter(int, FloatBlock, float[], VectorSimilarityMetric, Float, float[])} above,
     * but also checks that the field vector is unit length for DOT_PRODUCT.
     */
    @Evaluator(extraName = "RuntimeFilterForDotProduct", allNullsIsNull = false, warnExceptions = { IllegalArgumentException.class })
    static boolean runtimeFilterForDotProduct(
        @Position int position,
        FloatBlock fieldBlock,
        @Fixed float[] queryVector,
        @Fixed @Nullable Float similarityThreshold,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) float[] scratchVector
    ) {
        if (fieldBlock.isNull(position)) {
            return false;
        }
        int dimensions = fieldBlock.getValueCount(position);
        if (dimensions != queryVector.length) {
            throw new IllegalArgumentException("dense_vector dimensions do not match");
        }

        // we need to read the vector even if similarityThreshold is null, because we need to check that it is unit length for DOT_PRODUCT
        int first = fieldBlock.getFirstValueIndex(position);
        for (int i = 0; i < dimensions; i++) {
            scratchVector[i] = fieldBlock.getFloat(first + i);
        }
        requireUnitLength(scratchVector);
        if (similarityThreshold == null) {
            return true;
        }
        float similarity = DOT_PRODUCT.calculateSimilarity(scratchVector, queryVector);
        return DOT_PRODUCT.normalizeToRelevanceScore(similarity) >= DOT_PRODUCT.normalizeToRelevanceScore(similarityThreshold);
    }

    /**
     * Evaluator factory for runtime KNN scoring (double result): normalizes the vector similarity value to the unit interval
     * and applies boost.
     * We intentionally do not check for unit length here, because
     * {@link #runtimeFilterForDotProduct(int, FloatBlock, float[], Float, float[]) filter evaluator}
     * should have already done that for DOT_PRODUCT.
     */
    @Evaluator(extraName = "RuntimeScore", allNullsIsNull = false, warnExceptions = { IllegalArgumentException.class })
    static double runtimeScore(
        @Position int position,
        FloatBlock fieldBlock,
        @Fixed float[] queryVector,
        @Fixed VectorSimilarityMetric similarityMetric,
        @Fixed float boost,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) float[] scratchVector
    ) {
        if (fieldBlock.isNull(position)) {
            return 0.0;
        }
        int dimensions = fieldBlock.getValueCount(position);
        if (dimensions != queryVector.length) {
            throw new IllegalArgumentException("dense_vector dimensions do not match");
        }

        int first = fieldBlock.getFirstValueIndex(position);
        for (int i = 0; i < dimensions; i++) {
            scratchVector[i] = fieldBlock.getFloat(first + i);
        }
        return similarityMetric.normalizeToRelevanceScore(similarityMetric.calculateSimilarity(scratchVector, queryVector)) * boost;
    }
}

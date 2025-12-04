/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.KnnQuery;

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
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

public class Knn extends SingleFieldFullTextFunction implements OptionalArgument, VectorFunction, PostOptimizationVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Knn", Knn::readFrom);

    // Implicit k is not serialized as it's already included in the query builder on the rewrite step before being sent to data nodes
    private final transient Integer implicitK;
    // Expressions to be used as prefilters in knn query
    private final List<Expression> filterExpressions;

    public static final String MIN_CANDIDATES_OPTION = "min_candidates";

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(K_FIELD.getPreferredName(), INTEGER),
        entry(MIN_CANDIDATES_OPTION, INTEGER),
        entry(VECTOR_SIMILARITY_FIELD.getPreferredName(), FLOAT),
        entry(VISIT_PERCENTAGE_FIELD.getPreferredName(), FLOAT),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(KnnQuery.RESCORE_OVERSAMPLE_FIELD, FLOAT)
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Finds the k nearest vectors to a query vector, as measured by a similarity metric. "
            + "knn function finds nearest vectors through approximate search on indexed dense_vectors or semantic_text fields.",
        examples = { @Example(file = "knn-function", tag = "knn-function") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") }
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
        ) Expression options
    ) {
        this(source, field, query, options, null, null, List.of());
    }

    public Knn(
        Source source,
        Expression field,
        Expression query,
        Expression options,
        Integer implicitK,
        QueryBuilder queryBuilder,
        List<Expression> filterExpressions
    ) {
        super(source, field, query, options, expressionList(field, query, options), queryBuilder);
        this.implicitK = implicitK;
        this.filterExpressions = filterExpressions;
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

    public Knn withImplicitK(Integer k) {
        Check.notNull(k, "k must not be null");
        return new Knn(source(), field(), query(), options(), k, queryBuilder(), filterExpressions());
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
        return new Knn(source(), field(), query(), options(), implicitK(), queryBuilder, filterExpressions());
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        Translatable translatable = super.translatable(pushdownPredicates);
        // We need to check whether filter expressions are translatable as well
        for (Expression filterExpression : filterExpressions()) {
            translatable = translatable.merge(TranslationAware.translatable(filterExpression, pushdownPredicates));
        }

        return translatable;
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
        List<Number> queryFolded = queryAsObject();
        float[] queryAsFloats = new float[queryFolded.size()];
        for (int i = 0; i < queryFolded.size(); i++) {
            queryAsFloats[i] = queryFolded.get(i).floatValue();
        }
        return queryAsFloats;
    }

    public Expression withFilters(List<Expression> filterExpressions) {
        return new Knn(source(), field(), query(), options(), implicitK(), queryBuilder(), filterExpressions);
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
            filterExpressions()
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Knn::new, field(), query(), options(), implicitK(), queryBuilder(), filterExpressions());
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
        return new Knn(source, field, query, null, null, queryBuilder, filterExpressions);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());
        out.writeNamedWriteableCollection(filterExpressions());
    }

    @Override
    protected Set<DataType> getFieldDataTypes() {
        // Knn accepts DENSE_VECTOR or TEXT (for semantic_text), plus NULL for missing fields
        return Set.of(DENSE_VECTOR, TEXT, NULL);
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
        // Knn does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing two Knn functions
        if (o == null || getClass() != o.getClass()) return false;
        Knn knn = (Knn) o;
        return super.equals(knn)
            && Objects.equals(implicitK(), knn.implicitK())
            && Objects.equals(filterExpressions(), knn.filterExpressions());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), query(), queryBuilder(), implicitK(), filterExpressions());
    }

}

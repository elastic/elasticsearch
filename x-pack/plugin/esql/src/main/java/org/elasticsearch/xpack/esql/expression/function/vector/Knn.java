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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
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
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.KnnQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.K_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.NUM_CANDS_FIELD;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VECTOR_SIMILARITY_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.Match.getNameFromFieldAttribute;

public class Knn extends FullTextFunction implements OptionalArgument, VectorFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Knn", Knn::readFrom);

    private final Expression field;
    private final Expression options;

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(K_FIELD.getPreferredName(), INTEGER),
        entry(NUM_CANDS_FIELD.getPreferredName(), INTEGER),
        entry(VECTOR_SIMILARITY_FIELD.getPreferredName(), FLOAT),
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(KnnQuery.RESCORE_OVERSAMPLE_FIELD, FLOAT)
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Finds the k nearest vectors to a query vector, as measured by a similarity metric. " +
                "knn function finds nearest vectors through approximate search on indexed dense_vectors.",
        examples = {
            @Example(file = "knn-function", tag = "knn-function"),
            @Example(file = "knn-function", tag = "knn-function-options"), },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public Knn(
        Source source,
        @Param(name = "field", type = { "dense_vector" }, description = "Field that the query will target.") Expression field,
        @Param(
            name = "query",
            type = { "dense_vector" },
            description = "Vector value to find top nearest neighbours for."
        ) Expression query,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query."
                        + "Defaults to 1.0."
                ),
                @MapParam.MapParamEntry(
                    name = "k",
                    type = "integer",
                    valueHint = { "10" },
                    description = "The number of nearest neighbors to return from each shard. "
                        + "Elasticsearch collects k results from each shard, then merges them to find the global top results. "
                        + "This value must be less than or equal to num_candidates. Defaults to 10."
                ),
                @MapParam.MapParamEntry(
                    name = "num_candidates",
                    type = "integer",
                    valueHint = { "10" },
                    description = "The number of nearest neighbor candidates to consider per shard while doing knn search. "
                        + "Cannot exceed 10,000. Increasing num_candidates tends to improve the accuracy of the final results. "
                        + "Defaults to 1.5 * k"
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
                        + "See [oversampling and rescoring quantized vectors](docs-content://solutions/search/vector/knn.md#dense-vector-knn-search-rescoring) for details."
                ), },
            description = "(Optional) kNN additional options as <<esql-function-named-params,function named parameters>>."
                + " See <<query-dsl-knn-query,knn query>> for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, field, query, options, null);
    }

    public Knn(Source source, Expression field, Expression query, Expression options, QueryBuilder queryBuilder) {
        super(source, query, options == null ? List.of(field, query) : List.of(field, query, options), queryBuilder);
        this.field = field;
        this.options = options;
    }

    public Expression field() {
        return field;
    }

    public Expression options() {
        return options;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveParams() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNotNull(field(), sourceText(), FIRST).and(isType(field(), dt -> dt == DENSE_VECTOR, sourceText(), FIRST, "dense_vector"))
            .and(isType(query(), dt -> dt == DENSE_VECTOR, sourceText(), TypeResolutions.ParamOrdinal.SECOND, "dense_vector"));
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        var fieldAttribute = Match.fieldAsFieldAttribute(field());

        Check.notNull(fieldAttribute, "Match must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        @SuppressWarnings("unchecked")
        List<Number> queryFolded = (List<Number>) query().fold(FoldContext.small() /* TODO remove me */);
        float[] queryAsFloats = new float[queryFolded.size()];
        for (int i = 0; i < queryFolded.size(); i++) {
            queryAsFloats[i] = queryFolded.get(i).floatValue();
        }

        return new KnnQuery(source(), fieldName, queryAsFloats, queryOptions());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Knn(source(), field(), query(), options(), queryBuilder);
    }

    private Map<String, Object> queryOptions() throws InvalidArgumentException {
        if (options() == null) {
            return Map.of();
        }

        Map<String, Object> options = new HashMap<>();
        populateOptionsMap((MapExpression) options(), options, THIRD, sourceText(), ALLOWED_OPTIONS);
        return options;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Knn(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            queryBuilder()
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Knn::new, field(), query(), options());
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

        return new Knn(source, field, query, null, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    public boolean equals(Object o) {
        // Knn does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing two Knn functions
        if (o == null || getClass() != o.getClass()) return false;
        Knn knn = (Knn) o;
        return Objects.equals(field(), knn.field())
                && Objects.equals(query(), knn.query())
                && Objects.equals(queryBuilder(), knn.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), query(), queryBuilder());
    }

}

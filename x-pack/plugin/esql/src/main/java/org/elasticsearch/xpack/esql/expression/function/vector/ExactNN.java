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
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.ExactNNQuery;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;

/**
 * Exact nearest neighbour search using a dense_vector similarity function. Used to translate {@link Knn} into exact search
 * when it can't be pushed down to Lucene. Not exposed to users directly.
 */
public class ExactNN extends FullTextFunction implements OptionalArgument, VectorFunction, PostAnalysisPlanVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Knn", ExactNN::readFrom);

    private final Expression field;
    private final Expression minimumSimilarity;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Finds all nearest vectors to a query vector, as measured by a similarity metric. "
            + "performs brute force search over all vectors in the index.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public ExactNN(
        Source source,
        @Param(name = "field", type = { "dense_vector" }, description = "Field that the query will target.") Expression field,
        @Param(
            name = "query",
            type = { "dense_vector" },
            description = "Vector value to find top nearest neighbours for."
        ) Expression query,
        @Param(
            name = "similarity",
            type = { "double" },
            optional = true,
            description = "The minimum similarity required for a document to be considered a match. "
                + "The similarity value calculated relates to the raw similarity used, not the document score."
        ) Expression minimumSimilarity
    ) {
        this(source, field, query, minimumSimilarity, null);
    }

    public ExactNN(Source source, Expression field, Expression query, Expression minimumSimilarity, QueryBuilder queryBuilder) {
        super(source, query, minimumSimilarity == null ? List.of(field, query) : List.of(field, query, minimumSimilarity), queryBuilder);
        this.field = field;
        this.minimumSimilarity = minimumSimilarity;
    }

    public Expression field() {
        return field;
    }

    public Expression minimumSimilarity() {
        return minimumSimilarity;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveField().and(resolveQuery()).and(resolveMinimumSimilarity());
    }

    private TypeResolution resolveField() {
        return isNotNull(field(), sourceText(), FIRST).and(isType(field(), dt -> dt == DENSE_VECTOR, sourceText(), FIRST, "dense_vector"));
    }

    private TypeResolution resolveQuery() {
        return isNotNull(query(), sourceText(), SECOND).and(
            isType(query(), dt -> dt == DENSE_VECTOR, sourceText(), TypeResolutions.ParamOrdinal.SECOND, "dense_vector")
        );
    }

    private TypeResolution resolveMinimumSimilarity() {
        if (minimumSimilarity == null) {
            return TypeResolution.TYPE_RESOLVED;
        }

        return isNotNull(minimumSimilarity(), sourceText(), THIRD).and(isNumeric(minimumSimilarity(), sourceText(), THIRD));
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new ExactNN(source(), field(), query(), minimumSimilarity(), queryBuilder);
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fieldAttribute = Match.fieldAsFieldAttribute(field());

        Check.notNull(fieldAttribute, "Exact must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        @SuppressWarnings("unchecked")
        List<Number> queryFolded = (List<Number>) query().fold(FoldContext.small() /* TODO remove me */);
        float[] queryAsFloats = new float[queryFolded.size()];
        for (int i = 0; i < queryFolded.size(); i++) {
            queryAsFloats[i] = queryFolded.get(i).floatValue();
        }
        Float similarity = minimumSimilarity != null ? ((Number) minimumSimilarity().fold(FoldContext.small())).floatValue() : null;

        return new ExactNNQuery(source(), fieldName, queryAsFloats, similarity);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (plan, failures) -> {
            super.postAnalysisPlanVerification().accept(plan, failures);
            fieldVerifier(plan, this, field, failures);
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ExactNN(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? newChildren.get(2) : null,
            queryBuilder()
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ExactNN::new, field(), query(), minimumSimilarity(), queryBuilder());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static ExactNN readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        Expression minimumSimilarity = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        return new ExactNN(source, field, query, minimumSimilarity, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        out.writeNamedWriteable(minimumSimilarity());
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    public boolean equals(Object o) {
        // Knn does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing two Knn functions
        if (o == null || getClass() != o.getClass()) return false;
        ExactNN knn = (ExactNN) o;
        return Objects.equals(field(), knn.field())
            && Objects.equals(query(), knn.query())
            && Objects.equals(minimumSimilarity(), knn.minimumSimilarity())
            && Objects.equals(queryBuilder(), knn.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), query(), minimumSimilarity(), queryBuilder());
    }

}

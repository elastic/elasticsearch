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
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.KnnQuery;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.Match.getNameFromFieldAttribute;

public class Knn extends Function implements TranslationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Knn", Knn::readFrom);

    private final Expression field;
    private final Expression query;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = """
            Finds the k nearest vectors to a query vector, as measured by a similarity metric.
            knn function finds nearest vectors through approximate search on indexed dense_vectors
            """,
        appliesTo = {
            @FunctionAppliesTo(
                lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT
            ) }
    )
    public Knn(Source source, Expression field, Expression query) {
        super(source, List.of(field, query));
        this.field = field;
        this.query = query;
    }

    public Expression field() {
        return field;
    }

    public Expression query() {
        return query;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNotNull(field(), sourceText(), FIRST).and(isType(field(), dt -> dt == DENSE_VECTOR, sourceText(), FIRST, "dense_vector"))
            .and(TypeResolutions.isNumeric(query(), sourceText(), TypeResolutions.ParamOrdinal.SECOND));
    }

    @Override
    public boolean translatable(LucenePushdownPredicates pushdownPredicates) {
        return true;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fieldAttribute = Match.fieldAsFieldAttribute(field());

        Check.notNull(fieldAttribute, "Match must have a field attribute as the first argument");
        String fieldName = getNameFromFieldAttribute(fieldAttribute);
        @SuppressWarnings("unchecked")
        List<Double> queryFolded = (List<Double>) query().fold(FoldContext.small() /* TODO remove me */);
        float[] queryAsFloats = new float[queryFolded.size()];
        for (int i = 0; i < queryFolded.size(); i++) {
            queryAsFloats[i] = queryFolded.get(i).floatValue();
        }
        return new KnnQuery(source(), fieldName, queryAsFloats);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Knn(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Knn::new, field(), query());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private static Knn readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);

        return new Knn(source, field, query);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Knn knn = (Knn) o;
        return Objects.equals(field, knn.field) && Objects.equals(query, knn.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, query);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node representing a lookup source operation.
 * This represents the source of a lookup query before conversion to operators.
 * The QueryList is created later during operator creation from a factory
 * and will receive the Page to operate on in the runtime
 * when we call queryList.getQuery(position, inputPage).
 */
public class ParameterizedQueryExec extends LeafExec {
    private final List<Attribute> output;
    private final List<MatchConfig> matchFields;
    @Nullable
    private final Expression joinOnConditions;
    @Nullable
    private final QueryBuilder query;
    private final boolean emptyResult;

    /**
     * Runtime-only value set by the {@link org.elasticsearch.xpack.esql.optimizer.LookupPhysicalPlanOptimizer}
     * holding the left attribute of the lookup join when the join may make use of the bulk keyword lookup optimization.
     */
    @Nullable
    private final Attribute bulkLookupLeft;

    /**
     * Runtime-only value set by the {@link org.elasticsearch.xpack.esql.optimizer.LookupPhysicalPlanOptimizer}
     * holding the right attribute of the lookup join when the join may make use of the bulk keyword lookup optimization.
     */
    @Nullable
    private final Attribute bulkLookupRight;

    public ParameterizedQueryExec(
        Source source,
        List<Attribute> output,
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        @Nullable QueryBuilder query
    ) {
        this(source, output, matchFields, joinOnConditions, query, false, null, null);
    }

    public ParameterizedQueryExec(
        Source source,
        List<Attribute> output,
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        @Nullable QueryBuilder query,
        boolean emptyResult,
        @Nullable Attribute bulkLookupLeft,
        @Nullable Attribute bulkLookupRight
    ) {
        super(source);
        this.output = output;
        this.matchFields = matchFields;
        this.joinOnConditions = joinOnConditions;
        this.query = query;
        this.emptyResult = emptyResult;
        this.bulkLookupLeft = bulkLookupLeft;
        this.bulkLookupRight = bulkLookupRight;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public List<MatchConfig> matchFields() {
        return matchFields;
    }

    @Nullable
    public Expression joinOnConditions() {
        return joinOnConditions;
    }

    @Nullable
    public QueryBuilder query() {
        return query;
    }

    public boolean emptyResult() {
        return emptyResult;
    }

    @Nullable
    public Attribute bulkLookupLeft() {
        return bulkLookupLeft;
    }

    @Nullable
    public Attribute bulkLookupRight() {
        return bulkLookupRight;
    }

    public ParameterizedQueryExec withQuery(QueryBuilder query) {
        return Objects.equals(this.query, query)
            ? this
            : new ParameterizedQueryExec(
                source(),
                output,
                matchFields,
                joinOnConditions,
                query,
                emptyResult,
                bulkLookupLeft,
                bulkLookupRight
            );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<ParameterizedQueryExec> info() {
        return NodeInfo.create(
            this,
            ParameterizedQueryExec::new,
            output,
            matchFields,
            joinOnConditions,
            query,
            emptyResult,
            bulkLookupLeft,
            bulkLookupRight
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParameterizedQueryExec that = (ParameterizedQueryExec) o;
        return Objects.equals(output, that.output)
            && Objects.equals(matchFields, that.matchFields)
            && Objects.equals(joinOnConditions, that.joinOnConditions)
            && Objects.equals(query, that.query)
            && emptyResult == that.emptyResult
            && Objects.equals(bulkLookupLeft, that.bulkLookupLeft)
            && Objects.equals(bulkLookupRight, that.bulkLookupRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, matchFields, joinOnConditions, query, emptyResult, bulkLookupLeft, bulkLookupRight);
    }
}

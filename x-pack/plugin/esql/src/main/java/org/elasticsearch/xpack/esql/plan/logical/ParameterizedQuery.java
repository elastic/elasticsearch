/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a parameterized lookup query source.
 * This replaces {@link EsRelation} in the lookup node's logical plan,
 * representing a source that receives input pages (match keys) from the
 * exchange and generates parameterized queries against the lookup index.
 */
public class ParameterizedQuery extends LeafPlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "ParameterizedQuery",
        ParameterizedQuery::readFrom
    );

    private final List<Attribute> output;
    private final List<MatchConfig> matchFields;
    @Nullable
    private final Expression joinOnConditions;
    /**
     * Runtime-only flag set by the {@link org.elasticsearch.xpack.esql.optimizer.LookupLogicalOptimizer}
     * when a filter folds to {@code false}/{@code null}. Not serialized — it is computed locally on the
     * lookup node after deserialization.
     */
    private final boolean emptyResult;

    public ParameterizedQuery(Source source, List<Attribute> output, List<MatchConfig> matchFields, @Nullable Expression joinOnConditions) {
        this(source, output, matchFields, joinOnConditions, false);
    }

    public ParameterizedQuery(
        Source source,
        List<Attribute> output,
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        boolean emptyResult
    ) {
        super(source);
        this.output = output;
        this.matchFields = matchFields;
        this.joinOnConditions = joinOnConditions;
        this.emptyResult = emptyResult;
    }

    private static ParameterizedQuery readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        List<Attribute> output = in.readNamedWriteableCollectionAsList(Attribute.class);
        List<MatchConfig> matchFields = in.readCollectionAsList(MatchConfig::new);
        Expression joinOnConditions = in.readOptionalNamedWriteable(Expression.class);
        return new ParameterizedQuery(source, output, matchFields, joinOnConditions);
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

    public boolean emptyResult() {
        return emptyResult;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output);
        out.writeCollection(matchFields);
        out.writeOptionalNamedWriteable(joinOnConditions);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<ParameterizedQuery> info() {
        return NodeInfo.create(this, ParameterizedQuery::new, output, matchFields, joinOnConditions, emptyResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, matchFields, joinOnConditions, emptyResult);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ParameterizedQuery other = (ParameterizedQuery) obj;
        return Objects.equals(output, other.output)
            && Objects.equals(matchFields, other.matchFields)
            && Objects.equals(joinOnConditions, other.joinOnConditions)
            && emptyResult == other.emptyResult;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Unresolved leaf produced by the parser for the {@code EQL "<query>" [WITH {...}]} source command.
 *
 * <p>The command delegates execution to the EQL engine rather than re-implementing EQL as an ES|QL
 * logical plan. The {@code query} expression carries the raw EQL query string (a string literal or a
 * parameter reference); {@code options} carries the folded {@code WITH {...}} configuration (target
 * indices, size, timestamp field, etc.).
 *
 * <p>Resolution happens in the analyzer ({@code ResolveEqlRelation}): the EQL query string is parsed
 * to determine the result mode (event / sequence / sample) and — for sequence and sample queries —
 * the number of stages, which together fix the output schema. No field-caps round-trip is needed
 * because the event payload is exposed as an opaque {@code _source} column; the EQL engine validates
 * referenced fields at execution time and surfaces any errors through the search response.
 *
 * @see EqlRelation the resolved counterpart carrying the fixed output schema.
 */
public final class UnresolvedEqlRelation extends LeafPlan implements Unresolvable {

    private final Expression query;
    private final Map<String, Object> options;
    private final String unresolvedMsg;

    public UnresolvedEqlRelation(Source source, Expression query, Map<String, Object> options) {
        super(source);
        this.query = query;
        this.options = options;
        this.unresolvedMsg = "Unresolved EQL query [" + query.sourceText() + "]";
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<UnresolvedEqlRelation> info() {
        return NodeInfo.create(this, UnresolvedEqlRelation::new, query, options);
    }

    public Expression query() {
        return query;
    }

    public Map<String, Object> options() {
        return options;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public int hashCode() {
        // No source(): equals() below ignores it, and equal nodes must hash equal.
        return Objects.hash(query, options, unresolvedMsg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnresolvedEqlRelation other = (UnresolvedEqlRelation) obj;
        return Objects.equals(query, other.query)
            && Objects.equals(options, other.options)
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }

    @Override
    public List<Object> nodeProperties() {
        return Collections.singletonList(query);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + "EQL[" + query.sourceText() + "]";
    }
}

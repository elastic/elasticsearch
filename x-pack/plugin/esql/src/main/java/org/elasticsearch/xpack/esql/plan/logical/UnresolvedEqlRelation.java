/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Unresolved leaf produced by the parser for the {@code EQL <indexPattern> "<query>" [WITH {...}]} source command.
 *
 * <p>The command delegates execution to the EQL engine rather than re-implementing EQL as an ES|QL logical plan.
 * {@code indexPattern} is the target index pattern — a first-class leading argument like {@code FROM}, not a
 * {@code WITH} option; {@code query} carries the raw EQL query string (a literal or parameter reference);
 * {@code options} carries the folded {@code WITH {...}} tuning.
 *
 * <p>Resolution happens in the analyzer ({@code ResolveEqlRelation}): the index pattern rides the shared
 * field-caps path (the same {@code IndexResolver}/{@code IndexResolution} {@code FROM} uses) to a typed output
 * schema, and the EQL query string is parsed to determine the result mode (event / sequence / sample), which
 * prepends the sequence synthetics. {@code metadataFields} carries the declared {@code METADATA} attributes
 * (e.g. {@code _index}, {@code _id}, {@code _source}); the analyzer validates which ones the EQL delegate can
 * populate and appends them to the output, last.
 *
 * @see EqlRelation the resolved counterpart carrying the typed output schema.
 */
public final class UnresolvedEqlRelation extends LeafPlan implements Unresolvable, TelemetryAware {

    private final IndexPattern indexPattern;
    private final Expression query;
    private final Map<String, Object> options;
    private final List<NamedExpression> metadataFields;
    private final String unresolvedMsg;

    public UnresolvedEqlRelation(
        Source source,
        IndexPattern indexPattern,
        Expression query,
        Map<String, Object> options,
        List<NamedExpression> metadataFields
    ) {
        this(source, indexPattern, query, options, metadataFields, "Unresolved EQL query [" + query.sourceText() + "]");
    }

    public UnresolvedEqlRelation(
        Source source,
        IndexPattern indexPattern,
        Expression query,
        Map<String, Object> options,
        List<NamedExpression> metadataFields,
        String unresolvedMsg
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.query = query;
        this.options = options;
        this.metadataFields = metadataFields;
        this.unresolvedMsg = unresolvedMsg;
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
        return NodeInfo.create(this, UnresolvedEqlRelation::new, indexPattern, query, options, metadataFields, unresolvedMsg);
    }

    public IndexPattern indexPattern() {
        return indexPattern;
    }

    public Expression query() {
        return query;
    }

    public Map<String, Object> options() {
        return options;
    }

    public List<NamedExpression> metadataFields() {
        return metadataFields;
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

    /** Telemetry walks the pre-analysis plan, where this node (not {@link EqlRelation}) represents the command. */
    @Override
    public String telemetryLabel() {
        return "EQL";
    }

    @Override
    public int hashCode() {
        // No source(): equals() below ignores it, and equal nodes must hash equal.
        return Objects.hash(indexPattern, query, options, metadataFields, unresolvedMsg);
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
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(query, other.query)
            && Objects.equals(options, other.options)
            && Objects.equals(metadataFields, other.metadataFields)
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

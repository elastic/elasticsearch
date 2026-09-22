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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A wrapper whose main purpose is to keep track of {@code METADATA} fields from a {@code FROM} command.
 * It helps to act on fields that were requested, but weren't produced by the child plan, by e.g. filling
 * them with nulls in the Analyzer.
 * <p>
 * This is a transient, parse-time node: the Analyzer must consume every instance. It is {@link Unresolvable} so that
 * an instance which survives analysis is reported by the Verifier as a user-facing error rather than surfacing as an
 * "unsupported logical plan node" failure in the physical planner.
 */
public class UnresolvedMetadata extends UnaryPlan implements Unresolvable {

    private final List<NamedExpression> metadataFields;

    public UnresolvedMetadata(Source source, LogicalPlan child, List<NamedExpression> metadataFields) {
        super(source, child);
        this.metadataFields = metadataFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnresolvedMetadata is a transient parse-time node and is never serialized");
    }

    @Override
    public String getWriteableName() {
        return "UnresolvedMetadata";
    }

    public List<NamedExpression> metadataFields() {
        return metadataFields;
    }

    @Override
    protected NodeInfo<UnresolvedMetadata> info() {
        return NodeInfo.create(this, UnresolvedMetadata::new, child(), metadataFields);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new UnresolvedMetadata(source(), newChild, metadataFields);
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    /**
     * Never resolved, like every other {@link Unresolvable} plan node. Parents must not resolve against this node's
     * output: the Analyzer may still add the requested {@code METADATA} columns below it, and e.g. a {@code KEEP}
     * resolved too early would report them as unknown.
     */
    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public String unresolvedMessage() {
        List<NamedExpression> unresolved = metadataFields.stream().filter(e -> e.resolved() == false).toList();
        if (unresolved.isEmpty() == false) {
            return "unresolved metadata fields: " + unresolved;
        }
        return "metadata fields "
            + metadataFields.stream().map(NamedExpression::name).toList()
            + " could not be applied to the underlying plan";
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), metadataFields);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UnresolvedMetadata other = (UnresolvedMetadata) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(metadataFields, other.metadataFields);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append("[");
        sb.append(metadataFields.stream().map(NamedExpression::name).collect(Collectors.joining(",")));
        sb.append("]");
    }
}

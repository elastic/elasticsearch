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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * A table function standing in a source list: {@code FROM foo, SOMEFUNC(a_name, {"key": "value"})}.
 * The parser neither resolves the name nor checks the call against any signature — it does not know what functions
 * exist — so the call is parked here, and {@code TableFunctionResolver} replaces this leaf during analysis with
 * whatever relation the named function returns.
 * <p>
 * Deliberately NOT a {@link UnresolvedSourceRelation}: that marker is for traversals wanting any FROM-style leaf, and
 * all of them run after the rewrite, so joining it would add a branch per site for a shape that cannot arrive. Being
 * {@link Unresolvable} is what makes that safe rather than assumed: a skipped rewrite fails the query rather than
 * quietly reading nothing. Which stage reports it is not guaranteed — a leaf reaching the planner is refused by the
 * telemetry mapper before the verifier sees it — so the property to rely on is that it fails, not where.
 */
public final class UnresolvedTableFunctionRelation extends LeafPlan implements Unresolvable {

    private final TableFunctionInvocation call;
    private final List<NamedExpression> metadataFields;

    /**
     * @param call the call exactly as written: its name is not yet known to denote anything, and its arguments
     *             have not been checked against a signature
     * @param metadataFields the source command's {@code METADATA} names, verbatim, so a table function carries
     *                       metadata exactly as a pattern does
     */
    public UnresolvedTableFunctionRelation(Source source, TableFunctionInvocation call, List<NamedExpression> metadataFields) {
        super(source);
        this.call = Objects.requireNonNull(call, "call");
        this.metadataFields = Objects.requireNonNull(metadataFields, "metadataFields");
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
    protected NodeInfo<UnresolvedTableFunctionRelation> info() {
        return NodeInfo.create(this, UnresolvedTableFunctionRelation::new, call, metadataFields);
    }

    public TableFunctionInvocation call() {
        return call;
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
        return List.of();
    }

    @Override
    public String unresolvedMessage() {
        return "Unresolved table function [" + call.name() + "]";
    }

    @Override
    public int hashCode() {
        // No source(): equals() ignores it, and equal nodes must hash equal.
        return Objects.hash(call, metadataFields);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnresolvedTableFunctionRelation other = (UnresolvedTableFunctionRelation) obj;
        return Objects.equals(call, other.call) && Objects.equals(metadataFields, other.metadataFields);
    }

    @Override
    public List<Object> nodeProperties() {
        // The arguments are omitted, the map above all: a map argument can carry a credential-bearing setting,
        // and nodeProperties() reaches EXPLAIN. The name alone is what identifies the leaf to a reader.
        return List.of(call.name());
    }

    @Override
    public String toString() {
        String metadataSuffix = metadataFields.isEmpty() ? "" : " METADATA " + metadataFields;
        return UNRESOLVED_PREFIX + call.name().toUpperCase(Locale.ROOT) + "(...)" + metadataSuffix;
    }
}

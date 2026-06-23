/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * First-class representation of a {@code FROM <view>} reference in the logical plan.
 * <p>
 * A {@code View} is the boundary that today's {@code ViewResolver} substitution erases too early. It is a <b>leaf</b>
 * (the body is held as a field, <i>not</i> a tree child) so the boundary is opaque by default — the optimizer stops at
 * the view and does not push through it until the {@code InlineView} rule deliberately expands it. This is the Calcite
 * "expand on demand" shape rather than Spark's unary-wrap-the-child, chosen because a remote or materialized view has no
 * local body to hang as a child: the body slot is a locally-resolved {@link LogicalPlan} for a local view, and becomes a
 * remote handle / is absent for remote and materialized views (added in later phases).
 * <p>
 * The node carries the view's <b>output schema</b> ({@link #output()}) as the contract callers plan against. Per
 * ES|QL's dynamic-schema model the schema is resolved for <i>this</i> query, not frozen at view-creation time.
 * <p>
 * <b>Transient:</b> in the parity phase every {@code View} is inlined by {@code InlineView} before physical mapping, so it
 * never crosses the wire — {@link #writeTo} therefore throws. Wire-serialization (with a {@code TransportVersion} gate)
 * lands with the remote-view phase, alongside the contract + rights-mode fields.
 */
public class View extends LeafPlan {

    private final String name;
    private final LogicalPlan body;
    private final List<Attribute> output;

    public View(Source source, String name, LogicalPlan body, List<Attribute> output) {
        super(source);
        this.name = name;
        this.body = body;
        this.output = output;
    }

    /** The view name as written in the query (its identity). */
    public String viewName() {
        return name;
    }

    /** The resolved plan of the view's stored query — the implementation the {@code InlineView} rule folds in. */
    public LogicalPlan body() {
        return body;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        // A View is produced already resolved (body attached, schema pinned). The body's own resolution is independent
        // and verified where it is analyzed; the leaf itself has no expressions of its own to resolve.
        return true;
    }

    @Override
    protected NodeInfo<View> info() {
        return NodeInfo.create(this, View::new, name, body, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("View must be inlined before serialization");
    }

    @Override
    public String getWriteableName() {
        // Not registered in PlanWritables while transient; a stable name for diagnostics until the remote-view phase
        // adds real serialization.
        return "View";
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append('[').append(name).append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, body, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        View other = (View) obj;
        return Objects.equals(name, other.name) && Objects.equals(body, other.body) && Objects.equals(output, other.output);
    }
}

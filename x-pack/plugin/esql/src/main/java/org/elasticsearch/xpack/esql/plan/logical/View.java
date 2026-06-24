/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Objects;

/**
 * First-class representation of a {@code FROM <view>} reference in the logical plan — the boundary today's
 * {@code ViewResolver} substitution erases too early.
 * <p>
 * For a <b>local</b> view this is a <b>unary</b> node wrapping the view's resolved body as its child. Keeping the body a
 * real child (rather than a leaf-with-held-body) is what preserves parity: {@code PreAnalyzer} collects field-caps
 * patterns by walking the tree's naked relation leaves ({@code forEachUp(UnresolvedRelation.class, ...)}), and the
 * analyzer's {@code ResolveTable} resolves them — both descend into a child but not into a leaf's hidden field. So the
 * existing resolution chain sees through the {@code View} unchanged, and {@code output()} delegates to the body's output
 * (its schema). This is Spark Catalyst's {@code View} + {@code EliminateView} shape. A <b>remote</b> or
 * <b>materialized</b> view — which has no local child to wrap — is a later variant (a handle in place of the child).
 * <p>
 * The view is opaque to the optimizer only after analysis: {@code InlineView} folds it into its body (the peer-through
 * strategy) before the pushdown rules run, reproducing today's behaviour; the keep-opaque alternative is the
 * boundary-aware phase.
 * <p>
 * <b>Transient:</b> every {@code View} is inlined before physical mapping, so it never crosses the wire —
 * {@link #writeTo} throws and the node is not registered in {@code PlanWritables}. Wire-serialization (with a
 * {@code TransportVersion} gate), the contract/rights-mode fields, and the remote handle land in later phases.
 */
public class View extends UnaryPlan {

    private final String name;

    public View(Source source, String name, LogicalPlan body) {
        super(source, body);
        this.name = name;
    }

    /** The view name as written in the query (its identity). */
    public String viewName() {
        return name;
    }

    /** The resolved plan of the view's stored query — the implementation the {@code InlineView} rule folds in. */
    public LogicalPlan body() {
        return child();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new View(source(), name, newChild);
    }

    @Override
    public boolean expressionsResolved() {
        // A View wraps an already-resolved body and has no expressions of its own; resolution of the body is governed
        // by childrenResolved().
        return true;
    }

    @Override
    protected NodeInfo<View> info() {
        return NodeInfo.create(this, View::new, name, child());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("View must be inlined before serialization");
    }

    @Override
    public String getWriteableName() {
        // Not registered in PlanWritables while transient; a stable name for diagnostics until the remote-view phase.
        return "View";
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // Route the view name through the mapper so anonymization can mask it — a view name can be
        // as sensitive as an index name. A raw append here would leak it into the plan string.
        sb.append(nodeName()).append('[').append(mapper.index(name)).append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, child());
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
        return Objects.equals(name, other.name) && Objects.equals(child(), other.child());
    }
}

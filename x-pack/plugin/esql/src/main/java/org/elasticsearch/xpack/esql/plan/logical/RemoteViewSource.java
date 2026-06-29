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
 * First-class logical node the boundary-aware view rule lowers a {@code REMOTE} {@link View} to. Unlike a {@code LOCAL}
 * view — which {@code InlineView} folds into its body so the pushdown rules see through it — a remote view's body must
 * <b>not</b> execute locally: it runs on its home cluster and the coordinator only ever sees the resolved schema. So the
 * boundary survives optimization as this opaque leaf, carrying the remote-execution {@code handle} (the cluster the view
 * lives on, taken from the federation {@code resolve_schema} seam) and the resolved output {@link Attribute}s — the merge
 * currency the rest of the plan already resolves against.
 * <p>
 * It is a {@link LeafPlan}: the view's body is gone from the local tree (it executes remotely), only its schema remains.
 * The {@code Mapper} lowers this to {@code RemoteViewExec}, whose source operator is a POC stub.
 * <p>
 * <b>Transient (POC):</b> like {@link View} this is not wire-serialized yet ({@link #writeTo} throws); the cross-cluster
 * dispatch of the remote body lands with the federation execution leg.
 */
public class RemoteViewSource extends LeafPlan {

    private final String viewName;
    private final String handle;
    private final List<Attribute> output;

    /**
     * @param viewName the view's name as written in the query (its identity)
     * @param handle   the remote-execution handle — the home cluster on which the view's body runs (federation seam)
     * @param output   the view's resolved output schema, the only thing the coordinator sees of a remote view
     */
    public RemoteViewSource(Source source, String viewName, String handle, List<Attribute> output) {
        super(source);
        this.viewName = viewName;
        this.handle = handle;
        this.output = output;
    }

    /** The view name as written in the query (its identity). */
    public String viewName() {
        return viewName;
    }

    /** The remote-execution handle: the home cluster on which the view body runs (from the federation seam). */
    public String handle() {
        return handle;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<RemoteViewSource> info() {
        return NodeInfo.create(this, RemoteViewSource::new, viewName, handle, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RemoteViewSource is a transient POC node and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "RemoteViewSource";
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // A view name and a remote cluster handle can be as sensitive as an index name — route both through the mapper.
        sb.append(nodeName()).append('[').append(mapper.index(viewName)).append("]@[").append(mapper.index(handle)).append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewName, handle, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteViewSource other = (RemoteViewSource) obj;
        return Objects.equals(viewName, other.viewName) && Objects.equals(handle, other.handle) && Objects.equals(output, other.output);
    }
}

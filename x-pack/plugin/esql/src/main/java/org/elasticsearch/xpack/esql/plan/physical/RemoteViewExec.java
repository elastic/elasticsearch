/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.RemoteViewSource;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical lowering of a {@code REMOTE} view, mapped from {@link RemoteViewSource}. Represents "run this view's body on
 * its home cluster and stream back its rows". It carries the remote {@code handle} (home cluster) and the resolved
 * output schema.
 * <p>
 * <b>POC stub:</b> the architecture is the deliverable — a {@code REMOTE} {@link org.elasticsearch.xpack.esql.plan.logical.View}
 * survives analysis, the boundary-aware rule decides to keep it opaque and lowers it to {@link RemoteViewSource}, and the
 * {@code Mapper} lowers that here. The actual cross-cluster body dispatch + source operator are <b>not built</b>; this node
 * has no real execution and is not wire-serialized ({@link #writeTo} throws). A real source operator would dispatch the
 * remote body via the federation execution leg and adapt the foreign result into pages.
 */
public class RemoteViewExec extends LeafExec {

    private final String viewName;
    private final String handle;
    private final List<Attribute> output;

    public RemoteViewExec(Source source, String viewName, String handle, List<Attribute> output) {
        super(source);
        this.viewName = viewName;
        this.handle = handle;
        this.output = output;
    }

    public String viewName() {
        return viewName;
    }

    public String handle() {
        return handle;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<RemoteViewExec> info() {
        return NodeInfo.create(this, RemoteViewExec::new, viewName, handle, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RemoteViewExec is a POC stub with no execution and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "RemoteViewExec";
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
        RemoteViewExec other = (RemoteViewExec) obj;
        return Objects.equals(viewName, other.viewName) && Objects.equals(handle, other.handle) && Objects.equals(output, other.output);
    }
}

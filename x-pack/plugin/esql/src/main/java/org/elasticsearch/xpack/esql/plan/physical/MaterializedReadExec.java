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
import org.elasticsearch.xpack.esql.plan.logical.MaterializedReadSource;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical lowering of a {@code MATERIALIZED} view, mapped from {@link MaterializedReadSource}. Represents "read the
 * precomputed backing store" in place of executing the view body. It carries the {@code backingIndex} ref and the
 * resolved output schema.
 * <p>
 * <b>POC stub:</b> the architecture is the deliverable — a {@code MATERIALIZED}
 * {@link org.elasticsearch.xpack.esql.plan.logical.View} survives analysis, the boundary-aware rule decides to read the
 * backing store and lowers it to {@link MaterializedReadSource}, and the {@code Mapper} lowers that here. The actual
 * backing-store source operator is <b>not built</b>; this node has no real execution and is not wire-serialized
 * ({@link #writeTo} throws). A real lowering could instead rewrite to an {@code EsRelation}/{@code EsSourceExec} over the
 * backing index so it reuses the indexed read path wholesale.
 */
public class MaterializedReadExec extends LeafExec {

    private final String viewName;
    private final String backingIndex;
    private final List<Attribute> output;

    public MaterializedReadExec(Source source, String viewName, String backingIndex, List<Attribute> output) {
        super(source);
        this.viewName = viewName;
        this.backingIndex = backingIndex;
        this.output = output;
    }

    public String viewName() {
        return viewName;
    }

    public String backingIndex() {
        return backingIndex;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<MaterializedReadExec> info() {
        return NodeInfo.create(this, MaterializedReadExec::new, viewName, backingIndex, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("MaterializedReadExec is a POC stub with no execution and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "MaterializedReadExec";
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewName, backingIndex, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaterializedReadExec other = (MaterializedReadExec) obj;
        return Objects.equals(viewName, other.viewName)
            && Objects.equals(backingIndex, other.backingIndex)
            && Objects.equals(output, other.output);
    }
}

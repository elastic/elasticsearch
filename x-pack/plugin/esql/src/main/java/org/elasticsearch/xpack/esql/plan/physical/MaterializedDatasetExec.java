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
import org.elasticsearch.xpack.esql.plan.logical.Dataset;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical lowering of a {@code MATERIALIZED} dataset, mapped from a {@link Dataset.Boundary#MATERIALIZED}
 * {@link Dataset}. Represents "read the precomputed backing store" in place of reading the dataset's external source at
 * query time. It carries the {@code backingIndex} ref and the resolved output schema.
 * <p>
 * <b>POC stub:</b> the architecture is the deliverable — a {@code MATERIALIZED} {@link Dataset} survives analysis and the
 * {@code Mapper} lowers it here. The actual backing-store source operator is <b>not built</b>; this node has no real
 * execution and is not wire-serialized ({@link #writeTo} throws). A real lowering could instead rewrite to an
 * {@code EsRelation}/{@code EsSourceExec} over the backing index so it reuses the indexed read path wholesale. Mirrors
 * {@code MaterializedReadExec}.
 */
public class MaterializedDatasetExec extends LeafExec {

    private final String datasetName;
    private final String backingIndex;
    private final List<Attribute> output;

    public MaterializedDatasetExec(Source source, String datasetName, String backingIndex, List<Attribute> output) {
        super(source);
        this.datasetName = datasetName;
        this.backingIndex = backingIndex;
        this.output = output;
    }

    public String datasetName() {
        return datasetName;
    }

    public String backingIndex() {
        return backingIndex;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<MaterializedDatasetExec> info() {
        return NodeInfo.create(this, MaterializedDatasetExec::new, datasetName, backingIndex, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("MaterializedDatasetExec is a POC stub with no execution and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "MaterializedDatasetExec";
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetName, backingIndex, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaterializedDatasetExec other = (MaterializedDatasetExec) obj;
        return Objects.equals(datasetName, other.datasetName)
            && Objects.equals(backingIndex, other.backingIndex)
            && Objects.equals(output, other.output);
    }
}

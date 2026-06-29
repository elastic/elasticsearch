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
 * Physical lowering of a {@code REMOTE} dataset, mapped from a {@link Dataset.Boundary#REMOTE} {@link Dataset}. Represents "read
 * this dataset from its home cluster and stream back its rows" — unlike a {@code LOCAL} dataset, whose external read runs
 * on the coordinator, a remote dataset's data must <b>not</b> be read locally. It carries the remote {@code handle} (home
 * cluster, from the federation {@code resolve_schema} seam) and the resolved output schema — the merge currency the rest
 * of the plan already resolves against.
 * <p>
 * <b>POC stub:</b> the architecture is the deliverable — a {@code REMOTE} {@link Dataset} survives analysis and the
 * {@code Mapper} lowers it here. The actual cross-cluster dispatch + source operator are <b>not built</b>; this node has
 * no real execution and is not wire-serialized ({@link #writeTo} throws). Mirrors {@code RemoteViewExec}.
 */
public class RemoteDatasetExec extends LeafExec {

    private final String datasetName;
    private final String handle;
    private final List<Attribute> output;

    public RemoteDatasetExec(Source source, String datasetName, String handle, List<Attribute> output) {
        super(source);
        this.datasetName = datasetName;
        this.handle = handle;
        this.output = output;
    }

    public String datasetName() {
        return datasetName;
    }

    public String handle() {
        return handle;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<RemoteDatasetExec> info() {
        return NodeInfo.create(this, RemoteDatasetExec::new, datasetName, handle, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RemoteDatasetExec is a POC stub with no execution and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "RemoteDatasetExec";
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetName, handle, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteDatasetExec other = (RemoteDatasetExec) obj;
        return Objects.equals(datasetName, other.datasetName)
            && Objects.equals(handle, other.handle)
            && Objects.equals(output, other.output);
    }
}

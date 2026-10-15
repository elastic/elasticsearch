/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Leaf that reads rows from an incoming exchange edge of a non-coordinator stage. */
public class StageSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "StageSourceExec",
        StageSourceExec::new
    );

    private final List<Attribute> output;
    private final boolean intermediateAgg;
    private final String edgeKey;

    public StageSourceExec(Source source, List<Attribute> output, boolean intermediateAgg, String edgeKey) {
        super(source);
        this.output = output;
        this.intermediateAgg = intermediateAgg;
        this.edgeKey = edgeKey;
    }

    private StageSourceExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output);
        out.writeBoolean(intermediateAgg);
        out.writeString(edgeKey);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public boolean isIntermediateAgg() {
        return intermediateAgg;
    }

    public String edgeKey() {
        return edgeKey;
    }

    @Override
    protected NodeInfo<StageSourceExec> info() {
        return NodeInfo.create(this, StageSourceExec::new, output, intermediateAgg, edgeKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StageSourceExec other = (StageSourceExec) obj;
        return output.equals(other.output) && intermediateAgg == other.intermediateAgg && edgeKey.equals(other.edgeKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, intermediateAgg, edgeKey);
    }
}

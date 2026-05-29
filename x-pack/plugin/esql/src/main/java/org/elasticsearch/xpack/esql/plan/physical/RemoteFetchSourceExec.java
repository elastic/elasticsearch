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

/**
 * Leaf source used to compile remote-fetch post-fetch pushdown fragments.
 * <p>
 * The final output attribute is the synthetic position-mapping attribute. It maps each row that survives pushdown back to its
 * original input position and must stay aligned with the final block produced by the remote-fetch pushdown compiler.
 */
public class RemoteFetchSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RemoteFetchSourceExec",
        RemoteFetchSourceExec::new
    );

    private final List<Attribute> output;

    public RemoteFetchSourceExec(Source source, List<Attribute> output) {
        super(source);
        this.output = List.copyOf(output);
    }

    private RemoteFetchSourceExec(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Attribute.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<RemoteFetchSourceExec> info() {
        return NodeInfo.create(this, RemoteFetchSourceExec::new, output);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteFetchSourceExec other = (RemoteFetchSourceExec) obj;
        return Objects.equals(output, other.output);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

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
 * Logical source describing the initial remote-fetch fragment.
 * <p>
 * This node is serialized to remote nodes inside a {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}
 * and then converted into physical operators locally on the target node.
 */
public class RemoteFetchSource extends LeafPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "RemoteFetchSource",
        RemoteFetchSource::readFrom
    );

    /**
     * Name of the synthetic attribute that carries the input-position mapping through pushdown execution.
     * <p>
     * When a pushdown fragment is shipped to the data node, the last attribute of this source's output must be the
     * position-mapping attribute with this name, matching the trailing position channel that the data-node pipeline
     * appends to fetched pages.
     */
    public static final String POSITION_ATTRIBUTE_NAME = "_remote_fetch_position";

    private final List<Attribute> output;

    public RemoteFetchSource(Source source, List<Attribute> output) {
        super(source);
        this.output = Objects.requireNonNull(output, "output");
    }

    private static RemoteFetchSource readFrom(StreamInput in) throws IOException {
        return new RemoteFetchSource(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Attribute.class));
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
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<RemoteFetchSource> info() {
        return NodeInfo.create(this, RemoteFetchSource::new, output);
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
        RemoteFetchSource other = (RemoteFetchSource) obj;
        return Objects.equals(output, other.output);
    }
}

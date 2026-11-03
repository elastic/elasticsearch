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
 * The leaf of a fetch plan. On the node that owns the documents a fetch request names, it produces {@code _doc} rows
 * for exactly those documents, so the operators above it load fields for them and for nothing else. It never runs
 * on the coordinator, which builds the fetch plan and ships it inside the fetch request.
 */
public final class FetchSourceExec extends LeafExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FetchSourceExec",
        FetchSourceExec::new
    );

    private final Attribute doc;
    private final Integer estimatedRowSize;

    /**
     * @param doc the {@code _doc} attribute the source produces. Only meaningful inside the fetch plan.
     */
    public FetchSourceExec(Source source, Attribute doc, Integer estimatedRowSize) {
        super(source);
        this.doc = doc;
        this.estimatedRowSize = estimatedRowSize;
    }

    private FetchSourceExec(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Attribute.class), in.readOptionalVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(doc);
        out.writeOptionalVInt(estimatedRowSize);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute doc() {
        return doc;
    }

    /**
     * Estimate of the bytes per row of the pages the fetch plan builds, set by {@link EstimatesRowSize}.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public List<Attribute> output() {
        return List.of(doc);
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, output());
        int size = state.consumeAllFields(false);
        return Objects.equals(size, estimatedRowSize) ? this : new FetchSourceExec(source(), doc, size);
    }

    @Override
    protected NodeInfo<FetchSourceExec> info() {
        return NodeInfo.create(this, FetchSourceExec::new, doc, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FetchSourceExec other = (FetchSourceExec) obj;
        return doc.equals(other.doc) && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(doc, estimatedRowSize);
    }
}

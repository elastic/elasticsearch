/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Serialized planning contract for a fetch phase at an exchange boundary.
 * <p>
 * The child produces node-local document references and eager fields. Reduction planning consumes this node and replaces those
 * references with {@link #handleAttribute()} before pages cross the exchange. This node must not reach execution.
 */
public final class FetchBoundaryExec extends UnaryExec {
    public static final TransportVersion ESQL_FETCH_BOUNDARY = TransportVersion.fromName("esql_fetch_boundary");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FetchBoundaryExec",
        FetchBoundaryExec::new
    );

    private final Attribute handleAttribute;
    private final List<Attribute> handoffOutput;

    public FetchBoundaryExec(Source source, PhysicalPlan child, Attribute handleAttribute, List<Attribute> handoffOutput) {
        super(source, child);
        this.handleAttribute = Objects.requireNonNull(handleAttribute);
        this.handoffOutput = List.copyOf(handoffOutput);
        if (this.handoffOutput.contains(handleAttribute) == false) {
            throw new IllegalArgumentException("fetch handoff output must contain handle attribute [" + handleAttribute + "]");
        }
    }

    private FetchBoundaryExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(handleAttribute);
        out.writeNamedWriteableCollection(handoffOutput);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute handleAttribute() {
        return handleAttribute;
    }

    public List<Attribute> handoffOutput() {
        return handoffOutput;
    }

    public boolean requiresRetainedSearchContexts() {
        return true;
    }

    public TransportVersion minimumTransportVersion() {
        return ESQL_FETCH_BOUNDARY;
    }

    @Override
    public List<Attribute> output() {
        return handoffOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<FetchBoundaryExec> info() {
        return NodeInfo.create(this, FetchBoundaryExec::new, child(), handleAttribute, handoffOutput);
    }

    @Override
    public FetchBoundaryExec replaceChild(PhysicalPlan newChild) {
        return new FetchBoundaryExec(source(), newChild, handleAttribute, handoffOutput);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        sb.append("[handle=").append(handleAttribute.toString(format, mapper));
        sb.append(", handoffOutput=").append(handoffOutput.stream().map(attribute -> attribute.toString(format, mapper)).toList());
        sb.append(", requiresRetainedSearchContexts=true]");
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        FetchBoundaryExec other = (FetchBoundaryExec) obj;
        return handleAttribute.equals(other.handleAttribute) && handoffOutput.equals(other.handoffOutput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), handleAttribute, handoffOutput);
    }
}

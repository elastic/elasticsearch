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
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plugin.RemoteFetchHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Transport-safe contract between remote-fetch planning on the coordinator and reduction planning on a data node.
 * <p>
 * The child produces a node-local document reference followed by eager fields. Before rows cross the exchange, data-node reduction
 * replaces the document reference with the opaque handle while preserving the eager fields. This node is a planning boundary and must
 * be consumed before execution.
 */
public final class RemoteFetchBoundaryExec extends UnaryExec {
    public static final TransportVersion ESQL_REMOTE_FETCH_TOPN_REDUCTION = TransportVersion.fromName("esql_remote_fetch_topn_reduction");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RemoteFetchBoundaryExec",
        RemoteFetchBoundaryExec::new
    );

    private final Attribute documentAttribute;
    private final Attribute handleAttribute;
    private final List<Attribute> eagerAttributes;
    private final List<Attribute> dataOutput;
    private final List<Attribute> handoffOutput;

    public RemoteFetchBoundaryExec(
        Source source,
        PhysicalPlan child,
        Attribute documentAttribute,
        Attribute handleAttribute,
        List<Attribute> eagerAttributes
    ) {
        super(source, child);
        this.documentAttribute = Objects.requireNonNull(documentAttribute, "documentAttribute");
        this.handleAttribute = Objects.requireNonNull(handleAttribute, "handleAttribute");
        this.eagerAttributes = List.copyOf(eagerAttributes);
        this.dataOutput = outputWith(documentAttribute, this.eagerAttributes);
        this.handoffOutput = outputWith(handleAttribute, this.eagerAttributes);
        validate();
    }

    private RemoteFetchBoundaryExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    private static List<Attribute> outputWith(Attribute first, List<Attribute> attributes) {
        List<Attribute> output = new ArrayList<>(attributes.size() + 1);
        output.add(first);
        output.addAll(attributes);
        return List.copyOf(output);
    }

    private void validate() {
        if (EsQueryExec.isDocAttribute(documentAttribute) == false) {
            throw new IllegalArgumentException("remote-fetch document attribute must be _doc but was [" + documentAttribute + "]");
        }
        if (RemoteFetchHandle.isRemoteFetchHandleCarrier(handleAttribute) == false) {
            throw new IllegalArgumentException("invalid remote-fetch handle attribute [" + handleAttribute + "]");
        }
        List<Attribute> boundaryAttributes = new ArrayList<>(eagerAttributes.size() + 2);
        boundaryAttributes.add(documentAttribute);
        boundaryAttributes.add(handleAttribute);
        boundaryAttributes.addAll(eagerAttributes);
        validateUniqueNameIds("boundary attributes", boundaryAttributes);
        validateUniqueNameIds("data output", dataOutput);
        validateUniqueNameIds("handoff output", handoffOutput);
        validateUniqueNameIds("child output", child().output());
        validateChildDataContract();
    }

    private void validateChildDataContract() {
        if (child().output().size() != dataOutput.size()) {
            throw new IllegalArgumentException(
                "child output must match remote-fetch data output; child [" + child().output() + "], data output [" + dataOutput + "]"
            );
        }
        for (int i = 0; i < dataOutput.size(); i++) {
            Attribute childAttribute = child().output().get(i);
            Attribute dataAttribute = dataOutput.get(i);
            if (childAttribute.id().equals(dataAttribute.id()) == false) {
                throw new IllegalArgumentException(
                    "child output must match remote-fetch data output; child [" + child().output() + "], data output [" + dataOutput + "]"
                );
            }
            if (childAttribute.equals(dataAttribute) == false) {
                throw new IllegalArgumentException(
                    "remote-fetch child/data NameId collision ["
                        + childAttribute.id()
                        + "] identifies both ["
                        + childAttribute
                        + "] and ["
                        + dataAttribute
                        + "]"
                );
            }
        }
    }

    private static void validateUniqueNameIds(String contract, List<Attribute> attributes) {
        Map<NameId, Attribute> attributesById = new HashMap<>();
        for (Attribute attribute : attributes) {
            Attribute previous = attributesById.putIfAbsent(attribute.id(), attribute);
            if (previous != null) {
                throw new IllegalArgumentException(
                    "remote-fetch "
                        + contract
                        + " NameId collision ["
                        + attribute.id()
                        + "] between ["
                        + previous
                        + "] and ["
                        + attribute
                        + "]"
                );
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(documentAttribute);
        out.writeNamedWriteable(handleAttribute);
        out.writeNamedWriteableCollection(eagerAttributes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute documentAttribute() {
        return documentAttribute;
    }

    public Attribute handleAttribute() {
        return handleAttribute;
    }

    public List<Attribute> eagerAttributes() {
        return eagerAttributes;
    }

    public List<Attribute> dataOutput() {
        return dataOutput;
    }

    public List<Attribute> handoffOutput() {
        return handoffOutput;
    }

    /**
     * Remote-fetch handles refer to shard contexts created by the initial compute and therefore require retention until fetch completes.
     */
    public boolean requiresRetainedSearchContexts() {
        return true;
    }

    /**
     * The first transport version that can deserialize this boundary.
     */
    public TransportVersion minimumTransportVersion() {
        return ESQL_REMOTE_FETCH_TOPN_REDUCTION;
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
    protected NodeInfo<RemoteFetchBoundaryExec> info() {
        return NodeInfo.create(this, RemoteFetchBoundaryExec::new, child(), documentAttribute, handleAttribute, eagerAttributes);
    }

    @Override
    public RemoteFetchBoundaryExec replaceChild(PhysicalPlan newChild) {
        return new RemoteFetchBoundaryExec(source(), newChild, documentAttribute, handleAttribute, eagerAttributes);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        sb.append("[document=").append(documentAttribute.toString(format, mapper));
        sb.append(", handle=").append(handleAttribute.toString(format, mapper));
        sb.append(", eager=").append(eagerAttributes.stream().map(attribute -> attribute.toString(format, mapper)).toList());
        sb.append(", requiresRetainedSearchContexts=true]");
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        RemoteFetchBoundaryExec other = (RemoteFetchBoundaryExec) obj;
        return documentAttribute.equals(other.documentAttribute)
            && handleAttribute.equals(other.handleAttribute)
            && eagerAttributes.equals(other.eagerAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), documentAttribute, handleAttribute, eagerAttributes);
    }
}

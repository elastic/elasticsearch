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
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Replaces the node local document identity {@code _doc} with a {@link DataType#DOC_REF} that stays valid after the
 * rows leave the node. It runs on the data node, in the node reduce stage, right before the rows cross the cluster
 * exchange. The reference takes the place of {@code _doc} in the output, every other column passes through.
 * <p>
 * It sits above the node level cut, so only the shards that still have rows after that cut are referenced. The
 * reader contexts of the other shards can be released as soon as the node has answered.
 */
public final class DocRefEncodeExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "DocRefEncodeExec",
        DocRefEncodeExec::new
    );

    private final Attribute doc;
    private final ReferenceAttribute docRef;
    private List<Attribute> lazyOutput;

    /**
     * @param doc    the {@link DataType#DOC_DATA_TYPE} attribute of the child to replace
     * @param docRef the {@link DataType#DOC_REF} attribute that replaces it, minted by the coordinator
     */
    public DocRefEncodeExec(Source source, PhysicalPlan child, Attribute doc, ReferenceAttribute docRef) {
        super(source, child);
        this.doc = doc;
        this.docRef = docRef;
    }

    private DocRefEncodeExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            (ReferenceAttribute) in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(doc);
        out.writeNamedWriteable(docRef);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute doc() {
        return doc;
    }

    public ReferenceAttribute docRef() {
        return docRef;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> childOutput = child().output();
            List<Attribute> output = new ArrayList<>(childOutput.size());
            for (Attribute attribute : childOutput) {
                output.add(attribute.id().equals(doc.id()) ? docRef : attribute);
            }
            lazyOutput = output;
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(doc);
    }

    @Override
    public DocRefEncodeExec replaceChild(PhysicalPlan newChild) {
        return new DocRefEncodeExec(source(), newChild, doc, docRef);
    }

    @Override
    protected NodeInfo<DocRefEncodeExec> info() {
        return NodeInfo.create(this, DocRefEncodeExec::new, child(), doc, docRef);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        DocRefEncodeExec other = (DocRefEncodeExec) obj;
        return doc.equals(other.doc) && docRef.equals(other.docRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), doc, docRef);
    }
}

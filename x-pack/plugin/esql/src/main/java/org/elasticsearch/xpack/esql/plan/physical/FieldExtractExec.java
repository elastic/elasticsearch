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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FieldExtractExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FieldExtractExec",
        FieldExtractExec::new
    );

    private final List<Attribute> attributesToExtract;
    private final Attribute sourceAttribute;
    /**
     * Attributes that many be extracted as doc values even if that makes them
     * less accurate. This is mostly used for geo fields which lose a lot of
     * precision in their doc values, but in some cases doc values provides
     * <strong>enough</strong> precision to do the job.
     * <p>
     *     This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> docValuesAttributes;

    private List<Attribute> lazyOutput;

    public FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract) {
        this(source, child, attributesToExtract, Set.of());
    }

    private FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract, Set<Attribute> docValuesAttributes) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);
        this.docValuesAttributes = docValuesAttributes;
    }

    private FieldExtractExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
        // docValueAttributes are only used on the data node and never serialized.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(attributesToExtract());
        // docValueAttributes are only used on the data node and never serialized.
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public static Attribute extractSourceAttributesFrom(PhysicalPlan plan) {
        for (Attribute attribute : plan.outputSet()) {
            if (EsQueryExec.isSourceAttribute(attribute)) {
                return attribute;
            }
        }
        return null;
    }

    @Override
    protected AttributeSet computeReferences() {
        AttributeSet required = new AttributeSet(docValuesAttributes);

        required.add(sourceAttribute);
        required.addAll(attributesToExtract);

        return required;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract, docValuesAttributes);
    }

    public FieldExtractExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new FieldExtractExec(source(), child(), attributesToExtract, docValuesAttributes);
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public Attribute sourceAttribute() {
        return sourceAttribute;
    }

    public Set<Attribute> docValuesAttributes() {
        return docValuesAttributes;
    }

    public boolean hasDocValuesAttribute(Attribute attr) {
        return docValuesAttributes.contains(attr);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> childOutput = child().output();
            lazyOutput = new ArrayList<>(childOutput.size() + attributesToExtract.size());
            lazyOutput.addAll(childOutput);
            lazyOutput.addAll(attributesToExtract);
        }

        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, attributesToExtract);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributesToExtract, docValuesAttributes, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FieldExtractExec other = (FieldExtractExec) obj;
        return Objects.equals(attributesToExtract, other.attributesToExtract)
            && Objects.equals(docValuesAttributes, other.docValuesAttributes)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return nodeName() + NodeUtils.limitedToString(attributesToExtract) + "<" + NodeUtils.limitedToString(docValuesAttributes) + ">";
    }

}

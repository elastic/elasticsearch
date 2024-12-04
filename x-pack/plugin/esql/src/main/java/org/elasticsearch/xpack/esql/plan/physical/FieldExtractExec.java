/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
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
    private final @Nullable Attribute sourceAttribute;

    /**
     * Attributes that may be extracted as doc values even if that makes them
     * less accurate. This is mostly used for geo fields which lose a lot of
     * precision in their doc values, but in some cases doc values provides
     * <strong>enough</strong> precision to do the job.
     * <p>
     *     This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> docValuesAttributes;

    /**
     * Attributes of a shape whose extent can be extracted directly from the encoded geometry.
     * <p>
     *     This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> boundAttributes;

    private List<Attribute> lazyOutput;

    public FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract) {
        this(source, child, attributesToExtract, Set.of(), Set.of());
    }

    private FieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        Set<Attribute> docValuesAttributes,
        Set<Attribute> boundAttributes
    ) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);
        this.docValuesAttributes = docValuesAttributes;
        this.boundAttributes = boundAttributes;
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

    public static @Nullable Attribute extractSourceAttributesFrom(PhysicalPlan plan) {
        for (Attribute attribute : plan.outputSet()) {
            if (EsQueryExec.isSourceAttribute(attribute)) {
                return attribute;
            }
        }
        return null;
    }

    @Override
    protected AttributeSet computeReferences() {
        return sourceAttribute != null ? new AttributeSet(sourceAttribute) : AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract, docValuesAttributes, boundAttributes);
    }

    public FieldExtractExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new FieldExtractExec(source(), child(), attributesToExtract, docValuesAttributes, boundAttributes);
    }

    public FieldExtractExec withBoundAttributes(Set<Attribute> boundAttributes) {
        return new FieldExtractExec(source(), child(), attributesToExtract, docValuesAttributes, boundAttributes);
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public @Nullable Attribute sourceAttribute() {
        return sourceAttribute;
    }

    public Set<Attribute> docValuesAttributes() {
        return docValuesAttributes;
    }

    public Set<Attribute> boundAttributes() {
        return boundAttributes;
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
        return Objects.hash(attributesToExtract, docValuesAttributes, boundAttributes, child());
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
            && Objects.equals(boundAttributes, other.boundAttributes)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return Strings.format(
            "%s<%s,%s>",
            nodeName() + NodeUtils.limitedToString(attributesToExtract),
            docValuesAttributes,
            boundAttributes
        );
    }

}

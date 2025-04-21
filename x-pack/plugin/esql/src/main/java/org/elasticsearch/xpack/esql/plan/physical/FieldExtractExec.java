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
import org.elasticsearch.index.mapper.MappedFieldType;
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
     * The default for {@link #fieldExtractPreference} if the plan doesn't require
     * a preference.
     */
    private final MappedFieldType.FieldExtractPreference defaultPreference;

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
     * Attributes of a shape whose extent can be extracted directly from the doc-values encoded geometry.
     * <p>
     *     This is never serialized between nodes and only used locally.
     * </p>
     */
    private final Set<Attribute> boundsAttributes;

    private List<Attribute> lazyOutput;

    public FieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        MappedFieldType.FieldExtractPreference defaultPreference
    ) {
        this(source, child, attributesToExtract, defaultPreference, Set.of(), Set.of());
    }

    private FieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        MappedFieldType.FieldExtractPreference defaultPreference,
        Set<Attribute> docValuesAttributes,
        Set<Attribute> boundsAttributes
    ) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);
        this.docValuesAttributes = docValuesAttributes;
        this.boundsAttributes = boundsAttributes;
        this.defaultPreference = defaultPreference;
    }

    private FieldExtractExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            MappedFieldType.FieldExtractPreference.NONE
        );
        // defaultPreference is only used on the data node and never serialized.
        // docValueAttributes and boundsAttributes are only used on the data node and never serialized.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(attributesToExtract());
        // defaultPreference is only used on the data node and never serialized.
        // docValueAttributes and boundsAttributes are only used on the data node and never serialized.
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
        return sourceAttribute != null ? AttributeSet.of(sourceAttribute) : AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract, defaultPreference);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract, defaultPreference, docValuesAttributes, boundsAttributes);
    }

    public FieldExtractExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new FieldExtractExec(source(), child(), attributesToExtract, defaultPreference, docValuesAttributes, boundsAttributes);
    }

    public FieldExtractExec withBoundsAttributes(Set<Attribute> boundsAttributes) {
        return new FieldExtractExec(source(), child(), attributesToExtract, defaultPreference, docValuesAttributes, boundsAttributes);
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

    public Set<Attribute> boundsAttributes() {
        return boundsAttributes;
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
        return Objects.hash(attributesToExtract, docValuesAttributes, boundsAttributes, child());
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
            && Objects.equals(boundsAttributes, other.boundsAttributes)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return Strings.format(
            "%s<%s,%s>",
            nodeName() + NodeUtils.limitedToString(attributesToExtract),
            docValuesAttributes,
            boundsAttributes
        );
    }

    public MappedFieldType.FieldExtractPreference fieldExtractPreference(Attribute attr) {
        if (boundsAttributes.contains(attr)) {
            return MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS;
        }
        if (docValuesAttributes.contains(attr)) {
            return MappedFieldType.FieldExtractPreference.DOC_VALUES;
        }
        return defaultPreference;
    }
}

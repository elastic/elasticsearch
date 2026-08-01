/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Loads dimension fields after a time-series aggregation using {@code FirstDocId} documents.
 */
public class ReadDimsExec extends UnaryExec implements EstimatesRowSize {

    private final Attribute docAttribute;
    private final Attribute tsidAttribute;
    private final List<Attribute> dims;
    private final MappedFieldType.FieldExtractPreference fieldExtractPreference;
    @Nullable
    private final Attribute packedDims;
    private List<Attribute> lazyOutput;

    public ReadDimsExec(
        Source source,
        PhysicalPlan child,
        Attribute docAttribute,
        Attribute tsidAttribute,
        List<Attribute> dims,
        @Nullable Attribute packedDims,
        MappedFieldType.FieldExtractPreference fieldExtractPreference
    ) {
        super(source, child);
        this.docAttribute = docAttribute;
        this.tsidAttribute = tsidAttribute;
        this.dims = dims;
        this.packedDims = packedDims;
        this.fieldExtractPreference = fieldExtractPreference;
    }

    public Attribute docAttribute() {
        return docAttribute;
    }

    public Attribute tsidAttribute() {
        return tsidAttribute;
    }

    public List<Attribute> dims() {
        return dims;
    }

    public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return fieldExtractPreference;
    }

    @Nullable
    public Attribute packedDims() {
        return packedDims;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(docAttribute, tsidAttribute);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = CollectionUtils.concatLists(child().output(), generatingFields());

        }
        return lazyOutput;
    }

    public List<Attribute> generatingFields() {
        return packedDims != null ? List.of(packedDims) : dims;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, dims);
        return this;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ReadDimsExec(source(), newChild, docAttribute, tsidAttribute, dims, packedDims, fieldExtractPreference);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ReadDimsExec::new, child(), docAttribute, tsidAttribute, dims, packedDims, fieldExtractPreference);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("ReadDimsExec is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("ReadDimsExec is local only and not serialized");
    }

    @Override
    public int hashCode() {
        return Objects.hash(docAttribute, tsidAttribute, dims, fieldExtractPreference, packedDims, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ReadDimsExec other = (ReadDimsExec) obj;
        return Objects.equals(docAttribute, other.docAttribute)
            && Objects.equals(tsidAttribute, other.tsidAttribute)
            && Objects.equals(dims, other.dims)
            && Objects.equals(packedDims, other.packedDims)
            && fieldExtractPreference == other.fieldExtractPreference
            && Objects.equals(child(), other.child());
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        NodeUtils.toString(sb, dims, format, mapper);
        sb.append("<tsid=");
        tsidAttribute.nodeString(sb, format, mapper);
        sb.append(">");
    }
}

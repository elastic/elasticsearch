/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Packs the {@code dims} blocks into a single {@code BytesRef} block, zero-length values as absent with {@code emptyAsAbsent}.
 */
public class PackDimsExec extends UnaryExec {

    private final List<Attribute> dims;
    private final Attribute packed;
    private final boolean emptyAsAbsent;
    private List<Attribute> lazyOutput;

    public PackDimsExec(Source source, PhysicalPlan child, List<Attribute> dims, Attribute packed) {
        this(source, child, dims, packed, false);
    }

    public PackDimsExec(Source source, PhysicalPlan child, List<Attribute> dims, Attribute packed, boolean emptyAsAbsent) {
        super(source, child);
        this.dims = dims;
        this.packed = packed;
        this.emptyAsAbsent = emptyAsAbsent;
    }

    public List<Attribute> dims() {
        return dims;
    }

    public Attribute packed() {
        return packed;
    }

    public boolean emptyAsAbsent() {
        return emptyAsAbsent;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(dims);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = CollectionUtils.appendToCopy(child().output(), packed);
        }
        return lazyOutput;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new PackDimsExec(source(), newChild, dims, packed, emptyAsAbsent);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, PackDimsExec::new, child(), dims, packed, emptyAsAbsent);
    }

    @Override
    public List<Object> nodeProperties() {
        // A pack reading empty values as absent says so; every other pack renders as it always has.
        return emptyAsAbsent ? super.nodeProperties() : List.of(child(), dims, packed);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PackDimsExec is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PackDimsExec is local only and not serialized");
    }

    @Override
    public int hashCode() {
        return Objects.hash(dims, packed, emptyAsAbsent, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PackDimsExec other = (PackDimsExec) obj;
        return Objects.equals(dims, other.dims)
            && Objects.equals(packed, other.packed)
            && emptyAsAbsent == other.emptyAsAbsent
            && Objects.equals(child(), other.child());
    }

    int estimateRowSize() {
        int size = 4;
        for (Expression dim : dims) {
            size += EstimatesRowSize.estimateSize(dim.dataType().noText());
        }
        return size;
    }
}

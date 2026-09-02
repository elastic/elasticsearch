/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Packs the {@code dims} blocks into a single {@code BytesRef} block.
 */
public class PackDims extends UnaryPlan {
    public static final String PACKED_FIELD_NAME = "_$packed_dims";
    public static final String PACKED_GROUPING_NAME = "_$packed_grouping_dims";

    private final List<Attribute> dims;
    private final Attribute packed;
    private List<Attribute> lazyOutput;

    public PackDims(Source source, LogicalPlan child, List<Attribute> dims, Attribute packed) {
        super(source, child);
        this.dims = dims;
        this.packed = packed;
    }

    public List<Attribute> dims() {
        return dims;
    }

    public Attribute packed() {
        return packed;
    }

    public static FieldAttribute newPackedAttribute(Source source) {
        EsField field = new EsField(PACKED_FIELD_NAME, DataType.SOURCE, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION);
        return new FieldAttribute(source, null, null, PACKED_FIELD_NAME, field, Nullability.TRUE, new NameId(), true);
    }

    public static Alias newPackedGrouping(Source source, Attribute packed) {
        return new Alias(source, PACKED_GROUPING_NAME, packed);
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(dims);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = CollectionUtils.combine(child().output(), packed);
        }
        return lazyOutput;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(dims) && packed.resolved();
    }

    @Override
    public PackDims replaceChild(LogicalPlan newChild) {
        return new PackDims(source(), newChild, dims, packed);
    }

    @Override
    protected NodeInfo<PackDims> info() {
        return NodeInfo.create(this, PackDims::new, child(), dims, packed);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PackDims is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PackDims is local only and not serialized");
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dims, packed, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PackDims other = (PackDims) obj;
        return Objects.equals(dims, other.dims) && Objects.equals(packed, other.packed) && Objects.equals(child(), other.child());
    }
}

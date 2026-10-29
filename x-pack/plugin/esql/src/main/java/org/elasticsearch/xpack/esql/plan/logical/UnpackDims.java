/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Unpacks a single packed dimension block (produced by {@link PackDims}) back into the original dim blocks.
 */
public class UnpackDims extends UnaryPlan {

    private final Attribute packed;
    private final List<Attribute> dims;
    private List<Attribute> lazyOutput;

    public UnpackDims(Source source, LogicalPlan child, Attribute packed, List<Attribute> dims) {
        super(source, child);
        this.packed = packed;
        this.dims = dims;
    }

    public Attribute packed() {
        return packed;
    }

    public List<Attribute> dims() {
        return dims;
    }

    @Override
    protected AttributeSet computeReferences() {
        return packed.references();
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = CollectionUtils.combine(child().output(), dims);
        }
        return lazyOutput;
    }

    @Override
    public boolean expressionsResolved() {
        return packed.resolved() && Resolvables.resolved(dims);
    }

    @Override
    public UnpackDims replaceChild(LogicalPlan newChild) {
        return new UnpackDims(source(), newChild, packed, dims);
    }

    @Override
    protected NodeInfo<UnpackDims> info() {
        return NodeInfo.create(this, UnpackDims::new, child(), packed, dims);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnpackDims is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("UnpackDims is local only and not serialized");
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packed, dims, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnpackDims other = (UnpackDims) obj;
        return Objects.equals(packed, other.packed) && Objects.equals(dims, other.dims) && Objects.equals(child(), other.child());
    }
}

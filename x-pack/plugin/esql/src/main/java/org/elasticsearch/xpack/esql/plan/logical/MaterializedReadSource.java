/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * First-class logical node the boundary-aware view rule lowers a {@code MATERIALIZED} {@link View} to. A materialized
 * view's body is never executed at query time: its results live in a precomputed backing store, so the boundary survives
 * optimization as this opaque leaf reading that store. It carries the {@code backingIndex} ref (the precomputed store to
 * read) and the resolved output {@link Attribute}s.
 * <p>
 * It is a {@link LeafPlan} — the view body does not appear in the local tree, only the read of its materialization. The
 * {@code Mapper} lowers this to {@code MaterializedReadExec}, whose source operator is a POC stub. (A production lowering
 * could instead rewrite this to an {@code EsRelation} over the backing index; keeping it a distinct node makes the
 * boundary decision visible and the staleness / refresh policy a place to hang later.)
 * <p>
 * <b>Transient (POC):</b> like {@link View} this is not wire-serialized yet ({@link #writeTo} throws).
 */
public class MaterializedReadSource extends LeafPlan {

    private final String viewName;
    private final String backingIndex;
    private final List<Attribute> output;

    /**
     * @param viewName     the view's name as written in the query (its identity)
     * @param backingIndex the precomputed backing store to read in place of executing the view body
     * @param output       the view's resolved output schema
     */
    public MaterializedReadSource(Source source, String viewName, String backingIndex, List<Attribute> output) {
        super(source);
        this.viewName = viewName;
        this.backingIndex = backingIndex;
        this.output = output;
    }

    /** The view name as written in the query (its identity). */
    public String viewName() {
        return viewName;
    }

    /** The precomputed backing store this read targets in place of executing the view body. */
    public String backingIndex() {
        return backingIndex;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<MaterializedReadSource> info() {
        return NodeInfo.create(this, MaterializedReadSource::new, viewName, backingIndex, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("MaterializedReadSource is a transient POC node and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "MaterializedReadSource";
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // Both the view name and the backing index are index-like identifiers — route through the mapper for anonymization.
        sb.append(nodeName()).append('[').append(mapper.index(viewName)).append("]->[").append(mapper.index(backingIndex)).append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewName, backingIndex, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaterializedReadSource other = (MaterializedReadSource) obj;
        return Objects.equals(viewName, other.viewName)
            && Objects.equals(backingIndex, other.backingIndex)
            && Objects.equals(output, other.output);
    }
}

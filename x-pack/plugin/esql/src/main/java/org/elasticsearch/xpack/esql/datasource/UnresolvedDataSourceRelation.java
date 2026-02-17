/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasource.spi.DataSource;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceDescriptor;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Unresolved plan leaf for external data source references.
 *
 * <p>Created by the parser when it encounters a data source reference
 * (e.g., {@code FROM my_s3:logs}). Carries a {@link DataSourceDescriptor}
 * with the type, configuration, settings, and expression needed for
 * resolution.
 *
 * <p>During pre-analysis, the session resolves this node by calling
 * {@link DataSource#resolve} and replaces it with the resulting
 * {@link DataSourcePlan}.
 *
 * <p>This is the data source SPI equivalent of {@code UnresolvedRelation}
 * (for ES indices) and {@code UnresolvedExternalRelation} (from PR #141678).
 *
 * @see DataSourceDescriptor
 * @see DataSource#resolve
 */
public class UnresolvedDataSourceRelation extends LeafPlan implements Unresolvable {

    private final DataSourceDescriptor descriptor;
    private final String unresolvedMsg;

    public UnresolvedDataSourceRelation(Source source, DataSourceDescriptor descriptor) {
        this(source, descriptor, null);
    }

    public UnresolvedDataSourceRelation(Source source, DataSourceDescriptor descriptor, String unresolvedMsg) {
        super(source);
        this.descriptor = descriptor;
        this.unresolvedMsg = unresolvedMsg == null ? "Unknown data source [" + descriptor.describe() + "]" : unresolvedMsg;
    }

    /**
     * The descriptor for this unresolved data source reference.
     */
    public DataSourceDescriptor descriptor() {
        return descriptor;
    }

    // =========================================================================
    // UNRESOLVABLE
    // =========================================================================

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    // =========================================================================
    // NODE
    // =========================================================================

    @Override
    protected NodeInfo<UnresolvedDataSourceRelation> info() {
        return NodeInfo.create(this, UnresolvedDataSourceRelation::new, descriptor, unresolvedMsg);
    }

    @Override
    public List<Object> nodeProperties() {
        return List.of(descriptor, unresolvedMsg);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + descriptor.describe();
    }

    // =========================================================================
    // SERIALIZATION (not serialized — resolved before physical planning)
    // =========================================================================

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    // =========================================================================
    // EQUALITY
    // =========================================================================

    @Override
    public int hashCode() {
        return Objects.hash(source(), descriptor, unresolvedMsg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnresolvedDataSourceRelation other = (UnresolvedDataSourceRelation) obj;
        return Objects.equals(source(), other.source())
            && Objects.equals(descriptor, other.descriptor)
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }
}

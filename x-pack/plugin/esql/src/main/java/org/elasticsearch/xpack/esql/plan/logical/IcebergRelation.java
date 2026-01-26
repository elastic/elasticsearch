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
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata;
import org.elasticsearch.xpack.esql.plan.physical.IcebergSourceExec;

import java.util.List;
import java.util.Objects;

/**
 * Represents a resolved Iceberg table or Parquet file.
 * This is created by the ResolveIcebergRelations analyzer rule from UnresolvedExternalRelation.
 * <p>
 * As a subclass of {@link ExternalRelation}, this plan node is executed on the coordinator only
 * (implements {@link ExecutesOn.Coordinator}) and maps directly to a physical source operator
 * via LocalMapper, bypassing the FragmentExec/ExchangeExec dispatch mechanism.
 */
public class IcebergRelation extends ExternalRelation {

    private final IcebergTableMetadata metadata;

    public IcebergRelation(Source source, String tablePath, IcebergTableMetadata metadata, List<Attribute> attributes) {
        super(source, tablePath, attributes);
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("IcebergRelation is not yet serializable for cross-cluster operations");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("IcebergRelation is not yet serializable for cross-cluster operations");
    }

    @Override
    protected NodeInfo<IcebergRelation> info() {
        return NodeInfo.create(this, IcebergRelation::new, sourcePath(), metadata, output());
    }

    /**
     * @return the path to the Iceberg table or Parquet file
     * @deprecated Use {@link #sourcePath()} instead
     */
    @Deprecated
    public String tablePath() {
        return sourcePath();
    }

    public IcebergTableMetadata metadata() {
        return metadata;
    }

    @Override
    public String sourceType() {
        return metadata.sourceType();
    }

    @Override
    public IcebergSourceExec toPhysicalExec() {
        return new IcebergSourceExec(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (!super.equals(obj)) {
            return false;
        }

        IcebergRelation other = (IcebergRelation) obj;
        return Objects.equals(metadata, other.metadata);
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + sourcePath() + "][" + sourceType() + "]" + NodeUtils.toString(output(), format);
    }

    public IcebergRelation withAttributes(List<Attribute> newAttributes) {
        return new IcebergRelation(source(), sourcePath(), metadata, newAttributes);
    }
}

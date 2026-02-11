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
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.util.List;
import java.util.Objects;

/**
 * Logical plan node for external data source relations (e.g., Iceberg table, Parquet file).
 * This plan node is executed on the coordinator only (no dispatch to data nodes).
 * <p>
 * Unlike EsRelation which wraps into FragmentExec for data node dispatch,
 * ExternalRelation maps directly to physical source operators via LocalMapper,
 * similar to how LocalRelation works.
 * <p>
 * This class provides a source-agnostic logical plan node for external data sources.
 * It can represent any external source (Iceberg, Parquet, CSV, etc.) without requiring
 * source-specific subclasses in core ESQL code.
 * <p>
 * The source-specific metadata is stored in the {@link SourceMetadata} interface, which
 * provides:
 * <ul>
 *   <li>Schema attributes via {@link SourceMetadata#schema()}</li>
 *   <li>Source type via {@link SourceMetadata#sourceType()}</li>
 *   <li>Configuration via {@link SourceMetadata#config()}</li>
 *   <li>Opaque source metadata via {@link SourceMetadata#sourceMetadata()}</li>
 * </ul>
 * <p>
 * The {@link #toPhysicalExec()} method creates a generic {@link ExternalSourceExec} that
 * carries all necessary information for the operator factory to create the appropriate
 * source operator via the SPI.
 */
public class ExternalRelation extends LeafPlan implements ExecutesOn.Coordinator {

    private final String sourcePath;
    private final List<Attribute> output;
    private final SourceMetadata metadata;
    private final FileSet fileSet;

    public ExternalRelation(Source source, String sourcePath, SourceMetadata metadata, List<Attribute> output, FileSet fileSet) {
        super(source);
        if (sourcePath == null) {
            throw new IllegalArgumentException("sourcePath must not be null");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        if (output == null) {
            throw new IllegalArgumentException("output must not be null");
        }
        this.sourcePath = sourcePath;
        this.metadata = metadata;
        this.output = output;
        this.fileSet = fileSet;
    }

    public ExternalRelation(Source source, String sourcePath, SourceMetadata metadata, List<Attribute> output) {
        this(source, sourcePath, metadata, output, FileSet.UNRESOLVED);
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("ExternalRelation is not yet serializable for cross-cluster operations");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("ExternalRelation is not yet serializable for cross-cluster operations");
    }

    @Override
    protected NodeInfo<ExternalRelation> info() {
        return NodeInfo.create(this, ExternalRelation::new, sourcePath, metadata, output, fileSet);
    }

    public String sourcePath() {
        return sourcePath;
    }

    public SourceMetadata metadata() {
        return metadata;
    }

    public FileSet fileSet() {
        return fileSet;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    public String sourceType() {
        return metadata.sourceType();
    }

    public ExternalSourceExec toPhysicalExec() {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            metadata.sourceType(),
            output,
            metadata.config(),
            metadata.sourceMetadata(),
            null,
            null,
            fileSet
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, metadata, output, fileSet);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExternalRelation other = (ExternalRelation) obj;
        return Objects.equals(sourcePath, other.sourcePath)
            && Objects.equals(metadata, other.metadata)
            && Objects.equals(output, other.output)
            && Objects.equals(fileSet, other.fileSet);
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + sourcePath + "][" + sourceType() + "]" + NodeUtils.toString(output, format);
    }

    public ExternalRelation withAttributes(List<Attribute> newAttributes) {
        return new ExternalRelation(source(), sourcePath, metadata, newAttributes, fileSet);
    }
}

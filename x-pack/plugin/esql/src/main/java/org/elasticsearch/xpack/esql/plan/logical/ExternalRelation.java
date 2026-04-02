/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Logical plan node for external data source relations (e.g., Iceberg table, Parquet file).
 * <p>
 * Like {@link EsRelation}, the Mapper wraps this into a {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}
 * so that pipeline breakers (Aggregate, Limit, TopN) above it are distributed to data nodes
 * via ExchangeExec. On data nodes, {@code localPlan()} expands the FragmentExec through
 * LocalMapper into {@link ExternalSourceExec}, enabling local optimizations such as
 * filter pushdown via FilterPushdownRegistry.
 * <p>
 * The {@link ExecutesOn.Coordinator} marker is retained for logical plan validation
 * (e.g., Enrich/Join hoist rules that inspect whether a relation executes on the coordinator).
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

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "ExternalRelation",
        ExternalRelation::readFrom
    );

    private final String sourcePath;
    private final List<Attribute> output;
    private final SourceMetadata metadata;
    private final FileList fileList;

    public ExternalRelation(Source source, String sourcePath, SourceMetadata metadata, List<Attribute> output, FileList fileList) {
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
        this.fileList = fileList;
    }

    public ExternalRelation(Source source, String sourcePath, SourceMetadata metadata, List<Attribute> output) {
        this(source, sourcePath, metadata, output, FileList.UNRESOLVED);
    }

    private static ExternalRelation readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String sourcePath = in.readString();
        String sourceType = in.readString();
        var output = in.readNamedWriteableCollectionAsList(Attribute.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) in.readGenericValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> sourceMetadata = (Map<String, Object>) in.readGenericValue();
        var metadata = new SimpleSourceMetadata(output, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(source, sourcePath, metadata, output, FileList.UNRESOLVED);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(sourcePath);
        out.writeString(metadata.sourceType());
        out.writeNamedWriteableCollection(output);
        out.writeGenericValue(metadata.config());
        out.writeGenericValue(metadata.sourceMetadata());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<ExternalRelation> info() {
        return NodeInfo.create(this, ExternalRelation::new, sourcePath, metadata, output, fileList);
    }

    public String sourcePath() {
        return sourcePath;
    }

    public SourceMetadata metadata() {
        return metadata;
    }

    public FileList fileList() {
        return fileList;
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
        Map<String, Object> enrichedMetadata = metadata.statistics()
            .map(stats -> SourceStatisticsSerializer.embedStatistics(metadata.sourceMetadata(), stats))
            .orElse(metadata.sourceMetadata());
        return new ExternalSourceExec(
            source(),
            sourcePath,
            metadata.sourceType(),
            output,
            metadata.config(),
            enrichedMetadata,
            null,
            null,
            fileList
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, metadata, output, fileList);
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
            && Objects.equals(fileList, other.fileList);
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + sourcePath + "][" + sourceType() + "]" + NodeUtils.toString(output, format);
    }

    public ExternalRelation withAttributes(List<Attribute> newAttributes) {
        return new ExternalRelation(source(), sourcePath, metadata, newAttributes, fileList);
    }
}

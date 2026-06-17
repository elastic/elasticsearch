/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Logical plan node for external data source relations (e.g., Iceberg table, Parquet file).
 * <p>
 * Like {@link EsRelation}, the Mapper wraps this into a {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}
 * so that pipeline breakers (Aggregate, Limit, TopN) above it are distributed to data nodes
 * via ExchangeExec. On data nodes, {@code localPlan()} expands the FragmentExec through
 * LocalMapper into {@link ExternalSourceExec}, enabling local optimizations such as
 * filter pushdown via {@link org.elasticsearch.xpack.esql.datasources.spi.FormatReader#filterPushdownSupport()}.
 * <p>
 * The {@link ExecutesOn.Coordinator} marker is retained for logical plan validation
 * (e.g., Enrich/Join hoist rules that inspect whether a relation executes on the coordinator).
 * <p>
 * The source-specific metadata is stored in the {@link SourceMetadata} interface, which
 * provides:
 * <ul>
 *   <li>ExternalSchema attributes via {@link SourceMetadata#schema()}</li>
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

    private static final TransportVersion ESQL_EXTERNAL_SOURCE_READ_SCHEMA = TransportVersion.fromName("esql_external_source_read_schema");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "ExternalRelation",
        ExternalRelation::readFrom
    );

    private final String sourcePath;
    private final List<Attribute> output;
    private final SourceMetadata metadata;
    private final FileList fileList;
    // Coordinator-only — not serialized. Drives FileSplit.readSchema + UBN SchemaAdaptingIterator.
    private final Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;

    public ExternalRelation(
        Source source,
        String sourcePath,
        SourceMetadata metadata,
        List<Attribute> output,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap
    ) {
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
        this.schemaMap = schemaMap != null ? schemaMap : Map.of();
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
        // The source's full column layout is wire-encoded separately from {@code output} because the
        // optimizer narrows {@code output} (e.g. STATS projects to a single aggregated column) before
        // the plan crosses the coordinator → data-node boundary. The {@link SourceMetadata#schema()}
        // contract is "positional column layout for the source", which is the file's actual schema —
        // not the projection. Reusing {@code output} here would mis-align readSchema for any query
        // that triggers projection pushdown on the external source.
        List<Attribute> sourceSchema = in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_READ_SCHEMA)
            ? in.readNamedWriteableCollectionAsList(Attribute.class)
            : output;
        var metadata = new SimpleSourceMetadata(sourceSchema, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(source, sourcePath, metadata, output, FileList.UNRESOLVED, Map.of());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(sourcePath);
        out.writeString(metadata.sourceType());
        out.writeNamedWriteableCollection(output);
        out.writeGenericValue(metadata.config());
        out.writeGenericValue(metadata.sourceMetadata());
        // See {@link #readFrom} for why the schema is serialized separately from {@code output}.
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_READ_SCHEMA)) {
            out.writeNamedWriteableCollection(metadata.schema());
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<ExternalRelation> info() {
        return NodeInfo.create(this, ExternalRelation::new, sourcePath, metadata, output, fileList, schemaMap);
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

    public Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap() {
        return schemaMap;
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
            List.of(),
            FormatReader.NO_LIMIT,
            null,
            fileList,
            schemaMap,
            List.of()
        ).withUnifiedSchema(new ExternalSchema(dataOnlyUnifiedSchema()));
    }

    /**
     * Returns the pre-enrichment Unified schema — the data-only view that {@link SchemaReconciliation}
     * built the per-file {@link org.elasticsearch.xpack.esql.datasources.ColumnMapping}s against. The
     * post-enrichment {@code metadata.schema()} includes partition attributes appended by
     * {@code ExternalSourceResolver#enrichSchemaWithPartitionColumns} and {@code _file.*}
     * virtual columns appended by {@code enrichSchemaWithFileMetadataColumns}; both are wider
     * than the per-file mapping. Seeding {@code ExternalSourceExec.unifiedSchema} from the
     * wider view causes {@code ColumnMapping.pruneToPerFileQuery} to read past
     * {@code index.length} when the optimizer also prunes the projection.
     */
    private List<Attribute> dataOnlyUnifiedSchema() {
        PartitionMetadata partitionInfo = fileList != null ? fileList.partitionMetadata() : null;
        Set<String> partitionNames = partitionInfo != null && partitionInfo.isEmpty() == false
            ? partitionInfo.partitionColumns().keySet()
            : Set.of();
        return metadata.schema()
            .stream()
            .filter(a -> a instanceof VirtualAttribute == false && partitionNames.contains(a.name()) == false)
            .toList();
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, metadata, output, fileList, schemaMap);
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
            && Objects.equals(fileList, other.fileList)
            && Objects.equals(schemaMap, other.schemaMap);
    }

    @Override
    public List<Object> nodeProperties() {
        // metadata.config() may carry SecureString (dataset path) or plaintext String (inline
        // EXTERNAL path) secrets. Keep them out of EXPLAIN / debug-log
        // output. fileList and schemaMap are coordinator-only state, also omitted here.
        return List.of(sourcePath, output);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // sourcePath is a user-supplied external location (S3 URI / file / table path) — opaque
        // free-form content; redact under anonymization. sourceType is a low-cardinality format enum.
        sb.append(nodeName()).append("[").append(mapper.opaque(sourcePath)).append("][").append(sourceType()).append("]");
        NodeUtils.toString(sb, output, format, mapper);
    }

    public ExternalRelation withAttributes(List<Attribute> newAttributes) {
        return new ExternalRelation(source(), sourcePath, metadata, newAttributes, fileList, schemaMap);
    }
}

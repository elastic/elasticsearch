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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
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
    private static final TransportVersion ESQL_EXTERNAL_DATASET_NAME = TransportVersion.fromName("esql_external_dataset_name");
    private static final TransportVersion DATASET_DECLARED_SCHEMA = TransportVersion.fromName("dataset_declared_schema");

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
    // Registered dataset identifier (from FROM <dataset>), or null for inline EXTERNAL. Threaded
    // to the operator factory so the per-file _index synthesizer can emit the user-facing name.
    @Nullable
    private final String datasetName;
    /**
     * METADATA-clause expressions threaded through from the parser for the verifier to discover
     * if any remain unresolvable after analysis. Mirrors the indexed {@code EsRelation} pattern:
     * resolved standard / {@code _file.*} names are bound into {@link #output}; any name absent
     * from {@code MetadataAttribute.ATTRIBUTES_MAP} and {@code FileMetadataColumns} stays here
     * as an {@code UnresolvedMetadataAttributeExpression} so the verifier's
     * {@code checkUnresolvedAttributes} walk fires its native {@code "Unresolved metadata pattern
     * [...]"} error — same diagnostic users see on indexed {@code FROM x METADATA _typo}.
     * <p>
     * Coordinator-only: by the time this node crosses the wire, the verifier has already failed
     * any plan containing an unresolved expression, so the field is empty on the data-node side
     * and intentionally omitted from {@link #writeTo} / {@link #readFrom}.
     */
    private final List<? extends NamedExpression> metadataFields;
    /**
     * The declared mapping's read-instructions (logical&rarr;physical renames, {@code _id.path}), or
     * {@link DeclaredReadSpec#NONE}. Threaded to {@link ExternalSourceExec} via {@link #toPhysicalExec} and consumed on
     * the data node (physicalization + {@code _id} stamping); rides the wire gated on {@code dataset_declared_schema}.
     */
    private final DeclaredReadSpec declaredReadSpec;

    public ExternalRelation(
        Source source,
        String sourcePath,
        SourceMetadata metadata,
        List<Attribute> output,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap
    ) {
        this(source, sourcePath, metadata, output, fileList, schemaMap, null, List.of(), DeclaredReadSpec.NONE);
    }

    public ExternalRelation(
        Source source,
        String sourcePath,
        SourceMetadata metadata,
        List<Attribute> output,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable String datasetName
    ) {
        this(source, sourcePath, metadata, output, fileList, schemaMap, datasetName, List.of(), DeclaredReadSpec.NONE);
    }

    public ExternalRelation(
        Source source,
        String sourcePath,
        SourceMetadata metadata,
        List<Attribute> output,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable String datasetName,
        List<? extends NamedExpression> metadataFields,
        DeclaredReadSpec declaredReadSpec
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
        this.datasetName = datasetName;
        this.metadataFields = metadataFields != null ? metadataFields : List.of();
        this.declaredReadSpec = declaredReadSpec != null ? declaredReadSpec : DeclaredReadSpec.NONE;
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
        String datasetName = in.getTransportVersion().supports(ESQL_EXTERNAL_DATASET_NAME) ? in.readOptionalString() : null;
        DeclaredReadSpec declaredReadSpec = in.getTransportVersion().supports(DATASET_DECLARED_SCHEMA)
            ? DeclaredReadSpec.readFrom(in)
            : DeclaredReadSpec.NONE;
        var metadata = new SimpleSourceMetadata(sourceSchema, sourceType, sourcePath, null, null, sourceMetadata, config);
        return new ExternalRelation(
            source,
            sourcePath,
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of(),
            datasetName,
            List.of(),
            declaredReadSpec
        );
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
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_DATASET_NAME)) {
            out.writeOptionalString(datasetName);
        }
        if (out.getTransportVersion().supports(DATASET_DECLARED_SCHEMA)) {
            declaredReadSpec.writeTo(out);
        } else if (declaredReadSpec.isEmpty() == false) {
            // Silently dropping a non-empty spec toward an older data node would return wrong rows (physical names,
            // synthetic _id, unparsed dates). Reject loudly instead — mirrors PutDatasetAction's older-master reject.
            throw new IllegalArgumentException(
                "declared dataset read-instructions are not supported on all nodes in the cluster; retry after the upgrade"
            );
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<ExternalRelation> info() {
        return NodeInfo.create(
            this,
            ExternalRelation::new,
            sourcePath,
            metadata,
            output,
            fileList,
            schemaMap,
            datasetName,
            metadataFields,
            declaredReadSpec
        );
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

    /**
     * Registered dataset identifier, or {@code null} when this relation was produced by the inline
     * {@code EXTERNAL} command path. Carried to {@link ExternalSourceExec} via {@link #toPhysicalExec}.
     */
    @Nullable
    public String datasetName() {
        return datasetName;
    }

    /**
     * The declared mapping's read-instructions (renames, {@code _id.path}, per-column date formats), or {@link DeclaredReadSpec#NONE}.
     * Carried to {@link ExternalSourceExec} via {@link #toPhysicalExec}.
     */
    public DeclaredReadSpec declaredReadSpec() {
        return declaredReadSpec;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        for (NamedExpression e : metadataFields) {
            if (e.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * METADATA clause expressions threaded through from the parser. Resolved entries are bound
     * into {@link #output}; unresolved entries (typo'd or unknown names) stay here so the
     * verifier's {@code checkUnresolvedAttributes} walk picks them up and reports the
     * indexed-equivalent {@code "Unresolved metadata pattern [...]"} error.
     */
    public List<? extends NamedExpression> metadataFields() {
        return metadataFields;
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
        ).withUnifiedSchema(new ExternalSchema(dataOnlyUnifiedSchema()))
            .withDatasetName(datasetName)
            .withDeclaredReadSpec(declaredReadSpec);
    }

    /**
     * Names of the Hive-partition (path-derived) columns for this relation, read from the serialized stamp in
     * {@code metadata.sourceMetadata()} — never from {@link #fileList()}, so the answer is identical on the
     * coordinator and a data node (where the fileList deserializes to {@code UNRESOLVED}). Empty when the
     * source is not partitioned. Delegates to the one canonical stamp reader, like the
     * {@code partitionColumnNames()} accessor on {@code ExternalSourceExec}.
     */
    public Set<String> partitionColumnNames() {
        return SourceStatisticsSerializer.partitionColumnNames(metadata.sourceMetadata());
    }

    /**
     * Returns the pre-enrichment Unified schema — the data-only view that {@link SchemaReconciliation}
     * built the per-file {@link org.elasticsearch.xpack.esql.datasources.ColumnMapping}s against. The
     * post-enrichment {@code metadata.schema()} includes partition attributes appended by
     * {@code ExternalSourceResolver#enrichSchemaWithPartitionColumns}, which is wider
     * than the per-file mapping. Seeding {@code ExternalSourceExec.unifiedSchema} from the
     * wider view causes {@code ColumnMapping.pruneToPerFileQuery} to read past
     * {@code index.length} when the optimizer also prunes the projection.
     * <p>
     * Partition names come from the serialized stamp ({@link #partitionColumnNames()}), NOT the fileList, so
     * this produces the same narrow schema on a data node (where the fileList is {@code UNRESOLVED}) as on the
     * coordinator — previously the data-node build silently kept the wider, partition-inclusive view.
     */
    private List<Attribute> dataOnlyUnifiedSchema() {
        Set<String> partitionNames = partitionColumnNames();
        return metadata.schema()
            .stream()
            .filter(a -> a instanceof VirtualAttribute == false && partitionNames.contains(a.name()) == false)
            .toList();
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, metadata, output, fileList, schemaMap, datasetName, metadataFields, declaredReadSpec);
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
            && Objects.equals(schemaMap, other.schemaMap)
            && Objects.equals(datasetName, other.datasetName)
            && Objects.equals(metadataFields, other.metadataFields)
            && Objects.equals(declaredReadSpec, other.declaredReadSpec);
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
        return new ExternalRelation(
            source(),
            sourcePath,
            metadata,
            newAttributes,
            fileList,
            schemaMap,
            datasetName,
            metadataFields,
            declaredReadSpec
        );
    }

    /**
     * Returns a copy of this relation with its {@link #fileList()} replaced. Used by the fragment discovery path to
     * swap in {@link FileList#EMPTY} when coordinator-side split discovery prunes every file, so the read path scans
     * nothing instead of reading the whole dataset only to have a downstream row filter drop every row.
     */
    public ExternalRelation withFileList(FileList newFileList) {
        return new ExternalRelation(
            source(),
            sourcePath,
            metadata,
            output,
            newFileList,
            schemaMap,
            datasetName,
            metadataFields,
            declaredReadSpec
        );
    }
}

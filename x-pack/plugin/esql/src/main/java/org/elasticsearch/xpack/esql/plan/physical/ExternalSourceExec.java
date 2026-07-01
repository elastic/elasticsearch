/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Generic physical plan node for reading from external data sources (e.g., Iceberg tables, Parquet files).
 * <p>
 * This is the unified physical plan node for all external sources, replacing source-specific nodes.
 * It uses generic maps for configuration and metadata to avoid leaking
 * source-specific types (like S3Configuration) into core ESQL code.
 * <p>
 * Key design principles:
 * <ul>
 *   <li><b>Generic configuration</b>: Uses {@code Map<String, Object>} for config instead of
 *       source-specific classes like S3Configuration</li>
 *   <li><b>Opaque metadata</b>: Source-specific data (native schema, etc.) is stored in
 *       {@link #sourceMetadata()} and passed through without core understanding it</li>
 *   <li><b>Coordinator plan never holds it</b>: the coordinator {@code Mapper} leaves the source as
 *       {@code FragmentExec(ExternalRelation)}; this exec is materialized per-node by
 *       {@code LocalMapper} from
 *       {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec()} during
 *       {@code PlannerUtils.localPlan()}, on whichever node runs the compute. It is a serializable
 *       {@code NamedWriteable} (see {@link #writeTo} / {@link #readFrom}) so it can ride the wire,
 *       but only its execution inputs survive that round trip; see the field categories below.</li>
 * </ul>
 * <p>
 * Field categories. The fields fall into three groups, and only the first is serialized:
 * <ul>
 *   <li><b>Execution inputs (serialized):</b> {@code sourcePath}, {@code sourceType},
 *       {@code attributes}, {@code config}, {@code sourceMetadata}, {@code estimatedRowSize},
 *       {@code splits}, {@code datasetName}. These are what a data-node operator needs to read; in
 *       the distributed path the data node reads the files via the serialized {@code splits}.</li>
 *   <li><b>Build-time state (not serialized):</b> {@code fileList}, {@code schemaMap},
 *       {@code unifiedSchema}. These are coordinator-resolved and read off the exec by split
 *       discovery and {@code planExternalSource}. {@code fileList} and {@code schemaMap} originate
 *       on {@code ExternalRelation} and are coordinator-only (a data node deserializes the relation
 *       with {@code FileList.UNRESOLVED} / an empty {@code schemaMap}, so {@code toPhysicalExec()}
 *       does not rebuild them there); the data node relies on the serialized {@code splits} instead.
 *       {@code unifiedSchema} is the exception: {@code toPhysicalExec()} rebuilds it on every node
 *       from the serialized {@code metadata.schema()}.</li>
 *   <li><b>Local-execution pushdown hints (not serialized):</b> {@code pushedFilter},
 *       {@code pushedExpressions}, {@code pushedLimit}, {@code pushedTopN},
 *       {@code deferredExtraction}. These are decisions produced by {@code LocalPhysicalPlanOptimizer}
 *       rules that run <em>after</em> this exec is created, independently on each node. They are
 *       intentionally re-derived per-node and must not be serialized.</li>
 * </ul>
 * When adding a field: if a data-node operator consumes it directly, it is an execution input and
 * must be serialized; if it derives from the relation or from local physical optimization, it is
 * transient and stays out of {@link #writeTo}.
 */
public class ExternalSourceExec extends LeafExec implements EstimatesRowSize, DataSourceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExternalSourceExec",
        ExternalSourceExec::readFrom
    );

    private static final TransportVersion ESQL_EXTERNAL_SOURCE_SPLITS = TransportVersion.fromName("esql_external_source_splits");
    private static final TransportVersion ESQL_EXTERNAL_DATASET_NAME = TransportVersion.fromName("esql_external_dataset_name");
    private static final TransportVersion DATA_SOURCE_ENCRYPTED_DATA = TransportVersion.fromName("data_source_encrypted_data");

    // --- Execution inputs (serialized; see class Javadoc) ---
    private final String sourcePath;
    private final String sourceType;
    private final List<Attribute> attributes;
    private final Map<String, Object> config;
    private final Map<String, Object> sourceMetadata;
    private final Integer estimatedRowSize;
    private final List<ExternalSplit> splits;
    // Registered dataset identifier when this exec came from FROM <dataset>, null for inline
    // EXTERNAL. Serialized so the data-node operator factory can populate _index with the
    // user-facing dataset name without having to re-derive it from cluster state.
    @Nullable
    private final String datasetName;

    // --- Build-time state (not serialized; see class Javadoc) ---
    // Coordinator-resolved; on a data node toPhysicalExec() leaves these empty and the read goes
    // through the serialized splits instead.
    private final FileList fileList;
    // Drives FileSplit.readSchema + UBN SchemaAdaptingIterator.
    private final Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;
    // The pre-prune Unified schema, rebuilt on every node by toPhysicalExec() from the serialized
    // metadata.schema(). Survives the optimizer's projection prune of `attributes` so split
    // discovery can narrow per-file ColumnMappings to the post-prune Query schema without rebuilding
    // Unified names from per-file mappings.
    @Nullable
    private final ExternalSchema unifiedSchema;

    // --- Local-execution pushdown hints (not serialized; set per-node by LocalPhysicalPlanOptimizer) ---
    private final Object pushedFilter; // Opaque filter, interpreted only by the source-specific operator factory.
    private final List<Expression> pushedExpressions; // ESQL expressions for per-file re-translation.
    private final int pushedLimit;
    /**
     * Hint that the data above this source is a STATS aggregation followed by a TopN over a single
     * grouping key. When present, the {@link BlockHash} can prune non-competitive groups during
     * aggregation. Set locally on each node by {@code PushTopNIntoExternalSource}.
     */
    @Nullable
    private final BlockHash.TopNDef pushedTopN;
    /**
     * Flag set by {@code InsertExternalFieldExtraction} when (and only when) a paired
     * {@code ExternalFieldExtractExec} sits downstream to consume deferred-encoded columns. The
     * operator factory keys deferred extraction off this flag — NOT off {@code _rowPosition}
     * presence in the projection, which {@code InjectRowPositionForExternalId} also produces for
     * plain {@code _id} composition with no extract operator (enabling deferred mode there would
     * create a SourceExtractors registry that nothing ever closes).
     */
    private final boolean deferredExtraction;

    /**
     * Public 13-arg ctor used by {@link #info()} (via constructor reference) and by tree tests.
     * Passes {@code null} for {@code unifiedSchema}; callers that need to carry the Unified schema
     * (e.g. {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec})
     * apply it afterwards via {@link #withUnifiedSchema(ExternalSchema)}.
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        List<ExternalSplit> splits
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            null,
            estimatedRowSize,
            fileList,
            schemaMap,
            null,
            splits
        );
    }

    /**
     * Public 14-arg ctor used by {@link #info()} (via constructor reference) and by tree tests:
     * the 13-arg shape above plus {@code datasetName}, so node-reflection reconstruction
     * preserves the dataset name (it feeds the per-row {@code _index} value — losing it on a
     * generic plan rewrite would silently null {@code _index} mid-plan). Passes {@code null} for
     * {@code pushedTopN} / {@code unifiedSchema}; those are transient hints carried via their
     * {@code with*} methods.
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        List<ExternalSplit> splits,
        @Nullable String datasetName
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            null,
            estimatedRowSize,
            fileList,
            schemaMap,
            null,
            splits,
            datasetName,
            false
        );
    }

    /**
     * Primary constructor that also accepts the transient {@link BlockHash.TopNDef} hint for in-hash TopN pruning
     * and the coordinator-only {@link ExternalSchema} that carries the pre-prune Unified schema. Package-private on purpose
     * so the public, longest constructor (used by tooling and tree tests) remains the thirteen-arg one above.
     * Use {@link #withPushedTopN(BlockHash.TopNDef)} and {@link #withUnifiedSchema(ExternalSchema)} from outside the package.
     */
    ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        @Nullable BlockHash.TopNDef pushedTopN,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable ExternalSchema unifiedSchema,
        List<ExternalSplit> splits
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            null,
            false
        );
    }

    ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        @Nullable BlockHash.TopNDef pushedTopN,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable ExternalSchema unifiedSchema,
        List<ExternalSplit> splits,
        @Nullable String datasetName,
        boolean deferredExtraction
    ) {
        super(source);
        if (sourcePath == null) {
            throw new IllegalArgumentException("sourcePath must not be null");
        }
        if (sourceType == null) {
            throw new IllegalArgumentException("sourceType must not be null");
        }
        if (attributes == null) {
            throw new IllegalArgumentException("attributes must not be null");
        }
        this.sourcePath = sourcePath;
        this.sourceType = sourceType;
        this.attributes = attributes;
        this.config = config != null ? Map.copyOf(config) : Map.of();
        this.sourceMetadata = sourceMetadata != null ? Map.copyOf(sourceMetadata) : Map.of();
        this.pushedFilter = pushedFilter;
        this.pushedExpressions = pushedExpressions != null ? List.copyOf(pushedExpressions) : List.of();
        this.pushedLimit = pushedLimit;
        this.pushedTopN = pushedTopN;
        this.estimatedRowSize = estimatedRowSize;
        this.fileList = fileList;
        this.schemaMap = schemaMap != null ? schemaMap : Map.of();
        this.unifiedSchema = unifiedSchema;
        this.splits = splits != null ? List.copyOf(splits) : List.of();
        this.datasetName = datasetName;
        this.deferredExtraction = deferredExtraction;
    }

    /**
     * Convenience ctor (no splits/fileList/schema). Use {@link #withFileList(FileList)},
     * {@link #withSplits(List)}, and the other {@code with*} builders to attach the build-time and
     * pushdown state. Primarily for tests and the inline {@code EXTERNAL} path.
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        Integer estimatedRowSize
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            List.of(),
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            Map.of(),
            List.of()
        );
    }

    /** Convenience ctor with no pushed filter; see {@link #ExternalSourceExec(Source, String, String, List, Map, Map, Object, Integer)}. */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Integer estimatedRowSize
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            List.of(),
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            Map.of(),
            List.of()
        );
    }

    private static ExternalSourceExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String sourcePath = in.readString();
        String sourceType = in.readString();
        var attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) in.readGenericValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> sourceMetadata = (Map<String, Object>) in.readGenericValue();
        Integer estimatedRowSize = in.readOptionalVInt();
        List<ExternalSplit> splits = in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_SPLITS)
            ? in.readNamedWriteableCollectionAsList(ExternalSplit.class)
            : List.of();
        String datasetName = in.getTransportVersion().supports(ESQL_EXTERNAL_DATASET_NAME) ? in.readOptionalString() : null;

        return new ExternalSourceExec(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            List.of(),
            FormatReader.NO_LIMIT,
            null,
            estimatedRowSize,
            null,
            Map.of(),
            null,
            splits,
            datasetName,
            false
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(sourcePath);
        out.writeString(sourceType);
        out.writeNamedWriteableCollection(attributes);
        // Encrypted secrets in _datasource ride to data nodes that support the carrier; strip them for
        // older targets, which cannot deserialize the carrier and revert to prior behavior.
        out.writeGenericValue(
            out.getTransportVersion().supports(DATA_SOURCE_ENCRYPTED_DATA) ? config : ExternalSourceResolver.planConfig(config)
        );
        out.writeGenericValue(sourceMetadata);
        out.writeOptionalVInt(estimatedRowSize);
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_SPLITS)) {
            out.writeNamedWriteableCollection(splits);
        }
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_DATASET_NAME)) {
            out.writeOptionalString(datasetName);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String sourcePath() {
        return sourcePath;
    }

    public String sourceType() {
        return sourceType;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public Map<String, Object> config() {
        return config;
    }

    public Map<String, Object> sourceMetadata() {
        return sourceMetadata;
    }

    public Object pushedFilter() {
        return pushedFilter;
    }

    public List<Expression> pushedExpressions() {
        return pushedExpressions;
    }

    public int pushedLimit() {
        return pushedLimit;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    public FileList fileList() {
        return fileList;
    }

    public Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap() {
        return schemaMap;
    }

    @Nullable
    public ExternalSchema unifiedSchema() {
        return unifiedSchema;
    }

    public List<ExternalSplit> splits() {
        return splits;
    }

    public ExternalSourceExec withSplits(List<ExternalSplit> newSplits) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            newSplits,
            datasetName,
            deferredExtraction
        );
    }

    /**
     * Returns a copy carrying the given (coordinator-resolved) {@link FileList}. {@code fileList} is
     * build-time state copied from {@code ExternalRelation}; it is read off the exec by split
     * discovery and {@code planExternalSource} and is never serialized. See the class Javadoc.
     */
    public ExternalSourceExec withFileList(FileList newFileList) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            newFileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    public ExternalSourceExec withPushedFilter(Object newFilter) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            newFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    public ExternalSourceExec withPushedFilterAndExpressions(Object newFilter, List<Expression> newPushedExpressions) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            newFilter,
            newPushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    public ExternalSourceExec withPushedLimit(int newLimit) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            newLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    /**
     * Returns a copy of this source with the given output attribute list. Used by
     * {@code InsertExternalFieldExtraction} to narrow the source's projection (sort keys +
     * predicate columns) and inject a synthetic {@code _rowPosition} attribute that the paired
     * {@link org.elasticsearch.xpack.esql.plan.physical.ExternalFieldExtractExec} consumes.
     * <p>
     * Every other field — including pushed filter, pushed limit, splits, and the source path — is
     * preserved. The new attribute list is not validated against the source's reader schema; the
     * caller (the optimizer rule) is responsible for ensuring it is a valid subset plus
     * {@code _rowPosition}. Serialization is unaffected because attributes are part of the wire
     * format already.
     */
    public ExternalSourceExec withAttributes(List<Attribute> newAttributes) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            newAttributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    /**
     * Returns a copy of this source annotated with the given Top-N grouping hint. See {@link #pushedTopN()}.
     */
    public ExternalSourceExec withPushedTopN(@Nullable BlockHash.TopNDef newPushedTopN) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            newPushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    /**
     * The Top-N grouping hint set by {@code PushTopNIntoExternalSource}, or {@code null} if no Top-N pruning
     * should be performed during hash aggregation. This is a transient local-execution hint; it is never
     * serialized and is re-derived independently on each node.
     */
    @Nullable
    public BlockHash.TopNDef pushedTopN() {
        return pushedTopN;
    }

    /**
     * Returns a copy of this source carrying the given pre-prune Unified schema. See {@link #unifiedSchema()}.
     * Applied by {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec()} after
     * construction so the Unified schema does not appear in {@link #info()} (which would let the optimizer's
     * attribute-rewriting rules prune it along with {@code attributes}).
     */
    public ExternalSourceExec withUnifiedSchema(@Nullable ExternalSchema newUnifiedSchema) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            newUnifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    /**
     * Registered dataset identifier carried from {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation},
     * or {@code null} if this exec came from inline {@code EXTERNAL}. See
     * {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#datasetName()}.
     */
    @Nullable
    public String datasetName() {
        return datasetName;
    }

    /**
     * Whether a paired {@code ExternalFieldExtractExec} sits downstream consuming deferred-encoded
     * columns. See the field Javadoc for why this is its own signal rather than inferred from the
     * projection.
     */
    public boolean deferredExtraction() {
        return deferredExtraction;
    }

    /** Returns a copy of this source flagged for deferred extraction. See {@link #deferredExtraction()}. */
    public ExternalSourceExec withDeferredExtraction() {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            true
        );
    }

    /**
     * Returns a copy of this source carrying the given dataset name. Applied by
     * {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec()} after
     * construction; {@code datasetName} also flows through {@link #info()} so node-reflection
     * reconstruction preserves it.
     */
    public ExternalSourceExec withDatasetName(@Nullable String newDatasetName) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            newDatasetName,
            deferredExtraction
        );
    }

    @Override
    public PhysicalPlan estimateRowSize(EstimatesRowSize.State state) {
        int size = state.consumeAllFields(false);
        state.add(false, attributes);
        return Objects.equals(this.estimatedRowSize, size) ? this : withEstimatedRowSize(size);
    }

    protected ExternalSourceExec withEstimatedRowSize(Integer newEstimatedRowSize) {
        return new ExternalSourceExec(
            source(),
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            newEstimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        // pushedTopN: excluded — transient local-execution hint; including it would break the
        // node-reflection invariant in EsqlNodeSubclassTests#testInfoParameters. Preserved via
        // explicit with* methods; rendered in nodeString() for debuggability.
        // unifiedSchema: also excluded — the optimizer's attribute-rewriting rules walk every arg
        // in info() and would prune the Unified schema along with `attributes`, defeating its
        // whole purpose. Preserved through with* methods which carry it explicitly.
        // datasetName: INCLUDED — it is a plain String (attribute rewriting cannot prune it) and
        // it feeds the per-row _index value; excluding it would silently null _index whenever a
        // generic rule reconstructs this node via node reflection. Mirrors ExternalRelation#info.
        // deferredExtraction: excluded — transient local-execution signal like pushedTopN, set by
        // InsertExternalFieldExtraction after every reflection-driven rewrite has run; preserved
        // via withDeferredExtraction().
        return NodeInfo.create(
            this,
            ExternalSourceExec::new,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            estimatedRowSize,
            fileList,
            schemaMap,
            splits,
            datasetName
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExternalSourceExec other = (ExternalSourceExec) obj;
        return Objects.equals(sourcePath, other.sourcePath)
            && Objects.equals(sourceType, other.sourceType)
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(config, other.config)
            && Objects.equals(sourceMetadata, other.sourceMetadata)
            && Objects.equals(pushedFilter, other.pushedFilter)
            && Objects.equals(pushedExpressions, other.pushedExpressions)
            && pushedLimit == other.pushedLimit
            && Objects.equals(pushedTopN, other.pushedTopN)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(fileList, other.fileList)
            && Objects.equals(schemaMap, other.schemaMap)
            && Objects.equals(unifiedSchema, other.unifiedSchema)
            && Objects.equals(splits, other.splits)
            && Objects.equals(datasetName, other.datasetName)
            && deferredExtraction == other.deferredExtraction;
    }

    @Override
    public List<Object> nodeProperties() {
        // config and sourceMetadata may carry SecureString (dataset path) or plaintext String
        // (inline EXTERNAL path) secrets. Keep them out of EXPLAIN /
        // debug-log output.
        return List.of(sourcePath, sourceType, attributes);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // sourcePath (external location) and pushedFilter (opaque local-only filter) are free-form
        // user content — redact under anonymization. sourceType is a low-cardinality format enum.
        sb.append(nodeName()).append("[").append(mapper.opaque(sourcePath)).append("][").append(sourceType).append("]");
        if (pushedFilter != null) {
            sb.append("[filter=").append(mapper.opaque(String.valueOf(pushedFilter))).append("]");
        }
        if (pushedLimit != FormatReader.NO_LIMIT) {
            sb.append("[limit=").append(pushedLimit).append("]");
        }
        if (pushedTopN != null) {
            sb.append("[topN=order:")
                .append(pushedTopN.order())
                .append(",asc:")
                .append(pushedTopN.asc())
                .append(",nullsFirst:")
                .append(pushedTopN.nullsFirst())
                .append(",limit:")
                .append(pushedTopN.limit())
                .append("]");
        }
        if (splits.isEmpty() == false) {
            sb.append("[splits=").append(splits.size()).append("]");
        }
        if (datasetName != null) {
            // Dataset names are free-form user content, same as sourcePath — redact under anonymization.
            sb.append("[dataset=").append(mapper.opaque(datasetName)).append("]");
        }
        NodeUtils.toString(sb, attributes, format, mapper);
    }
}

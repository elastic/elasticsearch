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
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
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
 *   <li><b>Opaque pushed filter</b>: The {@link #pushedFilter()} is an opaque Object that only
 *       the source-specific operator factory interprets. It is NOT serialized; it is created
 *       locally on each data node by the LocalPhysicalPlanOptimizer via FormatReader.filterPushdownSupport()</li>
 *   <li><b>Data node execution</b>: Created on data nodes by LocalMapper from
 *       {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation} inside FragmentExec</li>
 *   <li><b>Optional anchor file schema</b>: For multi-file sources where the planner has resolved a
 *       single representative file's positional column schema (e.g. headerless CSV anchor inference),
 *       that schema travels here via {@link #fileSchema()} so runtime readers can use it as the
 *       authoritative positional layout instead of re-inferring per file. {@code null} when no
 *       anchor schema is available — readers fall back to existing per-file inference.</li>
 * </ul>
 */
public class ExternalSourceExec extends LeafExec implements EstimatesRowSize, DataSourceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExternalSourceExec",
        ExternalSourceExec::readFrom
    );

    private static final TransportVersion ESQL_EXTERNAL_SOURCE_SPLITS = TransportVersion.fromName("esql_external_source_splits");
    private static final TransportVersion ESQL_EXTERNAL_SOURCE_FILE_SCHEMA = TransportVersion.fromName("esql_external_source_file_schema");

    private final String sourcePath;
    private final String sourceType;
    private final List<Attribute> attributes;
    private final Map<String, Object> config;
    private final Map<String, Object> sourceMetadata;
    private final Object pushedFilter; // Opaque filter - NOT serialized, created locally on data nodes
    private final List<Expression> pushedExpressions; // NOT serialized - ESQL expressions for per-file re-translation
    private final int pushedLimit; // NOT serialized, set locally on data nodes
    /**
     * Transient hint that the data above this source is a STATS aggregation followed by a TopN over a single
     * grouping key. When present, the {@link BlockHash} can prune non-competitive groups during aggregation.
     * NOT serialized; set locally on each node by {@code PushTopNIntoExternalSource}.
     */
    @Nullable
    private final BlockHash.TopNDef pushedTopN;
    private final Integer estimatedRowSize;
    private final FileList fileList; // NOT serialized - resolved on coordinator, null on data nodes
    private final List<ExternalSplit> splits;
    /**
     * Positional file schema resolved at planning time (anchor file in a multi-file glob). Runtime
     * readers may use it as the authoritative positional column layout instead of per-file inference.
     * Serialized cross-node, gated by {@link #ESQL_EXTERNAL_SOURCE_FILE_SCHEMA}. {@code null} = no
     * anchor (older nodes, sources that don't compute one, or constructions that bypass
     * {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec()}).
     */
    @Nullable
    private final List<Attribute> fileSchema;

    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        Integer estimatedRowSize,
        FileList fileList
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            fileList,
            List.of(),
            null
        );
    }

    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        int pushedLimit,
        Integer estimatedRowSize,
        FileList fileList,
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
            pushedLimit,
            estimatedRowSize,
            fileList,
            splits,
            null
        );
    }

    /**
     * Public ctor that also accepts the optional anchor file schema. Use this from
     * {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec()} when the
     * planner has resolved an anchor schema; existing call sites that do not populate the schema
     * can keep using the shorter overload above (it forwards {@code null}).
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        int pushedLimit,
        Integer estimatedRowSize,
        FileList fileList,
        List<ExternalSplit> splits,
        List<Attribute> fileSchema
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
            pushedLimit,
            estimatedRowSize,
            fileList,
            splits,
            fileSchema
        );
    }

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
            estimatedRowSize,
            fileList,
            splits,
            null
        );
    }

    /**
     * Public 13-arg ctor (after Source): variant with pushed expressions AND optional anchor file schema.
     * Mirrors the canonical ctor below but without the transient {@code pushedTopN} hint.
     * Used by {@link #info()} so tree-rewrite preserves the anchor schema across optimizer transforms.
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
        List<ExternalSplit> splits,
        List<Attribute> fileSchema
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
            splits,
            fileSchema
        );
    }

    /**
     * Primary constructor that also accepts a transient {@link BlockHash.TopNDef} hint for in-hash TopN pruning.
     * Package-private on purpose so the public, longest constructor (used by tooling and tree tests) remains
     * the thirteen-arg one above. Use {@link #withPushedTopN(BlockHash.TopNDef)} from optimizer rules.
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
        List<ExternalSplit> splits,
        List<Attribute> fileSchema
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
        this.splits = splits != null ? List.copyOf(splits) : List.of();
        // Empty and null both mean "no anchor schema bound" — collapse them so readers see only null.
        this.fileSchema = (fileSchema == null || fileSchema.isEmpty()) ? null : List.copyOf(fileSchema);
    }

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
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            List.of(),
            null
        );
    }

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
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            List.of(),
            null
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
        // Older nodes don't write the field; on the wire empty list represents the in-memory null.
        List<Attribute> wireFileSchema = in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_FILE_SCHEMA)
            ? in.readNamedWriteableCollectionAsList(Attribute.class)
            : List.of();
        List<Attribute> fileSchema = wireFileSchema.isEmpty() ? null : wireFileSchema;

        return new ExternalSourceExec(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            splits,
            fileSchema
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(sourcePath);
        out.writeString(sourceType);
        out.writeNamedWriteableCollection(attributes);
        out.writeGenericValue(config);
        out.writeGenericValue(sourceMetadata);
        out.writeOptionalVInt(estimatedRowSize);
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_SPLITS)) {
            out.writeNamedWriteableCollection(splits);
        }
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_FILE_SCHEMA)) {
            // Empty list on the wire represents the in-memory null (no anchor schema).
            out.writeNamedWriteableCollection(fileSchema != null ? fileSchema : List.of());
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

    public List<ExternalSplit> splits() {
        return splits;
    }

    /**
     * Positional file schema resolved at planning time (anchor file in a multi-file glob).
     * {@code null} when no anchor was computed — readers fall back to per-file inference.
     */
    @Nullable
    public List<Attribute> fileSchema() {
        return fileSchema;
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
            newSplits,
            fileSchema
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
            splits,
            fileSchema
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
            splits,
            fileSchema
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
            splits,
            fileSchema
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
            splits,
            fileSchema
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
            splits,
            fileSchema
        );
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        // pushedTopN is intentionally excluded from info(): it is a transient local-execution hint set after the
        // plan has been distributed, and including it here would (a) require a public ctor that exposes the hint
        // and breaks the node-reflection invariant in EsqlNodeSubclassTests#testInfoParameters and (b) leak the
        // hint into any generic transform-based plan rewrite. The hint is preserved by the explicit with* methods
        // that need it and is rendered in nodeString() for debuggability.
        //
        // fileSchema IS included: it is a structural planning-time field that must survive tree-rewrites by
        // optimizer rules. Excluding it would silently drop it on every plan-rebuild.
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
            splits,
            fileSchema
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
            splits,
            fileSchema
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
            && Objects.equals(splits, other.splits)
            && Objects.equals(fileSchema, other.fileSchema);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format) {
        sb.append(nodeName()).append("[").append(sourcePath).append("][").append(sourceType).append("]");
        if (pushedFilter != null) {
            sb.append("[filter=").append(pushedFilter).append("]");
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
        if (fileSchema != null && fileSchema.isEmpty() == false) {
            sb.append("[fileSchema=").append(fileSchema.size()).append("]");
        }
        NodeUtils.toString(sb, attributes, format);
    }
}

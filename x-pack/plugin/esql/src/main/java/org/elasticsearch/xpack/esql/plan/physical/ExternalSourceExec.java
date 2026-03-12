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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
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
 *       locally on each data node by the LocalPhysicalPlanOptimizer via FilterPushdownRegistry</li>
 *   <li><b>Data node execution</b>: Created on data nodes by LocalMapper from
 *       {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation} inside FragmentExec</li>
 * </ul>
 */
public class ExternalSourceExec extends LeafExec implements EstimatesRowSize, DataSourceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExternalSourceExec",
        ExternalSourceExec::readFrom
    );

    private static final TransportVersion ESQL_EXTERNAL_SOURCE_SPLITS = TransportVersion.fromName("esql_external_source_splits");

    private final String sourcePath;
    private final String sourceType;
    private final List<Attribute> attributes;
    private final Map<String, Object> config;
    private final Map<String, Object> sourceMetadata;
    private final Object pushedFilter; // Opaque filter - NOT serialized, created locally on data nodes
    private final int pushedLimit; // NOT serialized, set locally on data nodes
    private final Integer estimatedRowSize;
    private final FileSet fileSet; // NOT serialized - resolved on coordinator, null on data nodes
    private final List<ExternalSplit> splits;

    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        Integer estimatedRowSize,
        FileSet fileSet
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
            fileSet,
            List.of()
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
        FileSet fileSet,
        List<ExternalSplit> splits
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
        this.pushedLimit = pushedLimit;
        this.estimatedRowSize = estimatedRowSize;
        this.fileSet = fileSet;
        this.splits = splits != null ? List.copyOf(splits) : List.of();
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
            List.of()
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
            splits
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

    public int pushedLimit() {
        return pushedLimit;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    public FileSet fileSet() {
        return fileSet;
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
            pushedLimit,
            estimatedRowSize,
            fileSet,
            newSplits
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
            pushedLimit,
            estimatedRowSize,
            fileSet,
            splits
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
            newLimit,
            estimatedRowSize,
            fileSet,
            splits
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
            pushedLimit,
            newEstimatedRowSize,
            fileSet,
            splits
        );
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            ExternalSourceExec::new,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedLimit,
            estimatedRowSize,
            fileSet,
            splits
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
            pushedLimit,
            estimatedRowSize,
            fileSet,
            splits
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
            && pushedLimit == other.pushedLimit
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(fileSet, other.fileSet)
            && Objects.equals(splits, other.splits);
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        String filterStr = pushedFilter != null ? "[filter=" + pushedFilter + "]" : "";
        String limitStr = pushedLimit != FormatReader.NO_LIMIT ? "[limit=" + pushedLimit + "]" : "";
        String splitsStr = splits.isEmpty() == false ? "[splits=" + splits.size() + "]" : "";
        return nodeName()
            + "["
            + sourcePath
            + "]["
            + sourceType
            + "]"
            + filterStr
            + limitStr
            + splitsStr
            + NodeUtils.toString(attributes, format);
    }
}

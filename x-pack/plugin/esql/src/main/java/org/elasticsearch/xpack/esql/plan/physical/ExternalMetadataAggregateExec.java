/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.MetadataAggregateReader;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushAggregatesToExternalSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Physical plan node that computes ungrouped {@code COUNT(*)}, {@code MIN}, and {@code MAX}
 * aggregates by reading per-file aggregate metadata in parallel at execution time, instead
 * of scanning row data.
 * <p>
 * Created by {@link PushAggregatesToExternalSource} when planning-time pushdown is
 * unavailable (e.g. multi-file globs where statistics would have to be read serially) and
 * the format reader implements {@link MetadataAggregateReader}.
 * <p>
 * Each split is dispatched to a driver in parallel; the operator emits one intermediate-format
 * {@code Page} per split, matching {@link AggregateExec#intermediateAttributes()} of the parent
 * aggregate so the existing FINAL-mode reducer can aggregate them without modification.
 * <p>
 * The node is only ever created on data nodes by {@code LocalPhysicalPlanOptimizer} and
 * consumed by the same node's {@code LocalExecutionPlanner}; it is never serialized across
 * the wire. The {@link NamedWriteable} implementation
 * exists only to satisfy the {@link PhysicalPlan} registry convention and supporting
 * infrastructure (round-trip tests, plan-equality checks).
 */
public final class ExternalMetadataAggregateExec extends LeafExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExternalMetadataAggregateExec",
        ExternalMetadataAggregateExec::readFrom
    );

    private final String sourcePath;
    private final String sourceType;
    private final Map<String, Object> config;
    private final List<ExternalSplit> splits;
    private final List<NamedExpression> aggregates;
    private final List<Attribute> intermediateAttributes;
    private final List<String> columnsToProbe;

    public ExternalMetadataAggregateExec(
        Source source,
        String sourcePath,
        String sourceType,
        Map<String, Object> config,
        List<ExternalSplit> splits,
        List<NamedExpression> aggregates,
        List<Attribute> intermediateAttributes,
        List<String> columnsToProbe
    ) {
        super(source);
        this.sourcePath = Objects.requireNonNull(sourcePath, "sourcePath");
        this.sourceType = Objects.requireNonNull(sourceType, "sourceType");
        this.config = config != null ? Map.copyOf(config) : Map.of();
        this.splits = splits != null ? List.copyOf(splits) : List.of();
        this.aggregates = List.copyOf(Objects.requireNonNull(aggregates, "aggregates"));
        this.intermediateAttributes = List.copyOf(Objects.requireNonNull(intermediateAttributes, "intermediateAttributes"));
        this.columnsToProbe = columnsToProbe != null ? List.copyOf(columnsToProbe) : List.of();
    }

    private static ExternalMetadataAggregateExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String sourcePath = in.readString();
        String sourceType = in.readString();
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) in.readGenericValue();
        List<ExternalSplit> splits = in.readNamedWriteableCollectionAsList(ExternalSplit.class);
        List<NamedExpression> aggregates = in.readNamedWriteableCollectionAsList(NamedExpression.class);
        List<Attribute> intermediateAttributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        List<String> columnsToProbe = in.readStringCollectionAsList();
        return new ExternalMetadataAggregateExec(
            source,
            sourcePath,
            sourceType,
            config,
            splits,
            aggregates,
            intermediateAttributes,
            columnsToProbe
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeString(sourcePath);
        out.writeString(sourceType);
        out.writeGenericValue(config);
        out.writeNamedWriteableCollection(splits);
        out.writeNamedWriteableCollection(aggregates);
        out.writeNamedWriteableCollection(intermediateAttributes);
        out.writeStringCollection(columnsToProbe);
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

    public Map<String, Object> config() {
        return config;
    }

    public List<ExternalSplit> splits() {
        return splits;
    }

    public List<NamedExpression> aggregates() {
        return aggregates;
    }

    public List<Attribute> intermediateAttributes() {
        return intermediateAttributes;
    }

    public List<String> columnsToProbe() {
        return columnsToProbe;
    }

    @Override
    public List<Attribute> output() {
        return intermediateAttributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            ExternalMetadataAggregateExec::new,
            sourcePath,
            sourceType,
            config,
            splits,
            aggregates,
            intermediateAttributes,
            columnsToProbe
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, sourceType, config, splits, aggregates, intermediateAttributes, columnsToProbe);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ExternalMetadataAggregateExec other = (ExternalMetadataAggregateExec) obj;
        return Objects.equals(sourcePath, other.sourcePath)
            && Objects.equals(sourceType, other.sourceType)
            && Objects.equals(config, other.config)
            && Objects.equals(splits, other.splits)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(intermediateAttributes, other.intermediateAttributes)
            && Objects.equals(columnsToProbe, other.columnsToProbe);
    }
}

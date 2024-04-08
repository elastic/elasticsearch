/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Custom cluster state metadata that stores all the snapshot lifecycle
 * policies and their associated metadata
 */
public class SnapshotLifecycleMetadata implements Metadata.Custom {

    public static final String TYPE = "snapshot_lifecycle";

    private static final ParseField OPERATION_MODE_FIELD = new ParseField("operation_mode");
    private static final ParseField POLICIES_FIELD = new ParseField("policies");
    private static final ParseField STATS_FIELD = new ParseField("stats");

    public static final SnapshotLifecycleMetadata EMPTY = new SnapshotLifecycleMetadata(
        Collections.emptyMap(),
        OperationMode.RUNNING,
        new SnapshotLifecycleStats()
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecycleMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        a -> new SnapshotLifecycleMetadata(
            ((List<SnapshotLifecyclePolicyMetadata>) a[0]).stream()
                .collect(Collectors.toMap(m -> m.getPolicy().getId(), Function.identity())),
            OperationMode.valueOf((String) a[1]),
            (SnapshotLifecycleStats) a[2]
        )
    );

    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> SnapshotLifecyclePolicyMetadata.parse(p, n),
            v -> {
                throw new IllegalArgumentException("ordered " + POLICIES_FIELD.getPreferredName() + " are not supported");
            },
            POLICIES_FIELD
        );
        PARSER.declareString(ConstructingObjectParser.constructorArg(), OPERATION_MODE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (v, o) -> SnapshotLifecycleStats.parse(v), STATS_FIELD);
    }

    private final Map<String, SnapshotLifecyclePolicyMetadata> snapshotConfigurations;
    private final OperationMode operationMode;
    private final SnapshotLifecycleStats slmStats;

    public SnapshotLifecycleMetadata(
        Map<String, SnapshotLifecyclePolicyMetadata> snapshotConfigurations,
        OperationMode operationMode,
        SnapshotLifecycleStats slmStats
    ) {
        this.snapshotConfigurations = new HashMap<>(snapshotConfigurations);
        this.operationMode = operationMode;
        this.slmStats = slmStats != null ? slmStats : new SnapshotLifecycleStats();
    }

    public SnapshotLifecycleMetadata(StreamInput in) throws IOException {
        this.snapshotConfigurations = in.readMap(SnapshotLifecyclePolicyMetadata::new);
        this.operationMode = in.readEnum(OperationMode.class);
        this.slmStats = new SnapshotLifecycleStats(in);
    }

    public Map<String, SnapshotLifecyclePolicyMetadata> getSnapshotConfigurations() {
        return Collections.unmodifiableMap(this.snapshotConfigurations);
    }

    /**
     * @deprecated use {@link LifecycleOperationMetadata#getSLMOperationMode()} instead. This may be incorrect.
     */
    @Deprecated(since = "8.7.0")
    public OperationMode getOperationMode() {
        return operationMode;
    }

    public SnapshotLifecycleStats getStats() {
        return this.slmStats;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new SnapshotLifecycleMetadataDiff((SnapshotLifecycleMetadata) previousState, this);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_4_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.snapshotConfigurations, StreamOutput::writeWriteable);
        out.writeEnum(this.operationMode);
        this.slmStats.writeTo(out);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            ChunkedToXContentHelper.xContentValuesMap(POLICIES_FIELD.getPreferredName(), this.snapshotConfigurations),
            Iterators.single((builder, params) -> {
                builder.field(OPERATION_MODE_FIELD.getPreferredName(), operationMode);
                builder.field(STATS_FIELD.getPreferredName(), this.slmStats);
                return builder;
            })
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.snapshotConfigurations, this.operationMode, this.slmStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotLifecycleMetadata other = (SnapshotLifecycleMetadata) obj;
        return this.snapshotConfigurations.equals(other.snapshotConfigurations)
            && this.operationMode.equals(other.operationMode)
            && this.slmStats.equals(other.slmStats);
    }

    public static class SnapshotLifecycleMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, SnapshotLifecyclePolicyMetadata>> lifecycles;
        final OperationMode operationMode;
        final SnapshotLifecycleStats slmStats;

        SnapshotLifecycleMetadataDiff(SnapshotLifecycleMetadata before, SnapshotLifecycleMetadata after) {
            this.lifecycles = DiffableUtils.diff(
                before.snapshotConfigurations,
                after.snapshotConfigurations,
                DiffableUtils.getStringKeySerializer()
            );
            this.operationMode = after.operationMode;
            this.slmStats = after.slmStats;
        }

        public SnapshotLifecycleMetadataDiff(StreamInput in) throws IOException {
            this.lifecycles = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                SnapshotLifecyclePolicyMetadata::new,
                SnapshotLifecycleMetadataDiff::readLifecyclePolicyDiffFrom
            );
            this.operationMode = in.readEnum(OperationMode.class);
            this.slmStats = new SnapshotLifecycleStats(in);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            TreeMap<String, SnapshotLifecyclePolicyMetadata> newLifecycles = new TreeMap<>(
                lifecycles.apply(((SnapshotLifecycleMetadata) part).snapshotConfigurations)
            );
            return new SnapshotLifecycleMetadata(newLifecycles, this.operationMode, this.slmStats);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            lifecycles.writeTo(out);
            out.writeEnum(this.operationMode);
            slmStats.writeTo(out);
        }

        static Diff<SnapshotLifecyclePolicyMetadata> readLifecyclePolicyDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(SnapshotLifecyclePolicyMetadata::new, in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_7_4_0;
        }

    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackPlugin.XPackMetaDataCustom;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Custom cluster state metadata that stores all the snapshot lifecycle
 * policies and their associated metadata
 */
public class SnapshotLifecycleMetadata implements XPackMetaDataCustom {

    public static final String TYPE = "snapshot_lifecycle";
    public static final ParseField POLICIES_FIELD = new ParseField("policies");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecycleMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE,
        a -> new SnapshotLifecycleMetadata(
            ((List<SnapshotLifecyclePolicyMetadata>) a[0]).stream()
                .collect(Collectors.toMap(m -> m.getPolicy().getId(), Function.identity()))));

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> SnapshotLifecyclePolicyMetadata.parse(p, n),
            v -> {
                throw new IllegalArgumentException("ordered " + POLICIES_FIELD.getPreferredName() + " are not supported");
            }, POLICIES_FIELD);
    }

    private final Map<String, SnapshotLifecyclePolicyMetadata> snapshotConfigurations;

    public SnapshotLifecycleMetadata(Map<String, SnapshotLifecyclePolicyMetadata> snapshotConfigurations) {
        this.snapshotConfigurations = new HashMap<>(snapshotConfigurations);
        // TODO: maybe operation mode here so it can be disabled/re-enabled separately like ILM is
    }

    public SnapshotLifecycleMetadata(StreamInput in) throws IOException {
        this.snapshotConfigurations = in.readMap(StreamInput::readString, SnapshotLifecyclePolicyMetadata::new);
    }

    public Map<String, SnapshotLifecyclePolicyMetadata> getSnapshotConfigurations() {
        return Collections.unmodifiableMap(this.snapshotConfigurations);
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    @Override
    public Diff<MetaData.Custom> diff(MetaData.Custom previousState) {
        return new SnapshotLifecycleMetadataDiff((SnapshotLifecycleMetadata) previousState, this);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0; // TODO: revisit this when we figure out where this goes
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.snapshotConfigurations, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(POLICIES_FIELD.getPreferredName(), this.snapshotConfigurations);
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class SnapshotLifecycleMetadataDiff implements NamedDiff<MetaData.Custom> {

        final Diff<Map<String, SnapshotLifecyclePolicyMetadata>> lifecycles;

        SnapshotLifecycleMetadataDiff(SnapshotLifecycleMetadata before, SnapshotLifecycleMetadata after) {
            this.lifecycles = DiffableUtils.diff(before.snapshotConfigurations, after.snapshotConfigurations,
                DiffableUtils.getStringKeySerializer());
        }

        public SnapshotLifecycleMetadataDiff(StreamInput in) throws IOException {
            this.lifecycles = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(),
                SnapshotLifecyclePolicyMetadata::new,
                SnapshotLifecycleMetadataDiff::readLifecyclePolicyDiffFrom);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, SnapshotLifecyclePolicyMetadata> newLifecycles = new TreeMap<>(
                lifecycles.apply(((SnapshotLifecycleMetadata) part).snapshotConfigurations));
            return new SnapshotLifecycleMetadata(newLifecycles);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            lifecycles.writeTo(out);
        }

        static Diff<SnapshotLifecyclePolicyMetadata> readLifecyclePolicyDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(SnapshotLifecyclePolicyMetadata::new, in);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IndexLifecycleMetadata implements Metadata.Custom {
    public static final String TYPE = "index_lifecycle";
    public static final ParseField OPERATION_MODE_FIELD = new ParseField("operation_mode");
    public static final ParseField POLICIES_FIELD = new ParseField("policies");
    public static final IndexLifecycleMetadata EMPTY = new IndexLifecycleMetadata(Collections.emptySortedMap(), OperationMode.RUNNING);

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<IndexLifecycleMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        a -> new IndexLifecycleMetadata(
            ((List<LifecyclePolicyMetadata>) a[0]).stream()
                .collect(Collectors.toMap(LifecyclePolicyMetadata::getName, Function.identity())),
            OperationMode.valueOf((String) a[1])
        )
    );
    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> LifecyclePolicyMetadata.parse(p, n),
            v -> { throw new IllegalArgumentException("ordered " + POLICIES_FIELD.getPreferredName() + " are not supported"); },
            POLICIES_FIELD
        );
        PARSER.declareString(ConstructingObjectParser.constructorArg(), OPERATION_MODE_FIELD);
    }

    private final Map<String, LifecyclePolicyMetadata> policyMetadatas;
    private final OperationMode operationMode;

    public IndexLifecycleMetadata(Map<String, LifecyclePolicyMetadata> policies, OperationMode operationMode) {
        this.policyMetadatas = Collections.unmodifiableMap(policies);
        this.operationMode = operationMode;
    }

    public IndexLifecycleMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        TreeMap<String, LifecyclePolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            policies.put(in.readString(), new LifecyclePolicyMetadata(in));
        }
        this.policyMetadatas = policies;
        this.operationMode = in.readEnum(OperationMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(policyMetadatas, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeEnum(operationMode);
    }

    public Map<String, LifecyclePolicyMetadata> getPolicyMetadatas() {
        return policyMetadatas;
    }

    public OperationMode getOperationMode() {
        return operationMode;
    }

    public Map<String, LifecyclePolicy> getPolicies() {
        return policyMetadatas.values()
            .stream()
            .map(LifecyclePolicyMetadata::getPolicy)
            .collect(Collectors.toMap(LifecyclePolicy::getName, Function.identity()));
    }

    @Override
    public Diff<Custom> diff(Custom previousState) {
        return new IndexLifecycleMetadataDiff((IndexLifecycleMetadata) previousState, this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.xContentValuesMap(POLICIES_FIELD.getPreferredName(), policyMetadatas);
        builder.field(OPERATION_MODE_FIELD.getPreferredName(), operationMode);
        return builder;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyMetadatas, operationMode);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        IndexLifecycleMetadata other = (IndexLifecycleMetadata) obj;
        return Objects.equals(policyMetadatas, other.policyMetadatas) && Objects.equals(operationMode, other.operationMode);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class IndexLifecycleMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, LifecyclePolicyMetadata>> policies;
        final OperationMode operationMode;

        IndexLifecycleMetadataDiff(IndexLifecycleMetadata before, IndexLifecycleMetadata after) {
            this.policies = DiffableUtils.diff(before.policyMetadatas, after.policyMetadatas, DiffableUtils.getStringKeySerializer());
            this.operationMode = after.operationMode;
        }

        public IndexLifecycleMetadataDiff(StreamInput in) throws IOException {
            this.policies = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                LifecyclePolicyMetadata::new,
                IndexLifecycleMetadataDiff::readLifecyclePolicyDiffFrom
            );
            this.operationMode = in.readEnum(OperationMode.class);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            TreeMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(
                policies.apply(((IndexLifecycleMetadata) part).policyMetadatas)
            );
            return new IndexLifecycleMetadata(newPolicies, this.operationMode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            policies.writeTo(out);
            out.writeEnum(operationMode);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        static Diff<LifecyclePolicyMetadata> readLifecyclePolicyDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(LifecyclePolicyMetadata::new, in);
        }
    }
}

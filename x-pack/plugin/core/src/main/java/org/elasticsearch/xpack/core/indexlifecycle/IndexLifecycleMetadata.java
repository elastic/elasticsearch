/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class IndexLifecycleMetadata implements MetaData.Custom {
    public static final String TYPE = "index_lifecycle";
    public static final ParseField POLICIES_FIELD = new ParseField("policies");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<IndexLifecycleMetadata, Void> PARSER = new ConstructingObjectParser<>(
            TYPE, a -> new IndexLifecycleMetadata(
                    ObjectParserUtils.convertListToMapValues(LifecyclePolicyMetadata::getName, (List<LifecyclePolicyMetadata>) a[0])));
    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> LifecyclePolicyMetadata.parse(p, n),
                v -> {
                    throw new IllegalArgumentException("ordered " + POLICIES_FIELD.getPreferredName() + " are not supported");
                }, POLICIES_FIELD);
    }

    private final Map<String, LifecyclePolicyMetadata> policyMetadatas;

    public IndexLifecycleMetadata(Map<String, LifecyclePolicyMetadata> policies) {
        this.policyMetadatas = Collections.unmodifiableMap(policies);
    }

    public IndexLifecycleMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        TreeMap<String, LifecyclePolicyMetadata> policies = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            policies.put(in.readString(), new LifecyclePolicyMetadata(in));
        }
        this.policyMetadatas = policies;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(policyMetadatas.size());
        for (Map.Entry<String, LifecyclePolicyMetadata> entry : policyMetadatas.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public Map<String, LifecyclePolicyMetadata> getPolicyMetadatas() {
        return policyMetadatas;
    }

    public Map<String, LifecyclePolicy> getPolicies() {
        return policyMetadatas.values().stream().map(LifecyclePolicyMetadata::getPolicy)
                .collect(Collectors.toMap(LifecyclePolicy::getName, Function.identity()));
    }

    @Override
    public Diff<Custom> diff(Custom previousState) {
        return new IndexLifecycleMetadataDiff((IndexLifecycleMetadata) previousState, this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(POLICIES_FIELD.getPreferredName(), policyMetadatas);
        return builder;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_0_0_alpha1;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policyMetadatas);
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
        return Objects.equals(policyMetadatas, other.policyMetadatas);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class IndexLifecycleMetadataDiff implements NamedDiff<MetaData.Custom> {

        final Diff<Map<String, LifecyclePolicyMetadata>> policies;

        IndexLifecycleMetadataDiff(IndexLifecycleMetadata before, IndexLifecycleMetadata after) {
            this.policies = DiffableUtils.diff(before.policyMetadatas, after.policyMetadatas, DiffableUtils.getStringKeySerializer());
        }

        public IndexLifecycleMetadataDiff(StreamInput in) throws IOException {
            this.policies = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), LifecyclePolicyMetadata::new,
                    IndexLifecycleMetadataDiff::readLifecyclePolicyDiffFrom);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            TreeMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(
                    policies.apply(((IndexLifecycleMetadata) part).policyMetadatas));
            return new IndexLifecycleMetadata(newPolicies);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            policies.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        static Diff<LifecyclePolicyMetadata> readLifecyclePolicyDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(LifecyclePolicyMetadata::new, in);
        }
    }
}

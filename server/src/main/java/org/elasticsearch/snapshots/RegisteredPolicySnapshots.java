/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.snapshots.SnapshotsService.POLICY_ID_METADATA_FIELD;

/**
 * {@link RegisteredPolicySnapshots} records a set of snapshot IDs along with their SLM policy name. It is used to infer
 * the failure of snapshots which did not record their failure in SnapshotLifecycleStats. The set is stored in the
 * cluster state as custom metadata. When a snapshot is started by SLM, it is added to this set. Upon completion,
 * is it removed. If a snapshot does not record its failure in SnapshotLifecycleStats, likely due to a master shutdown,
 * it will not be removed from the registered set. A subsequent snapshot will then find that a registered snapshot
 * is no longer running and will infer that it failed, updating SnapshotLifecycleStats accordingly.
 */
public class RegisteredPolicySnapshots implements Metadata.Custom {

    public static final String TYPE = "registered_snapshots";
    private static final ParseField SNAPSHOTS = new ParseField("snapshots");
    public static final RegisteredPolicySnapshots EMPTY = new RegisteredPolicySnapshots(List.of());

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RegisteredPolicySnapshots, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        a -> new RegisteredPolicySnapshots((List<PolicySnapshot>) a[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> PolicySnapshot.parse(p), SNAPSHOTS);
    }

    private final List<PolicySnapshot> snapshots;

    public RegisteredPolicySnapshots(List<PolicySnapshot> snapshots) {
        this.snapshots = Collections.unmodifiableList(snapshots);
    }

    public RegisteredPolicySnapshots(StreamInput in) throws IOException {
        this.snapshots = in.readCollectionAsImmutableList(PolicySnapshot::new);
    }

    public List<PolicySnapshot> getSnapshots() {
        return snapshots;
    }

    public boolean contains(SnapshotId snapshotId) {
        return snapshots.stream().map(PolicySnapshot::getSnapshotId).anyMatch(snapshotId::equals);
    }

    public List<SnapshotId> getSnapshotsByPolicy(String policy) {
        return snapshots.stream().filter(s -> s.getPolicy().equals(policy)).map(PolicySnapshot::getSnapshotId).toList();
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new RegisteredSnapshotsDiff((RegisteredPolicySnapshots) previousState, this);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_16_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(snapshots);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(Iterators.single((builder, params) -> {
            builder.field(SNAPSHOTS.getPreferredName(), snapshots);
            return builder;
        }));
    }

    public static RegisteredPolicySnapshots parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return "RegisteredSnapshots{" + "snapshots=" + snapshots + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RegisteredPolicySnapshots other = (RegisteredPolicySnapshots) obj;
        return Objects.equals(snapshots, other.snapshots);
    }

    public static class RegisteredSnapshotsDiff implements NamedDiff<Metadata.Custom> {
        final List<PolicySnapshot> snapshots;

        RegisteredSnapshotsDiff(RegisteredPolicySnapshots before, RegisteredPolicySnapshots after) {
            this.snapshots = after.snapshots;
        }

        public RegisteredSnapshotsDiff(StreamInput in) throws IOException {
            this.snapshots = new RegisteredPolicySnapshots(in).snapshots;
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new RegisteredPolicySnapshots(snapshots);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(snapshots);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_16_0;
        }
    }

    public Builder builder() {
        return new Builder(this);
    }

    private static String getPolicyFromMetadata(Map<String, Object> userMetadata) {
        if (userMetadata != null && userMetadata.get(POLICY_ID_METADATA_FIELD) instanceof String p) {
            return p;
        }
        return null;
    }

    public static class Builder {
        final List<PolicySnapshot> snapshots;

        Builder(RegisteredPolicySnapshots registeredPolicySnapshots) {
            this.snapshots = new ArrayList<>(registeredPolicySnapshots.snapshots);
        }

        /**
         * Add the snapshotId to the registered set if its metadata contains a policyId, meaning that it was initiated by SLM.
         * @param userMetadata metadata provided by the user in the CreateSnapshotRequest
         *                     If the request is from SLM it will contain a key "policy" with an SLM policy name as the value.
         * @param snapshotId the snapshotId to potentially add to the registered set
         */
        void maybeAdd(Map<String, Object> userMetadata, SnapshotId snapshotId) {
            final String policy = getPolicyFromMetadata(userMetadata);
            if (policy != null) {
                snapshots.add(new PolicySnapshot(policy, snapshotId));
            }
        }

        RegisteredPolicySnapshots build() {
            return new RegisteredPolicySnapshots(snapshots);
        }
    }

    public static class PolicySnapshot implements SimpleDiffable<PolicySnapshot>, Writeable, ToXContentObject {
        private final String policy;
        private final SnapshotId snapshotId;

        private static final ParseField POLICY = new ParseField("policy");
        private static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PolicySnapshot, String> PARSER = new ConstructingObjectParser<>(
            "snapshot",
            true,
            (a, id) -> new PolicySnapshot((String) a[0], (SnapshotId) a[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotId::parse, SNAPSHOT_ID);
        }

        public PolicySnapshot(String policy, SnapshotId snapshotId) {
            this.policy = policy;
            this.snapshotId = snapshotId;
        }

        public PolicySnapshot(StreamInput in) throws IOException {
            this.policy = in.readString();
            this.snapshotId = new SnapshotId(in);
        }

        public String getPolicy() {
            return policy;
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public static PolicySnapshot parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.policy);
            snapshotId.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(POLICY.getPreferredName(), this.policy);
            builder.field(SNAPSHOT_ID.getPreferredName(), this.snapshotId);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy, snapshotId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            PolicySnapshot other = (PolicySnapshot) obj;
            return Objects.equals(policy, other.policy) && Objects.equals(snapshotId, other.snapshotId);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}

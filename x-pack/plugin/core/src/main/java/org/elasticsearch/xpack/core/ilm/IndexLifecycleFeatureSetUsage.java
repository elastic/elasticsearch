/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IndexLifecycleFeatureSetUsage extends XPackFeatureSet.Usage {

    private List<PolicyStats> policyStats;

    public IndexLifecycleFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        if (input.readBoolean()) {
            policyStats = input.readList(PolicyStats::new);
        }
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        boolean hasPolicyStats = policyStats != null;
        out.writeBoolean(hasPolicyStats);
        if (hasPolicyStats) {
            out.writeList(policyStats);
        }
    }

    public IndexLifecycleFeatureSetUsage() {
        this((List<PolicyStats>) null);
    }

    public IndexLifecycleFeatureSetUsage(List<PolicyStats> policyStats) {
        super(XPackField.INDEX_LIFECYCLE, true, true);
        this.policyStats = policyStats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        if (policyStats != null) {
            builder.field("policy_count", policyStats.size());
            builder.xContentList("policy_stats", policyStats);
        }
    }

    public List<PolicyStats> getPolicyStats() {
        return policyStats;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, policyStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IndexLifecycleFeatureSetUsage other = (IndexLifecycleFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(policyStats, other.policyStats);
    }

    public static final class PolicyStats implements ToXContentObject, Writeable {

        public static final ParseField INDICES_MANAGED_FIELD = new ParseField("indices_managed");

        private final Map<String, PhaseStats> phaseStats;
        private final int indicesManaged;

        public PolicyStats(Map<String, PhaseStats> phaseStats, int numberIndicesManaged) {
            this.phaseStats = phaseStats;
            this.indicesManaged = numberIndicesManaged;
        }

        public PolicyStats(StreamInput in) throws IOException {
            this.phaseStats = in.readMap(StreamInput::readString, PhaseStats::new);
            this.indicesManaged = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(phaseStats, StreamOutput::writeString, (o, p) -> p.writeTo(o));
            out.writeVInt(indicesManaged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LifecyclePolicy.PHASES_FIELD.getPreferredName(), phaseStats);
            builder.field(INDICES_MANAGED_FIELD.getPreferredName(), indicesManaged);
            builder.endObject();
            return builder;
        }

        public Map<String, PhaseStats> getPhaseStats() {
            return phaseStats;
        }

        public int getIndicesManaged() {
            return indicesManaged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(phaseStats, indicesManaged);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PolicyStats other = (PolicyStats) obj;
            return Objects.equals(phaseStats, other.phaseStats) && Objects.equals(indicesManaged, other.indicesManaged);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final class PhaseStats implements ToXContentObject, Writeable {

        public static final ParseField CONFIGURATIONS_FIELD = new ParseField("configurations");

        private final String[] actionNames;
        private final ActionConfigStats configurations;
        private final TimeValue minimumAge;

        public PhaseStats(TimeValue after, String[] actionNames, ActionConfigStats configurations) {
            this.actionNames = Objects.requireNonNull(actionNames, "Missing required action names");
            this.configurations = Objects.requireNonNull(configurations, "Missing required action configurations");
            this.minimumAge = Objects.requireNonNull(after, "Missing required minimum age");
        }

        public PhaseStats(StreamInput in) throws IOException {
            actionNames = in.readStringArray();
            minimumAge = in.readTimeValue();
            configurations = new ActionConfigStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(actionNames);
            out.writeTimeValue(minimumAge);
            configurations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Phase.MIN_AGE.getPreferredName(), minimumAge.getMillis());
            builder.field(Phase.ACTIONS_FIELD.getPreferredName(), actionNames);
            builder.field(CONFIGURATIONS_FIELD.getPreferredName(), configurations);
            builder.endObject();
            return builder;
        }

        public String[] getActionNames() {
            return actionNames;
        }

        public TimeValue getAfter() {
            return minimumAge;
        }

        public ActionConfigStats getConfigurations() {
            return configurations;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(actionNames), configurations, minimumAge);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PhaseStats other = (PhaseStats) obj;
            return Objects.equals(minimumAge, other.minimumAge)
                && Objects.deepEquals(configurations, other.configurations)
                && Objects.deepEquals(actionNames, other.actionNames);
        }
    }

    public static final class ActionConfigStats implements ToXContentObject, Writeable {
        private final Integer allocateNumberOfReplicas;
        private final Integer forceMergeMaxNumberOfSegments;
        private final TimeValue rolloverMaxAge;
        private final Long rolloverMaxDocs;
        private final Long rolloverMaxPrimaryShardDocs;
        private final ByteSizeValue rolloverMaxPrimaryShardSize;
        private final ByteSizeValue rolloverMaxSize;
        private final TimeValue rolloverMinAge;
        private final Long rolloverMinDocs;
        private final Long rolloverMinPrimaryShardDocs;
        private final ByteSizeValue rolloverMinPrimaryShardSize;
        private final ByteSizeValue rolloverMinSize;
        private final Integer setPriorityPriority;
        private final ByteSizeValue shrinkMaxPrimaryShardSize;
        private final Integer shrinkNumberOfShards;

        public static Builder builder() {
            return new Builder();
        }

        public static Builder builder(ActionConfigStats existing) {
            return new Builder(existing);
        }

        public static final class Builder {
            private Integer allocateNumberOfReplicas;
            private Integer forceMergeMaxNumberOfSegments;
            private TimeValue rolloverMaxAge;
            private Long rolloverMaxDocs;
            private Long rolloverMaxPrimaryShardDocs;
            private ByteSizeValue rolloverMaxPrimaryShardSize;
            private ByteSizeValue rolloverMaxSize;
            private TimeValue rolloverMinAge;
            private Long rolloverMinDocs;
            private Long rolloverMinPrimaryShardDocs;
            private ByteSizeValue rolloverMinPrimaryShardSize;
            private ByteSizeValue rolloverMinSize;
            private Integer setPriorityPriority;
            private ByteSizeValue shrinkMaxPrimaryShardSize;
            private Integer shrinkNumberOfShards;

            public Builder() {}

            public Builder(ActionConfigStats existing) {
                this.allocateNumberOfReplicas = existing.allocateNumberOfReplicas;
                this.forceMergeMaxNumberOfSegments = existing.forceMergeMaxNumberOfSegments;
                this.rolloverMaxAge = existing.rolloverMaxAge;
                this.rolloverMaxDocs = existing.rolloverMaxDocs;
                this.rolloverMaxPrimaryShardDocs = existing.rolloverMaxPrimaryShardDocs;
                this.rolloverMaxPrimaryShardSize = existing.rolloverMaxPrimaryShardSize;
                this.rolloverMaxSize = existing.rolloverMaxSize;
                this.rolloverMinAge = existing.rolloverMinAge;
                this.rolloverMinDocs = existing.rolloverMinDocs;
                this.rolloverMinPrimaryShardDocs = existing.rolloverMinPrimaryShardDocs;
                this.rolloverMinPrimaryShardSize = existing.rolloverMinPrimaryShardSize;
                this.rolloverMinSize = existing.rolloverMinSize;
                this.setPriorityPriority = existing.setPriorityPriority;
                this.shrinkMaxPrimaryShardSize = existing.shrinkMaxPrimaryShardSize;
                this.shrinkNumberOfShards = existing.shrinkNumberOfShards;
            }

            public Builder setAllocateNumberOfReplicas(Integer allocateNumberOfReplicas) {
                this.allocateNumberOfReplicas = allocateNumberOfReplicas;
                return this;
            }

            public Builder setForceMergeMaxNumberOfSegments(Integer forceMergeMaxNumberOfSegments) {
                this.forceMergeMaxNumberOfSegments = forceMergeMaxNumberOfSegments;
                return this;
            }

            public Builder setRolloverMaxAge(TimeValue rolloverMaxAge) {
                this.rolloverMaxAge = rolloverMaxAge;
                return this;
            }

            public Builder setRolloverMaxDocs(Long rolloverMaxDocs) {
                this.rolloverMaxDocs = rolloverMaxDocs;
                return this;
            }

            public Builder setRolloverMaxPrimaryShardDocs(Long rolloverMaxPrimaryShardDocs) {
                this.rolloverMaxPrimaryShardDocs = rolloverMaxPrimaryShardDocs;
                return this;
            }

            public Builder setRolloverMaxPrimaryShardSize(ByteSizeValue rolloverMaxPrimaryShardSize) {
                this.rolloverMaxPrimaryShardSize = rolloverMaxPrimaryShardSize;
                return this;
            }

            public Builder setRolloverMaxSize(ByteSizeValue rolloverMaxSize) {
                this.rolloverMaxSize = rolloverMaxSize;
                return this;
            }

            public Builder setRolloverMinAge(TimeValue rolloverMinAge) {
                this.rolloverMinAge = rolloverMinAge;
                return this;
            }

            public Builder setRolloverMinDocs(Long rolloverMinDocs) {
                this.rolloverMinDocs = rolloverMinDocs;
                return this;
            }

            public Builder setRolloverMinPrimaryShardDocs(Long rolloverMinPrimaryShardDocs) {
                this.rolloverMinPrimaryShardDocs = rolloverMinPrimaryShardDocs;
                return this;
            }

            public Builder setRolloverMinPrimaryShardSize(ByteSizeValue rolloverMinPrimaryShardSize) {
                this.rolloverMinPrimaryShardSize = rolloverMinPrimaryShardSize;
                return this;
            }

            public Builder setRolloverMinSize(ByteSizeValue rolloverMinSize) {
                this.rolloverMinSize = rolloverMinSize;
                return this;
            }

            public Builder setPriority(Integer priority) {
                this.setPriorityPriority = priority;
                return this;
            }

            public Builder setShrinkMaxPrimaryShardSize(ByteSizeValue shrinkMaxPrimaryShardSize) {
                this.shrinkMaxPrimaryShardSize = shrinkMaxPrimaryShardSize;
                return this;
            }

            public Builder setShrinkNumberOfShards(Integer shrinkNumberOfShards) {
                this.shrinkNumberOfShards = shrinkNumberOfShards;
                return this;
            }

            public ActionConfigStats build() {
                return new ActionConfigStats(
                    allocateNumberOfReplicas,
                    forceMergeMaxNumberOfSegments,
                    rolloverMaxAge,
                    rolloverMaxDocs,
                    rolloverMaxPrimaryShardDocs,
                    rolloverMaxPrimaryShardSize,
                    rolloverMaxSize,
                    rolloverMinAge,
                    rolloverMinDocs,
                    rolloverMinPrimaryShardDocs,
                    rolloverMinPrimaryShardSize,
                    rolloverMinSize,
                    setPriorityPriority,
                    shrinkMaxPrimaryShardSize,
                    shrinkNumberOfShards
                );
            }
        }

        public ActionConfigStats(
            Integer allocateNumberOfReplicas,
            Integer forceMergeMaxNumberOfSegments,
            TimeValue rolloverMaxAge,
            Long rolloverMaxDocs,
            Long rolloverMaxPrimaryShardDocs,
            ByteSizeValue rolloverMaxPrimaryShardSize,
            ByteSizeValue rolloverMaxSize,
            TimeValue rolloverMinAge,
            Long rolloverMinDocs,
            Long rolloverMinPrimaryShardDocs,
            ByteSizeValue rolloverMinPrimaryShardSize,
            ByteSizeValue rolloverMinSize,
            Integer setPriorityPriority,
            ByteSizeValue shrinkMaxPrimaryShardSize,
            Integer shrinkNumberOfShards
        ) {
            this.allocateNumberOfReplicas = allocateNumberOfReplicas;
            this.forceMergeMaxNumberOfSegments = forceMergeMaxNumberOfSegments;
            this.rolloverMaxAge = rolloverMaxAge;
            this.rolloverMaxDocs = rolloverMaxDocs;
            this.rolloverMaxPrimaryShardDocs = rolloverMaxPrimaryShardDocs;
            this.rolloverMaxPrimaryShardSize = rolloverMaxPrimaryShardSize;
            this.rolloverMaxSize = rolloverMaxSize;
            this.rolloverMinAge = rolloverMinAge;
            this.rolloverMinDocs = rolloverMinDocs;
            this.rolloverMinPrimaryShardDocs = rolloverMinPrimaryShardDocs;
            this.rolloverMinPrimaryShardSize = rolloverMinPrimaryShardSize;
            this.rolloverMinSize = rolloverMinSize;
            this.setPriorityPriority = setPriorityPriority;
            this.shrinkMaxPrimaryShardSize = shrinkMaxPrimaryShardSize;
            this.shrinkNumberOfShards = shrinkNumberOfShards;
        }

        public ActionConfigStats(StreamInput in) throws IOException {
            this.allocateNumberOfReplicas = in.readOptionalVInt();
            this.forceMergeMaxNumberOfSegments = in.readOptionalVInt();
            this.rolloverMaxAge = in.readOptionalTimeValue();
            this.rolloverMaxDocs = in.readOptionalVLong();
            this.rolloverMaxPrimaryShardSize = in.readOptionalWriteable(ByteSizeValue::new);
            this.rolloverMaxSize = in.readOptionalWriteable(ByteSizeValue::new);
            this.setPriorityPriority = in.readOptionalVInt();
            this.shrinkMaxPrimaryShardSize = in.readOptionalWriteable(ByteSizeValue::new);
            this.shrinkNumberOfShards = in.readOptionalVInt();
            if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
                this.rolloverMaxPrimaryShardDocs = in.readOptionalVLong();
            } else {
                this.rolloverMaxPrimaryShardDocs = null;
            }
            if (in.getVersion().onOrAfter(Version.V_8_4_0)) {
                this.rolloverMinAge = in.readOptionalTimeValue();
                this.rolloverMinDocs = in.readOptionalVLong();
                this.rolloverMinPrimaryShardSize = in.readOptionalWriteable(ByteSizeValue::new);
                this.rolloverMinSize = in.readOptionalWriteable(ByteSizeValue::new);
                this.rolloverMinPrimaryShardDocs = in.readOptionalVLong();
            } else {
                this.rolloverMinAge = null;
                this.rolloverMinDocs = null;
                this.rolloverMinPrimaryShardSize = null;
                this.rolloverMinSize = null;
                this.rolloverMinPrimaryShardDocs = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(allocateNumberOfReplicas);
            out.writeOptionalVInt(forceMergeMaxNumberOfSegments);
            out.writeOptionalTimeValue(rolloverMaxAge);
            out.writeOptionalVLong(rolloverMaxDocs);
            out.writeOptionalWriteable(rolloverMaxPrimaryShardSize);
            out.writeOptionalWriteable(rolloverMaxSize);
            out.writeOptionalVInt(setPriorityPriority);
            out.writeOptionalWriteable(shrinkMaxPrimaryShardSize);
            out.writeOptionalVInt(shrinkNumberOfShards);
            if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
                out.writeOptionalVLong(rolloverMaxPrimaryShardDocs);
            }
            if (out.getVersion().onOrAfter(Version.V_8_4_0)) {
                out.writeOptionalTimeValue(rolloverMinAge);
                out.writeOptionalVLong(rolloverMinDocs);
                out.writeOptionalWriteable(rolloverMinPrimaryShardSize);
                out.writeOptionalWriteable(rolloverMinSize);
                out.writeOptionalVLong(rolloverMinPrimaryShardDocs);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (allocateNumberOfReplicas != null) {
                builder.startObject(AllocateAction.NAME);
                builder.field(AllocateAction.NUMBER_OF_REPLICAS_FIELD.getPreferredName(), allocateNumberOfReplicas);
                builder.endObject();
            }
            if (forceMergeMaxNumberOfSegments != null) {
                builder.startObject(ForceMergeAction.NAME);
                builder.field(ForceMergeAction.MAX_NUM_SEGMENTS_FIELD.getPreferredName(), forceMergeMaxNumberOfSegments);
                builder.endObject();
            }
            if (rolloverMaxAge != null
                || rolloverMaxDocs != null
                || rolloverMaxPrimaryShardDocs != null
                || rolloverMaxSize != null
                || rolloverMaxPrimaryShardSize != null
                || rolloverMinAge != null
                || rolloverMinDocs != null
                || rolloverMinPrimaryShardDocs != null
                || rolloverMinSize != null
                || rolloverMinPrimaryShardSize != null) {
                builder.startObject(RolloverAction.NAME);
                if (rolloverMaxAge != null) {
                    builder.field(RolloverAction.MAX_AGE_FIELD.getPreferredName(), rolloverMaxAge.getStringRep());
                    builder.field(RolloverAction.MAX_AGE_FIELD.getPreferredName() + "_millis", rolloverMaxAge.getMillis());
                }
                if (rolloverMaxDocs != null) {
                    builder.field(RolloverAction.MAX_DOCS_FIELD.getPreferredName(), rolloverMaxDocs);
                }
                if (rolloverMaxPrimaryShardDocs != null) {
                    builder.field(RolloverAction.MAX_PRIMARY_SHARD_DOCS_FIELD.getPreferredName(), rolloverMaxPrimaryShardDocs);
                }
                if (rolloverMaxSize != null) {
                    builder.field(RolloverAction.MAX_SIZE_FIELD.getPreferredName(), rolloverMaxSize.getStringRep());
                    builder.field(RolloverAction.MAX_SIZE_FIELD.getPreferredName() + "_bytes", rolloverMaxSize.getBytes());
                }
                if (rolloverMaxPrimaryShardSize != null) {
                    builder.field(
                        RolloverAction.MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName(),
                        rolloverMaxPrimaryShardSize.getStringRep()
                    );
                    builder.field(
                        RolloverAction.MAX_PRIMARY_SHARD_SIZE_FIELD.getPreferredName() + "_bytes",
                        rolloverMaxPrimaryShardSize.getBytes()
                    );
                }
                if (rolloverMinAge != null) {
                    builder.field(RolloverAction.MIN_AGE_FIELD.getPreferredName(), rolloverMinAge.getStringRep());
                    builder.field(RolloverAction.MIN_AGE_FIELD.getPreferredName() + "_millis", rolloverMinAge.getMillis());
                }
                if (rolloverMinDocs != null) {
                    builder.field(RolloverAction.MIN_DOCS_FIELD.getPreferredName(), rolloverMinDocs);
                }
                if (rolloverMinPrimaryShardDocs != null) {
                    builder.field(RolloverAction.MIN_PRIMARY_SHARD_DOCS_FIELD.getPreferredName(), rolloverMinPrimaryShardDocs);
                }
                if (rolloverMinSize != null) {
                    builder.field(RolloverAction.MIN_SIZE_FIELD.getPreferredName(), rolloverMinSize.getStringRep());
                    builder.field(RolloverAction.MIN_SIZE_FIELD.getPreferredName() + "_bytes", rolloverMinSize.getBytes());
                }
                if (rolloverMinPrimaryShardSize != null) {
                    builder.field(
                        RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD.getPreferredName(),
                        rolloverMinPrimaryShardSize.getStringRep()
                    );
                    builder.field(
                        RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD.getPreferredName() + "_bytes",
                        rolloverMinPrimaryShardSize.getBytes()
                    );
                }
                builder.endObject();
            }
            if (setPriorityPriority != null) {
                builder.startObject(SetPriorityAction.NAME);
                builder.field(SetPriorityAction.RECOVERY_PRIORITY_FIELD.getPreferredName(), setPriorityPriority);
                builder.endObject();
            }
            if (shrinkMaxPrimaryShardSize != null || shrinkNumberOfShards != null) {
                builder.startObject(ShrinkAction.NAME);
                if (shrinkMaxPrimaryShardSize != null) {
                    builder.field(ShrinkAction.MAX_PRIMARY_SHARD_SIZE.getPreferredName(), shrinkMaxPrimaryShardSize.getStringRep());
                    builder.field(ShrinkAction.MAX_PRIMARY_SHARD_SIZE.getPreferredName() + "_bytes", shrinkMaxPrimaryShardSize.getBytes());
                }
                if (shrinkNumberOfShards != null) {
                    builder.field(ShrinkAction.NUMBER_OF_SHARDS_FIELD.getPreferredName(), shrinkNumberOfShards);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        public Integer getAllocateNumberOfReplicas() {
            return allocateNumberOfReplicas;
        }

        public Integer getForceMergeMaxNumberOfSegments() {
            return forceMergeMaxNumberOfSegments;
        }

        public TimeValue getRolloverMaxAge() {
            return rolloverMaxAge;
        }

        public Long getRolloverMaxDocs() {
            return rolloverMaxDocs;
        }

        public Long getRolloverMaxPrimaryShardDocs() {
            return rolloverMaxPrimaryShardDocs;
        }

        public ByteSizeValue getRolloverMaxPrimaryShardSize() {
            return rolloverMaxPrimaryShardSize;
        }

        public ByteSizeValue getRolloverMaxSize() {
            return rolloverMaxSize;
        }

        public TimeValue getRolloverMinAge() {
            return rolloverMinAge;
        }

        public Long getRolloverMinDocs() {
            return rolloverMinDocs;
        }

        public Long getRolloverMinPrimaryShardDocs() {
            return rolloverMinPrimaryShardDocs;
        }

        public ByteSizeValue getRolloverMinPrimaryShardSize() {
            return rolloverMinPrimaryShardSize;
        }

        public ByteSizeValue getRolloverMinSize() {
            return rolloverMinSize;
        }

        public Integer getSetPriorityPriority() {
            return setPriorityPriority;
        }

        public ByteSizeValue getShrinkMaxPrimaryShardSize() {
            return shrinkMaxPrimaryShardSize;
        }

        public Integer getShrinkNumberOfShards() {
            return shrinkNumberOfShards;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ActionConfigStats that = (ActionConfigStats) o;
            return Objects.equals(allocateNumberOfReplicas, that.allocateNumberOfReplicas)
                && Objects.equals(forceMergeMaxNumberOfSegments, that.forceMergeMaxNumberOfSegments)
                && Objects.equals(rolloverMaxAge, that.rolloverMaxAge)
                && Objects.equals(rolloverMaxDocs, that.rolloverMaxDocs)
                && Objects.equals(rolloverMaxPrimaryShardDocs, that.rolloverMaxPrimaryShardDocs)
                && Objects.equals(rolloverMaxSize, that.rolloverMaxSize)
                && Objects.equals(rolloverMaxPrimaryShardSize, that.rolloverMaxPrimaryShardSize)
                && Objects.equals(rolloverMinAge, that.rolloverMinAge)
                && Objects.equals(rolloverMinDocs, that.rolloverMinDocs)
                && Objects.equals(rolloverMinPrimaryShardDocs, that.rolloverMinPrimaryShardDocs)
                && Objects.equals(rolloverMinSize, that.rolloverMinSize)
                && Objects.equals(rolloverMinPrimaryShardSize, that.rolloverMinPrimaryShardSize)
                && Objects.equals(setPriorityPriority, that.setPriorityPriority)
                && Objects.equals(shrinkMaxPrimaryShardSize, that.shrinkMaxPrimaryShardSize)
                && Objects.equals(shrinkNumberOfShards, that.shrinkNumberOfShards);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                allocateNumberOfReplicas,
                forceMergeMaxNumberOfSegments,
                rolloverMaxAge,
                rolloverMaxDocs,
                rolloverMaxPrimaryShardDocs,
                rolloverMaxSize,
                rolloverMaxPrimaryShardSize,
                rolloverMinAge,
                rolloverMinDocs,
                rolloverMinPrimaryShardDocs,
                rolloverMinSize,
                rolloverMinPrimaryShardSize,
                setPriorityPriority,
                shrinkMaxPrimaryShardSize,
                shrinkNumberOfShards
            );
        }
    }
}

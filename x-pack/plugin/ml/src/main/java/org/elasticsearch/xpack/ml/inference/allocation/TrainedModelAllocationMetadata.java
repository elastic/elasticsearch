/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;

public class TrainedModelAllocationMetadata implements Metadata.Custom {

    private static final TrainedModelAllocationMetadata EMPTY = new TrainedModelAllocationMetadata(Collections.emptyMap());
    public static final String NAME = "trained_model_allocation";
    private final Map<String, TrainedModelAllocation> modelRoutingEntries;

    public static TrainedModelAllocationMetadata fromXContent(XContentParser parser) throws IOException {
        return new TrainedModelAllocationMetadata(parser.map(LinkedHashMap::new, TrainedModelAllocation::fromXContent));
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new TrainedModelAllocationMetadata.TrainedModeAllocationDiff(in);
    }

    public static Builder builder(ClusterState clusterState) {
        return Builder.fromMetadata(fromState(clusterState));
    }

    public static TrainedModelAllocationMetadata fromState(ClusterState clusterState) {
        TrainedModelAllocationMetadata trainedModelAllocationMetadata = clusterState.getMetadata().custom(NAME);
        return trainedModelAllocationMetadata == null ? EMPTY : trainedModelAllocationMetadata;
    }

    public static Optional<TrainedModelAllocation> allocationForModelId(ClusterState clusterState, String modelId) {
        return Optional.ofNullable(TrainedModelAllocationMetadata.fromState(clusterState))
            .map(metadata -> metadata.getModelAllocation(modelId));
    }

    public TrainedModelAllocationMetadata(Map<String, TrainedModelAllocation> modelRoutingEntries) {
        this.modelRoutingEntries = ExceptionsHelper.requireNonNull(modelRoutingEntries, NAME);
    }

    public TrainedModelAllocationMetadata(StreamInput in) throws IOException {
        this.modelRoutingEntries = in.readOrderedMap(StreamInput::readString, TrainedModelAllocation::new);
    }

    public TrainedModelAllocation getModelAllocation(String modelId) {
        return modelRoutingEntries.get(modelId);
    }

    public Map<String, TrainedModelAllocation> modelAllocations() {
        return Collections.unmodifiableMap(modelRoutingEntries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.mapContents(modelRoutingEntries);
        return builder;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new TrainedModeAllocationDiff((TrainedModelAllocationMetadata) previousState, this);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(modelRoutingEntries, StreamOutput::writeString, (o, w) -> w.writeTo(o));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAllocationMetadata that = (TrainedModelAllocationMetadata) o;
        return Objects.equals(modelRoutingEntries, that.modelRoutingEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelRoutingEntries);
    }

    public static class Builder {

        public static Builder empty(){
            return new Builder();
        }

        private final Map<String, TrainedModelAllocation.Builder> modelRoutingEntries;
        private boolean isChanged;

        public static Builder fromMetadata(TrainedModelAllocationMetadata modelAllocationMetadata) {
            return new Builder(modelAllocationMetadata);
        }

        private Builder() {
            modelRoutingEntries = new LinkedHashMap<>();
        }

        private Builder(TrainedModelAllocationMetadata modelAllocationMetadata) {
            this.modelRoutingEntries = new LinkedHashMap<>();
            modelAllocationMetadata.modelRoutingEntries.forEach(
                (modelId, allocation) -> modelRoutingEntries.put(modelId, TrainedModelAllocation.Builder.fromAllocation(allocation))
            );
        }

        public boolean hasModel(String modelId) {
            return modelRoutingEntries.containsKey(modelId);
        }

        public Builder addNewAllocation(StartTrainedModelDeploymentAction.TaskParams taskParams) {
            if (modelRoutingEntries.containsKey(taskParams.getModelId())) {
                return this;
            }
            modelRoutingEntries.put(taskParams.getModelId(), TrainedModelAllocation.Builder.empty(taskParams));
            isChanged = true;
            return this;
        }

        public Builder updateAllocation(String modelId, String nodeId, RoutingStateAndReason state) {
            TrainedModelAllocation.Builder allocation = modelRoutingEntries.get(modelId);
            if (allocation == null) {
                return this;
            }
            isChanged |= allocation.updateExistingRoutingEntry(nodeId, state).isChanged();
            return this;
        }

        public Builder addNode(String modelId, String nodeId) {
            TrainedModelAllocation.Builder allocation = modelRoutingEntries.get(modelId);
            if (allocation == null) {
                throw new ResourceNotFoundException(
                    "unable to add node [{}] to model [{}] routing table as allocation does not exist",
                    nodeId,
                    modelId
                );
            }
            isChanged |= allocation.addNewRoutingEntry(nodeId).isChanged();
            return this;
        }

        public Builder addFailedNode(String modelId, String nodeId, String reason) {
            TrainedModelAllocation.Builder allocation = modelRoutingEntries.get(modelId);
            if (allocation == null) {
                throw new ResourceNotFoundException(
                    "unable to add failed node [{}] to model [{}] routing table as allocation does not exist",
                    nodeId,
                    modelId
                );
            }
            isChanged |= allocation.addNewFailedRoutingEntry(nodeId, reason).isChanged();
            return this;
        }

        Builder removeNode(String modelId, String nodeId) {
            TrainedModelAllocation.Builder allocation = modelRoutingEntries.get(modelId);
            if (allocation == null) {
                return this;
            }
            isChanged |= allocation.removeRoutingEntry(nodeId).isChanged();
            return this;
        }

        public Builder removeAllocation(String modelId) {
            isChanged |= modelRoutingEntries.remove(modelId) != null;
            return this;
        }

        public Builder setAllocationToStopping(String modelId) {
            TrainedModelAllocation.Builder allocation = modelRoutingEntries.get(modelId);
            if (allocation == null) {
                throw new ResourceNotFoundException(
                    "unable to set model allocation [{}] to stopping as it does not exist",
                    modelId
                );
            }
            isChanged |= allocation.stopAllocation().isChanged();
            return this;
        }

        public boolean isChanged() {
            return isChanged;
        }

        public TrainedModelAllocationMetadata build() {
            Map<String, TrainedModelAllocation> allocations = new LinkedHashMap<>();
            modelRoutingEntries.forEach((modelId, allocation) -> allocations.put(modelId, allocation.build()));
            return new TrainedModelAllocationMetadata(allocations);
        }
    }

    public static class TrainedModeAllocationDiff implements NamedDiff<Metadata.Custom> {

        private final Diff<Map<String, TrainedModelAllocation>> modelRoutingEntries;

        static Diff<TrainedModelAllocation> readFrom(final StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(TrainedModelAllocation::new, in);
        }

        public TrainedModeAllocationDiff(TrainedModelAllocationMetadata before, TrainedModelAllocationMetadata after) {
            this.modelRoutingEntries = DiffableUtils.diff(
                before.modelRoutingEntries,
                after.modelRoutingEntries,
                DiffableUtils.getStringKeySerializer()
            );
        }

        public TrainedModeAllocationDiff(final StreamInput in) throws IOException {
            this.modelRoutingEntries = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                TrainedModelAllocation::new,
                TrainedModeAllocationDiff::readFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new TrainedModelAllocationMetadata(
                new TreeMap<>(modelRoutingEntries.apply(((TrainedModelAllocationMetadata) part).modelRoutingEntries))
            );
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelRoutingEntries.writeTo(out);
        }
    }

}

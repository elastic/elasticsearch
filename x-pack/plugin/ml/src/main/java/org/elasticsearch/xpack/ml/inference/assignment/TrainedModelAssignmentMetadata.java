/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;

public class TrainedModelAssignmentMetadata implements Metadata.Custom {

    private static final TrainedModelAssignmentMetadata EMPTY = new TrainedModelAssignmentMetadata(Collections.emptyMap());
    public static final String DEPRECATED_NAME = "trained_model_allocation";
    public static final String NAME = "trained_model_assignment";
    private final Map<String, TrainedModelAssignment> deploymentRoutingEntries;
    private final String writeableName;

    public static TrainedModelAssignmentMetadata fromXContent(XContentParser parser) throws IOException {
        return new TrainedModelAssignmentMetadata(parser.map(LinkedHashMap::new, TrainedModelAssignment::fromXContent));
    }

    public static TrainedModelAssignmentMetadata fromStream(StreamInput input) throws IOException {
        return new TrainedModelAssignmentMetadata(input, NAME);
    }

    public static TrainedModelAssignmentMetadata fromStreamOld(StreamInput input) throws IOException {
        return new TrainedModelAssignmentMetadata(input, DEPRECATED_NAME);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new TrainedModelAssignmentMetadata.TrainedModeAssignmentDiff(in);
    }

    public static NamedDiff<Metadata.Custom> readDiffFromOld(StreamInput in) throws IOException {
        return new TrainedModelAssignmentMetadata.TrainedModeAssignmentDiff(in, DEPRECATED_NAME);
    }

    public static Builder builder(ClusterState clusterState) {
        return Builder.fromMetadata(fromState(clusterState));
    }

    public static TrainedModelAssignmentMetadata fromState(ClusterState clusterState) {
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = clusterState.getMetadata().custom(NAME);
        if (trainedModelAssignmentMetadata == null) {
            trainedModelAssignmentMetadata = clusterState.getMetadata().custom(DEPRECATED_NAME);
        }
        return trainedModelAssignmentMetadata == null ? EMPTY : trainedModelAssignmentMetadata;
    }

    public static List<TrainedModelAssignment> assignmentsForModelId(ClusterState clusterState, String modelId) {
        return TrainedModelAssignmentMetadata.fromState(clusterState)
            .allAssignments()
            .values()
            .stream()
            .filter(assignment -> modelId.equals(assignment.getModelId()))
            .collect(Collectors.toList());
    }

    public static Optional<TrainedModelAssignment> assignmentForDeploymentId(ClusterState clusterState, String deploymentId) {
        return Optional.ofNullable(TrainedModelAssignmentMetadata.fromState(clusterState))
            .map(metadata -> metadata.getDeploymentAssignment(deploymentId));
    }

    public TrainedModelAssignmentMetadata(Map<String, TrainedModelAssignment> modelRoutingEntries) {
        this(modelRoutingEntries, NAME);
    }

    private TrainedModelAssignmentMetadata(Map<String, TrainedModelAssignment> modelRoutingEntries, String writeableName) {
        this.deploymentRoutingEntries = ExceptionsHelper.requireNonNull(modelRoutingEntries, NAME);
        this.writeableName = writeableName;
    }

    private TrainedModelAssignmentMetadata(StreamInput in, String writeableName) throws IOException {
        this.deploymentRoutingEntries = in.readOrderedMap(StreamInput::readString, TrainedModelAssignment::new);
        this.writeableName = writeableName;
    }

    public TrainedModelAssignment getDeploymentAssignment(String deploymentId) {
        return deploymentRoutingEntries.get(deploymentId);
    }

    public boolean isAssigned(String deploymentId) {
        return deploymentRoutingEntries.containsKey(deploymentId);
    }

    public boolean modelIsDeployed(String modelId) {
        return deploymentRoutingEntries.values().stream().anyMatch(assignment -> modelId.equals(assignment.getModelId()));
    }

    public List<TrainedModelAssignment> getDeploymentsUsingModel(String modelId) {
        return deploymentRoutingEntries.values()
            .stream()
            .filter(assignment -> modelId.equals(assignment.getModelId()))
            .collect(Collectors.toList());
    }

    public Map<String, TrainedModelAssignment> allAssignments() {
        return Collections.unmodifiableMap(deploymentRoutingEntries);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return deploymentRoutingEntries.entrySet()
            .stream()
            .map(entry -> (ToXContent) (builder, params) -> entry.getValue().toXContent(builder.field(entry.getKey()), params))
            .iterator();
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new TrainedModeAssignmentDiff((TrainedModelAssignmentMetadata) previousState, this);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return writeableName;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(deploymentRoutingEntries, StreamOutput::writeString, (o, w) -> w.writeTo(o));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAssignmentMetadata that = (TrainedModelAssignmentMetadata) o;
        return Objects.equals(deploymentRoutingEntries, that.deploymentRoutingEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deploymentRoutingEntries);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public boolean hasOutdatedAssignments() {
        return deploymentRoutingEntries.values().stream().anyMatch(TrainedModelAssignment::hasOutdatedRoutingEntries);
    }

    public boolean hasDeployment(String deploymentId) {
        return deploymentRoutingEntries.containsKey(deploymentId);
    }

    public static class Builder {

        public static Builder empty() {
            return new Builder();
        }

        private final Map<String, TrainedModelAssignment.Builder> deploymentRoutingEntries;

        public static Builder fromMetadata(TrainedModelAssignmentMetadata modelAssignmentMetadata) {
            return new Builder(modelAssignmentMetadata);
        }

        private Builder() {
            deploymentRoutingEntries = new LinkedHashMap<>();
        }

        private Builder(TrainedModelAssignmentMetadata modelAssignmentMetadata) {
            this.deploymentRoutingEntries = new LinkedHashMap<>();
            modelAssignmentMetadata.deploymentRoutingEntries.forEach(
                (deploymentId, assignment) -> deploymentRoutingEntries.put(
                    deploymentId,
                    TrainedModelAssignment.Builder.fromAssignment(assignment)
                )
            );
        }

        public boolean hasModelDeployment(String deploymentId) {
            return deploymentRoutingEntries.containsKey(deploymentId);
        }

        public Builder addNewAssignment(String deploymentId, TrainedModelAssignment.Builder assignment) {
            if (deploymentRoutingEntries.containsKey(deploymentId)) {
                throw new ResourceAlreadyExistsException("[{}] assignment already exists", deploymentId);
            }
            deploymentRoutingEntries.put(deploymentId, assignment);
            return this;
        }

        public Builder updateAssignment(String deploymentId, TrainedModelAssignment.Builder assignment) {
            if (deploymentRoutingEntries.containsKey(deploymentId) == false) {
                throw new ResourceNotFoundException("[{}] assignment does not exist", deploymentId);
            }
            deploymentRoutingEntries.put(deploymentId, assignment);
            return this;
        }

        public TrainedModelAssignment.Builder getAssignment(String deploymentId) {
            return deploymentRoutingEntries.get(deploymentId);
        }

        public Builder removeAssignment(String deploymentId) {
            deploymentRoutingEntries.remove(deploymentId);
            return this;
        }

        public TrainedModelAssignmentMetadata build() {
            return build(NAME);
        }

        public TrainedModelAssignmentMetadata buildOld() {
            return build(DEPRECATED_NAME);
        }

        private TrainedModelAssignmentMetadata build(String writeableName) {
            Map<String, TrainedModelAssignment> assignments = new LinkedHashMap<>();
            deploymentRoutingEntries.forEach((deploymentId, assignment) -> assignments.put(deploymentId, assignment.build()));
            return new TrainedModelAssignmentMetadata(assignments, writeableName);
        }
    }

    public static class TrainedModeAssignmentDiff implements NamedDiff<Metadata.Custom> {

        private final Diff<Map<String, TrainedModelAssignment>> modelRoutingEntries;
        private final String writeableName;

        static Diff<TrainedModelAssignment> readFrom(final StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(TrainedModelAssignment::new, in);
        }

        public TrainedModeAssignmentDiff(TrainedModelAssignmentMetadata before, TrainedModelAssignmentMetadata after) {
            this.modelRoutingEntries = DiffableUtils.diff(
                before.deploymentRoutingEntries,
                after.deploymentRoutingEntries,
                DiffableUtils.getStringKeySerializer()
            );
            this.writeableName = NAME;
        }

        private TrainedModeAssignmentDiff(final StreamInput in) throws IOException {
            this.modelRoutingEntries = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                TrainedModelAssignment::new,
                TrainedModeAssignmentDiff::readFrom
            );
            this.writeableName = NAME;
        }

        private TrainedModeAssignmentDiff(final StreamInput in, String writeableName) throws IOException {
            this.modelRoutingEntries = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                TrainedModelAssignment::new,
                TrainedModeAssignmentDiff::readFrom
            );
            this.writeableName = writeableName;
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new TrainedModelAssignmentMetadata(
                new TreeMap<>(modelRoutingEntries.apply(((TrainedModelAssignmentMetadata) part).deploymentRoutingEntries))
            );
        }

        @Override
        public String getWriteableName() {
            return writeableName;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.V_8_0_0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelRoutingEntries.writeTo(out);
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
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

public class TrainedModelAssignmentMetadata implements Metadata.Custom {

    private static final TrainedModelAssignmentMetadata EMPTY = new TrainedModelAssignmentMetadata(Collections.emptyMap());
    public static final String DEPRECATED_NAME = "trained_model_allocation";
    public static final String NAME = "trained_model_assignment";
    private final Map<String, TrainedModelAssignment> modelRoutingEntries;
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

    public static Optional<TrainedModelAssignment> assignmentForModelId(ClusterState clusterState, String modelId) {
        return Optional.ofNullable(TrainedModelAssignmentMetadata.fromState(clusterState))
            .map(metadata -> metadata.getModelAssignment(modelId));
    }

    public TrainedModelAssignmentMetadata(Map<String, TrainedModelAssignment> modelRoutingEntries) {
        this(modelRoutingEntries, NAME);
    }

    private TrainedModelAssignmentMetadata(Map<String, TrainedModelAssignment> modelRoutingEntries, String writeableName) {
        this.modelRoutingEntries = ExceptionsHelper.requireNonNull(modelRoutingEntries, NAME);
        this.writeableName = writeableName;
    }

    private TrainedModelAssignmentMetadata(StreamInput in, String writeableName) throws IOException {
        this.modelRoutingEntries = in.readOrderedMap(StreamInput::readString, TrainedModelAssignment::new);
        this.writeableName = writeableName;
    }

    public TrainedModelAssignment getModelAssignment(String modelId) {
        return modelRoutingEntries.get(modelId);
    }

    public boolean isAssigned(String modelId) {
        return modelRoutingEntries.containsKey(modelId);
    }

    public Map<String, TrainedModelAssignment> modelAssignments() {
        return Collections.unmodifiableMap(modelRoutingEntries);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.mapContents(modelRoutingEntries);
        return builder;
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
        TrainedModelAssignmentMetadata that = (TrainedModelAssignmentMetadata) o;
        return Objects.equals(modelRoutingEntries, that.modelRoutingEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelRoutingEntries);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public boolean hasOutdatedAssignments() {
        return modelRoutingEntries.values().stream().anyMatch(TrainedModelAssignment::hasOutdatedRoutingEntries);
    }

    public boolean hasModel(String modelId) {
        return modelRoutingEntries.containsKey(modelId);
    }

    public static class Builder {

        public static Builder empty() {
            return new Builder();
        }

        private final Map<String, TrainedModelAssignment.Builder> modelRoutingEntries;

        public static Builder fromMetadata(TrainedModelAssignmentMetadata modelAssignmentMetadata) {
            return new Builder(modelAssignmentMetadata);
        }

        private Builder() {
            modelRoutingEntries = new LinkedHashMap<>();
        }

        private Builder(TrainedModelAssignmentMetadata modelAssignmentMetadata) {
            this.modelRoutingEntries = new LinkedHashMap<>();
            modelAssignmentMetadata.modelRoutingEntries.forEach(
                (modelId, assignment) -> modelRoutingEntries.put(modelId, TrainedModelAssignment.Builder.fromAssignment(assignment))
            );
        }

        public boolean hasModel(String modelId) {
            return modelRoutingEntries.containsKey(modelId);
        }

        public Builder addNewAssignment(String modelId, TrainedModelAssignment.Builder assignment) {
            if (modelRoutingEntries.containsKey(modelId)) {
                throw new ResourceAlreadyExistsException("[{}] assignment already exists", modelId);
            }
            modelRoutingEntries.put(modelId, assignment);
            return this;
        }

        public Builder updateAssignment(String modelId, TrainedModelAssignment.Builder assignment) {
            if (modelRoutingEntries.containsKey(modelId) == false) {
                throw new ResourceNotFoundException("[{}] assignment does not exist", modelId);
            }
            modelRoutingEntries.put(modelId, assignment);
            return this;
        }

        public TrainedModelAssignment.Builder getAssignment(String modelId) {
            return modelRoutingEntries.get(modelId);
        }

        public Builder removeAssignment(String modelId) {
            modelRoutingEntries.remove(modelId);
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
            modelRoutingEntries.forEach((modelId, assignment) -> assignments.put(modelId, assignment.build()));
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
                before.modelRoutingEntries,
                after.modelRoutingEntries,
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
                new TreeMap<>(modelRoutingEntries.apply(((TrainedModelAssignmentMetadata) part).modelRoutingEntries))
            );
        }

        @Override
        public String getWriteableName() {
            return writeableName;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_8_0_0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            modelRoutingEntries.writeTo(out);
        }
    }

}

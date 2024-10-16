/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

// TODO implement better diffable logic so that whole diff does not need to be serialized if only one part changes
/**
 * Trained model assignment object that contains assignment options and the assignment routing table
 */
public final class TrainedModelAssignment implements SimpleDiffable<TrainedModelAssignment>, ToXContentObject {

    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField ASSIGNMENT_STATE = new ParseField("assignment_state");
    // Used for reading old values,
    // not a deprecated field as users cannot really change it. Just read the old value and write out to the new field
    private static final ParseField LEGACY_ALLOCATION_STATE = new ParseField("allocation_state");
    private static final ParseField ROUTING_TABLE = new ParseField("routing_table");
    private static final ParseField TASK_PARAMETERS = new ParseField("task_parameters");
    private static final ParseField START_TIME = new ParseField("start_time");
    private static final ParseField MAX_ASSIGNED_ALLOCATIONS = new ParseField("max_assigned_allocations");
    public static final ParseField ADAPTIVE_ALLOCATIONS = new ParseField("adaptive_allocations");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TrainedModelAssignment, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_assignment",
        true,
        a -> new TrainedModelAssignment(
            (StartTrainedModelDeploymentAction.TaskParams) a[0],
            (Map<String, RoutingInfo>) a[1],
            a[2] == null ? null : AssignmentState.fromString((String) a[2]),
            a[3] == null ? null : AssignmentState.fromString((String) a[3]),
            (String) a[4],
            (Instant) a[5],
            (Integer) a[6],
            (AdaptiveAllocationsSettings) a[7]
        )
    );
    static {
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> StartTrainedModelDeploymentAction.TaskParams.fromXContent(p),
            TASK_PARAMETERS
        );
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(LinkedHashMap::new, RoutingInfo::fromXContent),
            ROUTING_TABLE
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ASSIGNMENT_STATE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEGACY_ALLOCATION_STATE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, START_TIME.getPreferredName()),
            START_TIME,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_ASSIGNED_ALLOCATIONS);
        PARSER.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> AdaptiveAllocationsSettings.PARSER.parse(p, c).build(),
            null,
            ADAPTIVE_ALLOCATIONS
        );
    }

    private final StartTrainedModelDeploymentAction.TaskParams taskParams;
    private final Map<String, RoutingInfo> nodeRoutingTable;
    private final AssignmentState assignmentState;
    private final String reason;
    private final Instant startTime;
    private final int maxAssignedAllocations;
    private final AdaptiveAllocationsSettings adaptiveAllocationsSettings;

    public static boolean useNewMemoryFields(TransportVersion minClusterVersion) {
        return minClusterVersion.onOrAfter(TransportVersions.V_8_11_X);
    }

    public static TrainedModelAssignment fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    private TrainedModelAssignment(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        Map<String, RoutingInfo> nodeRoutingTable,
        AssignmentState assignmentState,
        AssignmentState legacyAssignmentState,
        String reason,
        Instant startTime,
        Integer maxAssignedAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        this(
            taskParams,
            nodeRoutingTable,
            Optional.ofNullable(assignmentState).orElse(legacyAssignmentState),
            reason,
            startTime,
            maxAssignedAllocations,
            adaptiveAllocationsSettings
        );
    }

    /**
     * A long-lived object which defines a trained model deployment/assignment.
     *
     * @param taskParams the parameters provided by the StartTrainedModelDeploymentAction during the creation of the deployment/assignment
     * @param nodeRoutingTable shows where allocations for this assignment/deployment are located (on which nodes)
     * @param assignmentState used to track the state of the assignment for rebalancing, autoscaling, and more
     * @param reason may contain a human-readable explanation for the current state
     * @param startTime the time when the assignment was created
     * @param maxAssignedAllocations used for adaptive allocations
     * @param adaptiveAllocationsSettings how the assignment should scale based on usage
     */
    TrainedModelAssignment(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        Map<String, RoutingInfo> nodeRoutingTable,
        AssignmentState assignmentState,
        String reason,
        Instant startTime,
        Integer maxAssignedAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        this.taskParams = ExceptionsHelper.requireNonNull(taskParams, TASK_PARAMETERS);
        this.nodeRoutingTable = ExceptionsHelper.requireNonNull(nodeRoutingTable, ROUTING_TABLE);
        this.assignmentState = ExceptionsHelper.requireNonNull(assignmentState, ASSIGNMENT_STATE);
        this.reason = reason;
        this.startTime = ExceptionsHelper.requireNonNull(startTime, START_TIME);
        this.maxAssignedAllocations = maxAssignedAllocations == null
            ? totalCurrentAllocations()
            : Math.max(maxAssignedAllocations, totalCurrentAllocations());
        this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
    }

    public TrainedModelAssignment(StreamInput in) throws IOException {
        this.taskParams = new StartTrainedModelDeploymentAction.TaskParams(in);
        this.nodeRoutingTable = in.readOrderedMap(StreamInput::readString, RoutingInfo::new);
        this.assignmentState = in.readEnum(AssignmentState.class);
        this.reason = in.readOptionalString();
        this.startTime = in.readInstant();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            this.maxAssignedAllocations = in.readVInt();
        } else {
            this.maxAssignedAllocations = totalCurrentAllocations();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
            this.adaptiveAllocationsSettings = in.readOptionalWriteable(AdaptiveAllocationsSettings::new);
        } else {
            this.adaptiveAllocationsSettings = null;
        }
    }

    public boolean isRoutedToNode(String nodeId) {
        return nodeRoutingTable.containsKey(nodeId);
    }

    /**
     * @return shows where allocations for this assignment/deployment are located (on which nodes)
     */
    public Map<String, RoutingInfo> getNodeRoutingTable() {
        return Collections.unmodifiableMap(nodeRoutingTable);
    }

    public String getModelId() {
        return taskParams.getModelId();
    }

    public String getDeploymentId() {
        return taskParams.getDeploymentId();
    }

    public StartTrainedModelDeploymentAction.TaskParams getTaskParams() {
        return taskParams;
    }

    public AssignmentState getAssignmentState() {
        return assignmentState;
    }

    public String[] getStartedNodes() {
        return nodeRoutingTable.entrySet()
            .stream()
            .filter(entry -> RoutingState.STARTED.equals(entry.getValue().getState()))
            .map(Map.Entry::getKey)
            .toArray(String[]::new);
    }

    public boolean hasStartedRoutes() {
        return nodeRoutingTable.values().stream().anyMatch(routeInfo -> routeInfo.getState() == RoutingState.STARTED);
    }

    public List<Tuple<String, Integer>> selectRandomNodesWeighedOnAllocations(int numberOfRequests, RoutingState... acceptableStates) {
        List<String> nodeIds = new ArrayList<>(nodeRoutingTable.size());
        List<Integer> cumulativeAllocations = new ArrayList<>(nodeRoutingTable.size());
        int allocationSum = 0;
        for (Map.Entry<String, RoutingInfo> routingEntry : nodeRoutingTable.entrySet()) {
            if (routingEntry.getValue().getState().isAnyOf(acceptableStates)) {
                nodeIds.add(routingEntry.getKey());
                allocationSum += routingEntry.getValue().getCurrentAllocations();
                cumulativeAllocations.add(allocationSum);
            }
        }

        if (nodeIds.isEmpty()) {
            return List.of();
        }

        if (nodeIds.size() == 1) {
            return List.of(new Tuple<>(nodeIds.get(0), numberOfRequests));
        }

        if (allocationSum == 0) {
            // If we are in a mixed cluster where there are assignments prior to introducing allocation distribution
            // we could have a zero-sum of allocations. We fall back to returning a random started node.
            int[] counts = new int[nodeIds.size()];
            for (int i = 0; i < numberOfRequests; i++) {
                counts[Randomness.get().nextInt(nodeIds.size())]++;
            }

            var nodeCounts = new ArrayList<Tuple<String, Integer>>();
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    nodeCounts.add(new Tuple<>(nodeIds.get(i), counts[i]));
                }
            }
            return nodeCounts;
        }

        int[] counts = new int[nodeIds.size()];
        var randomIter = Randomness.get().ints(numberOfRequests, 1, allocationSum + 1).iterator();
        for (int i = 0; i < numberOfRequests; i++) {
            int randomInt = randomIter.nextInt();
            int nodeIndex = Collections.binarySearch(cumulativeAllocations, randomInt);
            if (nodeIndex < 0) {
                nodeIndex = -nodeIndex - 1;
            }

            counts[nodeIndex]++;
        }

        var nodeCounts = new ArrayList<Tuple<String, Integer>>();
        for (int i = 0; i < counts.length; i++) {
            // filter out zero counts
            if (counts[i] > 0) {
                nodeCounts.add(new Tuple<>(nodeIds.get(i), counts[i]));
            }
        }
        return nodeCounts;
    }

    public Optional<String> getReason() {
        return Optional.ofNullable(reason);
    }

    public Instant getStartTime() {
        return startTime;
    }

    public int getMaxAssignedAllocations() {
        return maxAssignedAllocations;
    }

    public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
        return adaptiveAllocationsSettings;
    }

    public boolean isSatisfied(Set<String> assignableNodeIds) {
        int allocations = nodeRoutingTable.entrySet()
            .stream()
            .filter(e -> assignableNodeIds.contains(e.getKey()))
            .filter(e -> e.getValue().getState().isAnyOf(RoutingState.STARTING, RoutingState.STARTED))
            .mapToInt(e -> e.getValue().getTargetAllocations())
            .sum();
        return allocations >= taskParams.getNumberOfAllocations();
    }

    public boolean hasOutdatedRoutingEntries() {
        return nodeRoutingTable.values().stream().anyMatch(RoutingInfo::isOutdated);
    }

    public int totalCurrentAllocations() {
        return nodeRoutingTable.values().stream().mapToInt(RoutingInfo::getCurrentAllocations).sum();
    }

    public int totalTargetAllocations() {
        return nodeRoutingTable.values().stream().mapToInt(RoutingInfo::getTargetAllocations).sum();
    }

    public int totalTargetProcessors() {
        return nodeRoutingTable.values().stream().mapToInt(r -> r.getTargetAllocations() * getTaskParams().getThreadsPerAllocation()).sum();
    }

    public int totalFailedAllocations() {
        return nodeRoutingTable.values().stream().mapToInt(RoutingInfo::getFailedAllocations).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelAssignment that = (TrainedModelAssignment) o;
        return Objects.equals(nodeRoutingTable, that.nodeRoutingTable)
            && Objects.equals(taskParams, that.taskParams)
            && Objects.equals(reason, that.reason)
            && Objects.equals(assignmentState, that.assignmentState)
            && Objects.equals(startTime, that.startTime)
            && maxAssignedAllocations == that.maxAssignedAllocations
            && Objects.equals(adaptiveAllocationsSettings, that.adaptiveAllocationsSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nodeRoutingTable,
            taskParams,
            assignmentState,
            reason,
            startTime,
            maxAssignedAllocations,
            adaptiveAllocationsSettings
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_PARAMETERS.getPreferredName(), taskParams);
        builder.field(ROUTING_TABLE.getPreferredName(), nodeRoutingTable);
        builder.field(ASSIGNMENT_STATE.getPreferredName(), assignmentState);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.timestampField(START_TIME.getPreferredName(), startTime);
        builder.field(MAX_ASSIGNED_ALLOCATIONS.getPreferredName(), maxAssignedAllocations);
        builder.field(ADAPTIVE_ALLOCATIONS.getPreferredName(), adaptiveAllocationsSettings);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskParams.writeTo(out);
        out.writeMap(nodeRoutingTable, StreamOutput::writeWriteable);
        out.writeEnum(assignmentState);
        out.writeOptionalString(reason);
        out.writeInstant(startTime);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeVInt(maxAssignedAllocations);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
            out.writeOptionalWriteable(adaptiveAllocationsSettings);
        }
    }

    public Optional<AllocationStatus> calculateAllocationStatus() {
        if (assignmentState.equals(AssignmentState.STOPPING)) {
            return Optional.empty();
        }
        int numStarted = nodeRoutingTable.values()
            .stream()
            .filter(RoutingInfo::isRoutable)
            .mapToInt(RoutingInfo::getCurrentAllocations)
            .sum();
        return Optional.of(new AllocationStatus(numStarted, taskParams.getNumberOfAllocations()));
    }

    public static class Builder {
        private final Map<String, RoutingInfo> nodeRoutingTable;
        private StartTrainedModelDeploymentAction.TaskParams taskParams;
        private AssignmentState assignmentState;
        private String reason;
        private Instant startTime;
        private int maxAssignedAllocations;
        private AdaptiveAllocationsSettings adaptiveAllocationsSettings;

        public static Builder fromAssignment(TrainedModelAssignment assignment) {
            return new Builder(
                assignment.taskParams,
                assignment.nodeRoutingTable,
                assignment.assignmentState,
                assignment.reason,
                assignment.startTime,
                assignment.maxAssignedAllocations,
                assignment.adaptiveAllocationsSettings
            );
        }

        public static Builder empty(CreateTrainedModelAssignmentAction.Request request) {
            return new Builder(request.getTaskParams(), request.getAdaptiveAllocationsSettings());
        }

        public static Builder empty(
            StartTrainedModelDeploymentAction.TaskParams taskParams,
            AdaptiveAllocationsSettings adaptiveAllocationsSettings
        ) {
            return new Builder(taskParams, adaptiveAllocationsSettings);
        }

        private Builder(
            StartTrainedModelDeploymentAction.TaskParams taskParams,
            Map<String, RoutingInfo> nodeRoutingTable,
            AssignmentState assignmentState,
            String reason,
            Instant startTime,
            int maxAssignedAllocations,
            AdaptiveAllocationsSettings adaptiveAllocationsSettings
        ) {
            this.taskParams = taskParams;
            this.nodeRoutingTable = new LinkedHashMap<>(nodeRoutingTable);
            this.assignmentState = assignmentState;
            this.reason = reason;
            this.startTime = startTime;
            this.maxAssignedAllocations = maxAssignedAllocations;
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
        }

        private Builder(StartTrainedModelDeploymentAction.TaskParams taskParams, AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this(taskParams, new LinkedHashMap<>(), AssignmentState.STARTING, null, Instant.now(), 0, adaptiveAllocationsSettings);
        }

        public Builder setStartTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder setMaxAssignedAllocations(int maxAssignedAllocations) {
            this.maxAssignedAllocations = maxAssignedAllocations;
            return this;
        }

        public Builder setAdaptiveAllocationsSettings(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
            return this;
        }

        public Builder addRoutingEntry(String nodeId, RoutingInfo routingInfo) {
            if (nodeRoutingTable.containsKey(nodeId)) {
                throw new ResourceAlreadyExistsException(
                    "routing entry for node [{}] for model [{}] deployment [{}] already exists",
                    nodeId,
                    taskParams.getModelId(),
                    taskParams.getDeploymentId()
                );
            }
            nodeRoutingTable.put(nodeId, routingInfo);
            return this;
        }

        public Builder updateExistingRoutingEntry(String nodeId, RoutingInfo routingInfo) {
            RoutingInfo existingRoutingInfo = nodeRoutingTable.get(nodeId);
            if (existingRoutingInfo == null) {
                throw new ResourceNotFoundException(
                    "routing entry for node [{}] for model [{}] deployment [{}] does not exist",
                    nodeId,
                    taskParams.getModelId(),
                    taskParams.getDeploymentId()
                );
            }
            if (existingRoutingInfo.equals(routingInfo)) {
                return this;
            }
            nodeRoutingTable.put(nodeId, routingInfo);
            return this;
        }

        /**
         * Adds the {@link RoutingInfo} regardless of whether it already exists.
         */
        public Builder addOrOverwriteRoutingEntry(String nodeId, RoutingInfo routingInfo) {
            nodeRoutingTable.put(nodeId, routingInfo);

            return this;
        }

        public Builder removeRoutingEntry(String nodeId) {
            nodeRoutingTable.remove(nodeId);
            return this;
        }

        public Builder setReason(String reason) {
            if (Objects.equals(reason, this.reason)) {
                return this;
            }
            this.reason = reason;
            return this;
        }

        public Builder stopAssignment(String stopReason) {
            if (assignmentState.equals(AssignmentState.STOPPING)) {
                return this;
            }
            this.reason = stopReason;
            assignmentState = AssignmentState.STOPPING;
            return this;
        }

        public AssignmentState calculateAssignmentState() {
            if (assignmentState.equals(AssignmentState.STOPPING)) {
                return assignmentState;
            }
            if (nodeRoutingTable.values().stream().anyMatch(r -> r.getState().equals(RoutingState.STARTED))) {
                return AssignmentState.STARTED;
            }
            return AssignmentState.STARTING;
        }

        public Builder calculateAndSetAssignmentState() {
            return setAssignmentState(calculateAssignmentState());
        }

        public Builder setAssignmentState(AssignmentState state) {
            if (assignmentState.equals(AssignmentState.STOPPING)) {
                return this;
            }
            if (assignmentState.equals(state)) {
                return this;
            }
            assignmentState = state;
            return this;
        }

        public Builder clearReason() {
            if (this.reason == null) {
                return this;
            }
            reason = null;
            return this;
        }

        public Builder clearNodeRoutingTable() {
            nodeRoutingTable.clear();

            return this;
        }

        public Builder setNumberOfAllocations(int numberOfAllocations) {
            this.taskParams = new StartTrainedModelDeploymentAction.TaskParams(
                taskParams.getModelId(),
                taskParams.getDeploymentId(),
                taskParams.getModelBytes(),
                numberOfAllocations,
                taskParams.getThreadsPerAllocation(),
                taskParams.getQueueCapacity(),
                taskParams.getCacheSize().orElse(null),
                taskParams.getPriority(),
                taskParams.getPerDeploymentMemoryBytes(),
                taskParams.getPerAllocationMemoryBytes()
            );
            return this;
        }

        public TrainedModelAssignment build() {
            return new TrainedModelAssignment(
                taskParams,
                nodeRoutingTable,
                assignmentState,
                reason,
                startTime,
                maxAssignedAllocations,
                adaptiveAllocationsSettings
            );
        }
    }
}

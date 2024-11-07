/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ml.MlTasks.trainedModelAssignmentTaskDescription;

public class StartTrainedModelDeploymentAction extends ActionType<CreateTrainedModelAssignmentAction.Response> {

    public static final StartTrainedModelDeploymentAction INSTANCE = new StartTrainedModelDeploymentAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/start";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    /**
     * This has been found to be approximately 300MB on linux by manual testing.
     * 30MB of this is accounted for via the space set aside to load the code into memory
     * that we always add as overhead (see MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD).
     * That would result in 270MB here, although the problem with this is that it would
     * mean few models would fit on a 2GB ML node in Cloud with logging and metrics enabled.
     * Therefore we push the boundary a bit and require a 240MB per-model overhead.
     * TODO Check if it is substantially different in other platforms.
     */
    private static final ByteSizeValue MEMORY_OVERHEAD = ByteSizeValue.ofMb(240);

    /**
     * The ELSER model turned out to use more memory than what we usually estimate.
     * We overwrite the estimate with this static value for ELSER v1 and v2 for now.
     * Soon to be replaced with a better estimate provided by the model.
     */
    private static final ByteSizeValue ELSER_1_OR_2_MEMORY_USAGE = ByteSizeValue.ofMb(2004);
    public static final AllocationStatus.State DEFAULT_WAITFOR_STATE = AllocationStatus.State.STARTED;
    public static final int DEFAULT_NUM_ALLOCATIONS = 1;
    public static final int DEFAULT_NUM_THREADS = 1;
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    public static final Priority DEFAULT_PRIORITY = Priority.NORMAL;

    public StartTrainedModelDeploymentAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        private static final AllocationStatus.State[] VALID_WAIT_STATES = new AllocationStatus.State[] {
            AllocationStatus.State.STARTED,
            AllocationStatus.State.STARTING,
            AllocationStatus.State.FULLY_ALLOCATED };

        private static final int MAX_THREADS_PER_ALLOCATION = 32;
        /**
         * If the queue is created then we can OOM when we create the queue.
         */
        private static final int MAX_QUEUE_CAPACITY = 100_000;

        public static final ParseField MODEL_ID = new ParseField("model_id");
        public static final ParseField DEPLOYMENT_ID = new ParseField("deployment_id");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField WAIT_FOR = new ParseField("wait_for");
        public static final ParseField THREADS_PER_ALLOCATION = new ParseField("threads_per_allocation", "inference_threads");
        public static final ParseField NUMBER_OF_ALLOCATIONS = new ParseField("number_of_allocations", "model_threads");
        public static final ParseField QUEUE_CAPACITY = TaskParams.QUEUE_CAPACITY;
        public static final ParseField CACHE_SIZE = TaskParams.CACHE_SIZE;
        public static final ParseField PRIORITY = TaskParams.PRIORITY;
        public static final ParseField ADAPTIVE_ALLOCATIONS = TrainedModelAssignment.ADAPTIVE_ALLOCATIONS;

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setModelId, MODEL_ID);
            PARSER.declareString(Request::setDeploymentId, DEPLOYMENT_ID);
            PARSER.declareString((request, val) -> request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareString((request, waitFor) -> request.setWaitForState(AllocationStatus.State.fromString(waitFor)), WAIT_FOR);
            PARSER.declareInt(Request::setThreadsPerAllocation, THREADS_PER_ALLOCATION);
            PARSER.declareInt(Request::setNumberOfAllocations, NUMBER_OF_ALLOCATIONS);
            PARSER.declareInt(Request::setQueueCapacity, QUEUE_CAPACITY);
            PARSER.declareField(
                Request::setCacheSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), CACHE_SIZE.getPreferredName()),
                CACHE_SIZE,
                ObjectParser.ValueType.VALUE
            );
            PARSER.declareString(Request::setPriority, PRIORITY);
            PARSER.declareObjectOrNull(
                Request::setAdaptiveAllocationsSettings,
                (p, c) -> AdaptiveAllocationsSettings.PARSER.parse(p, c).build(),
                null,
                ADAPTIVE_ALLOCATIONS
            );
        }

        public static Request parseRequest(String modelId, String deploymentId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getModelId() == null) {
                request.setModelId(modelId);
            } else if (Strings.isNullOrEmpty(modelId) == false && modelId.equals(request.getModelId()) == false) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, MODEL_ID, request.getModelId(), modelId)
                );
            }

            if (deploymentId != null) {
                request.setDeploymentId(deploymentId);
            }
            return request;
        }

        private String modelId;
        private String deploymentId;
        private TimeValue timeout = DEFAULT_TIMEOUT;
        private AllocationStatus.State waitForState = DEFAULT_WAITFOR_STATE;
        private ByteSizeValue cacheSize;
        private Integer numberOfAllocations;
        private AdaptiveAllocationsSettings adaptiveAllocationsSettings = null;
        private int threadsPerAllocation = DEFAULT_NUM_THREADS;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private Priority priority = DEFAULT_PRIORITY;

        private Request() {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
        }

        public Request(String modelId, String deploymentId) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            setModelId(modelId);
            setDeploymentId(deploymentId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            modelId = in.readString();
            timeout = in.readTimeValue();
            waitForState = in.readEnum(AllocationStatus.State.class);
            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
                numberOfAllocations = in.readOptionalVInt();
            } else {
                numberOfAllocations = in.readVInt();
            }
            threadsPerAllocation = in.readVInt();
            queueCapacity = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                this.cacheSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
                this.priority = in.readEnum(Priority.class);
            } else {
                this.priority = Priority.NORMAL;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                this.deploymentId = in.readString();
            } else {
                this.deploymentId = modelId;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
                this.adaptiveAllocationsSettings = in.readOptionalWriteable(AdaptiveAllocationsSettings::new);
            } else {
                this.adaptiveAllocationsSettings = null;
            }
        }

        public final void setModelId(String modelId) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        }

        public final void setDeploymentId(String deploymentId) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, DEPLOYMENT_ID);
        }

        public String getModelId() {
            return modelId;
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = ExceptionsHelper.requireNonNull(timeout, TIMEOUT);
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public AllocationStatus.State getWaitForState() {
            return waitForState;
        }

        public Request setWaitForState(AllocationStatus.State waitForState) {
            this.waitForState = ExceptionsHelper.requireNonNull(waitForState, WAIT_FOR);
            return this;
        }

        public Integer getNumberOfAllocations() {
            return numberOfAllocations;
        }

        public int computeNumberOfAllocations() {
            if (numberOfAllocations != null) {
                return numberOfAllocations;
            } else {
                if (adaptiveAllocationsSettings == null
                    || adaptiveAllocationsSettings.getMinNumberOfAllocations() == null
                    || adaptiveAllocationsSettings.getMinNumberOfAllocations() == 0) {
                    return DEFAULT_NUM_ALLOCATIONS;
                } else {
                    return adaptiveAllocationsSettings.getMinNumberOfAllocations();
                }
            }
        }

        public void setNumberOfAllocations(Integer numberOfAllocations) {
            this.numberOfAllocations = numberOfAllocations;
        }

        public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
            return adaptiveAllocationsSettings;
        }

        public void setAdaptiveAllocationsSettings(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
        }

        public int getThreadsPerAllocation() {
            return threadsPerAllocation;
        }

        public void setThreadsPerAllocation(int threadsPerAllocation) {
            this.threadsPerAllocation = threadsPerAllocation;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public ByteSizeValue getCacheSize() {
            return cacheSize;
        }

        public void setCacheSize(ByteSizeValue cacheSize) {
            this.cacheSize = cacheSize;
        }

        public Priority getPriority() {
            return priority;
        }

        public void setPriority(String priority) {
            this.priority = Priority.fromString(priority);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeTimeValue(timeout);
            out.writeEnum(waitForState);
            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
                out.writeOptionalVInt(numberOfAllocations);
            } else {
                out.writeVInt(numberOfAllocations);
            }
            out.writeVInt(threadsPerAllocation);
            out.writeVInt(queueCapacity);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                out.writeOptionalWriteable(cacheSize);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
                out.writeEnum(priority);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                out.writeString(deploymentId);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
                out.writeOptionalWriteable(adaptiveAllocationsSettings);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID.getPreferredName(), modelId);
            builder.field(DEPLOYMENT_ID.getPreferredName(), deploymentId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.field(WAIT_FOR.getPreferredName(), waitForState);
            if (numberOfAllocations != null) {
                builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            }
            if (adaptiveAllocationsSettings != null) {
                builder.field(ADAPTIVE_ALLOCATIONS.getPreferredName(), adaptiveAllocationsSettings);
            }
            builder.field(THREADS_PER_ALLOCATION.getPreferredName(), threadsPerAllocation);
            builder.field(QUEUE_CAPACITY.getPreferredName(), queueCapacity);
            if (cacheSize != null) {
                builder.field(CACHE_SIZE.getPreferredName(), cacheSize);
            }
            builder.field(PRIORITY.getPreferredName(), priority);
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            if (waitForState.isAnyOf(VALID_WAIT_STATES) == false) {
                validationException.addValidationError(
                    "invalid [wait_for] state ["
                        + waitForState
                        + "]; must be one of ["
                        + Strings.arrayToCommaDelimitedString(VALID_WAIT_STATES)
                );
            }
            if (numberOfAllocations != null) {
                if (numberOfAllocations < 1) {
                    validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] must be a positive integer");
                }
                if (adaptiveAllocationsSettings != null && adaptiveAllocationsSettings.getEnabled() == Boolean.TRUE) {
                    validationException.addValidationError(
                        "[" + NUMBER_OF_ALLOCATIONS + "] cannot be set if adaptive allocations is enabled"
                    );
                }
            }
            if (threadsPerAllocation < 1) {
                validationException.addValidationError("[" + THREADS_PER_ALLOCATION + "] must be a positive integer");
            }
            ActionRequestValidationException autoscaleException = adaptiveAllocationsSettings == null
                ? null
                : adaptiveAllocationsSettings.validate();
            if (autoscaleException != null) {
                validationException.addValidationErrors(autoscaleException.validationErrors());
            }
            if (threadsPerAllocation > MAX_THREADS_PER_ALLOCATION || isPowerOf2(threadsPerAllocation) == false) {
                validationException.addValidationError(
                    "[" + THREADS_PER_ALLOCATION + "] must be a power of 2 less than or equal to " + MAX_THREADS_PER_ALLOCATION
                );
            }
            if (queueCapacity < 1) {
                validationException.addValidationError("[" + QUEUE_CAPACITY + "] must be a positive integer");
            }
            if (queueCapacity > MAX_QUEUE_CAPACITY) {
                validationException.addValidationError("[" + QUEUE_CAPACITY + "] must be less than " + MAX_QUEUE_CAPACITY);
            }
            if (timeout.nanos() < 1L) {
                validationException.addValidationError("[" + TIMEOUT + "] must be positive");
            }
            if (priority == Priority.LOW) {
                if (numberOfAllocations != null && numberOfAllocations > 1) {
                    validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] must be 1 when [" + PRIORITY + "] is low");
                }
                if (threadsPerAllocation > 1) {
                    validationException.addValidationError("[" + THREADS_PER_ALLOCATION + "] must be 1 when [" + PRIORITY + "] is low");
                }
            }
            return validationException.validationErrors().isEmpty() ? null : validationException;
        }

        private static boolean isPowerOf2(int value) {
            return Integer.bitCount(value) == 1;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                modelId,
                deploymentId,
                timeout,
                waitForState,
                numberOfAllocations,
                adaptiveAllocationsSettings,
                threadsPerAllocation,
                queueCapacity,
                cacheSize,
                priority
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(modelId, other.modelId)
                && Objects.equals(deploymentId, other.deploymentId)
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(waitForState, other.waitForState)
                && Objects.equals(cacheSize, other.cacheSize)
                && Objects.equals(numberOfAllocations, other.numberOfAllocations)
                && Objects.equals(adaptiveAllocationsSettings, other.adaptiveAllocationsSettings)
                && threadsPerAllocation == other.threadsPerAllocation
                && queueCapacity == other.queueCapacity
                && priority == other.priority;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class TaskParams implements MlTaskParams, Writeable, ToXContentObject {

        // TODO add support for other roles? If so, it may have to be an instance method...
        // NOTE, whatever determines assignment should not be dynamically set on the node
        // Otherwise assignment logic might fail
        public static boolean mayAssignToNode(@Nullable DiscoveryNode node) {
            return node != null
                && node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)
                && MlConfigVersion.fromNode(node).onOrAfter(VERSION_INTRODUCED);
        }

        public static final MlConfigVersion VERSION_INTRODUCED = MlConfigVersion.V_8_0_0;

        private static final ParseField MODEL_BYTES = new ParseField("model_bytes");
        public static final ParseField NUMBER_OF_ALLOCATIONS = new ParseField("number_of_allocations");
        public static final ParseField THREADS_PER_ALLOCATION = new ParseField("threads_per_allocation");
        // number_of_allocations was previously named model_threads
        private static final ParseField LEGACY_MODEL_THREADS = new ParseField("model_threads");
        // threads_per_allocation was previously named inference_threads
        public static final ParseField LEGACY_INFERENCE_THREADS = new ParseField("inference_threads");
        public static final ParseField QUEUE_CAPACITY = new ParseField("queue_capacity");
        public static final ParseField CACHE_SIZE = new ParseField("cache_size");
        public static final ParseField PRIORITY = new ParseField("priority");
        public static final ParseField PER_DEPLOYMENT_MEMORY_BYTES = new ParseField("per_deployment_memory_bytes");
        public static final ParseField PER_ALLOCATION_MEMORY_BYTES = new ParseField("per_allocation_memory_bytes");

        private static final ConstructingObjectParser<TaskParams, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_deployment_params",
            true,
            a -> new TaskParams(
                (String) a[0],
                (String) a[1],
                (Long) a[2],
                (Integer) a[3],
                (Integer) a[4],
                (int) a[5],
                (ByteSizeValue) a[6],
                (Integer) a[7],
                (Integer) a[8],
                a[9] == null ? null : Priority.fromString((String) a[9]),
                (Long) a[10],
                (Long) a[11]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TrainedModelConfig.MODEL_ID);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), Request.DEPLOYMENT_ID);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODEL_BYTES);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUMBER_OF_ALLOCATIONS);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), THREADS_PER_ALLOCATION);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), QUEUE_CAPACITY);
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), CACHE_SIZE.getPreferredName()),
                CACHE_SIZE,
                ObjectParser.ValueType.VALUE
            );
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), LEGACY_MODEL_THREADS);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), LEGACY_INFERENCE_THREADS);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PRIORITY);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PER_DEPLOYMENT_MEMORY_BYTES);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PER_ALLOCATION_MEMORY_BYTES);
        }

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String modelId;
        private final String deploymentId;
        private final ByteSizeValue cacheSize;
        private final long modelBytes;
        // How many threads are used by the model during inference. Used to increase inference speed.
        private final int threadsPerAllocation;
        // How many threads are used when forwarding the request to the model. Used to increase throughput.
        private final int numberOfAllocations;
        private final int queueCapacity;
        private final Priority priority;
        private final long perDeploymentMemoryBytes;
        private final long perAllocationMemoryBytes;

        private TaskParams(
            String modelId,
            @Nullable String deploymentId,
            long modelBytes,
            Integer numberOfAllocations,
            Integer threadsPerAllocation,
            int queueCapacity,
            ByteSizeValue cacheSizeValue,
            Integer legacyModelThreads,
            Integer legacyInferenceThreads,
            Priority priority,
            Long perDeploymentMemoryBytes,
            Long perAllocationMemoryBytes
        ) {
            this(
                modelId,
                // deploymentId should only be null in a mixed cluster
                // with pre-8.8 nodes
                deploymentId == null ? modelId : deploymentId,
                modelBytes,
                numberOfAllocations == null ? legacyModelThreads : numberOfAllocations,
                threadsPerAllocation == null ? legacyInferenceThreads : threadsPerAllocation,
                queueCapacity,
                cacheSizeValue,
                priority == null ? Priority.NORMAL : priority,
                perDeploymentMemoryBytes == null ? 0 : perDeploymentMemoryBytes,
                perAllocationMemoryBytes == null ? 0 : perAllocationMemoryBytes
            );
        }

        public TaskParams(
            String modelId,
            String deploymentId,
            long modelBytes,
            int numberOfAllocations,
            int threadsPerAllocation,
            int queueCapacity,
            @Nullable ByteSizeValue cacheSize,
            Priority priority,
            long perDeploymentMemoryBytes,
            long perAllocationMemoryBytes
        ) {
            this.modelId = Objects.requireNonNull(modelId);
            this.deploymentId = Objects.requireNonNull(deploymentId);
            this.modelBytes = modelBytes;
            this.threadsPerAllocation = threadsPerAllocation;
            this.numberOfAllocations = numberOfAllocations;
            this.queueCapacity = queueCapacity;
            this.cacheSize = cacheSize;
            this.priority = Objects.requireNonNull(priority);
            this.perDeploymentMemoryBytes = perDeploymentMemoryBytes;
            this.perAllocationMemoryBytes = perAllocationMemoryBytes;
        }

        public TaskParams(StreamInput in) throws IOException {
            this.modelId = in.readString();
            this.modelBytes = in.readLong();
            this.threadsPerAllocation = in.readVInt();
            this.numberOfAllocations = in.readVInt();
            this.queueCapacity = in.readVInt();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                this.cacheSize = in.readOptionalWriteable(ByteSizeValue::readFrom);
            } else {
                this.cacheSize = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
                this.priority = in.readEnum(Priority.class);
            } else {
                this.priority = Priority.NORMAL;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                this.deploymentId = in.readString();
            } else {
                this.deploymentId = modelId;
            }

            if (in.getTransportVersion().onOrAfter(TrainedModelConfig.VERSION_ALLOCATION_MEMORY_ADDED)) {
                // We store additional model usage per allocation in the task params.
                this.perDeploymentMemoryBytes = in.readLong();
                this.perAllocationMemoryBytes = in.readLong();
            } else {
                this.perDeploymentMemoryBytes = 0L;
                this.perAllocationMemoryBytes = 0L;
            }
        }

        public String getModelId() {
            return modelId;
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        /**
         * @return the estimated memory (in bytes) required for the model deployment to run
         */
        public long estimateMemoryUsageBytes() {
            if (numberOfAllocations == 0) {
                return 0;
            }
            // We already take into account 2x the model bytes. If the cache size is larger than the model bytes, then
            // we need to take it into account when returning the estimate.
            if (cacheSize != null && cacheSize.getBytes() > modelBytes) {
                return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                    modelId,
                    modelBytes,
                    perDeploymentMemoryBytes,
                    perAllocationMemoryBytes,
                    numberOfAllocations
                ) + (cacheSize.getBytes() - modelBytes);
            }
            return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                modelId,
                modelBytes,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes,
                numberOfAllocations
            );
        }

        public MlConfigVersion getMinimalSupportedVersion() {
            return VERSION_INTRODUCED;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelId);
            out.writeLong(modelBytes);
            out.writeVInt(threadsPerAllocation);
            out.writeVInt(numberOfAllocations);
            out.writeVInt(queueCapacity);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
                out.writeOptionalWriteable(cacheSize);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
                out.writeEnum(priority);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                out.writeString(deploymentId);
            }
            if (out.getTransportVersion().onOrAfter(TrainedModelConfig.VERSION_ALLOCATION_MEMORY_ADDED)) {
                out.writeLong(perDeploymentMemoryBytes);
                out.writeLong(perAllocationMemoryBytes);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
            builder.field(Request.DEPLOYMENT_ID.getPreferredName(), deploymentId);
            builder.field(MODEL_BYTES.getPreferredName(), modelBytes);
            builder.field(THREADS_PER_ALLOCATION.getPreferredName(), threadsPerAllocation);
            builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            builder.field(QUEUE_CAPACITY.getPreferredName(), queueCapacity);
            if (cacheSize != null) {
                builder.field(CACHE_SIZE.getPreferredName(), cacheSize.getStringRep());
            }
            builder.field(PRIORITY.getPreferredName(), priority);
            builder.field(PER_DEPLOYMENT_MEMORY_BYTES.getPreferredName(), perDeploymentMemoryBytes);
            builder.field(PER_ALLOCATION_MEMORY_BYTES.getPreferredName(), perAllocationMemoryBytes);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                modelId,
                deploymentId,
                modelBytes,
                threadsPerAllocation,
                numberOfAllocations,
                queueCapacity,
                cacheSize,
                priority,
                perDeploymentMemoryBytes,
                perAllocationMemoryBytes
            );
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskParams other = (TaskParams) o;
            return Objects.equals(modelId, other.modelId)
                && Objects.equals(deploymentId, other.deploymentId)
                && modelBytes == other.modelBytes
                && threadsPerAllocation == other.threadsPerAllocation
                && numberOfAllocations == other.numberOfAllocations
                && Objects.equals(cacheSize, other.cacheSize)
                && queueCapacity == other.queueCapacity
                && priority == other.priority
                && perDeploymentMemoryBytes == other.perDeploymentMemoryBytes
                && perAllocationMemoryBytes == other.perAllocationMemoryBytes;
        }

        @Override
        public String getMlId() {
            return modelId;
        }

        public long getModelBytes() {
            return modelBytes;
        }

        /**
         * @return the number of threads per allocation used by the model during inference. each thread requires one processor.
         */
        public int getThreadsPerAllocation() {
            return threadsPerAllocation;
        }

        /**
         * @return the number of allocations requested by the user
         */
        public int getNumberOfAllocations() {
            return numberOfAllocations;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public Optional<ByteSizeValue> getCacheSize() {
            return Optional.ofNullable(cacheSize);
        }

        public long getCacheSizeBytes() {
            return Optional.ofNullable(cacheSize).map(ByteSizeValue::getBytes).orElse(modelBytes);
        }

        public Priority getPriority() {
            return priority;
        }

        public long getPerAllocationMemoryBytes() {
            return perAllocationMemoryBytes;
        }

        public long getPerDeploymentMemoryBytes() {
            return perDeploymentMemoryBytes;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public interface TaskMatcher {

        static boolean match(Task task, String expectedId) {
            if (task instanceof TaskMatcher) {
                if (Strings.isAllOrWildcard(expectedId)) {
                    return true;
                }
                String expectedDescription = trainedModelAssignmentTaskDescription(expectedId);
                return expectedDescription.equals(task.getDescription());
            }
            return false;
        }
    }

    public static long estimateMemoryUsageBytes(
        String modelId,
        long totalDefinitionLength,
        long perDeploymentMemoryBytes,
        long perAllocationMemoryBytes,
        int numberOfAllocations
    ) {
        if (numberOfAllocations == 0) {
            return 0;
        }
        // While loading the model in the process we need twice the model size.

        // 1. If ELSER v1 or v2 then 2004MB
        // 2. If static memory and dynamic memory are not set then 240MB + 2 * model size
        // 3. Else static memory + dynamic memory * allocations + model size

        // The model size is still added in option 3 to account for the temporary requirement to hold the zip file in memory
        // in `pytorch_inference`.
        if (isElserV1Or2Model(modelId)) {
            return ELSER_1_OR_2_MEMORY_USAGE.getBytes();
        } else {
            long baseSize = MEMORY_OVERHEAD.getBytes() + 2 * totalDefinitionLength;
            if (perDeploymentMemoryBytes == 0 && perAllocationMemoryBytes == 0) {
                return baseSize;
            } else {
                return Math.max(
                    baseSize,
                    perDeploymentMemoryBytes + perAllocationMemoryBytes * numberOfAllocations + totalDefinitionLength
                );
            }
        }
    }

    private static boolean isElserV1Or2Model(String modelId) {
        return modelId.startsWith(".elser_model_1") || modelId.startsWith(".elser_model_2");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
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
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.MlTasks.trainedModelAssignmentTaskDescription;

public class StartTrainedModelDeploymentAction extends ActionType<CreateTrainedModelAssignmentAction.Response> {

    public static final StartTrainedModelDeploymentAction INSTANCE = new StartTrainedModelDeploymentAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/start";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(20, TimeUnit.SECONDS);

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

    public StartTrainedModelDeploymentAction() {
        super(NAME, CreateTrainedModelAssignmentAction.Response::new);
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
        private static final int MAX_QUEUE_CAPACITY = 1_000_000;

        public static final ParseField MODEL_ID = new ParseField("model_id");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField WAIT_FOR = new ParseField("wait_for");
        public static final ParseField THREADS_PER_ALLOCATION = new ParseField("threads_per_allocation", "inference_threads");
        public static final ParseField NUMBER_OF_ALLOCATIONS = new ParseField("number_of_allocations", "model_threads");
        public static final ParseField QUEUE_CAPACITY = TaskParams.QUEUE_CAPACITY;
        public static final ParseField CACHE_SIZE = TaskParams.CACHE_SIZE;

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setModelId, MODEL_ID);
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
        }

        public static Request parseRequest(String modelId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getModelId() == null) {
                request.setModelId(modelId);
            } else if (Strings.isNullOrEmpty(modelId) == false && modelId.equals(request.getModelId()) == false) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, MODEL_ID, request.getModelId(), modelId)
                );
            }
            return request;
        }

        private String modelId;
        private TimeValue timeout = DEFAULT_TIMEOUT;
        private AllocationStatus.State waitForState = AllocationStatus.State.STARTED;
        private ByteSizeValue cacheSize;
        private int numberOfAllocations = 1;
        private int threadsPerAllocation = 1;
        private int queueCapacity = 1024;

        private Request() {}

        public Request(String modelId) {
            setModelId(modelId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            modelId = in.readString();
            timeout = in.readTimeValue();
            waitForState = in.readEnum(AllocationStatus.State.class);
            numberOfAllocations = in.readVInt();
            threadsPerAllocation = in.readVInt();
            queueCapacity = in.readVInt();
            if (in.getVersion().onOrAfter(Version.V_8_4_0)) {
                this.cacheSize = in.readOptionalWriteable(ByteSizeValue::new);
            }
        }

        public final void setModelId(String modelId) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        }

        public String getModelId() {
            return modelId;
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

        public int getNumberOfAllocations() {
            return numberOfAllocations;
        }

        public void setNumberOfAllocations(int numberOfAllocations) {
            this.numberOfAllocations = numberOfAllocations;
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeTimeValue(timeout);
            out.writeEnum(waitForState);
            out.writeVInt(numberOfAllocations);
            out.writeVInt(threadsPerAllocation);
            out.writeVInt(queueCapacity);
            if (out.getVersion().onOrAfter(Version.V_8_4_0)) {
                out.writeOptionalWriteable(cacheSize);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID.getPreferredName(), modelId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.field(WAIT_FOR.getPreferredName(), waitForState);
            builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            builder.field(THREADS_PER_ALLOCATION.getPreferredName(), threadsPerAllocation);
            builder.field(QUEUE_CAPACITY.getPreferredName(), queueCapacity);
            if (cacheSize != null) {
                builder.field(CACHE_SIZE.getPreferredName(), cacheSize);
            }
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
            if (numberOfAllocations < 1) {
                validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] must be a positive integer");
            }
            if (threadsPerAllocation < 1) {
                validationException.addValidationError("[" + THREADS_PER_ALLOCATION + "] must be a positive integer");
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
            return validationException.validationErrors().isEmpty() ? null : validationException;
        }

        private static boolean isPowerOf2(int value) {
            return Integer.bitCount(value) == 1;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, timeout, waitForState, numberOfAllocations, threadsPerAllocation, queueCapacity, cacheSize);
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
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(waitForState, other.waitForState)
                && Objects.equals(cacheSize, other.cacheSize)
                && numberOfAllocations == other.numberOfAllocations
                && threadsPerAllocation == other.threadsPerAllocation
                && queueCapacity == other.queueCapacity;
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
        public static boolean mayAssignToNode(DiscoveryNode node) {
            return node.getRoles().contains(DiscoveryNodeRole.ML_ROLE) && node.getVersion().onOrAfter(VERSION_INTRODUCED);
        }

        public static final Version VERSION_INTRODUCED = Version.V_8_0_0;
        private static final ParseField MODEL_BYTES = new ParseField("model_bytes");
        public static final ParseField NUMBER_OF_ALLOCATIONS = new ParseField("number_of_allocations");
        public static final ParseField THREADS_PER_ALLOCATION = new ParseField("threads_per_allocation");
        // number_of_allocations was previously named model_threads
        private static final ParseField LEGACY_MODEL_THREADS = new ParseField("model_threads");
        // threads_per_allocation was previously named inference_threads
        public static final ParseField LEGACY_INFERENCE_THREADS = new ParseField("inference_threads");
        public static final ParseField QUEUE_CAPACITY = new ParseField("queue_capacity");
        public static final ParseField CACHE_SIZE = new ParseField("cache_size");

        private static final ConstructingObjectParser<TaskParams, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_deployment_params",
            true,
            a -> new TaskParams(
                (String) a[0],
                (Long) a[1],
                (Integer) a[2],
                (Integer) a[3],
                (int) a[4],
                (ByteSizeValue) a[5],
                (Integer) a[6],
                (Integer) a[7]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TrainedModelConfig.MODEL_ID);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODEL_BYTES);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUMBER_OF_ALLOCATIONS);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), THREADS_PER_ALLOCATION);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), QUEUE_CAPACITY);
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), CACHE_SIZE.getPreferredName()),
                CACHE_SIZE,
                ObjectParser.ValueType.VALUE
            );
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), LEGACY_MODEL_THREADS);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), LEGACY_INFERENCE_THREADS);
        }

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final String modelId;
        private final ByteSizeValue cacheSize;
        private final long modelBytes;
        // How many threads are used by the model during inference. Used to increase inference speed.
        private final int threadsPerAllocation;
        // How many threads are used when forwarding the request to the model. Used to increase throughput.
        private final int numberOfAllocations;
        private final int queueCapacity;

        private TaskParams(
            String modelId,
            long modelBytes,
            Integer numberOfAllocations,
            Integer threadsPerAllocation,
            int queueCapacity,
            ByteSizeValue cacheSizeValue,
            Integer legacyModelThreads,
            Integer legacyInferenceThreads
        ) {
            this(
                modelId,
                modelBytes,
                numberOfAllocations == null ? legacyModelThreads : numberOfAllocations,
                threadsPerAllocation == null ? legacyInferenceThreads : threadsPerAllocation,
                queueCapacity,
                cacheSizeValue
            );
        }

        public TaskParams(
            String modelId,
            long modelBytes,
            int numberOfAllocations,
            int threadsPerAllocation,
            int queueCapacity,
            @Nullable ByteSizeValue cacheSize
        ) {
            this.modelId = Objects.requireNonNull(modelId);
            this.modelBytes = modelBytes;
            this.threadsPerAllocation = threadsPerAllocation;
            this.numberOfAllocations = numberOfAllocations;
            this.queueCapacity = queueCapacity;
            this.cacheSize = cacheSize;
        }

        public TaskParams(StreamInput in) throws IOException {
            this.modelId = in.readString();
            this.modelBytes = in.readLong();
            this.threadsPerAllocation = in.readVInt();
            this.numberOfAllocations = in.readVInt();
            this.queueCapacity = in.readVInt();
            if (in.getVersion().onOrAfter(Version.V_8_4_0)) {
                this.cacheSize = in.readOptionalWriteable(ByteSizeValue::new);
            } else {
                this.cacheSize = null;
            }
        }

        public String getModelId() {
            return modelId;
        }

        public long estimateMemoryUsageBytes() {
            // We already take into account 2x the model bytes. If the cache size is larger than the model bytes, then
            // we need to take it into account when returning the estimate.
            if (cacheSize != null && cacheSize.getBytes() > modelBytes) {
                return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(modelBytes) + (cacheSize.getBytes() - modelBytes);
            }
            return StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(modelBytes);
        }

        public Version getMinimalSupportedVersion() {
            return VERSION_INTRODUCED;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelId);
            out.writeLong(modelBytes);
            out.writeVInt(threadsPerAllocation);
            out.writeVInt(numberOfAllocations);
            out.writeVInt(queueCapacity);
            if (out.getVersion().onOrAfter(Version.V_8_4_0)) {
                out.writeOptionalWriteable(cacheSize);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
            builder.field(MODEL_BYTES.getPreferredName(), modelBytes);
            builder.field(THREADS_PER_ALLOCATION.getPreferredName(), threadsPerAllocation);
            builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            builder.field(QUEUE_CAPACITY.getPreferredName(), queueCapacity);
            if (cacheSize != null) {
                builder.field(CACHE_SIZE.getPreferredName(), cacheSize.getStringRep());
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, modelBytes, threadsPerAllocation, numberOfAllocations, queueCapacity, cacheSize);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskParams other = (TaskParams) o;
            return Objects.equals(modelId, other.modelId)
                && modelBytes == other.modelBytes
                && threadsPerAllocation == other.threadsPerAllocation
                && numberOfAllocations == other.numberOfAllocations
                && Objects.equals(cacheSize, other.cacheSize)
                && queueCapacity == other.queueCapacity;
        }

        @Override
        public String getMlId() {
            return modelId;
        }

        public long getModelBytes() {
            return modelBytes;
        }

        public int getThreadsPerAllocation() {
            return threadsPerAllocation;
        }

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

    public static long estimateMemoryUsageBytes(long totalDefinitionLength) {
        // While loading the model in the process we need twice the model size.
        return MEMORY_OVERHEAD.getBytes() + 2 * totalDefinitionLength;
    }
}

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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StartTrainedModelDeploymentAction extends ActionType<CreateTrainedModelAllocationAction.Response> {

    public static final StartTrainedModelDeploymentAction INSTANCE = new StartTrainedModelDeploymentAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/start";

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(20, TimeUnit.SECONDS);

    public StartTrainedModelDeploymentAction() {
        super(NAME, CreateTrainedModelAllocationAction.Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        public static final ParseField MODEL_ID = new ParseField("model_id");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        private String modelId;
        private TimeValue timeout = DEFAULT_TIMEOUT;

        public Request(String modelId) {
            setModelId(modelId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            modelId = in.readString();
            timeout = in.readTimeValue();
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeTimeValue(timeout);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MODEL_ID.getPreferredName(), modelId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, timeout);
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
            return Objects.equals(modelId, other.modelId) && Objects.equals(timeout, other.timeout);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class TaskParams implements PersistentTaskParams, MlTaskParams {

        // TODO add support for other roles? If so, it may have to be an instance method...
        // NOTE, whatever determines allocation should not be dynamically set on the node
        // Otherwise allocation logic might fail
        public static boolean mayAllocateToNode(DiscoveryNode node) {
            return node.getRoles().contains(DiscoveryNodeRole.ML_ROLE);
        }

        public static final Version VERSION_INTRODUCED = Version.V_8_0_0;
        private static final ParseField MODEL_BYTES = new ParseField("model_bytes");
        private static final ConstructingObjectParser<TaskParams, Void> PARSER = new ConstructingObjectParser<>(
            "trained_model_deployment_params",
            true,
            a -> new TaskParams((String)a[0], (String)a[1], (Long)a[2])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TrainedModelConfig.MODEL_ID);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), IndexLocation.INDEX);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODEL_BYTES);
        }

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /**
         * This has been found to be approximately 300MB on linux by manual testing.
         * We also subtract 30MB that we always add as overhead (see MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD).
         * TODO Check if it is substantially different in other platforms.
         */
        private static final ByteSizeValue MEMORY_OVERHEAD = ByteSizeValue.ofMb(270);

        private final String modelId;
        private final String index;
        private final long modelBytes;

        public TaskParams(String modelId, String index, long modelBytes) {
            this.modelId = Objects.requireNonNull(modelId);
            this.index = Objects.requireNonNull(index);
            this.modelBytes = modelBytes;
            if (modelBytes < 0) {
                throw new IllegalArgumentException("modelBytes must be non-negative");
            }
        }

        public TaskParams(StreamInput in) throws IOException {
            this.modelId = in.readString();
            this.index = in.readString();
            this.modelBytes = in.readVLong();
        }

        public String getModelId() {
            return modelId;
        }

        public String getIndex() {
            return index;
        }

        public long estimateMemoryUsageBytes() {
            // While loading the model in the process we need twice the model size.
            return MEMORY_OVERHEAD.getBytes() + 2 * modelBytes;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return VERSION_INTRODUCED;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelId);
            out.writeString(index);
            out.writeVLong(modelBytes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
            builder.field(IndexLocation.INDEX.getPreferredName(), index);
            builder.field(MODEL_BYTES.getPreferredName(), modelBytes);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, index, modelBytes);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskParams other = (TaskParams) o;
            return Objects.equals(modelId, other.modelId)
                && Objects.equals(index, other.index)
                && modelBytes == other.modelBytes;
        }

        @Override
        public String getMlId() {
            return modelId;
        }
    }

    public interface TaskMatcher {

        static boolean match(Task task, String expectedId) {
            if (task instanceof TaskMatcher) {
                if (Strings.isAllOrWildcard(expectedId)) {
                    return true;
                }
                String expectedDescription = MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_ID_PREFIX + expectedId;
                return expectedDescription.equals(task.getDescription());
            }
            return false;
        }
    }
}

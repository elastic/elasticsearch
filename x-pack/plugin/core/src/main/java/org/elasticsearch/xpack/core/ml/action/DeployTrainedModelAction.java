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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeployTrainedModelAction extends ActionType<NodeAcknowledgedResponse> {

    public static final DeployTrainedModelAction INSTANCE = new DeployTrainedModelAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/trained_model/deploy";

    public DeployTrainedModelAction() {
        super(NAME, NodeAcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        private static final ParseField MODEL_ID = new ParseField("model_id");

        private String modelId;

        public Request(String modelId) {
            setModelId(modelId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            modelId = in.readString();
        }

        public final void setModelId(String modelId) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MODEL_ID.getPreferredName(), modelId);
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
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
            return Objects.equals(modelId, other.modelId);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class TaskParams implements PersistentTaskParams {

        public static final Version VERSION_INTRODUCED = Version.V_7_13_0;

        private final String modelId;

        public TaskParams(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
        }

        public TaskParams(StreamInput in) throws IOException {
            this.modelId = in.readString();
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public String getWriteableName() {
            return MlTasks.DEPLOY_TRAINED_MODEL_TASK_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return VERSION_INTRODUCED;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TaskParams other = (TaskParams) o;
            return Objects.equals(modelId, other.modelId);
        }
    }

    public interface TaskMatcher {

        static boolean match(Task task, String expectedId) {
            if (task instanceof TaskMatcher) {
                if (Strings.isAllOrWildcard(expectedId)) {
                    return true;
                }
                String expectedDescription = MlTasks.DEPLOY_TRAINED_MODEL_TASK_ID_PREFIX + expectedId;
                return expectedDescription.equals(task.getDescription());
            }
            return false;
        }
    }
}

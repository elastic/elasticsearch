/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;

import java.io.IOException;
import java.util.Objects;

public class DeleteInferenceModelAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteInferenceModelAction INSTANCE = new DeleteInferenceModelAction();
    public static final String NAME = "cluster:admin/xpack/inference/delete";

    public DeleteInferenceModelAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<DeleteInferenceModelAction.Request> {

        private final String inferenceEntityId;
        private final TaskType taskType;

        public Request(String inferenceEntityId, TaskType taskType) {
            this.inferenceEntityId = inferenceEntityId;
            this.taskType = taskType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public TaskType getTaskType() {
            return taskType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteInferenceModelAction.Request request = (DeleteInferenceModelAction.Request) o;
            return Objects.equals(inferenceEntityId, request.inferenceEntityId) && taskType == request.taskType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceEntityId, taskType);
        }
    }
}

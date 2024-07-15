/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class DeleteInferenceEndpointAction extends ActionType<DeleteInferenceEndpointAction.Response> {

    public static final DeleteInferenceEndpointAction INSTANCE = new DeleteInferenceEndpointAction();
    public static final String NAME = "cluster:admin/xpack/inference/delete";

    public DeleteInferenceEndpointAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<DeleteInferenceEndpointAction.Request> {

        private final String inferenceEndpointId;
        private final TaskType taskType;
        private final boolean forceDelete;
        private final boolean dryRun;

        public Request(String inferenceEndpointId, TaskType taskType, boolean forceDelete, boolean dryRun) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            this.inferenceEndpointId = inferenceEndpointId;
            this.taskType = taskType;
            this.forceDelete = forceDelete;
            this.dryRun = dryRun;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEndpointId = in.readString();
            this.taskType = TaskType.fromStream(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_ENHANCE_DELETE_ENDPOINT)) {
                this.forceDelete = Boolean.TRUE.equals(in.readOptionalBoolean());
                this.dryRun = Boolean.TRUE.equals(in.readOptionalBoolean());
            } else {
                this.forceDelete = false;
                this.dryRun = false;
            }
        }

        public String getInferenceEndpointId() {
            return inferenceEndpointId;
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public boolean isForceDelete() {
            return forceDelete;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEndpointId);
            taskType.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_ENHANCE_DELETE_ENDPOINT)) {
                out.writeOptionalBoolean(forceDelete);
                out.writeOptionalBoolean(dryRun);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteInferenceEndpointAction.Request request = (DeleteInferenceEndpointAction.Request) o;
            return Objects.equals(inferenceEndpointId, request.inferenceEndpointId)
                && taskType == request.taskType
                && forceDelete == request.forceDelete
                && dryRun == request.dryRun;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceEndpointId, taskType, forceDelete, dryRun);
        }
    }

    public static class Response extends AcknowledgedResponse {

        private final String PIPELINE_IDS = "pipelines";
        Set<String> pipelineIds;
        private final String REFERENCED_INDEXES = "indexes";
        Set<String> indexes;
        private final String DRY_RUN_MESSAGE = "error_message"; // error message only returned in response for dry_run
        String dryRunMessage;

        public Response(boolean acknowledged, Set<String> pipelineIds, Set<String> semanticTextIndexes, @Nullable String dryRunMessage) {
            super(acknowledged);
            this.pipelineIds = pipelineIds;
            this.indexes = semanticTextIndexes;
            this.dryRunMessage = dryRunMessage;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_ENHANCE_DELETE_ENDPOINT)) {
                pipelineIds = in.readCollectionAsSet(StreamInput::readString);
            } else {
                pipelineIds = Set.of();
            }

            if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_DONT_DELETE_WHEN_SEMANTIC_TEXT_EXISTS)) {
                indexes = in.readCollectionAsSet(StreamInput::readString);
                dryRunMessage = in.readOptionalString();
            } else {
                indexes = Set.of();
                dryRunMessage = null;
            }

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_ENHANCE_DELETE_ENDPOINT)) {
                out.writeCollection(pipelineIds, StreamOutput::writeString);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_DONT_DELETE_WHEN_SEMANTIC_TEXT_EXISTS)) {
                out.writeCollection(indexes, StreamOutput::writeString);
                out.writeOptionalString(dryRunMessage);
            }
        }

        @Override
        protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
            super.addCustomFields(builder, params);
            builder.field(PIPELINE_IDS, pipelineIds);
            builder.field(REFERENCED_INDEXES, indexes);
            if (dryRunMessage != null) {
                builder.field(DRY_RUN_MESSAGE, dryRunMessage);
            }
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}

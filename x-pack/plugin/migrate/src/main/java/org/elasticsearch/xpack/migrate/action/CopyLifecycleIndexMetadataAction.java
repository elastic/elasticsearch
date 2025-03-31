/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class CopyLifecycleIndexMetadataAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "indices:admin/index/copy_lifecycle_index_metadata";

    public static final ActionType<AcknowledgedResponse> INSTANCE = new CopyLifecycleIndexMetadataAction();

    private CopyLifecycleIndexMetadataAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {
        private final String sourceIndex;
        private final String destIndex;

        public Request(TimeValue masterNodeTimeout, String sourceIndex, String destIndex) {
            super(masterNodeTimeout, DEFAULT_ACK_TIMEOUT);
            this.sourceIndex = sourceIndex;
            this.destIndex = destIndex;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.destIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(destIndex);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String sourceIndex() {
            return sourceIndex;
        }

        public String destIndex() {
            return destIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(sourceIndex, request.sourceIndex) && Objects.equals(destIndex, request.destIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, destIndex);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "copying lifecycle metadata for index " + sourceIndex;
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex, destIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }
}

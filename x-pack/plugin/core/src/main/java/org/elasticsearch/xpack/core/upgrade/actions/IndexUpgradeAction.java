/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.upgrade.actions;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.upgrade.IndexUpgradeServiceFields.UPGRADE_INDEX_OPTIONS;

public class IndexUpgradeAction extends ActionType<BulkByScrollResponse> {

    public static final IndexUpgradeAction INSTANCE = new IndexUpgradeAction();
    public static final String NAME = "cluster:admin/xpack/upgrade";

    private IndexUpgradeAction() {
        super(NAME, BulkByScrollResponse::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest {

        private String index = null;

        /**
         * Should this task store its result?
         */
        private boolean shouldStoreResult;

        // for serialization
        public Request() {

        }

        public Request(String index) {
            this.index = index;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        public String index() {
            return index;
        }

        /**
         * Sets the index.
         */
        public final Request index(String index) {
            this.index = index;
            return this;
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return UPGRADE_INDEX_OPTIONS;
        }

        /**
         * Should this task store its result after it has finished?
         */
        public Request setShouldStoreResult(boolean shouldStoreResult) {
            this.shouldStoreResult = shouldStoreResult;
            return this;
        }

        @Override
        public boolean getShouldStoreResult() {
            return shouldStoreResult;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (index == null) {
                validationException = addValidationError("index is missing", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(index, request.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, BulkByScrollResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }

        public RequestBuilder setIndex(String index) {
            request.index(index);
            return this;
        }
    }

}
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.upgrade.IndexUpgradeService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.upgrade.IndexUpgradeService.UPGRADE_INDEX_OPTIONS;

public class IndexUpgradeAction extends Action<IndexUpgradeAction.Request, BulkByScrollResponse,
        IndexUpgradeAction.RequestBuilder> {

    public static final IndexUpgradeAction INSTANCE = new IndexUpgradeAction();
    public static final String NAME = "cluster:admin/xpack/upgrade";

    private IndexUpgradeAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public BulkByScrollResponse newResponse() {
        return new BulkByScrollResponse();
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

        public String index() {
            return index;
        }

        /**
         * Sets the index.
         */
        @SuppressWarnings("unchecked")
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
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
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
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, BulkByScrollResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, IndexUpgradeAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder setIndex(String index) {
            request.index(index);
            return this;
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, BulkByScrollResponse> {

        private final IndexUpgradeService indexUpgradeService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexUpgradeService indexUpgradeService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, IndexUpgradeAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.indexUpgradeService = indexUpgradeService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected BulkByScrollResponse newResponse() {
            return new BulkByScrollResponse();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        @Override
        protected final void masterOperation(Task task, Request request, ClusterState state,
                                             ActionListener<BulkByScrollResponse> listener) {
            TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
            indexUpgradeService.upgrade(taskId, request.index(), state, listener);
        }

        @Override
        protected final void masterOperation(Request request, ClusterState state, ActionListener<BulkByScrollResponse> listener) {
            throw new UnsupportedOperationException("the task parameter is required");
        }

    }
}
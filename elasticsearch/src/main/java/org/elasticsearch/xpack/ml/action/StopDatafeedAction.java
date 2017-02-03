/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.xpack.persistent.RemovePersistentTaskAction;

import java.io.IOException;
import java.util.Objects;

public class StopDatafeedAction
        extends Action<StopDatafeedAction.Request, RemovePersistentTaskAction.Response, StopDatafeedAction.RequestBuilder> {

    public static final StopDatafeedAction INSTANCE = new StopDatafeedAction();
    public static final String NAME = "cluster:admin/ml/datafeeds/stop";

    private StopDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public RemovePersistentTaskAction.Response newResponse() {
        return new RemovePersistentTaskAction.Response();
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String datafeedId;

        public Request(String jobId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(jobId, DatafeedConfig.ID.getPreferredName());
        }

        Request() {
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, RemovePersistentTaskAction.Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, StopDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, RemovePersistentTaskAction.Response> {

        private final RemovePersistentTaskAction.TransportAction removePersistentTaskAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, RemovePersistentTaskAction.TransportAction removePersistentTaskAction) {
            super(settings, StopDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.removePersistentTaskAction = removePersistentTaskAction;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected RemovePersistentTaskAction.Response newResponse() {
            return new RemovePersistentTaskAction.Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state,
                                       ActionListener<RemovePersistentTaskAction.Response> listener) throws Exception {
            String datafeedId = request.getDatafeedId();
            MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
            validate(datafeedId, mlMetadata);

            PersistentTasksInProgress tasksInProgress = state.custom(PersistentTasksInProgress.TYPE);
            if (tasksInProgress != null) {
                for (PersistentTaskInProgress<?> taskInProgress : tasksInProgress.findEntries(StartDatafeedAction.NAME, p -> true)) {
                    StartDatafeedAction.Request storedRequest = (StartDatafeedAction.Request) taskInProgress.getRequest();
                    if (storedRequest.getDatafeedId().equals(datafeedId)) {
                        RemovePersistentTaskAction.Request cancelTasksRequest = new RemovePersistentTaskAction.Request();
                        cancelTasksRequest.setTaskId(taskInProgress.getId());
                        removePersistentTaskAction.execute(cancelTasksRequest, listener);
                        return;
                    }
                }
            }
            listener.onFailure(new ElasticsearchStatusException("datafeed already stopped, expected datafeed state [{}], but got [{}]",
                    RestStatus.CONFLICT, DatafeedState.STARTED, DatafeedState.STOPPED));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

    }

    static void validate(String datafeedId, MlMetadata mlMetadata) {
        DatafeedConfig datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw new ResourceNotFoundException(Messages.getMessage(Messages.DATAFEED_NOT_FOUND, datafeedId));
        }
    }
}

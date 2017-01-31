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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.datafeed.Datafeed;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedStatus;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.utils.DatafeedStatusObserver;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class StopDatafeedAction
        extends Action<StopDatafeedAction.Request, StopDatafeedAction.Response, StopDatafeedAction.RequestBuilder> {

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
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        private String datafeedId;
        private TimeValue stopTimeout = TimeValue.timeValueSeconds(20);

        public Request(String jobId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(jobId, DatafeedConfig.ID.getPreferredName());
        }

        Request() {
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public void setStopTimeout(TimeValue stopTimeout) {
            this.stopTimeout = stopTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
            stopTimeout = TimeValue.timeValueMillis(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeVLong(stopTimeout.millis());
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, stopTimeout);
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
            return Objects.equals(datafeedId, other.datafeedId) &&
                    Objects.equals(stopTimeout, other.stopTimeout);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, StopDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        private Response() {
            super(true);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final TransportListTasksAction listTasksAction;
        private final TransportCancelTasksAction cancelTasksAction;
        private final DatafeedStatusObserver datafeedStatusObserver;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, TransportCancelTasksAction cancelTasksAction,
                               TransportListTasksAction listTasksAction) {
            super(settings, StopDatafeedAction.NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.clusterService = clusterService;
            this.listTasksAction = listTasksAction;
            this.cancelTasksAction = cancelTasksAction;
            this.datafeedStatusObserver = new DatafeedStatusObserver(threadPool, clusterService);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            String datafeedId = request.getDatafeedId();
            MlMetadata mlMetadata = clusterService.state().metaData().custom(MlMetadata.TYPE);
            validate(datafeedId, mlMetadata);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setActions(InternalStartDatafeedAction.NAME);
            listTasksRequest.setDetailed(true);
            listTasksAction.execute(listTasksRequest, new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse listTasksResponse) {
                    String expectedJobDescription = "datafeed-" + datafeedId;
                    for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                        if (expectedJobDescription.equals(taskInfo.getDescription())) {
                            CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                            cancelTasksRequest.setTaskId(taskInfo.getTaskId());
                            cancelTasksAction.execute(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
                                @Override
                                public void onResponse(CancelTasksResponse cancelTasksResponse) {
                                    datafeedStatusObserver.waitForStatus(datafeedId, request.stopTimeout, DatafeedStatus.STOPPED, e -> {
                                        if (e != null) {
                                            listener.onFailure(e);
                                        } else {
                                            listener.onResponse(new Response());
                                        }
                                    });
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }
                            });
                            return;
                        }
                    }
                    listener.onFailure(new ResourceNotFoundException("No datafeed [" + datafeedId + "] running"));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }


    }

    static void validate(String datafeedId, MlMetadata mlMetadata) {
        Datafeed datafeed = mlMetadata.getDatafeed(datafeedId);
        if (datafeed == null) {
            throw new ResourceNotFoundException(Messages.getMessage(Messages.DATAFEED_NOT_FOUND, datafeedId));
        }

        if (datafeed.getStatus() == DatafeedStatus.STOPPED) {
            throw new ElasticsearchStatusException("datafeed already stopped, expected datafeed status [{}], but got [{}]",
                    RestStatus.CONFLICT, DatafeedStatus.STARTED, datafeed.getStatus());
        }
    }
}

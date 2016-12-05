/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class PostDataCloseAction extends Action<PostDataCloseAction.Request, PostDataCloseAction.Response,
        PostDataCloseAction.RequestBuilder> {

    public static final PostDataCloseAction INSTANCE = new PostDataCloseAction();
    public static final String NAME = "cluster:admin/prelert/data/post/close";

    private PostDataCloseAction() {
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

    public static class Request extends AcknowledgedRequest<Request> {

        private String jobId;

        Request() {}

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId);
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
            return Objects.equals(jobId, other.jobId);
        }
    }

    static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, PostDataCloseAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        private Response() {
        }

        private Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final JobManager jobManager;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               JobManager jobManager) {
            super(settings, PostDataCloseAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.jobManager = jobManager;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            UpdateJobStatusAction.Request updateStatusRequest = new UpdateJobStatusAction.Request(request.getJobId(), JobStatus.CLOSING);
            ActionListener<UpdateJobStatusAction.Response> delegateListener = new ActionListener<UpdateJobStatusAction.Response>() {

                @Override
                public void onResponse(UpdateJobStatusAction.Response response) {
                    respondWhenJobIsClosed(request.getJobId(), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };

            jobManager.setJobStatus(updateStatusRequest, delegateListener);
        }

        private void respondWhenJobIsClosed(String jobId, ActionListener<Response> listener) {
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(new Response(true));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new IllegalStateException("Cluster service closed while waiting for job [" + jobId
                            + "] status to change to [" + JobStatus.CLOSED + "]"));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new IllegalStateException(
                            "Timeout expired while waiting for job [" + jobId + "] status to change to [" + JobStatus.CLOSED + "]"));
                }
            }, new JobClosedChangePredicate(jobId), TimeValue.timeValueMinutes(30));
        }

        private class JobClosedChangePredicate implements ClusterStateObserver.ChangePredicate {

            private final String jobId;

            JobClosedChangePredicate(String jobId) {
                this.jobId = jobId;
            }

            @Override
            public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus, ClusterState newState,
                                 ClusterState.ClusterStateStatus newStatus) {
                return apply(newState);
            }

            @Override
            public boolean apply(ClusterChangedEvent changedEvent) {
                return apply(changedEvent.state());
            }

            boolean apply(ClusterState newState) {
                PrelertMetadata metadata = newState.getMetaData().custom(PrelertMetadata.TYPE);
                if (metadata != null) {
                    Allocation allocation = metadata.getAllocations().get(jobId);
                    return allocation != null && allocation.getStatus() == JobStatus.CLOSED;
                }
                return false;
            }
        }
        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

    }
}


/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
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

public class OpenJobAction extends Action<OpenJobAction.Request, OpenJobAction.Response, OpenJobAction.RequestBuilder> {

    public static final OpenJobAction INSTANCE = new OpenJobAction();
    public static final String NAME = "cluster:admin/prelert/job/open";

    private OpenJobAction() {
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

        public static final ParseField OPEN_TIMEOUT = new ParseField("open_timeout");

        private String jobId;
        private boolean ignoreDowntime;
        private TimeValue openTimeout = TimeValue.timeValueMinutes(30);

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

       Request() {}

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public boolean isIgnoreDowntime() {
            return ignoreDowntime;
        }

        public void setIgnoreDowntime(boolean ignoreDowntime) {
            this.ignoreDowntime = ignoreDowntime;
        }

        public TimeValue getOpenTimeout() {
            return openTimeout;
        }

        public void setOpenTimeout(TimeValue openTimeout) {
            this.openTimeout = openTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            ignoreDowntime = in.readBoolean();
            openTimeout = new TimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(ignoreDowntime);
            openTimeout.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, ignoreDowntime, openTimeout);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenJobAction.Request other = (OpenJobAction.Request) obj;
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(ignoreDowntime, other.ignoreDowntime) &&
                    Objects.equals(openTimeout, other.openTimeout);
        }
    }

    static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, OpenJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        private Response() {}

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
            super(settings, OpenJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
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
            ActionListener<Response> delegateListener = ActionListener.wrap(response -> respondWhenJobIsOpened(request, listener),
                    listener::onFailure);
            jobManager.openJob(request, delegateListener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        private void respondWhenJobIsOpened(Request request, ActionListener<Response> listener) {
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    String jobId = request.getJobId();
                    PrelertMetadata metadata = state.getMetaData().custom(PrelertMetadata.TYPE);
                    Allocation allocation = metadata.getAllocations().get(jobId);
                    if (allocation != null) {
                        if (allocation.getStatus() == JobStatus.OPENED) {
                            listener.onResponse(new Response(true));
                        } else {
                            String message = "[" +  jobId + "] expected job status [" + JobStatus.OPENED + "], but got [" +
                                    allocation.getStatus() + "], reason [" + allocation.getStatusReason() + "]";
                            listener.onFailure(new ElasticsearchStatusException(message, RestStatus.CONFLICT));
                        }
                    } else {
                        listener.onFailure(new IllegalStateException("no allocation for job [" + jobId + "]"));
                    }
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new IllegalStateException("Cluster service closed while waiting for job [" + request
                            + "] status to change to [" + JobStatus.OPENED + "]"));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new IllegalStateException(
                            "Timeout expired while waiting for job [" + request + "] status to change to [" + JobStatus.OPENED + "]"));
                }
            }, new JobOpenedChangePredicate(request.getJobId()), request.openTimeout);
        }

        private class JobOpenedChangePredicate implements ClusterStateObserver.ChangePredicate {

            private final String jobId;

            JobOpenedChangePredicate(String jobId) {
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
                    if (allocation != null) {
                        return allocation.getStatus().isAnyOf(JobStatus.OPENED, JobStatus.FAILED);
                    }
                }
                return false;
            }
        }
    }
}

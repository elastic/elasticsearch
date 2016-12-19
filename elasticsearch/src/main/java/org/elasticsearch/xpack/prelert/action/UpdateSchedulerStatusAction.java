/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.prelert.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class UpdateSchedulerStatusAction extends Action<UpdateSchedulerStatusAction.Request,
        UpdateSchedulerStatusAction.Response, UpdateSchedulerStatusAction.RequestBuilder> {

    public static final UpdateSchedulerStatusAction INSTANCE = new UpdateSchedulerStatusAction();
    public static final String NAME = "cluster:admin/prelert/scheduler/status/update";

    private UpdateSchedulerStatusAction() {
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

        private String schedulerId;
        private SchedulerStatus schedulerStatus;

        public Request(String schedulerId, SchedulerStatus schedulerStatus) {
            this.schedulerId = ExceptionsHelper.requireNonNull(schedulerId, SchedulerConfig.ID.getPreferredName());
            this.schedulerStatus = ExceptionsHelper.requireNonNull(schedulerStatus, "status");
        }

        Request() {}

        public String getSchedulerId() {
            return schedulerId;
        }

        public void setSchedulerId(String schedulerId) {
            this.schedulerId = schedulerId;
        }

        public SchedulerStatus getSchedulerStatus() {
            return schedulerStatus;
        }

        public void setSchedulerStatus(SchedulerStatus schedulerStatus) {
            this.schedulerStatus = schedulerStatus;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            schedulerId = in.readString();
            schedulerStatus = SchedulerStatus.fromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(schedulerId);
            schedulerStatus.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schedulerId, schedulerStatus);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            UpdateSchedulerStatusAction.Request other = (UpdateSchedulerStatusAction.Request) obj;
            return Objects.equals(schedulerId, other.schedulerId) && Objects.equals(schedulerStatus, other.schedulerStatus);
        }

        @Override
        public String toString() {
            return "Request{" +
                    SchedulerConfig.ID.getPreferredName() + "='" + schedulerId + "', " +
                    "status=" + schedulerStatus +
                    '}';
        }
    }

    static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, UpdateSchedulerStatusAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        public Response() {}

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
            super(settings, UpdateSchedulerStatusAction.NAME, transportService, clusterService, threadPool, actionFilters,
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
            jobManager.updateSchedulerStatus(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}

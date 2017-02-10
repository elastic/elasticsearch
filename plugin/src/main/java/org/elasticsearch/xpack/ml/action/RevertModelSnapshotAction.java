/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class RevertModelSnapshotAction
extends Action<RevertModelSnapshotAction.Request, RevertModelSnapshotAction.Response, RevertModelSnapshotAction.RequestBuilder> {

    public static final RevertModelSnapshotAction INSTANCE = new RevertModelSnapshotAction();
    public static final String NAME = "cluster:admin/ml/anomaly_detectors/model_snapshots/revert";

    private RevertModelSnapshotAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContent {

        public static final ParseField SNAPSHOT_ID = new ParseField("snapshot_id");
        public static final ParseField DELETE_INTERVENING = new ParseField("delete_intervening_results");

        private static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString((request, snapshotId) -> request.snapshotId = snapshotId, SNAPSHOT_ID);
            PARSER.declareBoolean(Request::setDeleteInterveningResults, DELETE_INTERVENING);
        }

        public static Request parseRequest(String jobId, String snapshotId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            if (snapshotId != null) {
                request.snapshotId = snapshotId;
            }
            return request;
        }

        private String jobId;
        private String snapshotId;
        private boolean deleteInterveningResults;

        Request() {
        }

        public Request(String jobId, String snapshotId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, SNAPSHOT_ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public String getSnapshotId() {
            return snapshotId;
        }

        public boolean getDeleteInterveningResults() {
            return deleteInterveningResults;
        }

        public void setDeleteInterveningResults(boolean deleteInterveningResults) {
            this.deleteInterveningResults = deleteInterveningResults;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            snapshotId = in.readString();
            deleteInterveningResults = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(snapshotId);
            out.writeBoolean(deleteInterveningResults);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(SNAPSHOT_ID.getPreferredName(), snapshotId);
            builder.field(DELETE_INTERVENING.getPreferredName(), deleteInterveningResults);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, snapshotId, deleteInterveningResults);
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
            return Objects.equals(jobId, other.jobId) && Objects.equals(snapshotId, other.snapshotId)
                    && Objects.equals(deleteInterveningResults, other.deleteInterveningResults);
        }
    }

    static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements StatusToXContentObject {

        private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
        private static final ParseField MODEL = new ParseField("model");
        private ModelSnapshot model;

        Response() {

        }

        public Response(ModelSnapshot modelSnapshot) {
            super(true);
            model = modelSnapshot;
        }

        public ModelSnapshot getModel() {
            return model;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            model = new ModelSnapshot(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            model.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ACKNOWLEDGED.getPreferredName(), true);
            builder.field(MODEL.getPreferredName());
            builder = model.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(model);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(model, other.model);
        }

        @Override
        public final String toString() {
            return Strings.toString(this);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final Client client;
        private final JobManager jobManager;
        private final JobProvider jobProvider;
        private final JobDataCountsPersister jobDataCountsPersister;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver, JobManager jobManager, JobProvider jobProvider,
                ClusterService clusterService, Client client, JobDataCountsPersister jobDataCountsPersister) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.jobManager = jobManager;
            this.jobProvider = jobProvider;
            this.jobDataCountsPersister = jobDataCountsPersister;
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
            logger.debug("Received request to revert to snapshot id '{}' for job '{}', deleting intervening results: {}",
                    request.getSnapshotId(), request.getJobId(), request.getDeleteInterveningResults());

            QueryPage<Job> job = jobManager.getJob(request.getJobId(), clusterService.state());
            JobState jobState = jobManager.getJobState(request.getJobId());
            if (job.count() > 0 && jobState.equals(JobState.CLOSED) == false) {
                throw ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_REVERT));
            }

            getModelSnapshot(request, jobProvider, modelSnapshot -> {
                ActionListener<Response> wrappedListener = listener;
                if (request.getDeleteInterveningResults()) {
                    wrappedListener = wrapDeleteOldDataListener(wrappedListener, modelSnapshot, request.getJobId());
                    wrappedListener = wrapRevertDataCountsListener(wrappedListener, modelSnapshot, request.getJobId());
                }
                jobManager.revertSnapshot(request, wrappedListener, modelSnapshot);
            }, listener::onFailure);
        }

        private void getModelSnapshot(Request request, JobProvider provider, Consumer<ModelSnapshot> handler,
                                      Consumer<Exception> errorHandler) {
            logger.info("Reverting to snapshot '" + request.getSnapshotId() + "'");

            provider.modelSnapshots(request.getJobId(), 0, 1, null, null,
                    ModelSnapshot.TIMESTAMP.getPreferredName(), true, request.getSnapshotId(), request.getDescription(),
                    page -> {
                        List<ModelSnapshot> revertCandidates = page.results();
                        if (revertCandidates == null || revertCandidates.isEmpty()) {
                            throw new ResourceNotFoundException(
                                    Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getJobId()));
                        }
                        ModelSnapshot modelSnapshot = revertCandidates.get(0);

                        // The quantiles can be large, and totally dominate the output -
                        // it's clearer to remove them
                        modelSnapshot.setQuantiles(null);
                        handler.accept(modelSnapshot);
                    }, errorHandler);
        }

        private ActionListener<RevertModelSnapshotAction.Response> wrapDeleteOldDataListener(
                ActionListener<RevertModelSnapshotAction.Response> listener,
                ModelSnapshot modelSnapshot, String jobId) {

            // If we need to delete buckets that occurred after the snapshot, we
            // wrap the listener with one that invokes the OldDataRemover on
            // acknowledged responses
            return ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    Date deleteAfter = modelSnapshot.getLatestResultTimeStamp();
                    logger.debug("Removing intervening records: last record: " + deleteAfter + ", last result: "
                            + modelSnapshot.getLatestResultTimeStamp());

                    logger.info("Deleting results after '" + deleteAfter + "'");

                    // NORELEASE: JobDataDeleter is basically delete-by-query.
                    // We should replace this whole abstraction with DBQ eventually
                    JobDataDeleter dataDeleter = new JobDataDeleter(client, jobId);
                    dataDeleter.deleteResultsFromTime(deleteAfter.getTime() + 1, new ActionListener<Boolean>() {
                        @Override
                        public void onResponse(Boolean success) {
                            dataDeleter.commit(ActionListener.wrap(
                                    bulkItemResponses -> {listener.onResponse(response);},
                                    listener::onFailure));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                }
            }, listener::onFailure);
        }

        private ActionListener<RevertModelSnapshotAction.Response> wrapRevertDataCountsListener(
                ActionListener<RevertModelSnapshotAction.Response> listener,
                ModelSnapshot modelSnapshot, String jobId) {


            return ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    jobProvider.dataCounts(jobId, counts -> {
                        counts.setLatestRecordTimeStamp(modelSnapshot.getLatestRecordTimeStamp());
                        jobDataCountsPersister.persistDataCounts(jobId, counts, new ActionListener<Boolean>() {
                            @Override
                            public void onResponse(Boolean aBoolean) {
                                listener.onResponse(response);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
                    }, listener::onFailure);
                }
            }, listener::onFailure);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}

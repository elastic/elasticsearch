/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DeleteModelSnapshotAction extends Action<DeleteModelSnapshotAction.Request,
        DeleteModelSnapshotAction.Response, DeleteModelSnapshotAction.RequestBuilder> {

    public static final DeleteModelSnapshotAction INSTANCE = new DeleteModelSnapshotAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/model_snapshots/delete";

    private DeleteModelSnapshotAction() {
        super(NAME);
    }

    @Override
    public DeleteModelSnapshotAction.RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public DeleteModelSnapshotAction.Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest {

        private String jobId;
        private String snapshotId;

        private Request() {
        }

        public Request(String jobId, String snapshotId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, ModelSnapshot.SNAPSHOT_ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public String getSnapshotId() {
            return snapshotId;
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
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(snapshotId);
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

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteModelSnapshotAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Client client;
        private final JobProvider jobProvider;
        private final JobManager jobManager;
        private final ClusterService clusterService;
        private final Auditor auditor;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               JobProvider jobProvider, JobManager jobManager, ClusterService clusterService,
                               Client client, Auditor auditor) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.jobProvider = jobProvider;
            this.jobManager = jobManager;
            this.clusterService = clusterService;
            this.auditor = auditor;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            // Verify the snapshot exists
            jobProvider.modelSnapshots(
                    request.getJobId(), 0, 1, null, null, null, true, request.getSnapshotId(),
                    page -> {
                        List<ModelSnapshot> deleteCandidates = page.results();
                        if (deleteCandidates.size() > 1) {
                            logger.warn("More than one model found for [job_id: " + request.getJobId()
                                    + ", snapshot_id: " + request.getSnapshotId() + "] tuple.");
                        }

                        if (deleteCandidates.isEmpty()) {
                            listener.onFailure(new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT,
                                    request.getSnapshotId(), request.getJobId())));
                            return;
                        }
                        ModelSnapshot deleteCandidate = deleteCandidates.get(0);

                        // Verify the snapshot is not being used
                        Job job = JobManager.getJobOrThrowIfUnknown(request.getJobId(), clusterService.state());
                        String currentModelInUse = job.getModelSnapshotId();
                        if (currentModelInUse != null && currentModelInUse.equals(request.getSnapshotId())) {
                            throw new IllegalArgumentException(Messages.getMessage(Messages.REST_CANNOT_DELETE_HIGHEST_PRIORITY,
                                    request.getSnapshotId(), request.getJobId()));
                        }

                        // Delete the snapshot and any associated state files
                        JobDataDeleter deleter = new JobDataDeleter(client, request.getJobId());
                        deleter.deleteModelSnapshots(Collections.singletonList(deleteCandidate), new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse bulkResponse) {
                                String msg = Messages.getMessage(Messages.JOB_AUDIT_SNAPSHOT_DELETED, deleteCandidate.getSnapshotId(),
                                        deleteCandidate.getDescription());
                                auditor.info(request.getJobId(), msg);
                                logger.debug("[{}] {}", request.getJobId(), msg);
                                // We don't care about the bulk response, just that it succeeded
                                listener.onResponse(new DeleteModelSnapshotAction.Response(true));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });

                    }, listener::onFailure);
        }
    }
}

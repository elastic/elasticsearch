/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;


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
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchBulkDeleter;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchBulkDeleterFactory;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchJobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.JobProvider;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;

public class DeleteModelSnapshotAction extends Action<DeleteModelSnapshotAction.Request,
        DeleteModelSnapshotAction.Response, DeleteModelSnapshotAction.RequestBuilder> {

    public static final DeleteModelSnapshotAction INSTANCE = new DeleteModelSnapshotAction();
    public static final String NAME = "cluster:admin/prelert/modelsnapshots/delete";

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
            this.jobId = ExceptionsHelper.requireNonNull(jobId, "jobId");
            this.snapshotId = ExceptionsHelper.requireNonNull(snapshotId, "snapshotId");
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

        private final JobProvider jobProvider;
        private final JobManager jobManager;
        private final ClusterService clusterService;
        private final ElasticsearchBulkDeleterFactory bulkDeleterFactory;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ElasticsearchJobProvider jobProvider, JobManager jobManager, ClusterService clusterService,
                               ElasticsearchBulkDeleterFactory bulkDeleterFactory) {
            super(settings, NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.jobProvider = jobProvider;
            this.jobManager = jobManager;
            this.clusterService = clusterService;
            this.bulkDeleterFactory = bulkDeleterFactory;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {

            // Verify the snapshot exists
            List<ModelSnapshot> deleteCandidates;
            deleteCandidates = jobProvider.modelSnapshots(
                    request.getJobId(), 0, 1, null, null, null, true, request.getSnapshotId(), null
            ).results();

            if (deleteCandidates.size() > 1) {
                logger.warn("More than one model found for [jobId: " + request.getJobId()
                        + ", snapshotId: " + request.getSnapshotId() + "] tuple.");
            }

            if (deleteCandidates.isEmpty()) {
                throw new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getJobId()));
            }
            ModelSnapshot deleteCandidate = deleteCandidates.get(0);

            // Verify the snapshot is not being used
            //
            // NORELEASE: technically, this could be stale and refuse a delete, but I think that's acceptable
            // since it is non-destructive
            QueryPage<Job> job = jobManager.getJob(request.getJobId(), clusterService.state());
            if (job.count() > 0) {
                String currentModelInUse = job.results().get(0).getModelSnapshotId();
                if (currentModelInUse != null && currentModelInUse.equals(request.getSnapshotId())) {
                    throw new IllegalArgumentException(Messages.getMessage(Messages.REST_CANNOT_DELETE_HIGHEST_PRIORITY,
                            request.getSnapshotId(), request.getJobId()));
                }
            }

            // Delete the snapshot and any associated state files
            ElasticsearchBulkDeleter deleter = bulkDeleterFactory.apply(request.getJobId());
            deleter.deleteModelSnapshot(deleteCandidate);
            deleter.commit(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    // We don't care about the bulk response, just that it succeeded
                    listener.onResponse(new DeleteModelSnapshotAction.Response(true));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

            jobManager.audit(request.getJobId()).info(Messages.getMessage(Messages.JOB_AUDIT_SNAPSHOT_DELETED,
                    deleteCandidate.getDescription()));
        }
    }
}

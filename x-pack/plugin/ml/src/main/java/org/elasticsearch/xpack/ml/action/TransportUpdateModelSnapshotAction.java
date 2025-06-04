/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportUpdateModelSnapshotAction extends HandledTransportAction<
    UpdateModelSnapshotAction.Request,
    UpdateModelSnapshotAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateModelSnapshotAction.class);

    private final JobResultsProvider jobResultsProvider;
    private final Client client;

    @Inject
    public TransportUpdateModelSnapshotAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        Client client
    ) {
        super(
            UpdateModelSnapshotAction.NAME,
            transportService,
            actionFilters,
            UpdateModelSnapshotAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.jobResultsProvider = jobResultsProvider;
        this.client = client;
    }

    @Override
    protected void doExecute(
        Task task,
        UpdateModelSnapshotAction.Request request,
        ActionListener<UpdateModelSnapshotAction.Response> listener
    ) {
        logger.debug("Received request to update model snapshot [{}] for job [{}]", request.getSnapshotId(), request.getJobId());
        // Even though the quantiles can be large we have to fetch them initially so that the updated document is complete
        jobResultsProvider.getModelSnapshot(request.getJobId(), request.getSnapshotId(), true, modelSnapshot -> {
            if (modelSnapshot == null) {
                listener.onFailure(
                    new ResourceNotFoundException(
                        Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getSnapshotId(), request.getJobId())
                    )
                );
            } else {
                Result<ModelSnapshot> updatedSnapshot = applyUpdate(request, modelSnapshot);
                indexModelSnapshot(updatedSnapshot, b -> {
                    // The quantiles can be large, and totally dominate the output - it's clearer to remove them at this stage
                    listener.onResponse(
                        new UpdateModelSnapshotAction.Response(new ModelSnapshot.Builder(updatedSnapshot.result).setQuantiles(null).build())
                    );
                }, listener::onFailure);
            }
        }, listener::onFailure);
    }

    private static Result<ModelSnapshot> applyUpdate(UpdateModelSnapshotAction.Request request, Result<ModelSnapshot> target) {
        ModelSnapshot.Builder updatedSnapshotBuilder = new ModelSnapshot.Builder(target.result);
        if (request.getDescription() != null) {
            updatedSnapshotBuilder.setDescription(request.getDescription());
        }
        if (request.getRetain() != null) {
            updatedSnapshotBuilder.setRetain(request.getRetain());
        }
        return new Result<>(target.index, updatedSnapshotBuilder.build());
    }

    private void indexModelSnapshot(Result<ModelSnapshot> modelSnapshot, Consumer<Boolean> handler, Consumer<Exception> errorHandler) {
        IndexRequest indexRequest = new IndexRequest(modelSnapshot.index).id(ModelSnapshot.documentId(modelSnapshot.result));
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            modelSnapshot.result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            indexRequest.source(builder);
        } catch (IOException e) {
            errorHandler.accept(e);
            return;
        }
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(indexRequest);
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            TransportBulkAction.TYPE,
            bulkRequestBuilder.request(),
            new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse indexResponse) {
                    handler.accept(true);
                }

                @Override
                public void onFailure(Exception e) {
                    errorHandler.accept(e);
                }
            }
        );
    }
}

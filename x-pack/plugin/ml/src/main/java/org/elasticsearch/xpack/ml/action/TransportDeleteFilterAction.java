/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteFilterAction extends HandledTransportAction<DeleteFilterAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final JobConfigProvider jobConfigProvider;

    @Inject
    public TransportDeleteFilterAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        JobConfigProvider jobConfigProvider
    ) {
        super(DeleteFilterAction.NAME, transportService, actionFilters, DeleteFilterAction.Request::new);
        this.client = client;
        this.jobConfigProvider = jobConfigProvider;
    }

    @Override
    protected void doExecute(Task task, DeleteFilterAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final String filterId = request.getFilterId();
        jobConfigProvider.findJobsWithCustomRules(listener.delegateFailureAndWrap((delegate, jobs) -> {
            List<String> currentlyUsedBy = findJobsUsingFilter(jobs, filterId);
            if (currentlyUsedBy.isEmpty() == false) {
                delegate.onFailure(
                    ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.FILTER_CANNOT_DELETE, filterId, currentlyUsedBy))
                );
            } else {
                deleteFilter(filterId, delegate);
            }
        }));
    }

    private static List<String> findJobsUsingFilter(List<Job> jobs, String filterId) {
        List<String> currentlyUsedBy = new ArrayList<>();
        for (Job job : jobs) {
            List<Detector> detectors = job.getAnalysisConfig().getDetectors();
            for (Detector detector : detectors) {
                if (detector.extractReferencedFilters().contains(filterId)) {
                    currentlyUsedBy.add(job.getId());
                    break;
                }
            }
        }
        return currentlyUsedBy;
    }

    private void deleteFilter(String filterId, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(MlMetaIndex.indexName(), MlFilter.documentId(filterId));
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(deleteRequest);
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(client, ML_ORIGIN, BulkAction.INSTANCE, bulkRequestBuilder.request(), new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                if (bulkResponse.getItems()[0].status() == RestStatus.NOT_FOUND) {
                    listener.onFailure(
                        new ResourceNotFoundException("Could not delete filter with ID [" + filterId + "] because it does not exist")
                    );
                } else {
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(ExceptionsHelper.serverError("Could not delete filter with ID [" + filterId + "]", e));
            }
        });
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteCalendarAction extends HandledTransportAction<DeleteCalendarAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;

    @Inject
    public TransportDeleteCalendarAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        JobManager jobManager,
        JobResultsProvider jobResultsProvider
    ) {
        super(DeleteCalendarAction.NAME, transportService, actionFilters, DeleteCalendarAction.Request::new);
        this.client = client;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
    }

    @Override
    protected void doExecute(Task task, DeleteCalendarAction.Request request, ActionListener<AcknowledgedResponse> listener) {

        final String calendarId = request.getCalendarId();

        ActionListener<Calendar> calendarListener = ActionListener.wrap(calendar -> {
            // Delete calendar and events
            DeleteByQueryRequest dbqRequest = buildDeleteByQuery(calendarId);
            executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, dbqRequest, ActionListener.wrap(response -> {
                if (response.getDeleted() == 0) {
                    listener.onFailure(new ResourceNotFoundException("No calendar with id [" + calendarId + "]"));
                    return;
                }

                jobManager.updateProcessOnCalendarChanged(
                    calendar.getJobIds(),
                    ActionListener.wrap(r -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
                );
            }, listener::onFailure));
        }, listener::onFailure);

        jobResultsProvider.calendar(calendarId, calendarListener);
    }

    private static DeleteByQueryRequest buildDeleteByQuery(String calendarId) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(MlMetaIndex.indexName());
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        request.setRefresh(true);

        QueryBuilder query = QueryBuilders.termsQuery(Calendar.ID.getPreferredName(), calendarId);
        request.setQuery(query);
        return request;
    }
}

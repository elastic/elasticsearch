/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteCalendarAction extends HandledTransportAction<DeleteCalendarAction.Request, DeleteCalendarAction.Response> {

    private final Client client;
    private final JobManager jobManager;
    private final JobProvider jobProvider;

    @Inject
    public TransportDeleteCalendarAction(Settings settings, TransportService transportService,
                                         ActionFilters actionFilters, Client client, JobManager jobManager, JobProvider jobProvider) {
        super(settings, DeleteCalendarAction.NAME, transportService, actionFilters,
            (Supplier<DeleteCalendarAction.Request>) DeleteCalendarAction.Request::new);
        this.client = client;
        this.jobManager = jobManager;
        this.jobProvider = jobProvider;
    }

    @Override
    protected void doExecute(DeleteCalendarAction.Request request, ActionListener<DeleteCalendarAction.Response> listener) {

        final String calendarId = request.getCalendarId();

        ActionListener<Calendar> calendarListener = ActionListener.wrap(
                calendar -> {
                    // Delete calendar and events
                    DeleteByQueryRequest dbqRequest = buildDeleteByQuery(calendarId);
                    executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, dbqRequest, ActionListener.wrap(
                            response -> {
                                if (response.getDeleted() == 0) {
                                    listener.onFailure(new ResourceNotFoundException("No calendar with id [" + calendarId + "]"));
                                    return;
                                }
                                jobManager.updateProcessOnCalendarChanged(calendar.getJobIds());
                                listener.onResponse(new DeleteCalendarAction.Response(true));
                            },
                            listener::onFailure));
                },
                listener::onFailure
        );

        jobProvider.calendar(calendarId, calendarListener);
    }

    private DeleteByQueryRequest buildDeleteByQuery(String calendarId) {
        SearchRequest searchRequest = new SearchRequest(MlMetaIndex.INDEX_NAME);
        // The DBQ request constructor wipes the search request source
        // so it has to be set after
        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        request.setSlices(5);
        request.setRefresh(true);

        QueryBuilder query = QueryBuilders.termsQuery(Calendar.ID.getPreferredName(), calendarId);
        searchRequest.source(new SearchSourceBuilder().query(query));
        return request;
    }
}

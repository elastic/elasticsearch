/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteCalendarEventAction extends HandledTransportAction<DeleteCalendarEventAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;

    @Inject
    public TransportDeleteCalendarEventAction(TransportService transportService, ActionFilters actionFilters,
                                              Client client, JobResultsProvider jobResultsProvider, JobManager jobManager) {
        super(DeleteCalendarEventAction.NAME, transportService, actionFilters, DeleteCalendarEventAction.Request::new);
        this.client = client;
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
    }

    @Override
    protected void doExecute(Task task, DeleteCalendarEventAction.Request request,
                             ActionListener<AcknowledgedResponse> listener) {
        final String eventId = request.getEventId();

        ActionListener<Calendar> calendarListener = ActionListener.wrap(
                calendar -> {
                    GetRequest getRequest = new GetRequest(MlMetaIndex.INDEX_NAME, eventId);
                    executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(
                            getResponse -> {
                                if (getResponse.isExists() == false) {
                                    listener.onFailure(new ResourceNotFoundException("No event with id [" + eventId + "]"));
                                    return;
                                }

                                Map<String, Object> source = getResponse.getSourceAsMap();
                                String calendarId = (String) source.get(Calendar.ID.getPreferredName());
                                if (calendarId == null) {
                                    listener.onFailure(ExceptionsHelper.badRequestException("Event [" + eventId + "] does not have a valid "
                                            + Calendar.ID.getPreferredName()));
                                    return;
                                }

                                if (calendarId.equals(request.getCalendarId()) == false) {
                                    listener.onFailure(ExceptionsHelper.badRequestException(
                                            "Event [" + eventId + "] has " + Calendar.ID.getPreferredName()
                                                    + " [" + calendarId + "] which does not match the request "
                                                    + Calendar.ID.getPreferredName() + " [" + request.getCalendarId() + "]"));
                                    return;
                                }

                                deleteEvent(eventId, calendar, listener);
                            }, listener::onFailure)
                    );
                }, listener::onFailure);

        // Get the calendar first so we check the calendar exists before checking the event exists
        jobResultsProvider.calendar(request.getCalendarId(), calendarListener);
    }

    private void deleteEvent(String eventId, Calendar calendar, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(MlMetaIndex.INDEX_NAME, eventId);
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, deleteRequest,
                new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse response) {

                        if (response.status() == RestStatus.NOT_FOUND) {
                            listener.onFailure(new ResourceNotFoundException("No event with id [" + eventId + "]"));
                        } else {
                            jobManager.updateProcessOnCalendarChanged(calendar.getJobIds(), ActionListener.wrap(
                                    r -> listener.onResponse(new AcknowledgedResponse(true)),
                                    listener::onFailure
                            ));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(ExceptionsHelper.serverError("Could not delete event [" + eventId + "]", e));
                    }
                });
    }
}

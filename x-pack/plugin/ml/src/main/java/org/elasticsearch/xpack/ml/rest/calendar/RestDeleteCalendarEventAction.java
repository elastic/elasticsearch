/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;
import static org.elasticsearch.xpack.ml.MachineLearning.PRE_V7_BASE_PATH;

public class RestDeleteCalendarEventAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(DELETE, BASE_PATH + "calendars/{" + Calendar.ID + "}/events/{" + ScheduledEvent.EVENT_ID + "}")
                .replaces(
                    DELETE,
                    PRE_V7_BASE_PATH + "calendars/{" + Calendar.ID + "}/events/{" + ScheduledEvent.EVENT_ID + "}",
                    RestApiVersion.V_7
                )
                .build()
        );
    }

    @Override
    public String getName() {
        return "ml_delete_calendar_event_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String eventId = restRequest.param(ScheduledEvent.EVENT_ID.getPreferredName());
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());
        DeleteCalendarEventAction.Request request = new DeleteCalendarEventAction.Request(calendarId, eventId);
        return channel -> client.execute(DeleteCalendarEventAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}

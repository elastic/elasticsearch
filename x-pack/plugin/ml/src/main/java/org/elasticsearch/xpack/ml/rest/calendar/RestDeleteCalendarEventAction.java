/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteCalendarEventAction extends BaseRestHandler {


    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(DELETE, MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/events/{" +
                ScheduledEvent.EVENT_ID.getPreferredName() + "}")
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

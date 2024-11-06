/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.ml.calendars.Calendar.ID;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestPostCalendarEventAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, BASE_PATH + "calendars/{" + ID + "}/events"));
    }

    @Override
    public String getName() {
        return "ml_post_calendar_event_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());

        XContentParser parser = restRequest.contentOrSourceParamParser();
        PostCalendarEventsAction.Request request = PostCalendarEventsAction.Request.parseRequest(calendarId, parser);
        return channel -> client.execute(PostCalendarEventsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}

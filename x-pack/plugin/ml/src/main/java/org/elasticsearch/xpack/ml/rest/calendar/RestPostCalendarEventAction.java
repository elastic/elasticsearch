/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;

import java.io.IOException;

public class RestPostCalendarEventAction extends BaseRestHandler {

    public RestPostCalendarEventAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/events", this);
        controller.registerHandler(RestRequest.Method.POST,
                MachineLearning.V7_BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/events", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_post_calendar_event_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());

        XContentParser parser = restRequest.contentOrSourceParamParser();
        PostCalendarEventsAction.Request request =
                PostCalendarEventsAction.Request.parseRequest(calendarId, parser);
        return channel -> client.execute(PostCalendarEventsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}

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
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestGetCalendarEventsAction extends BaseRestHandler {
    public RestGetCalendarEventsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/events", this);
        controller.registerHandler(RestRequest.Method.GET,
                MachineLearning.V7_BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/events", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_get_calendar_events_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());

        GetCalendarEventsAction.Request request;

        if (restRequest.hasContentOrSourceParam()) {
            try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
                request = GetCalendarEventsAction.Request.parseRequest(calendarId, parser);
            }
        } else {
            request = new GetCalendarEventsAction.Request(calendarId);
            request.setStart(restRequest.param(GetCalendarEventsAction.Request.START.getPreferredName(), null));
            request.setEnd(restRequest.param(GetCalendarEventsAction.Request.END.getPreferredName(), null));
            request.setJobId(restRequest.param(Job.ID.getPreferredName(), null));

            if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
                request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                        restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
            }
        }

        return channel -> client.execute(GetCalendarEventsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}

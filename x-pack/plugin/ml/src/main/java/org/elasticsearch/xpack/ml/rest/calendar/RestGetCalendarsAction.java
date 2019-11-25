/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestGetCalendarsAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestGetCalendarsAction.class));

    public RestGetCalendarsAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            GET, MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}", this,
            GET, MachineLearning.PRE_V7_BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}", deprecationLogger);
        controller.registerWithDeprecatedHandler(
            GET, MachineLearning.BASE_PATH + "calendars/", this,
            GET, MachineLearning.PRE_V7_BASE_PATH + "calendars/", deprecationLogger);

        // endpoints that support body parameters must also accept POST
        controller.registerWithDeprecatedHandler(
            POST, MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}", this,
            POST, MachineLearning.PRE_V7_BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}", deprecationLogger);
        controller.registerWithDeprecatedHandler(
            POST, MachineLearning.BASE_PATH + "calendars/", this,
            POST, MachineLearning.PRE_V7_BASE_PATH + "calendars/", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_get_calendars_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

        String calendarId = restRequest.param(Calendar.ID.getPreferredName());

        GetCalendarsAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            try (XContentParser parser = restRequest.contentOrSourceParamParser()) {
                request = GetCalendarsAction.Request.parseRequest(calendarId, parser);
            }
        } else  {
            request = new GetCalendarsAction.Request();
            if (!Strings.isNullOrEmpty(calendarId)) {
                request.setCalendarId(calendarId);
            }
            if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
                request.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                        restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
            }
        }

        return channel -> client.execute(GetCalendarsAction.INSTANCE, request, new RestStatusToXContentListener<>(channel));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.calendars.Calendar;

import java.io.IOException;

public class RestGetCalendarsAction extends BaseRestHandler {

    public RestGetCalendarsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}",
                this);
        controller.registerHandler(RestRequest.Method.GET, MachineLearning.BASE_PATH + "calendars/", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_get_calendars_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetCalendarsAction.Request getRequest = new GetCalendarsAction.Request();
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());
        if (!Strings.isNullOrEmpty(calendarId)) {
            getRequest.setCalendarId(calendarId);
        }

        if (restRequest.hasParam(PageParams.FROM.getPreferredName()) || restRequest.hasParam(PageParams.SIZE.getPreferredName())) {
            getRequest.setPageParams(new PageParams(restRequest.paramAsInt(PageParams.FROM.getPreferredName(), PageParams.DEFAULT_FROM),
                    restRequest.paramAsInt(PageParams.SIZE.getPreferredName(), PageParams.DEFAULT_SIZE)));
        }

        return channel -> client.execute(GetCalendarsAction.INSTANCE, getRequest, new RestStatusToXContentListener<>(channel));
    }
}

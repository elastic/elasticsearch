/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;

public class RestDeleteCalendarJobAction extends BaseRestHandler {

    public RestDeleteCalendarJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.DELETE,
                MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/jobs/{" +
                        Job.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.DELETE,
                MachineLearning.V7_BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/jobs/{" +
                        Job.ID.getPreferredName() + "}", this);
    }

    @Override
    public String getName() {
        return "xpack_ml_delete_calendar_job_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());
        String jobId = restRequest.param(Job.ID.getPreferredName());
        UpdateCalendarJobAction.Request request =
                new UpdateCalendarJobAction.Request(calendarId, null, jobId);
        return channel -> client.execute(UpdateCalendarJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}

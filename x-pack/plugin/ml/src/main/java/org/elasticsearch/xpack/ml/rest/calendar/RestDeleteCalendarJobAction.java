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
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteCalendarJobAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(DELETE, MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/jobs/{" +
                Job.ID.getPreferredName() + "}")
        );
    }

    @Override
    public String getName() {
        return "ml_delete_calendar_job_action";
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.calendar;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutCalendarJobAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(RestPutCalendarJobAction.class));

    public RestPutCalendarJobAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            PUT, MachineLearning.BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/jobs/{" +
                Job.ID.getPreferredName() + "}", this,
            PUT, MachineLearning.PRE_V7_BASE_PATH + "calendars/{" + Calendar.ID.getPreferredName() + "}/jobs/{" +
                Job.ID.getPreferredName() + "}", deprecationLogger);
    }

    @Override
    public String getName() {
        return "ml_put_calendar_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String calendarId = restRequest.param(Calendar.ID.getPreferredName());
        String jobId = restRequest.param(Job.ID.getPreferredName());
        UpdateCalendarJobAction.Request putCalendarRequest =
                new UpdateCalendarJobAction.Request(calendarId, jobId, null);
        return channel -> client.execute(UpdateCalendarJobAction.INSTANCE, putCalendarRequest, new RestToXContentListener<>(channel));
    }
}

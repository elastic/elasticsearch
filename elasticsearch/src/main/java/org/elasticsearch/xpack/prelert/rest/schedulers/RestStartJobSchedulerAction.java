/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.schedulers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.elasticsearch.xpack.prelert.action.StartJobSchedulerAction;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.messages.Messages;

import java.io.IOException;

public class RestStartJobSchedulerAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    private final StartJobSchedulerAction.TransportAction transportJobSchedulerAction;

    @Inject
    public RestStartJobSchedulerAction(Settings settings, RestController controller,
            StartJobSchedulerAction.TransportAction transportJobSchedulerAction) {
        super(settings);
        this.transportJobSchedulerAction = transportJobSchedulerAction;
        controller.registerHandler(RestRequest.Method.POST,
                PrelertPlugin.BASE_PATH + "schedulers/{" + Job.ID.getPreferredName() + "}/_start", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        StartJobSchedulerAction.Request jobSchedulerRequest;
        if (RestActions.hasBodyContent(restRequest)) {
            BytesReference bodyBytes = RestActions.getRestContent(restRequest);
            XContentParser parser = XContentFactory.xContent(bodyBytes).createParser(bodyBytes);
            jobSchedulerRequest = StartJobSchedulerAction.Request.parseRequest(jobId, parser, () -> parseFieldMatcher);
        } else {
            long startTimeMillis = parseDateOrThrow(restRequest.param(SchedulerState.START_TIME_MILLIS.getPreferredName(), DEFAULT_START),
                    SchedulerState.START_TIME_MILLIS.getPreferredName());
            Long endTimeMillis = null;
            if (restRequest.hasParam(SchedulerState.END_TIME_MILLIS.getPreferredName())) {
                endTimeMillis = parseDateOrThrow(restRequest.param(SchedulerState.END_TIME_MILLIS.getPreferredName()),
                        SchedulerState.END_TIME_MILLIS.getPreferredName());
            }
            SchedulerState schedulerState = new SchedulerState(JobSchedulerStatus.STARTING, startTimeMillis, endTimeMillis);
            jobSchedulerRequest = new StartJobSchedulerAction.Request(jobId, schedulerState);
        }
        return channel -> transportJobSchedulerAction.execute(jobSchedulerRequest, new AcknowledgedRestListener<>(channel));
    }

    static long parseDateOrThrow(String date, String paramName) {
        try {
            return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseMillis(date);
        } catch (IllegalArgumentException e) {
            String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName, date);
            throw new ElasticsearchParseException(msg, e);
        }
    }
}

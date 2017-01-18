/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.schedulers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.StartSchedulerAction;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;

import java.io.IOException;

public class RestStartSchedulerAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    @Inject
    public RestStartSchedulerAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
                MlPlugin.BASE_PATH + "schedulers/{" + SchedulerConfig.ID.getPreferredName() + "}/_start", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String schedulerId = restRequest.param(SchedulerConfig.ID.getPreferredName());
        StartSchedulerAction.Request jobSchedulerRequest;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            jobSchedulerRequest = StartSchedulerAction.Request.parseRequest(schedulerId, parser);
        } else {
            long startTimeMillis = parseDateOrThrow(restRequest.param(StartSchedulerAction.START_TIME.getPreferredName(),
                    DEFAULT_START), StartSchedulerAction.START_TIME.getPreferredName());
            Long endTimeMillis = null;
            if (restRequest.hasParam(StartSchedulerAction.END_TIME.getPreferredName())) {
                endTimeMillis = parseDateOrThrow(restRequest.param(StartSchedulerAction.END_TIME.getPreferredName()),
                        StartSchedulerAction.END_TIME.getPreferredName());
            }
            jobSchedulerRequest = new StartSchedulerAction.Request(schedulerId, startTimeMillis);
            jobSchedulerRequest.setEndTime(endTimeMillis);
            TimeValue startTimeout = restRequest.paramAsTime(StartSchedulerAction.START_TIMEOUT.getPreferredName(),
                    TimeValue.timeValueSeconds(30));
            jobSchedulerRequest.setStartTimeout(startTimeout);
        }
        return channel -> {
            client.execute(StartSchedulerAction.INSTANCE, jobSchedulerRequest, new RestToXContentListener<>(channel));
        };
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

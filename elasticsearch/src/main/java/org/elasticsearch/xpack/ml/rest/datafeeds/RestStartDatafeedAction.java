/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

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
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;

import java.io.IOException;

public class RestStartDatafeedAction extends BaseRestHandler {

    private static final String DEFAULT_START = "0";

    @Inject
    public RestStartDatafeedAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
                MlPlugin.BASE_PATH + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}/_start", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        StartDatafeedAction.Request jobDatafeedRequest;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            jobDatafeedRequest = StartDatafeedAction.Request.parseRequest(datafeedId, parser);
        } else {
            long startTimeMillis = parseDateOrThrow(restRequest.param(StartDatafeedAction.START_TIME.getPreferredName(),
                    DEFAULT_START), StartDatafeedAction.START_TIME.getPreferredName());
            Long endTimeMillis = null;
            if (restRequest.hasParam(StartDatafeedAction.END_TIME.getPreferredName())) {
                endTimeMillis = parseDateOrThrow(restRequest.param(StartDatafeedAction.END_TIME.getPreferredName()),
                        StartDatafeedAction.END_TIME.getPreferredName());
            }
            jobDatafeedRequest = new StartDatafeedAction.Request(datafeedId, startTimeMillis);
            jobDatafeedRequest.setEndTime(endTimeMillis);
            TimeValue startTimeout = restRequest.paramAsTime(StartDatafeedAction.START_TIMEOUT.getPreferredName(),
                    TimeValue.timeValueSeconds(30));
            jobDatafeedRequest.setStartTimeout(startTimeout);
        }
        return channel -> {
            client.execute(StartDatafeedAction.INSTANCE, jobDatafeedRequest, new RestToXContentListener<>(channel));
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

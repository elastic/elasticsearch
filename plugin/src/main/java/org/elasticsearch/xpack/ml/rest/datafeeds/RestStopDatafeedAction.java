/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;

import java.io.IOException;

public class RestStopDatafeedAction extends BaseRestHandler {

    public RestStopDatafeedAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, MachineLearning.BASE_PATH + "datafeeds/{"
                + DatafeedConfig.ID.getPreferredName() + "}/_stop", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        StopDatafeedAction.Request jobDatafeedRequest = new StopDatafeedAction.Request(
                restRequest.param(DatafeedConfig.ID.getPreferredName()));
        return channel -> client.execute(StopDatafeedAction.INSTANCE, jobDatafeedRequest, new AcknowledgedRestListener<>(channel));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;

import java.io.IOException;

public class RestGetDatafeedsAction extends BaseRestHandler {

    @Inject
    public RestGetDatafeedsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, MlPlugin.BASE_PATH
                + "datafeeds/{" + DatafeedConfig.ID.getPreferredName() + "}", this);
        controller.registerHandler(RestRequest.Method.GET, MlPlugin.BASE_PATH
                + "datafeeds", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String datafeedId = restRequest.param(DatafeedConfig.ID.getPreferredName());
        if (datafeedId == null) {
            datafeedId = GetDatafeedsAction.ALL;
        }
        GetDatafeedsAction.Request request = new GetDatafeedsAction.Request(datafeedId);
        return channel -> client.execute(GetDatafeedsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.downsample.DownsampleDateHistogramConfig;
import org.elasticsearch.xpack.core.downsample.action.DownSampleAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestDownSampleAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_downsample/{downsample_index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index = restRequest.param("index");
        String downsampleIndex = restRequest.param("downsample_index");
        DownsampleDateHistogramConfig config = DownsampleDateHistogramConfig.fromXContent(restRequest.contentParser());
        DownSampleAction.Request request = new DownSampleAction.Request(index, downsampleIndex, config);
        return channel -> client.execute(DownSampleAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "downsample_action";
    }

}

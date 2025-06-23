/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestDownsampleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_downsample/{target_index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String sourceIndex = restRequest.param("index");
        String targetIndex = restRequest.param("target_index");
        String timeout = restRequest.param("timeout");
        DownsampleConfig config;
        try (var parser = restRequest.contentParser()) {
            config = DownsampleConfig.fromXContent(parser);
        }
        DownsampleAction.Request request = new DownsampleAction.Request(
            RestUtils.getMasterNodeTimeout(restRequest),
            sourceIndex,
            targetIndex,
            TimeValue.parseTimeValue(timeout, null, "wait_timeout"),
            config
        );
        return channel -> client.execute(DownsampleAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "downsample_action";
    }

}

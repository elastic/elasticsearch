/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction.Request;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction.INSTANCE;

public class RestGetAutoFollowPatternAction extends BaseRestHandler {

    @Override
    public Map<String, List<Method>> handledMethodsAndPaths() {
        return Collections.unmodifiableMap(MapBuilder.<String, List<Method>>newMapBuilder()
            .put("/_ccr/auto_follow/{name}", singletonList(GET))
            .put("/_ccr/auto_follow", singletonList(GET))
            .map());
    }

    @Override
    public String getName() {
        return "ccr_get_auto_follow_pattern_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        Request request = new Request();
        request.setName(restRequest.param("name"));
        return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.logstash.action.GetPipelineAction;
import org.elasticsearch.xpack.logstash.action.GetPipelineRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestGetPipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_get_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_logstash/pipeline"), new Route(GET, "/_logstash/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final List<String> ids = List.of(request.paramAsStringArray("id", Strings.EMPTY_ARRAY));
        return restChannel -> client.execute(
            GetPipelineAction.INSTANCE,
            new GetPipelineRequest(ids),
            new RestToXContentListener<>(restChannel, r -> {
                if (r.pipelines().isEmpty() && ids.isEmpty() == false) {
                    return RestStatus.NOT_FOUND;
                }
                return RestStatus.OK;
            })
        );
    }
}

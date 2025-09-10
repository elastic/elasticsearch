/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xpack.logstash.action.DeletePipelineAction;
import org.elasticsearch.xpack.logstash.action.DeletePipelineRequest;
import org.elasticsearch.xpack.logstash.action.DeletePipelineResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeletePipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_delete_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_logstash/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        return restChannel -> client.execute(
            DeletePipelineAction.INSTANCE,
            new DeletePipelineRequest(id),
            new RestActionListener<>(restChannel) {
                @Override
                protected void processResponse(DeletePipelineResponse deletePipelineResponse) {
                    final RestStatus status = deletePipelineResponse.isDeleted() ? RestStatus.OK : RestStatus.NOT_FOUND;
                    channel.sendResponse(new RestResponse(status, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                }
            }
        );
    }
}

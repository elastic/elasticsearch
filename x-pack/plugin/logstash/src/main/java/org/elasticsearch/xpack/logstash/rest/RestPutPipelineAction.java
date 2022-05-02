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
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.logstash.Pipeline;
import org.elasticsearch.xpack.logstash.action.PutPipelineAction;
import org.elasticsearch.xpack.logstash.action.PutPipelineRequest;
import org.elasticsearch.xpack.logstash.action.PutPipelineResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutPipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_put_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_logstash/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        try (XContentParser parser = request.contentParser()) {
            // parse pipeline for validation
            Pipeline.PARSER.apply(parser, id);
        }

        return restChannel -> {
            final String content = request.content().utf8ToString();
            client.execute(
                PutPipelineAction.INSTANCE,
                new PutPipelineRequest(id, content, request.getXContentType()),
                new RestActionListener<>(restChannel) {
                    @Override
                    protected void processResponse(PutPipelineResponse putPipelineResponse) throws Exception {
                        channel.sendResponse(
                            new BytesRestResponse(putPipelineResponse.status(), XContentType.JSON.mediaType(), BytesArray.EMPTY)
                        );
                    }
                }
            );
        };
    }
}

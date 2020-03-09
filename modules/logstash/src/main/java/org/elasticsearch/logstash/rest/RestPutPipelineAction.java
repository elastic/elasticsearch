/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.logstash.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.logstash.Pipeline;
import org.elasticsearch.logstash.action.PutPipelineAction;
import org.elasticsearch.logstash.action.PutPipelineRequest;
import org.elasticsearch.logstash.action.PutPipelineResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestActionListener;

import java.io.IOException;
import java.util.List;

public class RestPutPipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_put_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(Method.PUT, "/_logstash/pipeline/{id}"));
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
                            new BytesRestResponse(putPipelineResponse.getStatus(), XContentType.JSON.mediaType(), BytesArray.EMPTY)
                        );
                    }
                }
            );
        };
    }
}

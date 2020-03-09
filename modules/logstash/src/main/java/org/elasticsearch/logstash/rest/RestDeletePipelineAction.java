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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.logstash.action.DeletePipelineAction;
import org.elasticsearch.logstash.action.DeletePipelineRequest;
import org.elasticsearch.logstash.action.DeletePipelineResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;

import java.io.IOException;
import java.util.List;

public class RestDeletePipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_delete_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(Method.DELETE, "/_logstash/pipeline/{id}"));
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
                    channel.sendResponse(new BytesRestResponse(status, XContentType.JSON.mediaType(), BytesArray.EMPTY));
                }
            }
        );
    }
}

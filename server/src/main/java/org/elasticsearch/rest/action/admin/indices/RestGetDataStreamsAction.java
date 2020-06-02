/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.datastream.GetDataStreamAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestGetDataStreamsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_data_streams_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_data_stream"),
            new Route(RestRequest.Method.GET, "/_data_stream/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetDataStreamAction.Request getDataStreamsRequest = new GetDataStreamAction.Request(request.param("name"));
        return channel -> client.admin().indices().getDataStreams(getDataStreamsRequest, new RestToXContentListener<>(channel));
    }
}

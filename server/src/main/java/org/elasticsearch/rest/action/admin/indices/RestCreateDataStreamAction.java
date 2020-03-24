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

import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RestCreateDataStreamAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "create_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_data_stream/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        CreateDataStreamAction.Request putDataStreamRequest = new CreateDataStreamAction.Request(request.param("name"));
        request.withContentOrSourceParamParserOrNull(parser -> {
            Map<String, Object> body = parser.map();
            String timeStampFieldName = (String) body.get(DataStream.TIMESTAMP_FIELD_FIELD.getPreferredName());
            if (timeStampFieldName != null) {
                putDataStreamRequest.setTimestampFieldName(timeStampFieldName);
            }
        });
        return channel -> client.admin().indices().createDataStream(putDataStreamRequest, new RestToXContentListener<>(channel));
    }
}

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

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

public class RestGetPipelineAction extends BaseRestHandler {

    public RestGetPipelineAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_ingest/pipeline", this);
        controller.registerHandler(RestRequest.Method.GET, "/_ingest/pipeline/{id}", this);
    }

    @Override
    public String getName() {
        return "ingest_get_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetPipelineRequest request = new GetPipelineRequest(Strings.splitStringByCommaToArray(restRequest.param("id")));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        return channel -> client.admin().cluster().getPipeline(request, new RestStatusToXContentListener<>(channel));
    }
}

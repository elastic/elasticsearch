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

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;


public class RestPutPipelineAction extends BaseRestHandler {

    public RestPutPipelineAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.PUT, "/_ingest/pipeline/{id}", this);
    }

    @Override
    public String getName() {
        return "ingest_put_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Tuple<XContentType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        PutPipelineRequest request = new PutPipelineRequest(restRequest.param("id"), sourceTuple.v2(), sourceTuple.v1());
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().putPipeline(request, new RestToXContentListener<>(channel));
    }

}

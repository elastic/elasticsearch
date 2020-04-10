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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;

import java.io.IOException;

final class IngestRequestConverters {

    private IngestRequestConverters() {}

    static Request getPipeline(GetPipelineRequest getPipelineRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ingest/pipeline")
            .addCommaSeparatedPathParts(getPipelineRequest.getIds())
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(getPipelineRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request putPipeline(PutPipelineRequest putPipelineRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ingest/pipeline")
            .addPathPart(putPipelineRequest.getId())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(putPipelineRequest.timeout());
        parameters.withMasterTimeout(putPipelineRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(putPipelineRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deletePipeline(DeletePipelineRequest deletePipelineRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ingest/pipeline")
            .addPathPart(deletePipelineRequest.getId())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(deletePipelineRequest.timeout());
        parameters.withMasterTimeout(deletePipelineRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request simulatePipeline(SimulatePipelineRequest simulatePipelineRequest) throws IOException {
        RequestConverters.EndpointBuilder builder = new RequestConverters.EndpointBuilder().addPathPartAsIs("_ingest/pipeline");
        if (simulatePipelineRequest.getId() != null && !simulatePipelineRequest.getId().isEmpty()) {
            builder.addPathPart(simulatePipelineRequest.getId());
        }
        builder.addPathPartAsIs("_simulate");
        String endpoint = builder.build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.putParam("verbose", Boolean.toString(simulatePipelineRequest.isVerbose()));
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(simulatePipelineRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }
}

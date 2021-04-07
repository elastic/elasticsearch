/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.client.core.MainRequest;

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
        if (simulatePipelineRequest.getId() != null && simulatePipelineRequest.getId().isEmpty() == false) {
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

    static Request geoIpStats(MainRequest ignore) {
        return new Request("GET", "_ingest/geoip/stats");
    }
}

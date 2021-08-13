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
import org.elasticsearch.client.ilm.DeleteLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.ExplainLifecycleRequest;
import org.elasticsearch.client.ilm.GetLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.LifecycleManagementStatusRequest;
import org.elasticsearch.client.ilm.PutLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RemoveIndexLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RetryLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.StartILMRequest;
import org.elasticsearch.client.ilm.StopILMRequest;
import org.elasticsearch.client.slm.DeleteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecycleRetentionRequest;
import org.elasticsearch.client.slm.GetSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.GetSnapshotLifecycleStatsRequest;
import org.elasticsearch.client.slm.PutSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.SnapshotLifecycleManagementStatusRequest;
import org.elasticsearch.client.slm.StartSLMRequest;
import org.elasticsearch.client.slm.StopSLMRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

final class IndexLifecycleRequestConverters {

    private IndexLifecycleRequestConverters() {}

    static Request getLifecyclePolicy(GetLifecyclePolicyRequest getLifecyclePolicyRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_ilm/policy")
                .addCommaSeparatedPathParts(getLifecyclePolicyRequest.getPolicyNames()).build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(getLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(getLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request putLifecyclePolicy(PutLifecyclePolicyRequest putLifecycleRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_ilm/policy")
            .addPathPartAsIs(putLifecycleRequest.getName())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(putLifecycleRequest.masterNodeTimeout());
        params.withTimeout(putLifecycleRequest.timeout());
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(putLifecycleRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteLifecyclePolicy(DeleteLifecyclePolicyRequest deleteLifecyclePolicyRequest) {
        Request request = new Request(HttpDelete.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_ilm/policy")
                .addPathPartAsIs(deleteLifecyclePolicyRequest.getLifecyclePolicy())
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(deleteLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(deleteLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request removeIndexLifecyclePolicy(RemoveIndexLifecyclePolicyRequest removePolicyRequest) {
        String[] indices = removePolicyRequest.indices() == null ?
                Strings.EMPTY_ARRAY : removePolicyRequest.indices().toArray(new String[] {});
        Request request = new Request(HttpPost.METHOD_NAME,
                new RequestConverters.EndpointBuilder()
                        .addCommaSeparatedPathParts(indices)
                        .addPathPartAsIs("_ilm", "remove")
                        .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(removePolicyRequest.indicesOptions());
        params.withMasterTimeout(removePolicyRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request startILM(StartILMRequest startILMRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_ilm")
                .addPathPartAsIs("start")
            .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(startILMRequest.masterNodeTimeout());
        params.withTimeout(startILMRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request stopILM(StopILMRequest stopILMRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_ilm")
                .addPathPartAsIs("stop")
            .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(stopILMRequest.masterNodeTimeout());
        params.withTimeout(stopILMRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request lifecycleManagementStatus(LifecycleManagementStatusRequest lifecycleManagementStatusRequest){
        Request request = new Request(HttpGet.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_ilm")
                .addPathPartAsIs("status")
            .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(lifecycleManagementStatusRequest.masterNodeTimeout());
        params.withTimeout(lifecycleManagementStatusRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request explainLifecycle(ExplainLifecycleRequest explainLifecycleRequest) {
        Request request = new Request(HttpGet.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addCommaSeparatedPathParts(explainLifecycleRequest.getIndices())
                .addPathPartAsIs("_ilm")
                .addPathPartAsIs("explain")
            .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withIndicesOptions(explainLifecycleRequest.indicesOptions());
        params.withMasterTimeout(explainLifecycleRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request retryLifecycle(RetryLifecyclePolicyRequest retryLifecyclePolicyRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addCommaSeparatedPathParts(retryLifecyclePolicyRequest.getIndices())
                .addPathPartAsIs("_ilm")
                .addPathPartAsIs("retry")
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(retryLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(retryLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request getSnapshotLifecyclePolicy(GetSnapshotLifecyclePolicyRequest getSnapshotLifecyclePolicyRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_slm/policy")
            .addCommaSeparatedPathParts(getSnapshotLifecyclePolicyRequest.getPolicyIds()).build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(getSnapshotLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(getSnapshotLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request putSnapshotLifecyclePolicy(PutSnapshotLifecyclePolicyRequest putSnapshotLifecyclePolicyRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_slm/policy")
            .addPathPartAsIs(putSnapshotLifecyclePolicyRequest.getPolicy().getId())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(putSnapshotLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(putSnapshotLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(putSnapshotLifecyclePolicyRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteSnapshotLifecyclePolicy(DeleteSnapshotLifecyclePolicyRequest deleteSnapshotLifecyclePolicyRequest) {
        Request request = new Request(HttpDelete.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_slm/policy")
                .addPathPartAsIs(deleteSnapshotLifecyclePolicyRequest.getPolicyId())
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(deleteSnapshotLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(deleteSnapshotLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request executeSnapshotLifecyclePolicy(ExecuteSnapshotLifecyclePolicyRequest executeSnapshotLifecyclePolicyRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_slm/policy")
                .addPathPartAsIs(executeSnapshotLifecyclePolicyRequest.getPolicyId())
                .addPathPartAsIs("_execute")
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(executeSnapshotLifecyclePolicyRequest.masterNodeTimeout());
        params.withTimeout(executeSnapshotLifecyclePolicyRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request executeSnapshotLifecycleRetention(ExecuteSnapshotLifecycleRetentionRequest executeSnapshotLifecycleRetentionRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_slm/_execute_retention")
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(executeSnapshotLifecycleRetentionRequest.masterNodeTimeout());
        params.withTimeout(executeSnapshotLifecycleRetentionRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request getSnapshotLifecycleStats(GetSnapshotLifecycleStatsRequest getSnapshotLifecycleStatsRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_slm/stats").build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(getSnapshotLifecycleStatsRequest.masterNodeTimeout());
        params.withTimeout(getSnapshotLifecycleStatsRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request snapshotLifecycleManagementStatus(SnapshotLifecycleManagementStatusRequest snapshotLifecycleManagementStatusRequest){
        Request request = new Request(HttpGet.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_slm")
                .addPathPartAsIs("status")
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(snapshotLifecycleManagementStatusRequest.masterNodeTimeout());
        params.withTimeout(snapshotLifecycleManagementStatusRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request startSLM(StartSLMRequest startSLMRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_slm")
                .addPathPartAsIs("start")
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(startSLMRequest.masterNodeTimeout());
        params.withTimeout(startSLMRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request stopSLM(StopSLMRequest stopSLMRequest) {
        Request request = new Request(HttpPost.METHOD_NAME,
            new RequestConverters.EndpointBuilder()
                .addPathPartAsIs("_slm")
                .addPathPartAsIs("stop")
                .build());
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(stopSLMRequest.masterNodeTimeout());
        params.withTimeout(stopSLMRequest.timeout());
        request.addParameters(params.asMap());
        return request;
    }
}

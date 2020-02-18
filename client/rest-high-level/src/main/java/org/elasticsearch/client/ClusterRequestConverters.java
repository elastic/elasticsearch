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

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

final class ClusterRequestConverters {

    private ClusterRequestConverters() {}

    static Request clusterPutSettings(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest) throws IOException {
        Request request = new Request(HttpPut.METHOD_NAME, "/_cluster/settings");

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withTimeout(clusterUpdateSettingsRequest.timeout());
        parameters.withMasterTimeout(clusterUpdateSettingsRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        request.setEntity(RequestConverters.createEntity(clusterUpdateSettingsRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request clusterGetSettings(ClusterGetSettingsRequest clusterGetSettingsRequest) throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "/_cluster/settings");
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withLocal(clusterGetSettingsRequest.local());
        parameters.withIncludeDefaults(clusterGetSettingsRequest.includeDefaults());
        parameters.withMasterTimeout(clusterGetSettingsRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request clusterHealth(ClusterHealthRequest healthRequest) {
        String[] indices = healthRequest.indices() == null ? Strings.EMPTY_ARRAY : healthRequest.indices();
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_cluster/health")
            .addCommaSeparatedPathParts(indices)
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params()
            .withWaitForStatus(healthRequest.waitForStatus())
            .withWaitForNoRelocatingShards(healthRequest.waitForNoRelocatingShards())
            .withWaitForNoInitializingShards(healthRequest.waitForNoInitializingShards())
            .withWaitForActiveShards(healthRequest.waitForActiveShards(), ActiveShardCount.NONE)
            .withWaitForNodes(healthRequest.waitForNodes())
            .withWaitForEvents(healthRequest.waitForEvents())
            .withTimeout(healthRequest.timeout())
            .withMasterTimeout(healthRequest.masterNodeTimeout())
            .withLocal(healthRequest.local())
            .withLevel(healthRequest.level());
        request.addParameters(params.asMap());
        return request;
    }

    static Request remoteInfo(RemoteInfoRequest remoteInfoRequest) {
        return new Request(HttpGet.METHOD_NAME, "/_remote/info");
    }
}

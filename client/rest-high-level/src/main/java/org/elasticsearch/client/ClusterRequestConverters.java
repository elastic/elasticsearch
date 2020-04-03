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
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.cluster.RemoteInfoRequest;
import org.elasticsearch.client.indices.ComponentTemplatesExistRequest;
import org.elasticsearch.client.indices.DeleteComponentTemplateRequest;
import org.elasticsearch.client.indices.GetComponentTemplatesRequest;
import org.elasticsearch.client.indices.PutComponentTemplateRequest;
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

    static Request putComponentTemplate(PutComponentTemplateRequest putComponentTemplateRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_component_template")
            .addPathPart(putComponentTemplateRequest.name()).build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(putComponentTemplateRequest.masterNodeTimeout());
        if (putComponentTemplateRequest.create()) {
            params.putParam("create", Boolean.TRUE.toString());
        }
        if (Strings.hasText(putComponentTemplateRequest.cause())) {
            params.putParam("cause", putComponentTemplateRequest.cause());
        }
        request.addParameters(params.asMap());
        request.setEntity(RequestConverters.createEntity(putComponentTemplateRequest, RequestConverters.REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getComponentTemplates(GetComponentTemplatesRequest getComponentTemplatesRequest){
        final String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_component_template")
            .addPathPart(getComponentTemplatesRequest.name())
            .build();
        final Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withLocal(getComponentTemplatesRequest.isLocal());
        params.withMasterTimeout(getComponentTemplatesRequest.getMasterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request componentTemplatesExist(ComponentTemplatesExistRequest componentTemplatesRequest) {
        final String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_component_template")
            .addPathPart(componentTemplatesRequest.name())
            .build();
        final Request request = new Request(HttpHead.METHOD_NAME, endpoint);
        final RequestConverters.Params params = new RequestConverters.Params();
        params.withLocal(componentTemplatesRequest.isLocal());
        params.withMasterTimeout(componentTemplatesRequest.getMasterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteComponentTemplate(DeleteComponentTemplateRequest deleteComponentTemplateRequest) {
        String name = deleteComponentTemplateRequest.getName();
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_component_template").addPathPart(name).build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withMasterTimeout(deleteComponentTemplateRequest.masterNodeTimeout());
        request.addParameters(params.asMap());
        return request;
    }
}

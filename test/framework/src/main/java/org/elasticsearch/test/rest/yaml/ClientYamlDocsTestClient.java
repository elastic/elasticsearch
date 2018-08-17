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

package org.elasticsearch.test.rest.yaml;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Used to execute REST requests according to the docs snippets that need to be tests. Wraps a
 * {@link RestClient} instance used to send the REST requests. Holds the {@link ClientYamlSuiteRestSpec} used to translate api calls into
 * REST calls. Supports raw requests besides the usual api calls based on the rest spec.
 */
public final class ClientYamlDocsTestClient extends ClientYamlTestClient {

    public ClientYamlDocsTestClient(
            final ClientYamlSuiteRestSpec restSpec,
            final RestClient restClient,
            final List<HttpHost> hosts,
            final Version esVersion,
            final Version masterVersion,
            final CheckedSupplier<RestClientBuilder, IOException> clientBuilderWithSniffedNodes) {
        super(restSpec, restClient, hosts, esVersion, masterVersion, clientBuilderWithSniffedNodes);
    }

    @Override
    public ClientYamlTestResponse callApi(String apiName, Map<String, String> params, HttpEntity entity,
            Map<String, String> headers, NodeSelector nodeSelector) throws IOException {

        if ("raw".equals(apiName)) {
            // Raw requests don't use the rest spec at all and are configured entirely by their parameters
            Map<String, String> queryStringParams = new HashMap<>(params);
            String method = Objects.requireNonNull(queryStringParams.remove("method"), "Method must be set to use raw request");
            String path = "/" + Objects.requireNonNull(queryStringParams.remove("path"), "Path must be set to use raw request");
            Request request = new Request(method, path);
            // All other parameters are url parameters
            for (Map.Entry<String, String> param : queryStringParams.entrySet()) {
                request.addParameter(param.getKey(), param.getValue());
            }
            request.setEntity(entity);
            setOptions(request, headers);
            try {
                Response response = getRestClient(nodeSelector).performRequest(request);
                return new ClientYamlTestResponse(response);
            } catch (ResponseException e) {
                throw new ClientYamlTestResponseException(e);
            }
        }
        return super.callApi(apiName, params, entity, headers, nodeSelector);
    }
}

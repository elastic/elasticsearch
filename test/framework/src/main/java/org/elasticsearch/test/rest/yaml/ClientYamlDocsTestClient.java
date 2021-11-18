/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        final String os,
        final CheckedSupplier<RestClientBuilder, IOException> clientBuilderWithSniffedNodes
    ) {
        super(restSpec, restClient, hosts, esVersion, masterVersion, os, clientBuilderWithSniffedNodes);
    }

    @Override
    public ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        HttpEntity entity,
        Map<String, String> headers,
        NodeSelector nodeSelector
    ) throws IOException {

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

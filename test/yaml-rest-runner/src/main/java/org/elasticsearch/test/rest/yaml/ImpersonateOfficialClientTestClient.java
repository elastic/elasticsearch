/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Impersonates an official test client by setting the @{code x-elastic-client-meta} header.
 */
public class ImpersonateOfficialClientTestClient extends ClientYamlTestClient {
    private final String meta;

    public ImpersonateOfficialClientTestClient(
        ClientYamlSuiteRestSpec restSpec,
        RestClient restClient,
        List<HttpHost> hosts,
        CheckedSupplier<RestClientBuilder, IOException> clientBuilderWithSniffedNodes,
        String meta
    ) {
        super(restSpec, restClient, hosts, clientBuilderWithSniffedNodes);
        this.meta = meta;
    }

    @Override
    public ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        HttpEntity entity,
        Map<String, String> headers,
        NodeSelector nodeSelector,
        BiPredicate<ClientYamlSuiteRestApi, ClientYamlSuiteRestApi.Path> pathPredicate
    ) throws IOException {
        headers.put("x-elastic-client-meta", meta);
        return super.callApi(apiName, params, entity, headers, nodeSelector, pathPredicate);
    }
}

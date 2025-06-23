/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilderFactory;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to create an API key
 */
@ServerlessScope(Scope.PUBLIC)
public final class RestCreateApiKeyAction extends ApiKeyBaseRestHandler {

    private final CreateApiKeyRequestBuilderFactory builderFactory;

    /**
     * @param settings       the node's settings
     * @param licenseState   the license state that will be used to determine if
     *                       security is licensed
     */
    public RestCreateApiKeyAction(Settings settings, XPackLicenseState licenseState, CreateApiKeyRequestBuilderFactory builderFactory) {
        super(settings, licenseState);
        this.builderFactory = builderFactory;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/api_key"), new Route(PUT, "/_security/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_create_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CreateApiKeyRequestBuilder builder = builderFactory.create(client).source(request.requiredContent(), request.getXContentType());
        String refresh = request.param("refresh");
        if (refresh != null) {
            builder.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(request.param("refresh")));
        } else {
            builder.setRefreshPolicy(ApiKeyService.defaultCreateDocRefreshPolicy(settings));
        }
        return channel -> builder.execute(new RestToXContentListener<>(channel));
    }
}

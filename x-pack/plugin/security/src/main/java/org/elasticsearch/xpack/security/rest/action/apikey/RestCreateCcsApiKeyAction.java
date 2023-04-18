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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyType;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCcsApiKeyAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest action to create an API key for CCS
 */
public final class RestCreateCcsApiKeyAction extends ApiKeyBaseRestHandler {

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestCreateCcsApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/_ccs/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_create_ccs_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String refresh = request.param("refresh");
        CreateApiKeyRequestBuilder builder = new CreateApiKeyRequestBuilder(client).source(
            request.requiredContent(),
            request.getXContentType()
        )
            .setRefreshPolicy(
                (refresh != null) ? WriteRequest.RefreshPolicy.parse(request.param("refresh")) : CreateApiKeyRequest.DEFAULT_REFRESH_POLICY
            )
            .setType(ApiKeyType.CCS);
        return channel -> client.execute(CreateCcsApiKeyAction.INSTANCE, builder.request(), new RestToXContentListener<>(channel));
    }
}

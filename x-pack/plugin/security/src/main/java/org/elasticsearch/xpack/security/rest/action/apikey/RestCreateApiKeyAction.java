/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequestBuilder;

import java.io.IOException;

/**
 * Rest action to create an API key
 */
public final class RestCreateApiKeyAction extends ApiKeyBaseRestHandler {

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestCreateApiKeyAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(RestRequest.Method.POST, "/_security/api_key", this);
        controller.registerHandler(RestRequest.Method.PUT, "/_security/api_key", this);
    }

    @Override
    public String getName() {
        return "xpack_security_create_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String refresh = request.param("refresh");
        CreateApiKeyRequestBuilder builder = new CreateApiKeyRequestBuilder(client)
            .source(request.requiredContent(), request.getXContentType())
            .setRefreshPolicy((refresh != null) ?
                WriteRequest.RefreshPolicy.parse(request.param("refresh")) : CreateApiKeyRequest.DEFAULT_REFRESH_POLICY);
        return channel -> builder.execute(new RestToXContentListener<>(channel));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.client.SecurityClient;

import java.io.IOException;

/**
 * Rest action to create an API key
 */
public final class RestCreateApiKeyAction extends SecurityBaseRestHandler {

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    protected RestCreateApiKeyAction(Settings settings, XPackLicenseState licenseState, RestController controller) {
        super(settings, licenseState);
        controller.registerHandler(RestRequest.Method.PUT, "/_security/api_key", this);
    }

    @Override
    public String getName() {
        return "create_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            CreateApiKeyRequestBuilder builder = new SecurityClient(client)
                    .prepareCreateApiKey(request.requiredContent(), request.getXContentType())
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.parse(request.param("refresh")));
            return channel -> builder.execute(new RestToXContentListener<CreateApiKeyResponse>(channel));
        }
    }
}
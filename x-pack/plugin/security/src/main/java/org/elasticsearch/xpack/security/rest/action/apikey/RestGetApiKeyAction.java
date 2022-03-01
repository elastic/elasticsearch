/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to get one or more API keys information.
 */
public final class RestGetApiKeyAction extends SecurityBaseRestHandler {

    public RestGetApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/api_key"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String apiKeyId = request.param("id");
        final String apiKeyName = request.param("name");
        final String userName = request.param("username");
        final String realmName = request.param("realm_name");
        final boolean myApiKeysOnly = request.paramAsBoolean("owner", false);
        final GetApiKeyRequest getApiKeyRequest = new GetApiKeyRequest(realmName, userName, apiKeyId, apiKeyName, myApiKeysOnly);
        return channel -> client.execute(GetApiKeyAction.INSTANCE, getApiKeyRequest, new RestBuilderListener<GetApiKeyResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetApiKeyResponse getApiKeyResponse, XContentBuilder builder) throws Exception {
                getApiKeyResponse.toXContent(builder, channel.request());

                // return HTTP status 404 if no API key found for API key id
                if (Strings.hasText(apiKeyId) && getApiKeyResponse.getApiKeyInfos().length == 0) {
                    return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                }
                return new BytesRestResponse(RestStatus.OK, builder);
            }

        });
    }

    @Override
    public String getName() {
        return "xpack_security_get_api_key";
    }

}

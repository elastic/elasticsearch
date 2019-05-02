/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GetMyApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetMyApiKeyRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

/**
 * Rest action to get information for one or more API keys owned by the authenticated user.
 */
public class RestGetMyApiKeyAction extends SecurityBaseRestHandler {

    public RestGetMyApiKeyAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(RestRequest.Method.GET, "/_security/api_key/my", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String apiKeyId = request.param("id");
        final String apiKeyName = request.param("name");
        final GetMyApiKeyRequest getApiKeyRequest = new GetMyApiKeyRequest(apiKeyId, apiKeyName);
        return channel -> client.execute(GetMyApiKeyAction.INSTANCE, getApiKeyRequest,
                new RestBuilderListener<GetApiKeyResponse>(channel) {
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
        return "security_get_my_api_key";
    }
}

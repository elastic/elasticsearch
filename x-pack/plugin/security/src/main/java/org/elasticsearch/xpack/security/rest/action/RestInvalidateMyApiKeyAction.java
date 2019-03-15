/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateMyApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateMyApiKeyRequest;

import java.io.IOException;

/**
 * Rest action to invalidate one or more API keys owned by the authenticated user.
 */
public final class RestInvalidateMyApiKeyAction extends SecurityBaseRestHandler {
    static final ConstructingObjectParser<InvalidateMyApiKeyRequest, Void> PARSER = new ConstructingObjectParser<>("invalidate_my_api_key",
            a -> {
                return new InvalidateMyApiKeyRequest((String) a[0], (String) a[1]);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("id"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("name"));
    }

    public RestInvalidateMyApiKeyAction(Settings settings, RestController controller, XPackLicenseState licenseState) {
        super(settings, licenseState);
        controller.registerHandler(RestRequest.Method.DELETE, "/_security/api_key/my", this);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final InvalidateMyApiKeyRequest invalidateApiKeyRequest = PARSER.parse(parser, null);
            return channel -> client.execute(InvalidateMyApiKeyAction.INSTANCE, invalidateApiKeyRequest,
                new RestBuilderListener<InvalidateApiKeyResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(InvalidateApiKeyResponse invalidateResp,
                                                      XContentBuilder builder) throws Exception {
                        invalidateResp.toXContent(builder, channel.request());
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                });
        }
    }

    @Override
    public String getName() {
        return "security_invalidate_my_api_key";
    }

}

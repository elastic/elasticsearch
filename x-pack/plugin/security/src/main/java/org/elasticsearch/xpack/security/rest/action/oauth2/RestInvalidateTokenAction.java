/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.oauth2;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
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
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest handler for handling access token invalidation requests
 */
public final class RestInvalidateTokenAction extends SecurityBaseRestHandler {

    static final ConstructingObjectParser<Tuple<String, String>, Void> PARSER =
            new ConstructingObjectParser<>("invalidate_token", a -> new Tuple<>((String) a[0], (String) a[1]));
    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("token"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("refresh_token"));
    }

    public RestInvalidateTokenAction(Settings settings, RestController controller, XPackLicenseState xPackLicenseState) {
        super(settings, xPackLicenseState);
        controller.registerHandler(DELETE, "/_xpack/security/oauth2/token", this);
    }

    @Override
    public String getName() {
        return "xpack_security_invalidate_token_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final Tuple<String, String> tuple = PARSER.parse(parser, null);
            final String token = tuple.v1();
            final String refreshToken = tuple.v2();

            final String tokenString;
            final InvalidateTokenRequest.Type type;
            if (Strings.hasLength(token) && Strings.hasLength(refreshToken)) {
                throw new IllegalArgumentException("only one of [token, refresh_token] may be sent per request");
            } else if (Strings.hasLength(token)) {
                tokenString = token;
                type = InvalidateTokenRequest.Type.ACCESS_TOKEN;
            } else if (Strings.hasLength(refreshToken)) {
                tokenString = refreshToken;
                type = InvalidateTokenRequest.Type.REFRESH_TOKEN;
            } else {
                tokenString = null;
                type = null;
            }

            final InvalidateTokenRequest tokenRequest = new InvalidateTokenRequest(tokenString, type);
            return channel -> client.execute(InvalidateTokenAction.INSTANCE, tokenRequest,
                    new RestBuilderListener<InvalidateTokenResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(InvalidateTokenResponse invalidateResp,
                                                          XContentBuilder builder) throws Exception {
                            return new BytesRestResponse(RestStatus.OK, builder.startObject()
                                    .field("created", invalidateResp.isCreated())
                                    .endObject());
                        }
                    });
        }
    }
}

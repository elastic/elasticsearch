/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action.oauth2;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Rest handler for handling access token invalidation requests
 */
public final class RestInvalidateTokenAction extends TokenBaseRestHandler {

    static final ConstructingObjectParser<InvalidateTokenRequest, Void> PARSER =
        new ConstructingObjectParser<>("invalidate_token", a -> {
            final String token = (String) a[0];
            final String refreshToken = (String) a[1];
            final String tokenString;
            final String tokenType;
            if (Strings.hasLength(token) && Strings.hasLength(refreshToken)) {
                throw new IllegalArgumentException("only one of [token, refresh_token] may be sent per request");
            } else if (Strings.hasLength(token)) {
                tokenString = token;
                tokenType = InvalidateTokenRequest.Type.ACCESS_TOKEN.getValue();
            } else if (Strings.hasLength(refreshToken)) {
                tokenString = refreshToken;
                tokenType = InvalidateTokenRequest.Type.REFRESH_TOKEN.getValue();
            } else {
                tokenString = null;
                tokenType = null;
            }
            return new InvalidateTokenRequest(tokenString, tokenType, (String) a[2], (String) a[3]);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("token"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("refresh_token"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("realm_name"));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("username"));
    }

    public RestInvalidateTokenAction(Settings settings, XPackLicenseState xPackLicenseState) {
        super(settings, xPackLicenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // TODO: remove deprecated endpoint in 8.0.0
        return Collections.singletonList(
            new ReplacedRoute(DELETE, "/_security/oauth2/token", DELETE, "/_xpack/security/oauth2/token")
        );
    }

    @Override
    public String getName() {
        return "security_invalidate_token_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            final InvalidateTokenRequest invalidateTokenRequest = PARSER.parse(parser, null);
            return channel -> client.execute(InvalidateTokenAction.INSTANCE, invalidateTokenRequest,
                new RestBuilderListener<InvalidateTokenResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(InvalidateTokenResponse invalidateResp,
                                                      XContentBuilder builder) throws Exception {
                        invalidateResp.toXContent(builder, channel.request());
                        return new BytesRestResponse(invalidateResp.getResult().getRestStatus(), builder);
                    }
                });
        }
    }
}

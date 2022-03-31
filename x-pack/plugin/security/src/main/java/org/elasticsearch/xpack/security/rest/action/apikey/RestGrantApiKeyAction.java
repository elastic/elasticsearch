/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to create an API key on behalf of another user. Loosely mimics the API of
 * {@link org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction} combined with {@link RestCreateApiKeyAction}
 */
public final class RestGrantApiKeyAction extends SecurityBaseRestHandler implements RestRequestFilter {

    static final ObjectParser<GrantApiKeyRequest, Void> PARSER = new ObjectParser<>("grant_api_key_request", GrantApiKeyRequest::new);
    static {
        PARSER.declareString((req, str) -> req.getGrant().setType(str), new ParseField("grant_type"));
        PARSER.declareString((req, str) -> req.getGrant().setUsername(str), new ParseField("username"));
        PARSER.declareField(
            (req, secStr) -> req.getGrant().setPassword(secStr),
            RestGrantApiKeyAction::getSecureString,
            new ParseField("password"),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            (req, secStr) -> req.getGrant().setAccessToken(secStr),
            RestGrantApiKeyAction::getSecureString,
            new ParseField("access_token"),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareObject(
            (req, api) -> req.setApiKeyRequest(api),
            (parser, ignore) -> CreateApiKeyRequestBuilder.parse(parser),
            new ParseField("api_key")
        );
    }

    private static SecureString getSecureString(XContentParser parser) throws IOException {
        return new SecureString(
            Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength())
        );
    }

    public RestGrantApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/api_key/grant"), new Route(PUT, "/_security/api_key/grant"));
    }

    @Override
    public String getName() {
        return "xpack_security_grant_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String refresh = request.param("refresh");
        try (XContentParser parser = request.contentParser()) {
            final GrantApiKeyRequest grantRequest = PARSER.parse(parser, null);
            if (refresh != null) {
                grantRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refresh));
            }
            return channel -> client.execute(
                GrantApiKeyAction.INSTANCE,
                grantRequest,
                new RestToXContentListener<CreateApiKeyResponse>(channel).delegateResponse((listener, ex) -> {
                    RestStatus status = ExceptionsHelper.status(ex);
                    if (status == RestStatus.UNAUTHORIZED) {
                        listener.onFailure(
                            new ElasticsearchSecurityException("Failed to authenticate api key grant", RestStatus.FORBIDDEN, ex)
                        );
                    } else {
                        listener.onFailure(ex);
                    }
                })
            );
        }
    }

    private static final Set<String> FILTERED_FIELDS = Set.of("password", "access_token");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}

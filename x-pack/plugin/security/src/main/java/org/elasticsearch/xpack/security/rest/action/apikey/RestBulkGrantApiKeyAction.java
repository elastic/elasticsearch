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
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest action to create multiple API keys on behalf of another user in a single request.
 * Grant credentials are shared across all keys; each element of {@code api_keys} matches
 * the {@code api_key} object of {@link RestGrantApiKeyAction}.
 */
@ServerlessScope(Scope.INTERNAL)
public final class RestBulkGrantApiKeyAction extends ApiKeyBaseRestHandler implements RestRequestFilter {
    public interface RequestTranslator {
        BulkGrantApiKeyRequest translate(RestRequest request) throws IOException;

        class Default implements RequestTranslator {
            private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder()
                .allowRestriction(true)
                .build();
            private static final ObjectParser<BulkGrantApiKeyRequest, Void> PARSER = createParser(ROLE_DESCRIPTOR_PARSER::parse);

            protected static ObjectParser<BulkGrantApiKeyRequest, Void> createParser(
                CheckedBiFunction<String, XContentParser, RoleDescriptor, IOException> roleDescriptorParser
            ) {
                final ConstructingObjectParser<CreateApiKeyRequest, Void> apiKeyParser = CreateApiKeyRequestBuilder.createParser(
                    roleDescriptorParser
                );
                final ObjectParser<BulkGrantApiKeyRequest, Void> parser = new ObjectParser<>(
                    "bulk_grant_api_key_request",
                    BulkGrantApiKeyRequest::new
                );
                parser.declareString((req, str) -> req.getGrant().setType(str), new ParseField("grant_type"));
                parser.declareString((req, str) -> req.getGrant().setUsername(str), new ParseField("username"));
                parser.declareField(
                    (req, secStr) -> req.getGrant().setPassword(secStr),
                    SecurityBaseRestHandler::getSecureString,
                    new ParseField("password"),
                    ObjectParser.ValueType.STRING
                );
                parser.declareField(
                    (req, secStr) -> req.getGrant().setAccessToken(secStr),
                    SecurityBaseRestHandler::getSecureString,
                    new ParseField("access_token"),
                    ObjectParser.ValueType.STRING
                );
                parser.declareString((req, str) -> req.getGrant().setRunAsUsername(str), new ParseField("run_as"));
                parser.declareObject(
                    (req, clientAuthentication) -> req.getGrant().setClientAuthentication(clientAuthentication),
                    CLIENT_AUTHENTICATION_PARSER,
                    new ParseField("client_authentication")
                );
                parser.declareObjectArray(
                    BulkGrantApiKeyRequest::setApiKeyRequests,
                    (p, ignore) -> apiKeyParser.parse(p, null),
                    new ParseField("api_keys")
                );
                return parser;
            }

            @Override
            public BulkGrantApiKeyRequest translate(RestRequest request) throws IOException {
                try (XContentParser parser = request.contentParser()) {
                    return fromXContent(parser);
                }
            }

            public static BulkGrantApiKeyRequest fromXContent(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }
        }
    }

    private final RequestTranslator requestTranslator;

    public RestBulkGrantApiKeyAction(Settings settings, XPackLicenseState licenseState, RequestTranslator requestTranslator) {
        super(settings, licenseState);
        this.requestTranslator = requestTranslator;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/api_key/_bulk_grant"));
    }

    @Override
    public String getName() {
        return "xpack_security_bulk_grant_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final BulkGrantApiKeyRequest grantRequest = requestTranslator.translate(request);
        final String refresh = request.param("refresh");
        if (refresh != null) {
            grantRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refresh));
        } else {
            grantRequest.setRefreshPolicy(ApiKeyService.defaultCreateDocRefreshPolicy(settings));
        }
        return channel -> client.execute(
            BulkGrantApiKeyAction.INSTANCE,
            grantRequest,
            new RestToXContentListener<BulkGrantApiKeyResponse>(channel).delegateResponse((listener, ex) -> {
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

    @Override
    public Set<String> getFilteredFields() {
        return Set.of("password", "access_token", "client_authentication.value");
    }
}

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.CloneApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CloneApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST action to clone an existing API key. Creates a new key with the same role descriptors as the source,
 * with a new name, id, and optional expiration and metadata.
 */
@ServerlessScope(Scope.INTERNAL)
public final class RestCloneApiKeyAction extends ApiKeyBaseRestHandler implements RestRequestFilter {

    static final ObjectParser<CloneApiKeyRequest, Void> PARSER = new ObjectParser<>("clone_api_key_request", CloneApiKeyRequest::new);

    static {
        PARSER.declareField(
            CloneApiKeyRequest::setApiKey,
            SecurityBaseRestHandler::getSecureString,
            new ParseField("api_key"),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(CloneApiKeyRequest::setName, new ParseField("name"));
        PARSER.declareField(CloneApiKeyRequest::setExpiration, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                p.nextToken();
                return TimeValue.MINUS_ONE;
            }
            return TimeValue.parseTimeValue(p.text(), null, "expiration");
        }, new ParseField("expiration"), ObjectParser.ValueType.STRING_OR_NULL);
        PARSER.declareObject(CloneApiKeyRequest::setMetadata, (p, c) -> p.map(), new ParseField("metadata"));
    }

    public RestCloneApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/api_key/clone"), new Route(PUT, "/_security/api_key/clone"));
    }

    @Override
    public String getName() {
        return "xpack_security_clone_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CloneApiKeyRequest cloneRequest;
        try (XContentParser parser = request.contentParser()) {
            cloneRequest = PARSER.parse(parser, null);
        }
        final String refresh = request.param("refresh");
        if (refresh != null) {
            cloneRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refresh));
        } else {
            cloneRequest.setRefreshPolicy(ApiKeyService.defaultCreateDocRefreshPolicy(settings));
        }
        return channel -> client.execute(
            CloneApiKeyAction.INSTANCE,
            cloneRequest,
            new RestToXContentListener<CreateApiKeyResponse>(channel).delegateResponse((listener, ex) -> {
                RestStatus status = ExceptionsHelper.status(ex);
                if (status == RestStatus.UNAUTHORIZED) {
                    listener.onFailure(new ElasticsearchSecurityException("Failed to clone API key", RestStatus.FORBIDDEN, ex));
                } else {
                    listener.onFailure(ex);
                }
            })
        );
    }

    @Override
    public Set<String> getFilteredFields() {
        return Set.of("api_key");
    }
}

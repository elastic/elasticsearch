/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to create an API key on behalf of another user. Loosely mimics the API of
 * {@link org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction} combined with {@link RestCreateApiKeyAction}
 */
public final class RestGrantApiKeyAction extends ApiKeyBaseRestHandler {

    final Logger logger = LogManager.getLogger();

    static final ObjectParser<GrantApiKeyRequest, Void> PARSER = new ObjectParser<>("grant_api_key_request", GrantApiKeyRequest::new);
    static {
        PARSER.declareString((req, str) -> req.getGrant().setType(str), new ParseField("grant_type"));
        PARSER.declareString((req, str) -> req.getGrant().setUsername(str), new ParseField("username"));
        PARSER.declareField((req, secStr) -> req.getGrant().setPassword(secStr), RestGrantApiKeyAction::getSecureString,
            new ParseField("password"), ObjectParser.ValueType.STRING);
        PARSER.declareField((req, secStr) -> req.getGrant().setAccessToken(secStr), RestGrantApiKeyAction::getSecureString,
            new ParseField("access_token"), ObjectParser.ValueType.STRING);
        PARSER.declareObject((req, api) -> req.setApiKeyRequest(api), (parser, ignore) -> CreateApiKeyRequestBuilder.parse(parser),
            new ParseField("api_key"));
    }

    private static SecureString getSecureString(XContentParser parser) throws IOException {
        return new SecureString(
            Arrays.copyOfRange(parser.textCharacters(), parser.textOffset(), parser.textOffset() + parser.textLength()));
    }

    public RestGrantApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_security/api_key/grant"),
            new Route(PUT, "/_security/api_key/grant"));
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
            return channel -> client.execute(GrantApiKeyAction.INSTANCE, grantRequest, new ResponseListener(channel));
        }
    }

    private class ResponseListener implements ActionListener<CreateApiKeyResponse> {
        private final RestChannel channel;

        ResponseListener(RestChannel channel) {
            this.channel = channel;
        }

        @Override
        public void onResponse(CreateApiKeyResponse response) {
            try (XContentBuilder builder = channel.newBuilder()) {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, response.toXContent(builder, channel.request())));
            } catch (IOException e) {
                sendFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            RestStatus status = ExceptionsHelper.status(e);
            if (status == RestStatus.UNAUTHORIZED) {
                sendFailure(new ElasticsearchSecurityException("Failed to authenticate api key grant", RestStatus.FORBIDDEN, e));
            } else {
                sendFailure(e);
            }
        }

        void sendFailure(Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("failed to send failure response", inner);
            }
        }

    }
}

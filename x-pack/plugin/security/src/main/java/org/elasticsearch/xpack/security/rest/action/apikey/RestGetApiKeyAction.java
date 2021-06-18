/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action to get one or more API keys information.
 */
public final class RestGetApiKeyAction extends SecurityBaseRestHandler {

    private static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "get_api_key_request",
        a -> new Payload((String) a[0], (String) a[1], (String) a[2], (a[3] == null) ? false : (Boolean) a[3], (QueryBuilder) a[4]));

    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField("id"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("username"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("realm_name"));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("owner"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseInnerQueryBuilder(p), new ParseField("query"));
    }

    public RestGetApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/api_key"));
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String apiKeyId;
        final GetApiKeyRequest getApiKeyRequest;
        if (request.hasContentOrSourceParam()) {
            if (false == request.params().isEmpty()) {
                throw new IllegalArgumentException("cannot specify both request parameters and request body at the same time");
            }
            final Payload payload = PARSER.parse(request.contentOrSourceParamParser(), null);
            apiKeyId = payload.id;
            getApiKeyRequest = new GetApiKeyRequest(payload.realmName, payload.userName, apiKeyId, null, payload.owner, payload.query);
        } else {
            apiKeyId = request.param("id");
            final String userName = request.param("username");
            final String realmName = request.param("realm_name");
            final boolean myApiKeysOnly = request.paramAsBoolean("owner", false);
            final String apiKeyName = request.param("name");
            getApiKeyRequest = new GetApiKeyRequest(realmName, userName, apiKeyId, apiKeyName, myApiKeysOnly);
        }

        return channel -> client.execute(GetApiKeyAction.INSTANCE, getApiKeyRequest,
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
        return "xpack_security_get_api_key";
    }

    private static class Payload {
        final String id;
        final String userName;
        final String realmName;
        final boolean owner;
        final QueryBuilder query;

        Payload(String id, String userName, String realmName, boolean owner, QueryBuilder query) {
            this.id = id;
            this.userName = userName;
            this.realmName = realmName;
            this.owner = owner;
            this.query = query;
        }
    }
}

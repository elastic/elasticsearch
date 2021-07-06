/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.apikey.SearchApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.SearchApiKeyRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest action to search for API keys
 */
public final class RestSearchApiKeyAction extends SecurityBaseRestHandler {

    private static final ConstructingObjectParser<SearchApiKeyRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_api_key_request",
        a -> new SearchApiKeyRequest((QueryBuilder) a[0]));

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseInnerQueryBuilder(p), new ParseField("query"));
    }

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestSearchApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_security/_search/api_key"),
            new Route(POST, "/_security/_search/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_search_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final SearchApiKeyRequest searchApiKeyRequest =
            request.hasContentOrSourceParam() ? PARSER.parse(request.contentOrSourceParamParser(), null) : new SearchApiKeyRequest();

        return channel -> client.execute(SearchApiKeyAction.INSTANCE, searchApiKeyRequest, new RestToXContentListener<>(channel));
    }
}

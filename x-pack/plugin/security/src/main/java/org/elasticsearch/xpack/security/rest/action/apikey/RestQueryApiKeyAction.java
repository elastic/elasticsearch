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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
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
public final class RestQueryApiKeyAction extends SecurityBaseRestHandler {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<QueryApiKeyRequest, Void> PARSER = new ConstructingObjectParser<>(
        "query_api_key_request",
        a -> new QueryApiKeyRequest((QueryBuilder) a[0], (Integer) a[1], (Integer) a[2],
            (List<FieldSortBuilder>) a[3], (SearchAfterBuilder) a[4]));

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseInnerQueryBuilder(p), new ParseField("query"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("from"));
        PARSER.declareInt(optionalConstructorArg(), new ParseField("size"));
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new FieldSortBuilder(p.text());
            } else if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
                final FieldSortBuilder fieldSortBuilder = FieldSortBuilder.fromXContent(p, p.currentName());
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
                return fieldSortBuilder;
            } else {
                throw new IllegalArgumentException("mal-formatted sort object");
            }
        }, new ParseField("sort"));
        PARSER.declareField(optionalConstructorArg(), (p, c) -> SearchAfterBuilder.fromXContent(p),
            new ParseField("search_after"), ObjectParser.ValueType.VALUE_ARRAY);
    }

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestQueryApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return org.elasticsearch.core.List.of(
            new Route(GET, "/_security/_query/api_key"),
            new Route(POST, "/_security/_query/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_query_api_key";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final QueryApiKeyRequest queryApiKeyRequest =
            request.hasContentOrSourceParam() ? PARSER.parse(request.contentOrSourceParamParser(), null) : new QueryApiKeyRequest();

        return channel -> client.execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest, new RestToXContentListener<>(channel));
    }
}

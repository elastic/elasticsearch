/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Searches the user-managed service accounts. The body may carry a {@code query}, {@code from}, {@code size},
 * {@code sort} and {@code search_after}, each optional. No body selects every account.
 */
public final class RestQueryServiceAccountAction extends SecurityBaseRestHandler {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "query_service_account_request_payload",
        a -> new Payload((QueryBuilder) a[0], (Integer) a[1], (Integer) a[2], (List<FieldSortBuilder>) a[3], (SearchAfterBuilder) a[4])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseTopLevelQuery(p), new ParseField("query"));
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
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> SearchAfterBuilder.fromXContent(p),
            new ParseField("search_after"),
            ObjectParser.ValueType.VALUE_ARRAY
        );
    }

    private final boolean userManagedServiceAccountsAvailable;

    public RestQueryServiceAccountAction(Settings settings, XPackLicenseState licenseState, boolean userManagedServiceAccountsAvailable) {
        super(settings, licenseState);
        this.userManagedServiceAccountsAvailable = userManagedServiceAccountsAvailable;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/_query/service_account"), new Route(POST, "/_security/_query/service_account"));
    }

    @Override
    public String getName() {
        return "xpack_security_query_service_account";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final QueryServiceAccountRequest queryRequest;
        if (request.hasContentOrSourceParam()) {
            final Payload payload = PARSER.parse(request.contentOrSourceParamParser(), null);
            queryRequest = new QueryServiceAccountRequest(
                payload.queryBuilder,
                payload.from,
                payload.size,
                payload.fieldSortBuilders,
                payload.searchAfterBuilder
            );
        } else {
            queryRequest = new QueryServiceAccountRequest(null, null, null, null, null);
        }
        return channel -> client.execute(QueryServiceAccountAction.INSTANCE, queryRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return UserManagedServiceAccountRestCapabilities.supportedCapabilities(userManagedServiceAccountsAvailable);
    }

    private record Payload(
        @Nullable QueryBuilder queryBuilder,
        @Nullable Integer from,
        @Nullable Integer size,
        @Nullable List<FieldSortBuilder> fieldSortBuilders,
        @Nullable SearchAfterBuilder searchAfterBuilder
    ) {}
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

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
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.action.user.QueryUserRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Rest action to search for Users
 */
public final class RestQueryUserAction extends SecurityBaseRestHandler {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "query_user_request_payload",
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

    /**
     * @param settings the node's settings
     * @param licenseState the license state that will be used to determine if
     * security is licensed
     */
    public RestQueryUserAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/_query/user"), new Route(POST, "/_security/_query/user"));
    }

    @Override
    public String getName() {
        return "xpack_security_query_user";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean withProfileUid = request.paramAsBoolean("with_profile_uid", false);
        final QueryUserRequest queryUserRequest;
        if (request.hasContentOrSourceParam()) {
            final Payload payload = PARSER.parse(request.contentOrSourceParamParser(), null);
            queryUserRequest = new QueryUserRequest(
                payload.queryBuilder,
                payload.from,
                payload.size,
                payload.fieldSortBuilders,
                payload.searchAfterBuilder,
                withProfileUid
            );
        } else {
            queryUserRequest = new QueryUserRequest(null, null, null, null, null, withProfileUid);
        }
        return channel -> client.execute(ActionTypes.QUERY_USER_ACTION, queryUserRequest, new RestToXContentListener<>(channel));
    }

    private record Payload(
        @Nullable QueryBuilder queryBuilder,
        @Nullable Integer from,
        @Nullable Integer size,
        @Nullable List<FieldSortBuilder> fieldSortBuilders,
        @Nullable SearchAfterBuilder searchAfterBuilder
    ) {}
}

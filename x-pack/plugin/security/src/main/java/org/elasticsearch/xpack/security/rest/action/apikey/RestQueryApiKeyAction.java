/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.aggregations.AggregatorFactories.parseAggregators;
import static org.elasticsearch.search.builder.SearchSourceBuilder.AGGREGATIONS_FIELD;
import static org.elasticsearch.search.builder.SearchSourceBuilder.AGGS_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Rest action to search for API keys
 */
@ServerlessScope(Scope.PUBLIC)
public final class RestQueryApiKeyAction extends ApiKeyBaseRestHandler {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Payload, Void> PARSER = new ConstructingObjectParser<>(
        "query_api_key_request_payload",
        a -> {
            if (a[1] != null && a[2] != null) {
                throw new IllegalArgumentException("Duplicate 'aggs' or 'aggregations' field");
            } else {
                return new Payload(
                    (QueryBuilder) a[0],
                    (AggregatorFactories.Builder) (a[1] != null ? a[1] : a[2]),
                    (Integer) a[3],
                    (Integer) a[4],
                    (List<FieldSortBuilder>) a[5],
                    (SearchAfterBuilder) a[6]
                );
            }
        }
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseTopLevelQuery(p), new ParseField("query"));
        // only one of aggs or aggregations is allowed
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseAggregators(p), AGGREGATIONS_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseAggregators(p), AGGS_FIELD);
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
    public RestQueryApiKeyAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_security/_query/api_key"), new Route(POST, "/_security/_query/api_key"));
    }

    @Override
    public String getName() {
        return "xpack_security_query_api_key";
    }

    @Override
    protected Set<String> responseParams() {
        return Set.of(RestSearchAction.TYPED_KEYS_PARAM);
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean withLimitedBy = request.paramAsBoolean("with_limited_by", false);
        final boolean withProfileUid = request.paramAsBoolean("with_profile_uid", false);
        final QueryApiKeyRequest queryApiKeyRequest;
        if (request.hasContentOrSourceParam()) {
            final Payload payload = PARSER.parse(request.contentOrSourceParamParser(), null);
            queryApiKeyRequest = new QueryApiKeyRequest(
                payload.queryBuilder,
                payload.aggsBuilder,
                payload.from,
                payload.size,
                payload.fieldSortBuilders,
                payload.searchAfterBuilder,
                withLimitedBy,
                withProfileUid
            );
        } else {
            queryApiKeyRequest = new QueryApiKeyRequest(null, null, null, null, null, null, withLimitedBy, withProfileUid);
        }
        return channel -> client.execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest, new RestToXContentListener<>(channel));
    }

    private record Payload(
        @Nullable QueryBuilder queryBuilder,
        @Nullable AggregatorFactories.Builder aggsBuilder,
        @Nullable Integer from,
        @Nullable Integer size,
        @Nullable List<FieldSortBuilder> fieldSortBuilders,
        @Nullable SearchAfterBuilder searchAfterBuilder
    ) {}
}

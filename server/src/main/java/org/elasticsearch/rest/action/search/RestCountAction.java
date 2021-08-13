/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.RestActions.buildBroadcastShardsHeader;
import static org.elasticsearch.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER;

public class RestCountAction extends BaseRestHandler {
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCountAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in count requests is deprecated.";
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_count"),
            new Route(POST, "/_count"),
            new Route(GET, "/{index}/_count"),
            new Route(POST, "/{index}/_count"),
            Route.builder(GET, "/{index}/{type}/_count")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(POST, "/{index}/{type}/_count")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build());
    }

    @Override
    public String getName() {
        return "count_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            deprecationLogger.compatibleApiWarning("count_with_types", TYPES_DEPRECATION_MESSAGE);
            request.param("type");
        }
        SearchRequest countRequest = new SearchRequest(Strings.splitStringByCommaToArray(request.param("index")));
        countRequest.indicesOptions(IndicesOptions.fromRequest(request, countRequest.indicesOptions()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        countRequest.source(searchSourceBuilder);
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser == null) {
                QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
                if (queryBuilder != null) {
                    searchSourceBuilder.query(queryBuilder);
                }
            } else {
                searchSourceBuilder.query(RestActions.getQueryContent(parser));
            }
        });
        countRequest.routing(request.param("routing"));
        float minScore = request.paramAsFloat("min_score", -1f);
        if (minScore != -1f) {
            searchSourceBuilder.minScore(minScore);
        }

        countRequest.preference(request.param("preference"));

        final int terminateAfter = request.paramAsInt("terminate_after", DEFAULT_TERMINATE_AFTER);
        searchSourceBuilder.terminateAfter(terminateAfter);
        return channel -> client.search(countRequest, new RestBuilderListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                if (terminateAfter != DEFAULT_TERMINATE_AFTER) {
                    builder.field("terminated_early", response.isTerminatedEarly());
                }
                builder.field("count", response.getHits().getTotalHits().value);
                buildBroadcastShardsHeader(builder, request, response.getTotalShards(), response.getSuccessfulShards(),
                    0, response.getFailedShards(), response.getShardFailures());

                builder.endObject();
                return new BytesRestResponse(response.status(), builder);
            }
        });
    }

}

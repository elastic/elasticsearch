/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.count;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;
import static org.elasticsearch.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER;

/**
 *
 */
public class RestCountAction extends BaseRestHandler {

    private final IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestCountAction(Settings settings, RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings, client);
        controller.registerHandler(POST, "/_count", this);
        controller.registerHandler(GET, "/_count", this);
        controller.registerHandler(POST, "/{index}/_count", this);
        controller.registerHandler(GET, "/{index}/_count", this);
        controller.registerHandler(POST, "/{index}/{type}/_count", this);
        controller.registerHandler(GET, "/{index}/{type}/_count", this);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        SearchRequest countRequest = new SearchRequest(Strings.splitStringByCommaToArray(request.param("index")));
        countRequest.indicesOptions(IndicesOptions.fromRequest(request, countRequest.indicesOptions()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0);
        countRequest.source(searchSourceBuilder);
        if (RestActions.hasBodyContent(request)) {
            BytesReference restContent = RestActions.getRestContent(request);
            searchSourceBuilder.query(RestActions.getQueryContent(restContent, indicesQueriesRegistry, parseFieldMatcher));
        } else {
            QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
            if (queryBuilder != null) {
                searchSourceBuilder.query(queryBuilder);
            }
        }
        countRequest.routing(request.param("routing"));
        float minScore = request.paramAsFloat("min_score", -1f);
        if (minScore != -1f) {
            searchSourceBuilder.minScore(minScore);
        }
        countRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        countRequest.preference(request.param("preference"));

        final int terminateAfter = request.paramAsInt("terminate_after", DEFAULT_TERMINATE_AFTER);
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        } else if (terminateAfter > 0) {
            searchSourceBuilder.terminateAfter(terminateAfter);
        }
        client.search(countRequest, new RestBuilderListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                if (terminateAfter != DEFAULT_TERMINATE_AFTER) {
                    builder.field("terminated_early", response.isTerminatedEarly());
                }
                builder.field("count", response.getHits().totalHits());
                buildBroadcastShardsHeader(builder, request, response.getTotalShards(), response.getSuccessfulShards(),
                        response.getFailedShards(), response.getShardFailures());

                builder.endObject();
                return new BytesRestResponse(response.status(), builder);
            }
        });
    }
}

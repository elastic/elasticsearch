/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *  {
 *   "requests": [{
 *           "id": "amsterdam_query",
 *           "request": {
 *               "query": {
 *                   "match": {
 *                       "text": "amsterdam"
 *                   }
 *               }
 *          },
 *          "ratings": [{
 *                   "_index": "foo",
 *                   "_id": "doc1",
 *                   "rating": 0
 *               },
 *               {
 *                   "_index": "foo",
 *                   "_id": "doc2",
 *                   "rating": 1
 *               },
 *               {
 *                   "_index": "foo",
 *                   "_id": "doc3",
 *                   "rating": 1
 *               }
 *           ]
 *       },
 *       {
 *           "id": "berlin_query",
 *           "request": {
 *               "query": {
 *                   "match": {
 *                       "text": "berlin"
 *                   }
 *               },
 *               "size": 10
 *           },
 *           "ratings": [{
 *               "_index": "foo",
 *               "_id": "doc1",
 *               "rating": 1
 *           }]
 *       }
 *   ],
 *   "metric": {
 *       "precision": {
 *           "ignore_unlabeled": true
 *       }
 *   }
 * }
 */
@ServerlessScope(Scope.PUBLIC)
public class RestRankEvalAction extends BaseRestHandler {

    public static final String ENDPOINT = "_rank_eval";

    private Predicate<NodeFeature> clusterSupportsFeature;

    public RestRankEvalAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/" + ENDPOINT),
            new Route(POST, "/" + ENDPOINT),
            new Route(GET, "/{index}/" + ENDPOINT),
            new Route(POST, "/{index}/" + ENDPOINT)
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RankEvalRequest rankEvalRequest = new RankEvalRequest();
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parseRankEvalRequest(rankEvalRequest, request, parser, clusterSupportsFeature);
        }
        return channel -> client.executeLocally(
            RankEvalPlugin.ACTION,
            rankEvalRequest,
            new RestToXContentListener<RankEvalResponse>(channel)
        );
    }

    private static void parseRankEvalRequest(
        RankEvalRequest rankEvalRequest,
        RestRequest request,
        XContentParser parser,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        rankEvalRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        rankEvalRequest.indicesOptions(IndicesOptions.fromRequest(request, rankEvalRequest.indicesOptions()));
        if (request.hasParam("search_type")) {
            rankEvalRequest.searchType(SearchType.fromString(request.param("search_type")));
        }
        RankEvalSpec spec = RankEvalSpec.parse(parser, clusterSupportsFeature);
        rankEvalRequest.setRankEvalSpec(spec);
    }

    @Override
    public String getName() {
        return "rank_eval_action";
    }
}

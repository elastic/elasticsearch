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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
public class RestRankEvalAction extends BaseRestHandler {

    public RestRankEvalAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_rank_eval", this);
        controller.registerHandler(POST, "/_rank_eval", this);
        controller.registerHandler(GET, "/{index}/_rank_eval", this);
        controller.registerHandler(POST, "/{index}/_rank_eval", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RankEvalRequest rankEvalRequest = new RankEvalRequest();
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parseRankEvalRequest(rankEvalRequest, request, parser);
        }
        return channel -> client.executeLocally(RankEvalAction.INSTANCE, rankEvalRequest,
                new RestToXContentListener<RankEvalResponse>(channel));
    }

    private static void parseRankEvalRequest(RankEvalRequest rankEvalRequest, RestRequest request, XContentParser parser) {
        List<String> indices = Arrays.asList(Strings.splitStringByCommaToArray(request.param("index")));
        RankEvalSpec spec = RankEvalSpec.parse(parser);
        spec.addIndices(indices);
        rankEvalRequest.setRankEvalSpec(spec);
    }

    @Override
    public String getName() {
        return "rank_eval_action";
    }
}

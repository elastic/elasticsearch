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

package org.elasticsearch.graphql.api.resolver;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.graphql.api.GqlApiUtils.futureToListener;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;

public class ResolverSearch {

    @SuppressWarnings("unchecked")
    public static CompletableFuture<SearchResponse> exec(NodeClient client, String indexName, String q) throws Exception {
        SearchRequest searchRequest = new SearchRequest(indexName);

        if (q.length() < 1) {
            throw new Exception("No query provided");
        }

        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(q);
        searchRequest.source().query(queryBuilder);

        CompletableFuture<SearchResponse> future = new CompletableFuture<SearchResponse>();
        client.search(searchRequest, futureToListener(future));

        /*

        Response:

        {"took":64,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":0.2876821,"hits":[{"_index":"twitter","_type":"_doc","_id":"1","_score":0.2876821,"_source":{
   │          "user" : "kimchy",
   │          "post_date" : "2009-11-15T14:12:12",
   │          "message" : "trying out Elasticsearch"
   │      }}]}}

         */

        return future;
    }
}

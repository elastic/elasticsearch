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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Accepted input format:
 *
 * General Format:
 *
 *
  {
    "spec_id": "human_readable_id",
    "requests": [{
        "id": "human_readable_id",
        "request": { ... request to check ... },
        "ratings": { ... mapping from doc id to rating value ... }
     }],
    "metric": {
        "... metric_name... ": {
            "... metric_parameter_key ...": ...metric_parameter_value...
        }
    }
  }
 *
 * Example:
 *
 *
   {"spec_id": "huge_weight_on_location",
    "requests": [{
        "id": "amsterdam_query",
        "request": {
             "query": {
                    "bool": {
                        "must": [
                            {"match": {"beverage": "coffee"}},
                            {"term": {"browser": {"value": "safari"}}},
                            {"term": {"time_of_day": {"value": "morning","boost": 2}}},
                            {"term": {"ip_location": {"value": "ams","boost": 10}}}]}
                },
                "size": 10
        },
        "ratings": [
             {\"index\": \"test\", \"type\": \"my_type\", \"doc_id\": \"1\", \"rating\" : 1 },
             {\"index\": \"test\", \"type\": \"my_type\", \"doc_id\": \"2\", \"rating\" : 0 },
             {\"index\": \"test\", \"type\": \"my_type\", \"doc_id\": \"3\", \"rating\" : 1 }
         ]
    }, {
        "id": "berlin_query",
        "request": {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"beverage": "club mate"}},
                            {"term": {"browser": {"value": "chromium"}}},
                            {"term": {"time_of_day": {"value": "evening","boost": 2}}},
                            {"term": {"ip_location": {"value": "ber","boost": 10}}}]}
                },
                "size": 10
        },
        "ratings": [ ... ]
    }],
    "metric": {
        "precisionAtN": {
            "size": 10
            }
    }
  }

 *
 * Output format:
 *
 * General format:
 *
 *
 {
    "took": 59,
    "timed_out": false,
    "_shards": {
        "total": 5,
        "successful": 5,
        "failed": 0
    },
    "quality_level": ... quality level ...,
    "unknown_docs": {"user_request_id": [... list of unknown docs ...]}
}

 *
 * Example:
 *
 *
 *
  {
    "took": 59,
    "timed_out": false,
    "_shards": {
        "total": 5,
        "successful": 5,
        "failed": 0
    },
    "rank_eval": [{
        "spec_id": "huge_weight_on_location",
        "quality_level": 0.4,
        "unknown_docs": {
            "amsterdam_query": [
                { "index" : "test", "type" : "my_type", "doc_id" : "21"},
                { "index" : "test", "type" : "my_type", "doc_id" : "5"},
                { "index" : "test", "type" : "my_type", "doc_id" : "9"}
            ]
        }, {
            "berlin_query": [
                { "index" : "test", "type" : "my_type", "doc_id" : "42"}
            ]
        }
    }]
  }


 * */
public class RestRankEvalAction extends BaseRestHandler {
    private SearchRequestParsers searchRequestParsers;
    private ScriptService scriptService;

    @Inject
    public RestRankEvalAction(
            Settings settings, 
            RestController controller, 
            SearchRequestParsers searchRequestParsers, 
            ScriptService scriptService) {
        super(settings);
        this.searchRequestParsers = searchRequestParsers;
        this.scriptService = scriptService;
        controller.registerHandler(GET, "/_rank_eval", this);
        controller.registerHandler(POST, "/_rank_eval", this);
        controller.registerHandler(GET, "/{index}/_rank_eval", this);
        controller.registerHandler(POST, "/{index}/_rank_eval", this);
        controller.registerHandler(GET, "/{index}/{type}/_rank_eval", this);
        controller.registerHandler(POST, "/{index}/{type}/_rank_eval", this);

        controller.registerHandler(GET, "/_rank_eval/template", this);
        controller.registerHandler(POST, "/_rank_eval/template", this);
        controller.registerHandler(GET, "/{index}/_rank_eval/template", this);
        controller.registerHandler(POST, "/{index}/_rank_eval/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_rank_eval/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_rank_eval/template", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RankEvalRequest rankEvalRequest = new RankEvalRequest();
        BytesReference restContent = RestActions.hasBodyContent(request) ? RestActions.getRestContent(request) : null;
        try (XContentParser parser = XContentFactory.xContent(restContent).createParser(restContent)) {
            QueryParseContext parseContext = new QueryParseContext(searchRequestParsers.queryParsers, parser, parseFieldMatcher);
            if (restContent != null) {
                parseRankEvalRequest(rankEvalRequest, request,
                        // TODO can we get rid of aggregators parsers and suggesters?
                        new RankEvalContext(parseFieldMatcher, parseContext, searchRequestParsers, scriptService));
            }
        }
        return channel -> client.executeLocally(RankEvalAction.INSTANCE, rankEvalRequest,
                new RestToXContentListener<RankEvalResponse>(channel));
    }

    public static void parseRankEvalRequest(RankEvalRequest rankEvalRequest, RestRequest request, RankEvalContext context)
            throws IOException {
        List<String> indices = Arrays.asList(Strings.splitStringByCommaToArray(request.param("index")));
        List<String> types = Arrays.asList(Strings.splitStringByCommaToArray(request.param("type")));
        RankEvalSpec spec = null;
        boolean containsTemplate = request.path().contains("template");
        spec = RankEvalSpec.parse(context.parser(), context, containsTemplate);
        for (RatedRequest specification : spec.getSpecifications()) {
            specification.setIndices(indices);
            specification.setTypes(types);
        };

        rankEvalRequest.setRankEvalSpec(spec);
    }
}

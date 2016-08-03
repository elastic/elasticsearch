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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

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
            }
        },
        "ratings": {
            "1": 1,
            "2": 0,
            "3": 1,
            "4": 1
        }
        }
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
            }
        },
        "ratings": {
            "1": 0,
            "5": 1,
            "6": 1
        }
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
    "unknown_docs": [{"user_request_id": [... list of unknown docs ...]}]
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
        "unknown_docs": [{
            "amsterdam_query": [5, 10, 23]
        }, {
            "berlin_query": [42]
        }]
    }]
  }


 * */
public class RestRankEvalAction extends BaseRestHandler {

    private IndicesQueriesRegistry queryRegistry;
    private AggregatorParsers aggregators;
    private Suggesters suggesters;

    @Inject
    public RestRankEvalAction(Settings settings, RestController controller, IndicesQueriesRegistry queryRegistry,
            AggregatorParsers aggParsers, Suggesters suggesters) {
        super(settings);
        this.queryRegistry = queryRegistry;
        this.aggregators = aggParsers;
        this.suggesters = suggesters;
        controller.registerHandler(GET, "/_rank_eval", this);
        controller.registerHandler(POST, "/_rank_eval", this);
        controller.registerHandler(GET, "/{index}/_rank_eval", this);
        controller.registerHandler(POST, "/{index}/_rank_eval", this);
        controller.registerHandler(GET, "/{index}/{type}/_rank_eval", this);
        controller.registerHandler(POST, "/{index}/{type}/_rank_eval", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws IOException {
        RankEvalRequest rankEvalRequest = new RankEvalRequest();
        BytesReference restContent = RestActions.hasBodyContent(request) ? RestActions.getRestContent(request) : null;
        try (XContentParser parser = XContentFactory.xContent(restContent).createParser(restContent)) {
            QueryParseContext parseContext = new QueryParseContext(queryRegistry, parser, parseFieldMatcher);
            if (restContent != null) {
                parseRankEvalRequest(rankEvalRequest, request,
                        // TODO can we get rid of aggregators parsers and suggesters?
                        new RankEvalContext(parseFieldMatcher, parseContext, aggregators, suggesters));
            }
        }
        client.execute(RankEvalAction.INSTANCE, rankEvalRequest, new RestToXContentListener<RankEvalResponse>(channel));
    }

    private static final ParseField SPECID_FIELD = new ParseField("spec_id");
    private static final ParseField METRIC_FIELD = new ParseField("metric");
    private static final ParseField REQUESTS_FIELD = new ParseField("requests");
    private static final ObjectParser<RankEvalSpec, RankEvalContext> PARSER = new ObjectParser<>("rank_eval", RankEvalSpec::new);

    static {
        PARSER.declareString(RankEvalSpec::setTaskId, SPECID_FIELD);
        PARSER.declareObject(RankEvalSpec::setEvaluator, (p, c) -> {
            try {
                return RankedListQualityMetric.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , METRIC_FIELD);
        PARSER.declareObjectArray(RankEvalSpec::setSpecifications, (p, c) -> {
            try {
                return QuerySpec.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , REQUESTS_FIELD);
    }

    public static void parseRankEvalRequest(RankEvalRequest rankEvalRequest, RestRequest request, RankEvalContext context)
            throws IOException {
        List<String> indices = Arrays.asList(Strings.splitStringByCommaToArray(request.param("index")));
        List<String> types = Arrays.asList(Strings.splitStringByCommaToArray(request.param("type")));
        RankEvalSpec spec = PARSER.parse(context.parser(), context);
        for (QuerySpec specification : spec.getSpecifications()) {
            specification.setIndices(indices);
            specification.setTypes(types);
        };

        rankEvalRequest.setRankEvalSpec(spec);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public enum SearchResponseUtils {
    ;

    public static TotalHits getTotalHits(SearchRequestBuilder request) {
        var resp = request.get();
        try {
            return resp.getHits().getTotalHits();
        } finally {
            resp.decRef();
        }
    }

    public static long getTotalHitsValue(SearchRequestBuilder request) {
        return getTotalHits(request).value;
    }

    public static SearchResponse responseAsSearchResponse(Response searchResponse) throws IOException {
        try (var parser = ESRestTestCase.responseAsParser(searchResponse)) {
            return parseSearchResponse(parser);
        }
    }

    public static SearchResponse emptyWithTotalHits(
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        SearchResponse.Clusters clusters
    ) {
        return new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            null,
            null,
            false,
            null,
            null,
            1,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardFailures,
            clusters
        );
    }

    public static SearchResponse parseSearchResponse(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        return parseInnerSearchResponse(parser);
    }

    private static final ParseField RESPONSES = new ParseField(MultiSearchResponse.Fields.RESPONSES);
    private static final ParseField TOOK_IN_MILLIS = new ParseField("took");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<MultiSearchResponse, Void> MULTI_SEARCH_RESPONSE_PARSER = new ConstructingObjectParser<>(
        "multi_search",
        true,
        a -> new MultiSearchResponse(((List<MultiSearchResponse.Item>) a[0]).toArray(new MultiSearchResponse.Item[0]), (long) a[1])
    );
    static {
        MULTI_SEARCH_RESPONSE_PARSER.declareObjectArray(constructorArg(), (p, c) -> itemFromXContent(p), RESPONSES);
        MULTI_SEARCH_RESPONSE_PARSER.declareLong(constructorArg(), TOOK_IN_MILLIS);
    }

    public static MultiSearchResponse parseMultiSearchResponse(XContentParser parser) {
        return MULTI_SEARCH_RESPONSE_PARSER.apply(parser, null);
    }

    private static MultiSearchResponse.Item itemFromXContent(XContentParser parser) throws IOException {
        // This parsing logic is a bit tricky here, because the multi search response itself is tricky:
        // 1) The json objects inside the responses array are either a search response or a serialized exception
        // 2) Each response json object gets a status field injected that ElasticsearchException.failureFromXContent(...) does not parse,
        // but SearchResponse.innerFromXContent(...) parses and then ignores. The status field is not needed to parse
        // the response item. However in both cases this method does need to parse the 'status' field otherwise the parsing of
        // the response item in the next json array element will fail due to parsing errors.

        MultiSearchResponse.Item item = null;
        String fieldName = null;

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        outer: for (; token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    fieldName = parser.currentName();
                    if ("error".equals(fieldName)) {
                        item = new MultiSearchResponse.Item(null, ElasticsearchException.failureFromXContent(parser));
                    } else if ("status".equals(fieldName) == false) {
                        item = new MultiSearchResponse.Item(parseInnerSearchResponse(parser), null);
                        break outer;
                    }
                    break;
                case VALUE_NUMBER:
                    if ("status".equals(fieldName)) {
                        // Ignore the status value
                    }
                    break;
            }
        }
        assert parser.currentToken() == XContentParser.Token.END_OBJECT;
        return item;
    }

    public static SearchResponse parseInnerSearchResponse(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        SearchHits hits = null;
        InternalAggregations aggs = null;
        Suggest suggest = null;
        SearchProfileResults profile = null;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
        int numReducePhases = 1;
        long tookInMillis = -1;
        int successfulShards = -1;
        int totalShards = -1;
        int skippedShards = 0; // 0 for BWC
        String scrollId = null;
        String searchContextId = null;
        List<ShardSearchFailure> failures = new ArrayList<>();
        SearchResponse.Clusters clusters = SearchResponse.Clusters.EMPTY;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchResponse.SCROLL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    scrollId = parser.text();
                } else if (SearchResponse.POINT_IN_TIME_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchContextId = parser.text();
                } else if (SearchResponse.TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                    tookInMillis = parser.longValue();
                } else if (SearchResponse.TIMED_OUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    timedOut = parser.booleanValue();
                } else if (SearchResponse.TERMINATED_EARLY.match(currentFieldName, parser.getDeprecationHandler())) {
                    terminatedEarly = parser.booleanValue();
                } else if (SearchResponse.NUM_REDUCE_PHASES.match(currentFieldName, parser.getDeprecationHandler())) {
                    numReducePhases = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchHits.Fields.HITS.equals(currentFieldName)) {
                    hits = SearchHits.fromXContent(parser);
                } else if (InternalAggregations.AGGREGATIONS_FIELD.equals(currentFieldName)) {
                    aggs = InternalAggregations.fromXContent(parser);
                } else if (Suggest.NAME.equals(currentFieldName)) {
                    suggest = Suggest.fromXContent(parser);
                } else if (SearchProfileResults.PROFILE_FIELD.equals(currentFieldName)) {
                    profile = SearchProfileResults.fromXContent(parser);
                } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.intValue(); // we don't need it but need to consume it
                            } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if (RestActions.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    failures.add(ShardSearchFailure.fromXContent(parser));
                                }
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (SearchResponse.Clusters._CLUSTERS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    clusters = parseClusters(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }

        return new SearchResponse(
            hits,
            aggs,
            suggest,
            timedOut,
            terminatedEarly,
            profile,
            numReducePhases,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            failures.toArray(ShardSearchFailure.EMPTY_ARRAY),
            clusters,
            searchContextId
        );
    }

    private static SearchResponse.Clusters parseClusters(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        int total = -1;
        int successful = -1;
        int skipped = -1;
        int running = 0;    // 0 for BWC
        int partial = 0;    // 0 for BWC
        int failed = 0;     // 0 for BWC
        Map<String, SearchResponse.Cluster> clusterInfoMap = ConcurrentCollections.newConcurrentMap();
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchResponse.Clusters.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    total = parser.intValue();
                } else if (SearchResponse.Clusters.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    successful = parser.intValue();
                } else if (SearchResponse.Clusters.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    skipped = parser.intValue();
                } else if (SearchResponse.Clusters.RUNNING_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    running = parser.intValue();
                } else if (SearchResponse.Clusters.PARTIAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    partial = parser.intValue();
                } else if (SearchResponse.Clusters.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    failed = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchResponse.Clusters.DETAILS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    String currentDetailsFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentDetailsFieldName = parser.currentName();  // cluster alias
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            SearchResponse.Cluster c = parseCluster(currentDetailsFieldName, parser);
                            clusterInfoMap.put(currentDetailsFieldName, c);
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        if (clusterInfoMap.isEmpty()) {
            assert running == 0 && partial == 0 && failed == 0
                : "Non cross-cluster should have counter for running, partial and failed equal to 0";
            return new SearchResponse.Clusters(total, successful, skipped);
        } else {
            return new SearchResponse.Clusters(clusterInfoMap);
        }
    }

    private static SearchResponse.Cluster parseCluster(String clusterAlias, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        String clusterName = clusterAlias;
        if (clusterAlias.equals(SearchResponse.LOCAL_CLUSTER_NAME_REPRESENTATION)) {
            clusterName = "";
        }
        String indexExpression = null;
        String status = "running";
        boolean timedOut = false;
        long took = -1L;
        // these are all from the _shards section
        int totalShards = -1;
        int successfulShards = -1;
        int skippedShards = -1;
        int failedShards = -1;
        List<ShardSearchFailure> failures = new ArrayList<>();

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchResponse.Cluster.INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    indexExpression = parser.text();
                } else if (SearchResponse.Cluster.STATUS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    status = parser.text();
                } else if (SearchResponse.TIMED_OUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    timedOut = parser.booleanValue();
                } else if (SearchResponse.TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                    took = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            failedShards = parser.intValue();
                        } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            successfulShards = parser.intValue();
                        } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            totalShards = parser.intValue();
                        } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            skippedShards = parser.intValue();
                        } else {
                            parser.skipChildren();
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (RestActions.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        failures.add(ShardSearchFailure.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }

        Integer totalShardsFinal = totalShards == -1 ? null : totalShards;
        Integer successfulShardsFinal = successfulShards == -1 ? null : successfulShards;
        Integer skippedShardsFinal = skippedShards == -1 ? null : skippedShards;
        Integer failedShardsFinal = failedShards == -1 ? null : failedShards;
        TimeValue tookTimeValue = took == -1L ? null : new TimeValue(took);
        return new SearchResponse.Cluster(
            clusterName,
            indexExpression,
            // skipUnavailable is not exposed to XContent, so just use default
            SearchResponse.Cluster.SKIP_UNAVAILABLE_DEFAULT,
            SearchResponse.Cluster.Status.valueOf(status.toUpperCase(Locale.ROOT)),
            totalShardsFinal,
            successfulShardsFinal,
            skippedShardsFinal,
            failedShardsFinal,
            failures,
            tookTimeValue,
            timedOut
        );
    }
}

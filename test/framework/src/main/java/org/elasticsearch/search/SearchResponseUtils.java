/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseTypedKeysObject;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public enum SearchResponseUtils {
    ;

    // All fields on the root level of the parsed SearchHit are interpreted as metadata fields
    // public because we use it in a completion suggestion option
    @SuppressWarnings("unchecked")
    public static final ObjectParser.UnknownFieldConsumer<Map<String, Object>> unknownMetaFieldConsumer = (map, fieldName, fieldValue) -> {
        Map<String, DocumentField> fieldMap = (Map<String, DocumentField>) map.computeIfAbsent(
            SearchHit.METADATA_FIELDS,
            v -> new HashMap<String, DocumentField>()
        );
        if (fieldName.equals(IgnoredFieldMapper.NAME)) {
            fieldMap.put(fieldName, new DocumentField(fieldName, (List<Object>) fieldValue));
        } else {
            fieldMap.put(fieldName, new DocumentField(fieldName, Collections.singletonList(fieldValue)));
        }
    };

    public static TotalHits getTotalHits(SearchRequestBuilder request) {
        var resp = request.get();
        try {
            return resp.getHits().getTotalHits();
        } finally {
            resp.decRef();
        }
    }

    public static long getTotalHitsValue(SearchRequestBuilder request) {
        return getTotalHits(request).value();
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
        BytesReference searchContextId = null;
        List<ShardSearchFailure> failures = new ArrayList<>();
        SearchResponse.Clusters clusters = SearchResponse.Clusters.EMPTY;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchResponse.SCROLL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    scrollId = parser.text();
                } else if (SearchResponse.POINT_IN_TIME_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchContextId = new BytesArray(Base64.getUrlDecoder().decode(parser.text()));
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
                    hits = parseSearchHits(parser);
                } else if (InternalAggregations.AGGREGATIONS_FIELD.equals(currentFieldName)) {
                    aggs = parseInternalAggregations(parser);
                } else if (Suggest.NAME.equals(currentFieldName)) {
                    suggest = parseSuggest(parser);
                } else if (SearchProfileResults.PROFILE_FIELD.equals(currentFieldName)) {
                    profile = parseSearchProfileResults(parser);
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
                                    failures.add(parseShardSearchFailure(parser));
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
                        failures.add(parseShardSearchFailure(parser));
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

    public static SearchProfileResults parseSearchProfileResults(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        Map<String, SearchProfileShardResult> profileResults = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_ARRAY) {
                if (SearchProfileResults.SHARDS_FIELD.equals(parser.currentName())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseProfileResultsEntry(parser, profileResults);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            }
        }
        return new SearchProfileResults(profileResults);
    }

    private static void parseProfileResultsEntry(XContentParser parser, Map<String, SearchProfileShardResult> searchProfileResults)
        throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        SearchProfileDfsPhaseResult searchProfileDfsPhaseResult = null;
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
        AggregationProfileShardResult aggProfileShardResult = null;
        ProfileResult fetchResult = null;
        String id = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchProfileResults.ID_FIELD.equals(currentFieldName)) {
                    id = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("searches".equals(currentFieldName)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(parseQueryProfileShardResult(parser));
                    }
                } else if (AggregationProfileShardResult.AGGREGATIONS.equals(currentFieldName)) {
                    aggProfileShardResult = readAggregationProfileShardResult(parser);
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("dfs".equals(currentFieldName)) {
                    searchProfileDfsPhaseResult = parseProfileDfsPhaseResult(parser);
                } else if ("fetch".equals(currentFieldName)) {
                    fetchResult = parseProfileResult(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchProfileShardResult result = new SearchProfileShardResult(
            new SearchProfileQueryPhaseResult(queryProfileResults, aggProfileShardResult),
            fetchResult
        );
        result.getQueryPhase().setSearchProfileDfsPhaseResult(searchProfileDfsPhaseResult);
        searchProfileResults.put(id, result);
    }

    private static final InstantiatingObjectParser<SearchProfileDfsPhaseResult, Void> PROFILE_DFS_PHASE_RESULT_PARSER;

    static {
        InstantiatingObjectParser.Builder<SearchProfileDfsPhaseResult, Void> parser = InstantiatingObjectParser.builder(
            "search_profile_dfs_phase_result",
            true,
            SearchProfileDfsPhaseResult.class
        );
        parser.declareObject(optionalConstructorArg(), (p, c) -> parseProfileResult(p), SearchProfileDfsPhaseResult.STATISTICS);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> parseQueryProfileShardResult(p), SearchProfileDfsPhaseResult.KNN);
        PROFILE_DFS_PHASE_RESULT_PARSER = parser.build();
    }

    public static SearchProfileDfsPhaseResult parseProfileDfsPhaseResult(XContentParser parser) throws IOException {
        return PROFILE_DFS_PHASE_RESULT_PARSER.parse(parser, null);
    }

    public static QueryProfileShardResult parseQueryProfileShardResult(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        List<ProfileResult> queryProfileResults = new ArrayList<>();
        long rewriteTime = 0;
        Long vectorOperationsCount = null;
        CollectorResult collector = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QueryProfileShardResult.REWRITE_TIME.equals(currentFieldName)) {
                    rewriteTime = parser.longValue();
                } else if (QueryProfileShardResult.VECTOR_OPERATIONS_COUNT.equals(currentFieldName)) {
                    vectorOperationsCount = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QueryProfileShardResult.QUERY_ARRAY.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(parseProfileResult(parser));
                    }
                } else if (QueryProfileShardResult.COLLECTOR.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        collector = parseCollectorResult(parser);
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new QueryProfileShardResult(queryProfileResults, rewriteTime, collector, vectorOperationsCount);
    }

    public static SearchHits parseSearchHits(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        }
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        List<SearchHit> hits = new ArrayList<>();
        TotalHits totalHits = null;
        float maxScore = 0f;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SearchHits.Fields.TOTAL.equals(currentFieldName)) {
                    // For BWC with nodes pre 7.0
                    long value = parser.longValue();
                    totalHits = value == -1 ? null : new TotalHits(value, TotalHits.Relation.EQUAL_TO);
                } else if (SearchHits.Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = parser.floatValue();
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (SearchHits.Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = Float.NaN; // NaN gets rendered as null-field
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (SearchHits.Fields.HITS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        hits.add(parseSearchHit(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SearchHits.Fields.TOTAL.equals(currentFieldName)) {
                    totalHits = SearchHits.parseTotalHitsFragment(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return SearchHits.unpooled(hits.toArray(SearchHits.EMPTY), totalHits, maxScore);
    }

    /**
     * This parser outputs a temporary map of the objects needed to create the
     * SearchHit instead of directly creating the SearchHit. The reason for this
     * is that this way we can reuse the parser when parsing xContent from
     * {@link org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option} which unfortunately inlines
     * the output of
     * {@link SearchHit#toInnerXContent(XContentBuilder, org.elasticsearch.xcontent.ToXContent.Params)}
     * of the included search hit. The output of the map is used to create the
     * actual SearchHit instance via {@link SearchResponseUtils#searchHitFromMap(Map)}
     */
    static final ObjectParser<Map<String, Object>, Void> MAP_PARSER = new ObjectParser<>(
        "innerHitParser",
        unknownMetaFieldConsumer,
        HashMap::new
    );

    static {
        declareInnerHitsParseFields(MAP_PARSER);
    }

    public static SearchHit parseSearchHit(XContentParser parser) {
        return searchHitFromMap(MAP_PARSER.apply(parser, null));
    }

    public static void declareInnerHitsParseFields(ObjectParser<Map<String, Object>, Void> parser) {
        parser.declareString((map, value) -> map.put(SearchHit.Fields._INDEX, value), new ParseField(SearchHit.Fields._INDEX));
        parser.declareString((map, value) -> map.put(SearchHit.Fields._ID, value), new ParseField(SearchHit.Fields._ID));
        parser.declareString((map, value) -> map.put(SearchHit.Fields._NODE, value), new ParseField(SearchHit.Fields._NODE));
        parser.declareField(
            (map, value) -> map.put(SearchHit.Fields._SCORE, value),
            SearchResponseUtils::parseScore,
            new ParseField(SearchHit.Fields._SCORE),
            ObjectParser.ValueType.FLOAT_OR_NULL
        );
        parser.declareInt((map, value) -> map.put(SearchHit.Fields._RANK, value), new ParseField(SearchHit.Fields._RANK));

        parser.declareLong((map, value) -> map.put(SearchHit.Fields._VERSION, value), new ParseField(SearchHit.Fields._VERSION));
        parser.declareLong((map, value) -> map.put(SearchHit.Fields._SEQ_NO, value), new ParseField(SearchHit.Fields._SEQ_NO));
        parser.declareLong((map, value) -> map.put(SearchHit.Fields._PRIMARY_TERM, value), new ParseField(SearchHit.Fields._PRIMARY_TERM));
        parser.declareField(
            (map, value) -> map.put(SearchHit.Fields._SHARD, value),
            (p, c) -> ShardId.fromString(p.text()),
            new ParseField(SearchHit.Fields._SHARD),
            ObjectParser.ValueType.STRING
        );
        parser.declareObject(
            (map, value) -> map.put(SourceFieldMapper.NAME, value),
            (p, c) -> parseSourceBytes(p),
            new ParseField(SourceFieldMapper.NAME)
        );
        parser.declareObject(
            (map, value) -> map.put(SearchHit.Fields.HIGHLIGHT, value),
            (p, c) -> parseHighlightFields(p),
            new ParseField(SearchHit.Fields.HIGHLIGHT)
        );
        parser.declareObject((map, value) -> {
            Map<String, DocumentField> fieldMap = get(SearchHit.Fields.FIELDS, map, new HashMap<>());
            fieldMap.putAll(value);
            map.put(SearchHit.DOCUMENT_FIELDS, fieldMap);
        }, (p, c) -> parseFields(p), new ParseField(SearchHit.Fields.FIELDS));
        parser.declareObject(
            (map, value) -> map.put(SearchHit.Fields._EXPLANATION, value),
            (p, c) -> parseExplanation(p),
            new ParseField(SearchHit.Fields._EXPLANATION)
        );
        parser.declareObject(
            (map, value) -> map.put(SearchHit.NestedIdentity._NESTED, value),
            (p, ignored) -> parseNestedIdentity(p),
            new ParseField(SearchHit.NestedIdentity._NESTED)
        );
        parser.declareObject(
            (map, value) -> map.put(SearchHit.Fields.INNER_HITS, value),
            (p, c) -> parseInnerHits(p),
            new ParseField(SearchHit.Fields.INNER_HITS)
        );

        parser.declareField((p, map, context) -> {
            XContentParser.Token token = p.currentToken();
            Map<String, Float> matchedQueries = new LinkedHashMap<>();
            if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = null;
                while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = p.currentName();
                    } else if (token.isValue()) {
                        matchedQueries.put(fieldName, p.floatValue());
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while (p.nextToken() != XContentParser.Token.END_ARRAY) {
                    matchedQueries.put(p.text(), Float.NaN);
                }
            }
            map.put(SearchHit.Fields.MATCHED_QUERIES, matchedQueries);
        }, new ParseField(SearchHit.Fields.MATCHED_QUERIES), ObjectParser.ValueType.OBJECT_ARRAY);

        parser.declareField(
            (map, list) -> map.put(SearchHit.Fields.SORT, list),
            SearchResponseUtils::parseSearchSortValues,
            new ParseField(SearchHit.Fields.SORT),
            ObjectParser.ValueType.OBJECT_ARRAY
        );
    }

    private static float parseScore(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER || parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parser.floatValue();
        } else {
            return Float.NaN;
        }
    }

    private static BytesReference parseSourceBytes(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
            // the original document gets slightly modified: whitespaces or
            // pretty printing are not preserved,
            // it all depends on the current builder settings
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static Map<String, DocumentField> parseFields(XContentParser parser) throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            DocumentField field = DocumentField.fromXContent(parser);
            fields.put(field.getName(), field);
        }
        return fields;
    }

    private static Map<String, SearchHits> parseInnerHits(XContentParser parser) throws IOException {
        Map<String, SearchHits> innerHits = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String name = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchHits.Fields.HITS);
            innerHits.put(name, parseSearchHits(parser));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
        return innerHits;
    }

    private static Map<String, HighlightField> parseHighlightFields(XContentParser parser) throws IOException {
        Map<String, HighlightField> highlightFields = new HashMap<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            HighlightField highlightField = parseHighlightField(parser);
            highlightFields.put(highlightField.name(), highlightField);
        }
        return highlightFields;
    }

    private static Explanation parseExplanation(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParser.Token token;
        Float value = null;
        String description = null;
        List<Explanation> details = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (SearchHit.Fields.VALUE.equals(currentFieldName)) {
                value = parser.floatValue();
            } else if (SearchHit.Fields.DESCRIPTION.equals(currentFieldName)) {
                description = parser.textOrNull();
            } else if (SearchHit.Fields.DETAILS.equals(currentFieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    details.add(parseExplanation(parser));
                }
            } else {
                parser.skipChildren();
            }
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation value");
        }
        if (description == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing explanation description");
        }
        return Explanation.match(value, description, details);
    }

    /**
     * this parsing method assumes that the leading "suggest" field name has already been parsed by the caller
     */
    public static Suggest parseSuggest(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions =
            new ArrayList<>();
        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String currentField = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestion = parseSuggestion(
                parser
            );
            if (suggestion != null) {
                suggestions.add(suggestion);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "Could not parse suggestion keyed as [%s]", currentField)
                );
            }
        }
        return new Suggest(suggestions);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> parseSuggestion(
        XContentParser parser
    ) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        SetOnce<Suggest.Suggestion> suggestion = new SetOnce<>();
        XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Suggest.Suggestion.class, suggestion::set);
        return suggestion.get();
    }

    private static final ConstructingObjectParser<SearchHit.NestedIdentity, Void> NESTED_IDENTITY_PARSER = new ConstructingObjectParser<>(
        "nested_identity",
        true,
        ctorArgs -> new SearchHit.NestedIdentity((String) ctorArgs[0], (int) ctorArgs[1], (SearchHit.NestedIdentity) ctorArgs[2])
    );
    static {
        NESTED_IDENTITY_PARSER.declareString(constructorArg(), new ParseField(SearchHit.NestedIdentity.FIELD));
        NESTED_IDENTITY_PARSER.declareInt(constructorArg(), new ParseField(SearchHit.NestedIdentity.OFFSET));
        NESTED_IDENTITY_PARSER.declareObject(
            optionalConstructorArg(),
            NESTED_IDENTITY_PARSER,
            new ParseField(SearchHit.NestedIdentity._NESTED)
        );
    }

    public static SearchHit.NestedIdentity parseNestedIdentity(XContentParser parser) {
        return NESTED_IDENTITY_PARSER.apply(parser, null);
    }

    public static SearchHit searchHitFromMap(Map<String, Object> values) {
        String id = get(SearchHit.Fields._ID, values, null);
        String index = get(SearchHit.Fields._INDEX, values, null);
        String clusterAlias = null;
        if (index != null) {
            String[] split = RemoteClusterAware.splitIndexName(index);
            clusterAlias = split[0];
            index = split[1];
        }
        ShardId shardId = get(SearchHit.Fields._SHARD, values, null);
        String nodeId = get(SearchHit.Fields._NODE, values, null);
        final SearchShardTarget shardTarget;
        if (shardId != null && nodeId != null) {
            assert shardId.getIndexName().equals(index);
            shardTarget = new SearchShardTarget(nodeId, shardId, clusterAlias);
            index = shardTarget.getIndex();
            clusterAlias = shardTarget.getClusterAlias();
        } else {
            shardTarget = null;
        }
        return new SearchHit(
            -1,
            get(SearchHit.Fields._SCORE, values, SearchHit.DEFAULT_SCORE),
            get(SearchHit.Fields._RANK, values, SearchHit.NO_RANK),
            id == null ? null : new Text(id),
            get(SearchHit.NestedIdentity._NESTED, values, null),
            get(SearchHit.Fields._VERSION, values, -1L),
            get(SearchHit.Fields._SEQ_NO, values, SequenceNumbers.UNASSIGNED_SEQ_NO),
            get(SearchHit.Fields._PRIMARY_TERM, values, SequenceNumbers.UNASSIGNED_PRIMARY_TERM),
            get(SourceFieldMapper.NAME, values, null),
            get(SearchHit.Fields.HIGHLIGHT, values, null),
            get(SearchHit.Fields.SORT, values, SearchSortValues.EMPTY),
            get(SearchHit.Fields.MATCHED_QUERIES, values, null),
            get(SearchHit.Fields._EXPLANATION, values, null),
            shardTarget,
            index,
            clusterAlias,
            null,
            get(SearchHit.Fields.INNER_HITS, values, null),
            get(SearchHit.DOCUMENT_FIELDS, values, Collections.emptyMap()),
            get(SearchHit.METADATA_FIELDS, values, Collections.emptyMap()),
            RefCounted.ALWAYS_REFERENCED // TODO: do we ever want pooling here?
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(String key, Map<String, Object> map, T defaultValue) {
        return (T) map.getOrDefault(key, defaultValue);
    }

    public static AggregationProfileShardResult readAggregationProfileShardResult(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
        List<ProfileResult> aggProfileResults = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            aggProfileResults.add(parseProfileResult(parser));
        }
        return new AggregationProfileShardResult(aggProfileResults);
    }

    public static CollectorResult parseCollectorResult(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        String name = null, reason = null;
        long time = -1;
        List<CollectorResult> children = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CollectorResult.NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    name = parser.text();
                } else if (CollectorResult.REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else if (CollectorResult.TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    // we need to consume this value, but we use the raw nanosecond value
                    parser.text();
                } else if (CollectorResult.TIME_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CollectorResult.CHILDREN.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        children.add(parseCollectorResult(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new CollectorResult(name, reason, time, children);
    }

    public static HighlightField parseHighlightField(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        Text[] fragments;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            List<Text> values = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                values.add(new Text(parser.text()));
            }
            fragments = values.toArray(Text.EMPTY_ARRAY);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            fragments = null;
        } else {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token type [" + token + "]");
        }
        return new HighlightField(fieldName, fragments);
    }

    private static InternalAggregations parseInternalAggregations(XContentParser parser) throws IOException {
        final List<InternalAggregation> aggregations = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                SetOnce<InternalAggregation> typedAgg = new SetOnce<>();
                String currentField = parser.currentName();
                parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, InternalAggregation.class, typedAgg::set);
                if (typedAgg.get() != null) {
                    aggregations.add(typedAgg.get());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Could not parse aggregation keyed as [%s]", currentField)
                    );
                }
            }
        }
        return new InternalAggregations(aggregations);
    }

    private static final InstantiatingObjectParser<ProfileResult, Void> PROFILE_RESULT_PARSER;
    static {
        InstantiatingObjectParser.Builder<ProfileResult, Void> parser = InstantiatingObjectParser.builder(
            "profile_result",
            true,
            ProfileResult.class
        );
        parser.declareString(constructorArg(), ProfileResult.TYPE);
        parser.declareString(constructorArg(), ProfileResult.DESCRIPTION);
        parser.declareObject(
            constructorArg(),
            (p, c) -> p.map().entrySet().stream().collect(toMap(Map.Entry::getKey, e -> ((Number) e.getValue()).longValue())),
            ProfileResult.BREAKDOWN
        );
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), ProfileResult.DEBUG);
        parser.declareLong(constructorArg(), ProfileResult.NODE_TIME_RAW);
        parser.declareObjectArray(optionalConstructorArg(), (p, c) -> parseProfileResult(p), ProfileResult.CHILDREN);
        PROFILE_RESULT_PARSER = parser.build();
    }

    public static ProfileResult parseProfileResult(XContentParser p) throws IOException {
        return PROFILE_RESULT_PARSER.parse(p, null);
    }

    public static SearchSortValues parseSearchSortValues(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        return new SearchSortValues(parser.list().toArray());
    }

    public static ShardSearchFailure parseShardSearchFailure(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String currentFieldName = null;
        int shardId = -1;
        String indexName = null;
        String clusterAlias = null;
        String nodeId = null;
        ElasticsearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (ShardSearchFailure.SHARD_FIELD.equals(currentFieldName)) {
                    shardId = parser.intValue();
                } else if (ShardSearchFailure.INDEX_FIELD.equals(currentFieldName)) {
                    indexName = parser.text();
                    int indexOf = indexName.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                    if (indexOf > 0) {
                        clusterAlias = indexName.substring(0, indexOf);
                        indexName = indexName.substring(indexOf + 1);
                    }
                } else if (ShardSearchFailure.NODE_FIELD.equals(currentFieldName)) {
                    nodeId = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (ShardSearchFailure.REASON_FIELD.equals(currentFieldName)) {
                    exception = ElasticsearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchShardTarget searchShardTarget = null;
        if (nodeId != null) {
            searchShardTarget = new SearchShardTarget(
                nodeId,
                new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), shardId),
                clusterAlias
            );
        }
        return new ShardSearchFailure(exception, searchShardTarget);
    }
}

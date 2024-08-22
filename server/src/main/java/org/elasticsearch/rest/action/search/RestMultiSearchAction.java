/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestMultiSearchAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]"
        + " Specifying types in multi search template requests is deprecated.";

    private static final Set<String> RESPONSE_PARAMS = Set.of(RestSearchAction.TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM);

    private final boolean allowExplicitIndex;
    private final SearchUsageHolder searchUsageHolder;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestMultiSearchAction(Settings settings, SearchUsageHolder searchUsageHolder, Predicate<NodeFeature> clusterSupportsFeature) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
        this.searchUsageHolder = searchUsageHolder;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_msearch"),
            new Route(POST, "/_msearch"),
            new Route(GET, "/{index}/_msearch"),
            new Route(POST, "/{index}/_msearch"),
            Route.builder(GET, "/{index}/{type}/_msearch").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/_msearch").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "msearch_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final MultiSearchRequest multiSearchRequest = parseRequest(request, allowExplicitIndex, searchUsageHolder, clusterSupportsFeature);
        return channel -> {
            final RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(
                TransportMultiSearchAction.TYPE,
                multiSearchRequest,
                new RestRefCountedChunkedToXContentListener<>(channel)
            );
        };
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchRequest}
     */
    public static MultiSearchRequest parseRequest(
        RestRequest restRequest,
        boolean allowExplicitIndex,
        SearchUsageHolder searchUsageHolder,
        Predicate<NodeFeature> clusterSupportsFeature
    ) throws IOException {
        return parseRequest(restRequest, allowExplicitIndex, searchUsageHolder, clusterSupportsFeature, (k, v, r) -> false);
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchRequest}. This variation allows the caller to specify if
     * wait_for_checkpoints functionality is supported.
     */
    public static MultiSearchRequest parseRequest(
        RestRequest restRequest,
        boolean allowExplicitIndex,
        SearchUsageHolder searchUsageHolder,
        Predicate<NodeFeature> clusterSupportsFeature,
        TriFunction<String, Object, SearchRequest, Boolean> extraParamParser
    ) throws IOException {
        if (restRequest.getRestApiVersion() == RestApiVersion.V_7 && restRequest.hasParam("type")) {
            restRequest.param("type");
        }

        MultiSearchRequest multiRequest = new MultiSearchRequest();
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(restRequest, multiRequest.indicesOptions());
        multiRequest.indicesOptions(indicesOptions);
        if (restRequest.hasParam("max_concurrent_searches")) {
            multiRequest.maxConcurrentSearchRequests(restRequest.paramAsInt("max_concurrent_searches", 0));
        }

        Integer preFilterShardSize = null;
        if (restRequest.hasParam("pre_filter_shard_size")) {
            preFilterShardSize = restRequest.paramAsInt("pre_filter_shard_size", SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE);
        }

        final Integer maxConcurrentShardRequests;
        if (restRequest.hasParam("max_concurrent_shard_requests")) {
            // only set if we have the parameter since we auto adjust the max concurrency on the coordinator
            // based on the number of nodes in the cluster
            maxConcurrentShardRequests = restRequest.paramAsInt("max_concurrent_shard_requests", Integer.MIN_VALUE);
        } else {
            maxConcurrentShardRequests = null;
        }

        parseMultiLineRequest(restRequest, multiRequest.indicesOptions(), allowExplicitIndex, (searchRequest, parser) -> {
            searchRequest.source(new SearchSourceBuilder().parseXContent(parser, false, searchUsageHolder, clusterSupportsFeature));
            RestSearchAction.validateSearchRequest(restRequest, searchRequest);
            if (searchRequest.pointInTimeBuilder() != null) {
                RestSearchAction.preparePointInTime(searchRequest, restRequest);
            } else {
                searchRequest.setCcsMinimizeRoundtrips(
                    restRequest.paramAsBoolean("ccs_minimize_roundtrips", searchRequest.isCcsMinimizeRoundtrips())
                );
            }
            multiRequest.add(searchRequest);
        }, extraParamParser);
        List<SearchRequest> requests = multiRequest.requests();
        for (SearchRequest request : requests) {
            // preserve if it's set on the request
            if (preFilterShardSize != null && request.getPreFilterShardSize() == null) {
                request.setPreFilterShardSize(preFilterShardSize);
            }
            if (maxConcurrentShardRequests != null) {
                request.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
            }
        }
        return multiRequest;
    }

    /**
     * Parses a multi-line {@link RestRequest} body, instantiating a {@link SearchRequest} for each line and applying the given consumer.
     */
    public static void parseMultiLineRequest(
        RestRequest request,
        IndicesOptions indicesOptions,
        boolean allowExplicitIndex,
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer
    ) throws IOException {
        parseMultiLineRequest(request, indicesOptions, allowExplicitIndex, consumer, (k, v, r) -> false);
    }

    /**
     * Parses a multi-line {@link RestRequest} body, instantiating a {@link SearchRequest} for each line and applying the given consumer.
     * This variation allows the caller to provider a param parser.
     */
    public static void parseMultiLineRequest(
        RestRequest request,
        IndicesOptions indicesOptions,
        boolean allowExplicitIndex,
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer,
        TriFunction<String, Object, SearchRequest, Boolean> extraParamParser
    ) throws IOException {

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String searchType = request.param("search_type");
        boolean ccsMinimizeRoundtrips = request.paramAsBoolean("ccs_minimize_roundtrips", true);
        String routing = request.param("routing");

        final Tuple<XContentType, BytesReference> sourceTuple = request.contentOrSourceParam();
        final XContent xContent = sourceTuple.v1().xContent();
        final BytesReference data = sourceTuple.v2();
        MultiSearchRequest.readMultiLineFormat(
            xContent,
            request.contentParserConfig(),
            data,
            consumer,
            indices,
            indicesOptions,
            routing,
            searchType,
            ccsMinimizeRoundtrips,
            allowExplicitIndex,
            extraParamParser
        );
    }

    @Override
    public boolean supportsBulkContent() {
        return true;
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}

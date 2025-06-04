/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;

@ServerlessScope(Scope.PUBLIC)
public class RestSearchAction extends BaseRestHandler {

    /**
     * Indicates whether hits.total should be rendered as an integer or an object
     * in the rest search response.
     */
    public static final String TOTAL_HITS_AS_INT_PARAM = "rest_total_hits_as_int";
    public static final String TYPED_KEYS_PARAM = "typed_keys";
    public static final String INCLUDE_NAMED_QUERIES_SCORE_PARAM = "include_named_queries_score";
    public static final Set<String> RESPONSE_PARAMS = Set.of(TYPED_KEYS_PARAM, TOTAL_HITS_AS_INT_PARAM, INCLUDE_NAMED_QUERIES_SCORE_PARAM);

    private final SearchUsageHolder searchUsageHolder;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestSearchAction(SearchUsageHolder searchUsageHolder, Predicate<NodeFeature> clusterSupportsFeature) {
        this.searchUsageHolder = searchUsageHolder;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public String getName() {
        return "search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_search"),
            new Route(POST, "/_search"),
            new Route(GET, "/{index}/_search"),
            new Route(POST, "/{index}/_search")
        );
    }

    @Override
    public Set<String> supportedCapabilities() {
        return SearchCapabilities.CAPABILITIES;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (client.threadPool() != null && client.threadPool().getThreadContext() != null) {
            client.threadPool().getThreadContext().setErrorTraceTransportHeader(request);
        }
        SearchRequest searchRequest = new SearchRequest();
        // access the BwC param, but just drop it
        // this might be set by old clients
        request.param("min_compatible_shard_node");
        /*
         * We have to pull out the call to `source().size(size)` because
         * _update_by_query and _delete_by_query uses this same parsing
         * path but sets a different variable when it sees the `size`
         * url parameter.
         *
         * Note that we can't use `searchRequest.source()::size` because
         * `searchRequest.source()` is null right now. We don't have to
         * guard against it being null in the IntConsumer because it can't
         * be null later. If that is confusing to you then you are in good
         * company.
         */
        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(
            parser -> parseSearchRequest(searchRequest, request, parser, clusterSupportsFeature, setSize, searchUsageHolder)
        );

        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(TransportSearchAction.TYPE, searchRequest, new RestRefCountedChunkedToXContentListener<>(channel));
        };
    }

    /**
     * Parses the rest request on top of the SearchRequest, preserving values that are not overridden by the rest request.
     *
     * @param searchRequest the search request that will hold what gets parsed
     * @param request the rest request to read from
     * @param requestContentParser body of the request to read. This method does not attempt to read the body from the {@code request}
     *        parameter
     * @param clusterSupportsFeature used to check if certain features are available in this cluster
     * @param setSize how the size url parameter is handled. {@code udpate_by_query} and regular search differ here.
     */
    public static void parseSearchRequest(
        SearchRequest searchRequest,
        RestRequest request,
        XContentParser requestContentParser,
        Predicate<NodeFeature> clusterSupportsFeature,
        IntConsumer setSize
    ) throws IOException {
        parseSearchRequest(searchRequest, request, requestContentParser, clusterSupportsFeature, setSize, null);
    }

    /**
     * Parses the rest request on top of the SearchRequest, preserving values that are not overridden by the rest request.
     *
     * @param searchRequest the search request that will hold what gets parsed
     * @param request the rest request to read from
     * @param requestContentParser body of the request to read. This method does not attempt to read the body from the {@code request}
     *        parameter, will be null when there is no request body to parse
     * @param clusterSupportsFeature used to check if certain features are available in this cluster
     * @param setSize how the size url parameter is handled. {@code udpate_by_query} and regular search differ here.
     * @param searchUsageHolder the holder of search usage stats
     */
    public static void parseSearchRequest(
        SearchRequest searchRequest,
        RestRequest request,
        @Nullable XContentParser requestContentParser,
        Predicate<NodeFeature> clusterSupportsFeature,
        IntConsumer setSize,
        @Nullable SearchUsageHolder searchUsageHolder
    ) throws IOException {
        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }
        searchRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        if (requestContentParser != null) {
            if (searchUsageHolder == null) {
                searchRequest.source().parseXContent(requestContentParser, true, clusterSupportsFeature);
            } else {
                searchRequest.source().parseXContent(requestContentParser, true, searchUsageHolder, clusterSupportsFeature);
            }
        }

        final int batchedReduceSize = request.paramAsInt("batched_reduce_size", searchRequest.getBatchedReduceSize());
        searchRequest.setBatchedReduceSize(batchedReduceSize);
        if (request.hasParam("pre_filter_shard_size")) {
            searchRequest.setPreFilterShardSize(request.paramAsInt("pre_filter_shard_size", SearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE));
        }
        if (request.hasParam("enable_fields_emulation")) {
            // this flag is a no-op from 8.0 on, we only want to consume it so its presence doesn't cause errors
            request.paramAsBoolean("enable_fields_emulation", false);
        }
        if (request.hasParam("max_concurrent_shard_requests")) {
            // only set if we have the parameter since we auto adjust the max concurrency on the coordinator
            // based on the number of nodes in the cluster
            final int maxConcurrentShardRequests = request.paramAsInt(
                "max_concurrent_shard_requests",
                searchRequest.getMaxConcurrentShardRequests()
            );
            searchRequest.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        }

        if (request.hasParam("allow_partial_search_results")) {
            // only set if we have the parameter passed to override the cluster-level default
            searchRequest.allowPartialSearchResults(request.paramAsBoolean("allow_partial_search_results", null));
        }

        searchRequest.searchType(request.param("search_type"));
        parseSearchSource(searchRequest.source(), request, setSize);
        searchRequest.requestCache(request.paramAsBoolean("request_cache", searchRequest.requestCache()));

        String scroll = request.param("scroll");
        if (scroll != null) {
            searchRequest.scroll(parseTimeValue(scroll, null, "scroll"));
        }
        searchRequest.routing(request.param("routing"));
        searchRequest.preference(request.param("preference"));
        searchRequest.indicesOptions(IndicesOptions.fromRequest(request, searchRequest.indicesOptions()));

        validateSearchRequest(request, searchRequest);

        if (searchRequest.pointInTimeBuilder() != null) {
            preparePointInTime(searchRequest, request);
        } else {
            searchRequest.setCcsMinimizeRoundtrips(
                request.paramAsBoolean("ccs_minimize_roundtrips", searchRequest.isCcsMinimizeRoundtrips())
            );
        }
        if (request.paramAsBoolean("force_synthetic_source", false)) {
            searchRequest.setForceSyntheticSource(true);
        }
    }

    /**
     * Parses the rest request on top of the SearchSourceBuilder, preserving
     * values that are not overridden by the rest request.
     */
    private static void parseSearchSource(final SearchSourceBuilder searchSourceBuilder, RestRequest request, IntConsumer setSize) {
        QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }

        if (request.hasParam("from")) {
            searchSourceBuilder.from(request.paramAsInt("from", 0));
        }
        if (request.hasParam("size")) {
            setSize.accept(request.paramAsInt("size", SearchService.DEFAULT_SIZE));
        }
        if (request.hasParam("explain")) {
            searchSourceBuilder.explain(request.paramAsBoolean("explain", null));
        }
        if (request.hasParam("version")) {
            searchSourceBuilder.version(request.paramAsBoolean("version", null));
        }
        if (request.hasParam("seq_no_primary_term")) {
            searchSourceBuilder.seqNoAndPrimaryTerm(request.paramAsBoolean("seq_no_primary_term", null));
        }
        if (request.hasParam("timeout")) {
            searchSourceBuilder.timeout(request.paramAsTime("timeout", null));
        }
        if (request.hasParam("terminate_after")) {
            int terminateAfter = request.paramAsInt("terminate_after", SearchContext.DEFAULT_TERMINATE_AFTER);
            searchSourceBuilder.terminateAfter(terminateAfter);
        }

        StoredFieldsContext storedFieldsContext = StoredFieldsContext.fromRestRequest(
            SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(),
            request
        );
        if (storedFieldsContext != null) {
            searchSourceBuilder.storedFields(storedFieldsContext);
        }
        String sDocValueFields = request.param("docvalue_fields");
        if (sDocValueFields != null) {
            if (Strings.hasText(sDocValueFields)) {
                String[] sFields = Strings.splitStringByCommaToArray(sDocValueFields);
                for (String field : sFields) {
                    searchSourceBuilder.docValueField(field, null);
                }
            }
        }
        FetchSourceContext fetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        if (fetchSourceContext != null) {
            searchSourceBuilder.fetchSource(fetchSourceContext);
        }

        if (request.hasParam("track_scores")) {
            searchSourceBuilder.trackScores(request.paramAsBoolean("track_scores", false));
        }

        if (request.hasParam("track_total_hits")) {
            if (Booleans.isBoolean(request.param("track_total_hits"))) {
                searchSourceBuilder.trackTotalHits(request.paramAsBoolean("track_total_hits", true));
            } else {
                searchSourceBuilder.trackTotalHitsUpTo(
                    request.paramAsInt("track_total_hits", SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO)
                );
            }
        }

        String sSorts = request.param("sort");
        if (sSorts != null) {
            String[] sorts = Strings.splitStringByCommaToArray(sSorts);
            for (String sort : sorts) {
                int delimiter = sort.lastIndexOf(':');
                if (delimiter != -1) {
                    String sortField = sort.substring(0, delimiter);
                    String reverse = sort.substring(delimiter + 1);
                    if ("asc".equals(reverse)) {
                        searchSourceBuilder.sort(sortField, SortOrder.ASC);
                    } else if ("desc".equals(reverse)) {
                        searchSourceBuilder.sort(sortField, SortOrder.DESC);
                    }
                } else {
                    searchSourceBuilder.sort(sort);
                }
            }
        }

        String sStats = request.param("stats");
        if (sStats != null) {
            searchSourceBuilder.stats(Arrays.asList(Strings.splitStringByCommaToArray(sStats)));
        }
        SuggestBuilder suggestBuilder = parseSuggestUrlParameters(request);
        if (suggestBuilder != null) {
            searchSourceBuilder.suggest(suggestBuilder);
        }
    }

    private static final String[] suggestQueryStringParams = new String[] { "suggest_text", "suggest_size", "suggest_mode" };

    /**
     * package private for testing
     */
    static SuggestBuilder parseSuggestUrlParameters(RestRequest request) {
        String suggestField = request.param("suggest_field");
        if (suggestField != null) {
            String suggestText = request.param("suggest_text", request.param("q"));
            int suggestSize = request.paramAsInt("suggest_size", 5);
            String suggestMode = request.param("suggest_mode");
            return new SuggestBuilder().addSuggestion(
                suggestField,
                termSuggestion(suggestField).text(suggestText)
                    .size(suggestSize)
                    .suggestMode(TermSuggestionBuilder.SuggestMode.resolve(suggestMode))
            );
        } else {
            List<String> unconsumedParams = Arrays.stream(suggestQueryStringParams).filter(key -> request.param(key) != null).toList();
            if (unconsumedParams.isEmpty() == false) {
                // this would lead to a non-descriptive error from RestBaseHandler#unrecognized later, so throw a better IAE here
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains parameters %s but missing 'suggest_field' parameter.",
                        request.path(),
                        unconsumedParams.toString()
                    )
                );
            }
        }
        return null;
    }

    static void preparePointInTime(SearchRequest request, RestRequest restRequest) {
        assert request.pointInTimeBuilder() != null;
        ActionRequestValidationException validationException = null;
        if (restRequest.paramAsBoolean("ccs_minimize_roundtrips", false)) {
            validationException = addValidationError("[ccs_minimize_roundtrips] cannot be used with point in time", validationException);
            request.setCcsMinimizeRoundtrips(false);
        }
        ExceptionsHelper.reThrowIfNotNull(validationException);
    }

    /**
     * Validates that no search request parameters conflict. This method
     * might modify the search request to align certain parameters.
     */
    public static void validateSearchRequest(RestRequest restRequest, SearchRequest searchRequest) {
        checkRestTotalHits(restRequest, searchRequest);
        checkSearchType(restRequest, searchRequest);
        // ensures that the rest param is consumed
        restRequest.paramAsBoolean(INCLUDE_NAMED_QUERIES_SCORE_PARAM, false);
    }

    /**
     * Modify the search request to accurately count the total hits that match the query
     * if {@link #TOTAL_HITS_AS_INT_PARAM} is set.
     *
     * @throws IllegalArgumentException if {@link #TOTAL_HITS_AS_INT_PARAM}
     * is used in conjunction with a lower bound value (other than {@link SearchContext#DEFAULT_TRACK_TOTAL_HITS_UP_TO})
     * for the track_total_hits option.
     */
    private static void checkRestTotalHits(RestRequest restRequest, SearchRequest searchRequest) {
        boolean totalHitsAsInt = restRequest.paramAsBoolean(TOTAL_HITS_AS_INT_PARAM, false);
        if (totalHitsAsInt == false) {
            return;
        }
        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }
        Integer trackTotalHitsUpTo = searchRequest.source().trackTotalHitsUpTo();
        if (trackTotalHitsUpTo == null) {
            searchRequest.source().trackTotalHits(true);
        } else if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_ACCURATE
            && trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                throw new IllegalArgumentException(
                    "["
                        + TOTAL_HITS_AS_INT_PARAM
                        + "] cannot be used "
                        + "if the tracking of total hits is not accurate, got "
                        + trackTotalHitsUpTo
                );
            }
    }

    private static void checkSearchType(RestRequest restRequest, SearchRequest searchRequest) {
        if (restRequest.hasParam("search_type") && searchRequest.hasKnnSearch()) {
            throw new IllegalArgumentException(
                "cannot set [search_type] when using [knn] search, since the search type is determined automatically"
            );
        }
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

}

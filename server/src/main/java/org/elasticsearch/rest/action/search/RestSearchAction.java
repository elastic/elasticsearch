/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;
import static org.elasticsearch.core.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;

public class RestSearchAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSearchAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" + " Specifying types in search requests is deprecated.";

    /**
     * Indicates whether hits.total should be rendered as an integer or an object
     * in the rest search response.
     */
    public static final String TOTAL_HITS_AS_INT_PARAM = "rest_total_hits_as_int";
    public static final String TYPED_KEYS_PARAM = "typed_keys";
    public static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(Arrays.asList(TYPED_KEYS_PARAM, TOTAL_HITS_AS_INT_PARAM));
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
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
            new Route(POST, "/{index}/_search"),
            Route.builder(GET, "/{index}/{type}/_search").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(POST, "/{index}/{type}/_search").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        SearchRequest searchRequest;
        if (request.hasParam("min_compatible_shard_node")) {
            searchRequest = new SearchRequest(Version.fromString(request.param("min_compatible_shard_node")));
        } else {
            searchRequest = new SearchRequest();
        }
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
            parser -> parseSearchRequest(searchRequest, request, parser, client.getNamedWriteableRegistry(), setSize)
        );

        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
        };
    }

    /**
     * Parses the rest request on top of the SearchRequest, preserving values that are not overridden by the rest request.
     *
     * @param requestContentParser body of the request to read. This method does not attempt to read the body from the {@code request}
     *        parameter
     * @param setSize how the size url parameter is handled. {@code udpate_by_query} and regular search differ here.
     */
    public static void parseSearchRequest(
        SearchRequest searchRequest,
        RestRequest request,
        XContentParser requestContentParser,
        NamedWriteableRegistry namedWriteableRegistry,
        IntConsumer setSize
    ) throws IOException {
        parseSearchRequest(searchRequest, request, requestContentParser, namedWriteableRegistry, setSize, (r, sr) -> {});
    }

    /**
     * Parses the rest request on top of the SearchRequest, preserving values that are not overridden by the rest request. This variation
     * allows the caller to specify if wait_for_checkpoints functionality is supported.
     */
    public static void parseSearchRequest(
        SearchRequest searchRequest,
        RestRequest request,
        XContentParser requestContentParser,
        NamedWriteableRegistry namedWriteableRegistry,
        IntConsumer setSize,
        BiConsumer<RestRequest, SearchRequest> extraParamParser
    ) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
            request.param("type");
            deprecationLogger.compatibleCritical("search_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }
        searchRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        if (requestContentParser != null) {
            searchRequest.source().parseXContent(requestContentParser, true);
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
            searchRequest.scroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }
        searchRequest.routing(request.param("routing"));
        searchRequest.preference(request.param("preference"));
        searchRequest.indicesOptions(IndicesOptions.fromRequest(request, searchRequest.indicesOptions()));

        checkRestTotalHits(request, searchRequest);

        if (searchRequest.pointInTimeBuilder() != null) {
            preparePointInTime(searchRequest, request, namedWriteableRegistry);
        } else {
            searchRequest.setCcsMinimizeRoundtrips(
                request.paramAsBoolean("ccs_minimize_roundtrips", searchRequest.isCcsMinimizeRoundtrips())
            );
        }

        extraParamParser.accept(request, searchRequest);
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
            int size = request.paramAsInt("size", SearchService.DEFAULT_SIZE);
            if (request.getRestApiVersion() == RestApiVersion.V_7 && size == -1) {
                // we treat -1 as not-set, but deprecate it to be able to later remove this funny extra treatment
                deprecationLogger.compatibleCritical(
                    "search-api-size-1",
                    "Using search size of -1 is deprecated and will be removed in future versions. "
                        + "Instead, don't use the `size` parameter if you don't want to set it explicitly."
                );
            } else {
                setSize.accept(size);
            }
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
                int delimiter = sort.lastIndexOf(":");
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

        String suggestField = request.param("suggest_field");
        if (suggestField != null) {
            String suggestText = request.param("suggest_text", request.param("q"));
            int suggestSize = request.paramAsInt("suggest_size", 5);
            String suggestMode = request.param("suggest_mode");
            searchSourceBuilder.suggest(
                new SuggestBuilder().addSuggestion(
                    suggestField,
                    termSuggestion(suggestField).text(suggestText).size(suggestSize).suggestMode(SuggestMode.resolve(suggestMode))
                )
            );
        }
    }

    static void preparePointInTime(SearchRequest request, RestRequest restRequest, NamedWriteableRegistry namedWriteableRegistry) {
        assert request.pointInTimeBuilder() != null;
        ActionRequestValidationException validationException = null;
        if (request.indices().length > 0) {
            validationException = addValidationError(
                "[indices] cannot be used with point in time. Do " + "not specify any index with point in time.",
                validationException
            );
        }
        if (request.indicesOptions().equals(DEFAULT_INDICES_OPTIONS) == false) {
            validationException = addValidationError("[indicesOptions] cannot be used with point in time", validationException);
        }
        if (request.routing() != null) {
            validationException = addValidationError("[routing] cannot be used with point in time", validationException);
        }
        if (request.preference() != null) {
            validationException = addValidationError("[preference] cannot be used with point in time", validationException);
        }
        if (restRequest.paramAsBoolean("ccs_minimize_roundtrips", false)) {
            validationException = addValidationError("[ccs_minimize_roundtrips] cannot be used with point in time", validationException);
            request.setCcsMinimizeRoundtrips(false);
        }
        ExceptionsHelper.reThrowIfNotNull(validationException);

        final IndicesOptions indicesOptions = request.indicesOptions();
        final IndicesOptions stricterIndicesOptions = IndicesOptions.fromOptions(
            indicesOptions.ignoreUnavailable(),
            indicesOptions.allowNoIndices(),
            false,
            false,
            false,
            true,
            true,
            indicesOptions.ignoreThrottled()
        );
        request.indicesOptions(stricterIndicesOptions);
        final SearchContextId searchContextId = request.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry);
        request.indices(searchContextId.getActualIndices());
    }

    /**
     * Modify the search request to accurately count the total hits that match the query
     * if {@link #TOTAL_HITS_AS_INT_PARAM} is set.
     *
     * @throws IllegalArgumentException if {@link #TOTAL_HITS_AS_INT_PARAM}
     * is used in conjunction with a lower bound value (other than {@link SearchContext#DEFAULT_TRACK_TOTAL_HITS_UP_TO})
     * for the track_total_hits option.
     */
    public static void checkRestTotalHits(RestRequest restRequest, SearchRequest searchRequest) {
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

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}

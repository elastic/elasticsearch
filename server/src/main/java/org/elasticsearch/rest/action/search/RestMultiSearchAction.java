/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestMultiSearchAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
        " Specifying types in multi search requests is deprecated.";

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(
            Arrays.asList(RestSearchAction.TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM)
        );
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    private final boolean allowExplicitIndex;

    public RestMultiSearchAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_msearch"),
            new Route(POST, "/_msearch"),
            new Route(GET, "/{index}/_msearch"),
            new Route(POST, "/{index}/_msearch"),
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_msearch"),
            new Route(POST, "/{index}/{type}/_msearch")));
    }

    @Override
    public String getName() {
        return "msearch_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        MultiSearchRequest multiSearchRequest = parseRequest(request,  client.getNamedWriteableRegistry(), allowExplicitIndex);

        // Emit a single deprecation message if any search request contains types.
        for (SearchRequest searchRequest : multiSearchRequest.requests()) {
            if (searchRequest.types().length > 0) {
                deprecationLogger.deprecate(DeprecationCategory.TYPES, "msearch_with_types", TYPES_DEPRECATION_MESSAGE);
                break;
            }
        }
        return channel -> {
            final RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(MultiSearchAction.INSTANCE, multiSearchRequest, new RestToXContentListener<>(channel));
        };
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchRequest}
     */
    public static MultiSearchRequest parseRequest(RestRequest restRequest,
                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                  boolean allowExplicitIndex) throws IOException {
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
            searchRequest.source(SearchSourceBuilder.fromXContent(parser, false));
            RestSearchAction.checkRestTotalHits(restRequest, searchRequest);
            if (searchRequest.pointInTimeBuilder() != null) {
                RestSearchAction.preparePointInTime(searchRequest, restRequest, namedWriteableRegistry);
            } else {
                searchRequest.setCcsMinimizeRoundtrips(
                    restRequest.paramAsBoolean("ccs_minimize_roundtrips", searchRequest.isCcsMinimizeRoundtrips())
                );
            }
            multiRequest.add(searchRequest);
        });
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
    public static void parseMultiLineRequest(RestRequest request, IndicesOptions indicesOptions, boolean allowExplicitIndex,
            CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer) throws IOException {

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        String searchType = request.param("search_type");
        boolean ccsMinimizeRoundtrips = request.paramAsBoolean("ccs_minimize_roundtrips", true);
        String routing = request.param("routing");

        final Tuple<XContentType, BytesReference> sourceTuple = request.contentOrSourceParam();
        final XContent xContent = sourceTuple.v1().xContent();
        final BytesReference data = sourceTuple.v2();
        MultiSearchRequest.readMultiLineFormat(data, xContent, consumer, indices, indicesOptions, types, routing,
                searchType, ccsMinimizeRoundtrips, request.getXContentRegistry(), allowExplicitIndex, deprecationLogger);
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}

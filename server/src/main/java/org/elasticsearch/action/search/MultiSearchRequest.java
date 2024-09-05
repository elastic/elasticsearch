/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * A multi search API request.
 */
public class MultiSearchRequest extends ActionRequest implements CompositeIndicesRequest {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSearchAction.class);
    public static final String FIRST_LINE_EMPTY_DEPRECATION_MESSAGE =
        "support for empty first line before any action metadata in msearch API is deprecated "
            + "and will be removed in the next major version";
    public static final int MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT = 0;

    private int maxConcurrentSearchRequests = 0;
    private final List<SearchRequest> requests = new ArrayList<>();

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    public MultiSearchRequest() {}

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequest add(SearchRequestBuilder request) {
        requests.add(request.request());
        return this;
    }

    /**
     * Add a search request to execute. Note, the order is important, the search response will be returned in the
     * same order as the search requests.
     */
    public MultiSearchRequest add(SearchRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Returns the amount of search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public int maxConcurrentSearchRequests() {
        return maxConcurrentSearchRequests;
    }

    /**
     * Sets how many search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public MultiSearchRequest maxConcurrentSearchRequests(int maxConcurrentSearchRequests) {
        if (maxConcurrentSearchRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentSearchRequests must be positive");
        }

        this.maxConcurrentSearchRequests = maxConcurrentSearchRequests;
        return this;
    }

    public List<SearchRequest> requests() {
        return this.requests;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (int i = 0; i < requests.size(); i++) {
            ActionRequestValidationException ex = requests.get(i).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public MultiSearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public MultiSearchRequest(StreamInput in) throws IOException {
        super(in);
        maxConcurrentSearchRequests = in.readVInt();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            SearchRequest request = new SearchRequest(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(maxConcurrentSearchRequests);
        out.writeCollection(requests);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiSearchRequest that = (MultiSearchRequest) o;
        return maxConcurrentSearchRequests == that.maxConcurrentSearchRequests
            && Objects.equals(requests, that.requests)
            && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxConcurrentSearchRequests, requests, indicesOptions);
    }

    public static void readMultiLineFormat(
        XContent xContent,
        XContentParserConfiguration parserConfig,
        BytesReference data,
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer,
        String[] indices,
        IndicesOptions indicesOptions,
        String routing,
        String searchType,
        Boolean ccsMinimizeRoundtrips,
        boolean allowExplicitIndex
    ) throws IOException {
        readMultiLineFormat(
            xContent,
            parserConfig,
            data,
            consumer,
            indices,
            indicesOptions,
            routing,
            searchType,
            ccsMinimizeRoundtrips,
            allowExplicitIndex,
            (s, o, r) -> false
        );

    }

    public static void readMultiLineFormat(
        XContent xContent,
        XContentParserConfiguration parserConfig,
        BytesReference data,
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer,
        String[] indices,
        IndicesOptions indicesOptions,
        String routing,
        String searchType,
        Boolean ccsMinimizeRoundtrips,
        boolean allowExplicitIndex,
        TriFunction<String, Object, SearchRequest, Boolean> extraParamParser
    ) throws IOException {
        int from = 0;
        byte marker = xContent.bulkSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data);
            if (nextMarker == -1) {
                break;
            }
            // support first line with \n
            if (parserConfig.restApiVersion() == RestApiVersion.V_7 && nextMarker == 0) {
                deprecationLogger.compatibleCritical("msearch_first_line_empty", FIRST_LINE_EMPTY_DEPRECATION_MESSAGE);
                from = nextMarker + 1;
                continue;
            }

            SearchRequest searchRequest = new SearchRequest();
            if (indices != null) {
                searchRequest.indices(indices);
            }
            if (indicesOptions != null) {
                searchRequest.indicesOptions(indicesOptions);
            }
            if (routing != null) {
                searchRequest.routing(routing);
            }
            if (searchType != null) {
                searchRequest.searchType(searchType);
            }
            if (ccsMinimizeRoundtrips != null) {
                searchRequest.setCcsMinimizeRoundtrips(ccsMinimizeRoundtrips);
            }
            IndicesOptions defaultOptions = searchRequest.indicesOptions();
            // now parse the action
            if (nextMarker - from > 0) {
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        parserConfig,
                        data.slice(from, nextMarker - from),
                        xContent.type()
                    )
                ) {
                    Map<String, Object> source = parser.map();
                    if (parser.nextToken() != null) {
                        throw new XContentParseException(parser.getTokenLocation(), "Unexpected token after end of object");
                    }
                    Object expandWildcards = null;
                    Object ignoreUnavailable = null;
                    Object ignoreThrottled = null;
                    Object allowNoIndices = null;
                    for (Map.Entry<String, Object> entry : source.entrySet()) {
                        Object value = entry.getValue();
                        if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                            if (allowExplicitIndex == false) {
                                throw new IllegalArgumentException("explicit index in multi search is not allowed");
                            }
                            searchRequest.indices(nodeStringArrayValue(value));
                        } else if ("search_type".equals(entry.getKey()) || "searchType".equals(entry.getKey())) {
                            searchRequest.searchType(nodeStringValue(value, null));
                        } else if ("ccs_minimize_roundtrips".equals(entry.getKey()) || "ccsMinimizeRoundtrips".equals(entry.getKey())) {
                            searchRequest.setCcsMinimizeRoundtrips(nodeBooleanValue(value));
                        } else if ("request_cache".equals(entry.getKey()) || "requestCache".equals(entry.getKey())) {
                            searchRequest.requestCache(nodeBooleanValue(value, entry.getKey()));
                        } else if ("preference".equals(entry.getKey())) {
                            searchRequest.preference(nodeStringValue(value, null));
                        } else if ("routing".equals(entry.getKey())) {
                            searchRequest.routing(nodeStringValue(value, null));
                        } else if ("allow_partial_search_results".equals(entry.getKey())) {
                            searchRequest.allowPartialSearchResults(nodeBooleanValue(value, null));
                        } else if ("expand_wildcards".equals(entry.getKey()) || "expandWildcards".equals(entry.getKey())) {
                            expandWildcards = value;
                        } else if ("ignore_unavailable".equals(entry.getKey()) || "ignoreUnavailable".equals(entry.getKey())) {
                            ignoreUnavailable = value;
                        } else if ("allow_no_indices".equals(entry.getKey()) || "allowNoIndices".equals(entry.getKey())) {
                            allowNoIndices = value;
                        } else if ("ignore_throttled".equals(entry.getKey()) || "ignoreThrottled".equals(entry.getKey())) {
                            ignoreThrottled = value;
                        } else if (parserConfig.restApiVersion() == RestApiVersion.V_7
                            && ("type".equals(entry.getKey()) || "types".equals(entry.getKey()))) {
                                deprecationLogger.compatibleCritical("msearch_with_types", RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
                            } else if (extraParamParser.apply(entry.getKey(), value, searchRequest)) {
                                // Skip, the parser handled the key/value
                            } else {
                                throw new IllegalArgumentException("key [" + entry.getKey() + "] is not supported in the metadata section");
                            }
                    }
                    defaultOptions = IndicesOptions.fromParameters(
                        expandWildcards,
                        ignoreUnavailable,
                        allowNoIndices,
                        ignoreThrottled,
                        defaultOptions
                    );
                }
            }
            searchRequest.indicesOptions(defaultOptions);

            // move pointers
            from = nextMarker + 1;
            // now for the body
            nextMarker = findNextMarker(marker, from, data);
            if (nextMarker == -1) {
                break;
            }
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    parserConfig,
                    data.slice(from, nextMarker - from),
                    xContent.type()
                )
            ) {
                consumer.accept(searchRequest, parser);
                if (parser.nextToken() != null) {
                    throw new XContentParseException(parser.getTokenLocation(), "Unexpected token after end of object");
                }
            }
            // move pointers
            from = nextMarker + 1;
        }
    }

    private static int findNextMarker(byte marker, int from, BytesReference data) {
        final int res = data.indexOf(marker, from);
        if (res != -1) {
            assert res >= 0;
            return res;
        }
        if (from != data.length()) {
            throw new IllegalArgumentException("The msearch request must be terminated by a newline [\n]");
        }
        return -1;
    }

    public static byte[] writeMultiLineFormat(MultiSearchRequest multiSearchRequest, XContent xContent) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (SearchRequest request : multiSearchRequest.requests()) {
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                writeSearchRequestParams(request, xContentBuilder);
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.bulkSeparator());
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                if (request.source() != null) {
                    request.source().toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                } else {
                    xContentBuilder.startObject();
                    xContentBuilder.endObject();
                }
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.bulkSeparator());
        }
        return output.toByteArray();
    }

    public static void writeSearchRequestParams(SearchRequest request, XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.startObject();
        if (request.indices() != null) {
            xContentBuilder.field("index", request.indices());
        }
        if (request.indicesOptions().equals(SearchRequest.DEFAULT_INDICES_OPTIONS) == false) {
            request.indicesOptions().wildcardOptions().toXContent(xContentBuilder, true);
            request.indicesOptions().concreteTargetOptions().toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        }
        if (request.searchType() != null) {
            xContentBuilder.field("search_type", request.searchType().name().toLowerCase(Locale.ROOT));
        }
        xContentBuilder.field("ccs_minimize_roundtrips", request.isCcsMinimizeRoundtrips());
        if (request.requestCache() != null) {
            xContentBuilder.field("request_cache", request.requestCache());
        }
        if (request.preference() != null) {
            xContentBuilder.field("preference", request.preference());
        }
        if (request.routing() != null) {
            xContentBuilder.field("routing", request.routing());
        }
        if (request.allowPartialSearchResults() != null) {
            xContentBuilder.field("allow_partial_search_results", request.allowPartialSearchResults());
        }
        xContentBuilder.endObject();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "requests["
                    + requests.size()
                    + "]: "
                    + requests.stream().map(SearchRequest::buildDescription).collect(Collectors.joining(" | "));
            }
        };
    }
}

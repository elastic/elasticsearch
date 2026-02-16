/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.action.search.SearchParamsParser;
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
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * A multi search API request.
 */
public class MultiSearchRequest extends LegacyActionRequest implements CompositeIndicesRequest, IndicesRequest.CrossProjectCandidate {
    public static final int MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT = 0;

    private int maxConcurrentSearchRequests = 0;
    private final List<SearchRequest> requests = new ArrayList<>();

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    @Nullable
    private String projectRouting;

    private static final TransportVersion MSEARCH_PROJECT_ROUTING = TransportVersion.fromName("msearch_project_routing");

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
        if (in.getTransportVersion().supports(MSEARCH_PROJECT_ROUTING)) {
            this.projectRouting = in.readOptionalString();
        } else {
            this.projectRouting = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(maxConcurrentSearchRequests);
        out.writeCollection(requests);
        if (out.getTransportVersion().supports(MSEARCH_PROJECT_ROUTING)) {
            out.writeOptionalString(this.projectRouting);
        }
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
        boolean allowExplicitIndex,
        /*
         * Refer to RestSearchAction#parseSearchRequest()'s JavaDoc to understand why this is an Optional
         * and what its values mean with respect to an endpoint's Cross Project Search status/support.
         */
        Optional<Boolean> crossProjectEnabled,
        @Nullable String projectRouting
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
            (s, o, r) -> false,
            crossProjectEnabled,
            projectRouting
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
        TriFunction<String, Object, SearchRequest, Boolean> extraParamParser,
        /*
         * Refer to RestSearchAction#parseSearchRequest()'s JavaDoc to understand why this is an Optional
         * and what its values mean with respect to an endpoint's Cross Project Search status/support.
         */
        Optional<Boolean> crossProjectEnabled,
        @Nullable String projectRouting
    ) throws IOException {
        int from = 0;
        byte marker = xContent.bulkSeparator();
        boolean warnedMrtForCps = false;
        while (true) {
            int nextMarker = findNextMarker(marker, from, data);
            if (nextMarker == -1) {
                break;
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
            /*
             * This `ccsMinimizeRoundtrips` refers to the value specified as the query parameter and is extracted in
             * `RestMultiSearchAction#parseMultiLineRequest()`. If in a Cross Project Search environment, it is
             * guaranteed to be `true`. Otherwise, its value is whatever that the user is provided.
             */
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
                    // In Jackson 2.21+, parser.map() may leave the parser past END_OBJECT
                    // Only check for extra tokens if we're still positioned in the stream
                    if (parser.currentToken() != null && parser.nextToken() != null) {
                        throw new XContentParseException(parser.getTokenLocation(), "Unexpected token after end of object");
                    }
                    Object expandWildcards = null;
                    Object ignoreUnavailable = null;
                    Object ignoreThrottled = null;
                    Object allowNoIndices = null;
                    for (Map.Entry<String, Object> entry : source.entrySet()) {
                        Object value = entry.getValue();
                        if (crossProjectEnabled.orElse(false)
                            && ("project_routing".equals(entry.getKey()) || "projectRouting".equals(entry.getKey()))) {
                            searchRequest.setProjectRouting(nodeStringValue(value));
                        } else if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                            if (allowExplicitIndex == false) {
                                throw new IllegalArgumentException("explicit index in multi search is not allowed");
                            }
                            searchRequest.indices(nodeStringArrayValue(value));
                        } else if ("search_type".equals(entry.getKey()) || "searchType".equals(entry.getKey())) {
                            searchRequest.searchType(nodeStringValue(value, null));
                        } else if ("ccs_minimize_roundtrips".equals(entry.getKey()) || "ccsMinimizeRoundtrips".equals(entry.getKey())) {
                            searchRequest.setCcsMinimizeRoundtrips(crossProjectEnabled.orElse(false) || nodeBooleanValue(value));
                            if (crossProjectEnabled.orElse(false) && warnedMrtForCps == false) {
                                HeaderWarning.addWarning(SearchParamsParser.MRT_SET_IN_CPS_WARN);
                                warnedMrtForCps = true;
                            }
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

            /*
             * There are 2 different places where project_routing can appear:
             * 1. As a query parameter, i.e. top-level, and,
             * 2. Within the request's body.
             *
             * When it appears within the request's body, we override the query parameter: this is how msearch options and params
             * work.
             *
             * At this point, if search#getProjectRouting() returns `null`, it means that we did not see any specific value within
             * the request's body. So we'll pick up whatever that was provided in the top-level.
             */
            if (crossProjectEnabled.orElse(false) && searchRequest.getProjectRouting() == null && projectRouting != null) {
                searchRequest.setProjectRouting(projectRouting);
            }

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
                // In Jackson 2.21+, after parsing, the parser may be past END_OBJECT
                // Only check for extra tokens if we're still positioned in the stream
                if (parser.currentToken() != null && parser.nextToken() != null) {
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

    @Override
    public boolean allowsCrossProject() {
        return true;
    }

    public void setProjectRouting(String projectRouting) {
        if (this.projectRouting != null) {
            throw new IllegalArgumentException("project_routing is already set to [" + this.projectRouting + "]");
        }

        this.projectRouting = projectRouting;
    }

    public String getProjectRouting() {
        return projectRouting;
    }
}

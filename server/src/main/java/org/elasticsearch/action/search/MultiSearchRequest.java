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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * A multi search API request.
 */
public class MultiSearchRequest extends ActionRequest implements CompositeIndicesRequest {

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
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            request.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiSearchRequest that = (MultiSearchRequest) o;
        return maxConcurrentSearchRequests == that.maxConcurrentSearchRequests &&
                Objects.equals(requests, that.requests) &&
                Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxConcurrentSearchRequests, requests, indicesOptions);
    }

    public static void readMultiLineFormat(BytesReference data,
                                           XContent xContent,
                                           CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer,
                                           String[] indices,
                                           IndicesOptions indicesOptions,
                                           String routing,
                                           String searchType,
                                           Boolean ccsMinimizeRoundtrips,
                                           NamedXContentRegistry registry,
                                           boolean allowExplicitIndex) throws IOException {
        int from = 0;
        byte marker = xContent.streamSeparator();
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
            if (ccsMinimizeRoundtrips != null) {
                searchRequest.setCcsMinimizeRoundtrips(ccsMinimizeRoundtrips);
            }
            IndicesOptions defaultOptions = searchRequest.indicesOptions();
            // now parse the action
            if (nextMarker - from > 0) {
                try (InputStream stream = data.slice(from, nextMarker - from).streamInput();
                     XContentParser parser = xContent.createParser(registry, LoggingDeprecationHandler.INSTANCE, stream)) {
                    Map<String, Object> source = parser.map();
                    Object expandWildcards = null;
                    Object ignoreUnavailable = null;
                    Object ignoreThrottled = null;
                    Object allowNoIndices = null;
                    for (Map.Entry<String, Object> entry : source.entrySet()) {
                        Object value = entry.getValue();
                        if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                            if (!allowExplicitIndex) {
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
                        } else {
                            throw new IllegalArgumentException("key [" + entry.getKey() + "] is not supported in the metadata section");
                        }
                    }
                    defaultOptions = IndicesOptions.fromParameters(expandWildcards, ignoreUnavailable, allowNoIndices, ignoreThrottled,
                        defaultOptions);
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
            BytesReference bytes = data.slice(from, nextMarker - from);
            try (InputStream stream = bytes.streamInput();
                 XContentParser parser = xContent.createParser(registry, LoggingDeprecationHandler.INSTANCE, stream)) {
                consumer.accept(searchRequest, parser);
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
            output.write(xContent.streamSeparator());
            try (XContentBuilder xContentBuilder = XContentBuilder.builder(xContent)) {
                if (request.source() != null) {
                    request.source().toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                } else {
                    xContentBuilder.startObject();
                    xContentBuilder.endObject();
                }
                BytesReference.bytes(xContentBuilder).writeTo(output);
            }
            output.write(xContent.streamSeparator());
        }
        return output.toByteArray();
    }

    public static void writeSearchRequestParams(SearchRequest request, XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.startObject();
        if (request.indices() != null) {
            xContentBuilder.field("index", request.indices());
        }
        if (request.indicesOptions() != null && request.indicesOptions() != SearchRequest.DEFAULT_INDICES_OPTIONS) {
            WildcardStates.toXContent(request.indicesOptions().getExpandWildcards(), xContentBuilder);
            xContentBuilder.field("ignore_unavailable", request.indicesOptions().ignoreUnavailable());
            xContentBuilder.field("allow_no_indices", request.indicesOptions().allowNoIndices());
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

}

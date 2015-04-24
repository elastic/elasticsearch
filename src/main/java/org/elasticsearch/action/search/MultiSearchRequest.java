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

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * A multi search API request.
 */
public class MultiSearchRequest extends ActionRequest<MultiSearchRequest> implements CompositeIndicesRequest {

    private List<SearchRequest> requests = Lists.newArrayList();

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

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

    public MultiSearchRequest add(byte[] data, int from, int length,
                                  @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType) throws Exception {
        return add(new BytesArray(data, from, length), indices, types, searchType, null, IndicesOptions.strictExpandOpenAndForbidClosed(), true);
    }

    public MultiSearchRequest add(BytesReference data, @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType, IndicesOptions indicesOptions) throws Exception {
        return add(data, indices, types, searchType, null, indicesOptions, true);
    }

    public MultiSearchRequest add(BytesReference data, @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType, @Nullable String routing, IndicesOptions indicesOptions, boolean allowExplicitIndex) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            // support first line with \n
            if (nextMarker == 0) {
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
            if (types != null && types.length > 0) {
                searchRequest.types(types);
            }
            if (routing != null) {
                searchRequest.routing(routing);
            }
            searchRequest.searchType(searchType);

            IndicesOptions defaultOptions = IndicesOptions.strictExpandOpenAndForbidClosed();


            // now parse the action
            if (nextMarker - from > 0) {
                try (XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from))) {
                    Map<String, Object> source = parser.map();
                    for (Map.Entry<String, Object> entry : source.entrySet()) {
                        Object value = entry.getValue();
                        if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                            if (!allowExplicitIndex) {
                                throw new ElasticsearchIllegalArgumentException("explicit index in multi percolate is not allowed");
                            }
                            searchRequest.indices(nodeStringArrayValue(value));
                        } else if ("type".equals(entry.getKey()) || "types".equals(entry.getKey())) {
                            searchRequest.types(nodeStringArrayValue(value));
                        } else if ("search_type".equals(entry.getKey()) || "searchType".equals(entry.getKey())) {
                            searchRequest.searchType(nodeStringValue(value, null));
                        } else if ("query_cache".equals(entry.getKey()) || "queryCache".equals(entry.getKey())) {
                            searchRequest.queryCache(nodeBooleanValue(value));
                        } else if ("preference".equals(entry.getKey())) {
                            searchRequest.preference(nodeStringValue(value, null));
                        } else if ("routing".equals(entry.getKey())) {
                            searchRequest.routing(nodeStringValue(value, null));
                        }
                    }
                    defaultOptions = IndicesOptions.fromMap(source, defaultOptions);
                }
            }
            searchRequest.indicesOptions(defaultOptions);

            // move pointers
            from = nextMarker + 1;
            // now for the body
            nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }

            searchRequest.source(data.slice(from, nextMarker - from));
            // move pointers
            from = nextMarker + 1;

            add(searchRequest);
        }

        return this;
    }

    private String[] parseArray(XContentParser parser) throws IOException {
        final List<String> list = new ArrayList<>();
        assert parser.currentToken() == XContentParser.Token.START_ARRAY;
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            list.add(parser.text());
        }
        return list.toArray(new String[list.size()]);
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }

    public List<SearchRequest> requests() {
        return this.requests;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            SearchRequest request = new SearchRequest();
            request.readFrom(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            request.writeTo(out);
        }
    }
}

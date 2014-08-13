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

import static org.elasticsearch.action.ValidateActions.addValidationError;

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

    public MultiSearchRequest add(byte[] data, int from, int length, boolean contentUnsafe,
                                  @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType) throws Exception {
        return add(new BytesArray(data, from, length), contentUnsafe, indices, types, searchType, null, IndicesOptions.strictExpandOpenAndForbidClosed(), true);
    }

    public MultiSearchRequest add(BytesReference data, boolean contentUnsafe, @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType, IndicesOptions indicesOptions) throws Exception {
        return add(data, contentUnsafe, indices, types, searchType, null, indicesOptions, true);
    }

    public MultiSearchRequest add(BytesReference data, boolean contentUnsafe, @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType, @Nullable String routing, IndicesOptions indicesOptions, boolean allowExplicitIndex) throws Exception {
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
            boolean ignoreUnavailable = defaultOptions.ignoreUnavailable();
            boolean allowNoIndices = defaultOptions.allowNoIndices();
            boolean expandWildcardsOpen = defaultOptions.expandWildcardsOpen();
            boolean expandWildcardsClosed = defaultOptions.expandWildcardsClosed();

            // now parse the action
            if (nextMarker - from > 0) {
                try (XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from))) {
                    // Move to START_OBJECT, if token is null, its an empty data
                    XContentParser.Token token = parser.nextToken();
                    if (token != null) {
                        assert token == XContentParser.Token.START_OBJECT;
                        String currentFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("index".equals(currentFieldName) || "indices".equals(currentFieldName)) {
                                    if (!allowExplicitIndex) {
                                        throw new ElasticsearchIllegalArgumentException("explicit index in multi search is not allowed");
                                    }
                                    searchRequest.indices(Strings.splitStringByCommaToArray(parser.text()));
                                } else if ("type".equals(currentFieldName) || "types".equals(currentFieldName)) {
                                    searchRequest.types(Strings.splitStringByCommaToArray(parser.text()));
                                } else if ("search_type".equals(currentFieldName) || "searchType".equals(currentFieldName)) {
                                    searchRequest.searchType(parser.text());
                                } else if ("query_cache".equals(currentFieldName) || "queryCache".equals(currentFieldName)) {
                                    searchRequest.queryCache(parser.booleanValue());
                                } else if ("preference".equals(currentFieldName)) {
                                    searchRequest.preference(parser.text());
                                } else if ("routing".equals(currentFieldName)) {
                                    searchRequest.routing(parser.text());
                                } else if ("ignore_unavailable".equals(currentFieldName) || "ignoreUnavailable".equals(currentFieldName)) {
                                    ignoreUnavailable = parser.booleanValue();
                                } else if ("allow_no_indices".equals(currentFieldName) || "allowNoIndices".equals(currentFieldName)) {
                                    allowNoIndices = parser.booleanValue();
                                } else if ("expand_wildcards".equals(currentFieldName) || "expandWildcards".equals(currentFieldName)) {
                                    String[] wildcards = Strings.splitStringByCommaToArray(parser.text());
                                    for (String wildcard : wildcards) {
                                        if ("open".equals(wildcard)) {
                                            expandWildcardsOpen = true;
                                        } else if ("closed".equals(wildcard)) {
                                            expandWildcardsClosed = true;
                                        } else {
                                            throw new ElasticsearchIllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
                                        }
                                    }
                                }
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                if ("index".equals(currentFieldName) || "indices".equals(currentFieldName)) {
                                    if (!allowExplicitIndex) {
                                        throw new ElasticsearchIllegalArgumentException("explicit index in multi search is not allowed");
                                    }
                                    searchRequest.indices(parseArray(parser));
                                } else if ("type".equals(currentFieldName) || "types".equals(currentFieldName)) {
                                    searchRequest.types(parseArray(parser));
                                } else if ("expand_wildcards".equals(currentFieldName) || "expandWildcards".equals(currentFieldName)) {
                                    String[] wildcards = parseArray(parser);
                                    for (String wildcard : wildcards) {
                                        if ("open".equals(wildcard)) {
                                            expandWildcardsOpen = true;
                                        } else if ("closed".equals(wildcard)) {
                                            expandWildcardsClosed = true;
                                        } else {
                                            throw new ElasticsearchIllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
                                        }
                                    }
                                } else {
                                    throw new ElasticsearchParseException(currentFieldName + " doesn't support arrays");
                                }
                            }
                        }
                    }
                }
            }
            searchRequest.indicesOptions(IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandWildcardsOpen, expandWildcardsClosed, defaultOptions));

            // move pointers
            from = nextMarker + 1;
            // now for the body
            nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }

            searchRequest.source(data.slice(from, nextMarker - from), contentUnsafe);
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

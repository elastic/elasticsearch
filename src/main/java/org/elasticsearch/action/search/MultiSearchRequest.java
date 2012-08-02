/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
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
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A multi search API request.
 */
public class MultiSearchRequest implements ActionRequest {

    private List<SearchRequest> requests = Lists.newArrayList();

    private boolean listenerThreaded = false;

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
        return add(new BytesArray(data, from, length), contentUnsafe, indices, types, searchType);
    }

    public MultiSearchRequest add(BytesReference data, boolean contentUnsafe,
                                  @Nullable String[] indices, @Nullable String[] types, @Nullable String searchType) throws Exception {
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

            SearchRequest searchRequest = new SearchRequest(indices);
            if (types != null && types.length > 0) {
                searchRequest.types(types);
            }
            searchRequest.searchType(searchType);

            // now parse the action
            if (nextMarker - from > 0) {
                XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from));
                try {
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
                                    searchRequest.indices(Strings.splitStringByCommaToArray(parser.text()));
                                } else if ("type".equals(currentFieldName) || "types".equals(currentFieldName)) {
                                    searchRequest.types(Strings.splitStringByCommaToArray(parser.text()));
                                } else if ("search_type".equals(currentFieldName) || "searchType".equals(currentFieldName)) {
                                    searchRequest.searchType(parser.text());
                                } else if ("preference".equals(currentFieldName)) {
                                    searchRequest.preference(parser.text());
                                } else if ("routing".equals(currentFieldName)) {
                                    searchRequest.routing(parser.text());
                                } else if ("query_hint".equals(currentFieldName) || "queryHint".equals(currentFieldName)) {
                                    searchRequest.queryHint(parser.text());
                                }
                            }
                        }
                    }
                } finally {
                    parser.close();
                }
            }

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

    @Override
    public boolean listenerThreaded() {
        return listenerThreaded;
    }

    @Override
    public MultiSearchRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            SearchRequest request = new SearchRequest();
            request.readFrom(in);
            requests.add(request);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(requests.size());
        for (SearchRequest request : requests) {
            request.writeTo(out);
        }
    }
}

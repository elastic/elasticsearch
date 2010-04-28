/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
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

import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.util.Bytes;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.Actions.*;
import static org.elasticsearch.search.Scroll.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link org.elasticsearch.client.Requests#searchRequest(String...)}.
 *
 * <p>Note, the search {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including facets and such.
 *
 * <p>There is an option to specify an addition search source using the {@link #extraSource(org.elasticsearch.search.builder.SearchSourceBuilder)}.
 *
 * @author kimchy (shay.banon)
 * @see org.elasticsearch.client.Requests#searchRequest(String...)
 * @see org.elasticsearch.client.Client#search(SearchRequest)
 * @see SearchResponse
 */
public class SearchRequest implements ActionRequest {

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices;

    private String queryHint;

    private byte[] source;

    private byte[] extraSource;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    private TimeValue timeout;

    private boolean listenerThreaded = false;
    private SearchOperationThreading operationThreading = SearchOperationThreading.SINGLE_THREAD;

    SearchRequest() {
    }

    /**
     * Constructs a new search request against the indices. No indices provided here means that search
     * will run against all indices.
     */
    public SearchRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     */
    public SearchRequest(String[] indices, byte[] source) {
        this.indices = indices;
        this.source = source;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null && extraSource == null) {
            validationException = addValidationError("search source is missing", validationException);
        }
        return validationException;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public boolean listenerThreaded() {
        return listenerThreaded;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public SearchRequest listenerThreaded(boolean listenerThreaded) {
        this.listenerThreaded = listenerThreaded;
        return this;
    }

    /**
     * Controls the the search operation threading model.
     */
    public SearchOperationThreading operationThreading() {
        return this.operationThreading;
    }

    /**
     * Controls the the search operation threading model.
     */
    public SearchRequest operationThreading(SearchOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return this;
    }

    /**
     * Sets the string representation of the operation threading model. Can be one of
     * "no_threads", "single_thread" and "thread_per_shard".
     */
    public SearchRequest operationThreading(String operationThreading) {
        return operationThreading(SearchOperationThreading.fromString(operationThreading, this.operationThreading));
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public String[] types() {
        return types;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public SearchRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public SearchRequest searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequest searchType(String searchType) throws ElasticSearchIllegalArgumentException {
        return searchType(SearchType.fromString(searchType));
    }

    /**
     * The source of the search request.
     */
    public SearchRequest source(SearchSourceBuilder sourceBuilder) {
        return source(sourceBuilder.build());
    }

    /**
     * The source of the search request. Consider using either {@link #source(byte[])} or
     * {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}.
     */
    public SearchRequest source(String source) {
        return source(Unicode.fromStringAsBytes(source));
    }

    /**
     * The source of the search request in the form of a map.
     */
    public SearchRequest source(Map source) {
        try {
            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(XContentType.JSON);
            builder.map(source);
            this.source = builder.copiedBytes();
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source) {
        this.source = source;
        return this;
    }

    /**
     * The search source to execute.
     */
    public byte[] source() {
        return source;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(SearchSourceBuilder sourceBuilder) {
        return extraSource(sourceBuilder.build());
    }

    public SearchRequest extraSource(Map extraSource) {
        try {
            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(XContentType.JSON);
            builder.map(extraSource);
            this.extraSource = builder.copiedBytes();
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Allows to provide additional source that will use used as well.
     */
    public SearchRequest extraSource(String source) {
        return extraSource(Unicode.fromStringAsBytes(source));
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source) {
        this.extraSource = source;
        return this;
    }

    /**
     * Additional search source to execute.
     */
    public byte[] extraSource() {
        return this.extraSource;
    }

    /**
     * The tye of search to execute.
     */
    public SearchType searchType() {
        return searchType;
    }

    /**
     * The indices
     */
    public String[] indices() {
        return indices;
    }

    /**
     * A query hint to optionally later be used when routing the request.
     */
    public SearchRequest queryHint(String queryHint) {
        this.queryHint = queryHint;
        return this;
    }

    /**
     * A query hint to optionally later be used when routing the request.
     */
    public String queryHint() {
        return queryHint;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public Scroll scroll() {
        return scroll;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest scroll(TimeValue keepAlive) {
        return scroll(new Scroll(keepAlive));
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public TimeValue timeout() {
        return timeout;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public SearchRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        operationThreading = SearchOperationThreading.fromId(in.readByte());
        searchType = SearchType.fromId(in.readByte());

        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readUTF();
        }

        if (in.readBoolean()) {
            queryHint = in.readUTF();
        }

        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
        if (in.readBoolean()) {
            timeout = readTimeValue(in);
        }
        int size = in.readVInt();
        if (size == 0) {
            source = Bytes.EMPTY_ARRAY;
        } else {
            source = new byte[size];
            in.readFully(source);
        }
        size = in.readVInt();
        if (size == 0) {
            extraSource = Bytes.EMPTY_ARRAY;
        } else {
            extraSource = new byte[size];
            in.readFully(extraSource);
        }

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(operationThreading.id());
        out.writeByte(searchType.id());

        out.writeVInt(indices.length);
        for (String index : indices) {
            out.writeUTF(index);
        }

        if (queryHint == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryHint);
        }

        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        if (timeout == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            timeout.writeTo(out);
        }
        if (source == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(source.length);
            out.writeBytes(source);
        }
        if (extraSource == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(extraSource.length);
            out.writeBytes(extraSource);
        }
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}

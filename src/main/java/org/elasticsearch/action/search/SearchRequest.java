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

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link org.elasticsearch.client.Requests#searchRequest(String...)}.
 * <p/>
 * <p>Note, the search {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including facets and such.
 * <p/>
 * <p>There is an option to specify an addition search source using the {@link #extraSource(org.elasticsearch.search.builder.SearchSourceBuilder)}.
 *
 * @see org.elasticsearch.client.Requests#searchRequest(String...)
 * @see org.elasticsearch.client.Client#search(SearchRequest)
 * @see SearchResponse
 */
public class SearchRequest implements ActionRequest {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices;

    @Nullable
    private String queryHint;
    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private byte[] source;
    private int sourceOffset;
    private int sourceLength;
    private boolean sourceUnsafe;

    private byte[] extraSource;
    private int extraSourceOffset;
    private int extraSourceLength;
    private boolean extraSourceUnsafe;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    private boolean listenerThreaded = false;
    private SearchOperationThreading operationThreading = SearchOperationThreading.THREAD_PER_SHARD;

    public SearchRequest() {
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

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        // no need to check, we resolve to match all query
//        if (source == null && extraSource == null) {
//            validationException = addValidationError("search source is missing", validationException);
//        }
        return validationException;
    }

    public void beforeStart() {
        // we always copy over if needed, the reason is that a request might fail while being search remotely
        // and then we need to keep the buffer around
        if (source != null && sourceUnsafe) {
            source = Arrays.copyOfRange(source, sourceOffset, sourceOffset + sourceLength);
            sourceOffset = 0;
            sourceUnsafe = false;
        }
        if (extraSource != null && extraSourceUnsafe) {
            extraSource = Arrays.copyOfRange(extraSource, extraSourceOffset, extraSourceOffset + extraSourceLength);
            extraSourceOffset = 0;
            extraSourceUnsafe = false;
        }
    }

    /**
     * Internal.
     */
    public void beforeLocalFork() {
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override
    public boolean listenerThreaded() {
        return listenerThreaded;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override
    public SearchRequest listenerThreaded(boolean listenerThreaded) {
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
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SearchRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SearchRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public SearchRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
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
        BytesStream bos = sourceBuilder.buildAsBytesStream(Requests.CONTENT_TYPE);
        this.source = bos.underlyingBytes();
        this.sourceOffset = 0;
        this.sourceLength = bos.size();
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source of the search request. Consider using either {@link #source(byte[])} or
     * {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}.
     */
    public SearchRequest source(String source) {
        UnicodeUtil.UTF8Result result = Unicode.fromStringAsUtf8(source);
        this.source = result.result;
        this.sourceOffset = 0;
        this.sourceLength = result.length;
        this.sourceUnsafe = true;
        return this;
    }

    /**
     * The source of the search request in the form of a map.
     */
    public SearchRequest source(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public SearchRequest source(XContentBuilder builder) {
        try {
            this.source = builder.underlyingBytes();
            this.sourceOffset = 0;
            this.sourceLength = builder.underlyingBytesLength();
            this.sourceUnsafe = false;
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + builder + "]", e);
        }
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source) {
        return source(source, 0, source.length, false);
    }


    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source, int offset, int length, boolean unsafe) {
        this.source = source;
        this.sourceOffset = offset;
        this.sourceLength = length;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * The search source to execute.
     */
    public byte[] source() {
        return source;
    }

    public int sourceOffset() {
        return sourceOffset;
    }

    public int sourceLength() {
        return sourceLength;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(SearchSourceBuilder sourceBuilder) {
        if (sourceBuilder == null) {
            extraSource = null;
            return this;
        }
        BytesStream bos = sourceBuilder.buildAsBytesStream(Requests.CONTENT_TYPE);
        this.extraSource = bos.underlyingBytes();
        this.extraSourceOffset = 0;
        this.extraSourceLength = bos.size();
        this.extraSourceUnsafe = false;
        return this;
    }

    public SearchRequest extraSource(Map extraSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(extraSource);
            return extraSource(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public SearchRequest extraSource(XContentBuilder builder) {
        try {
            this.extraSource = builder.underlyingBytes();
            this.extraSourceOffset = 0;
            this.extraSourceLength = builder.underlyingBytesLength();
            this.extraSourceUnsafe = false;
            return this;
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + builder + "]", e);
        }
    }

    /**
     * Allows to provide additional source that will use used as well.
     */
    public SearchRequest extraSource(String source) {
        UnicodeUtil.UTF8Result result = Unicode.fromStringAsUtf8(source);
        this.extraSource = result.result;
        this.extraSourceOffset = 0;
        this.extraSourceLength = result.length;
        this.extraSourceUnsafe = true;
        return this;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source) {
        return extraSource(source, 0, source.length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source, int offset, int length) {
        return extraSource(source, offset, length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source, int offset, int length, boolean unsafe) {
        this.extraSource = source;
        this.extraSourceOffset = offset;
        this.extraSourceLength = length;
        this.extraSourceUnsafe = unsafe;
        return this;
    }

    /**
     * Additional search source to execute.
     */
    public byte[] extraSource() {
        return this.extraSource;
    }

    public int extraSourceOffset() {
        return extraSourceOffset;
    }

    public int extraSourceLength() {
        return extraSourceLength;
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
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest scroll(String keepAlive) {
        return scroll(new Scroll(TimeValue.parseTimeValue(keepAlive, null)));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
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
            routing = in.readUTF();
        }
        if (in.readBoolean()) {
            preference = in.readUTF();
        }

        if (in.readBoolean()) {
            scroll = readScroll(in);
        }

        BytesHolder bytes = in.readBytesReference();
        sourceUnsafe = false;
        source = bytes.bytes();
        sourceOffset = bytes.offset();
        sourceLength = bytes.length();

        bytes = in.readBytesReference();
        extraSourceUnsafe = false;
        extraSource = bytes.bytes();
        extraSourceOffset = bytes.offset();
        extraSourceLength = bytes.length();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        if (routing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(routing);
        }
        if (preference == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(preference);
        }

        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        out.writeBytesHolder(source, sourceOffset, sourceLength);
        out.writeBytesHolder(extraSource, extraSourceOffset, extraSourceLength);
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}

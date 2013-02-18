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
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link org.elasticsearch.client.Requests#searchRequest(String...)}.
 * <p/>
 * <p>Note, the search {@link #setSource(org.elasticsearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including facets and such.
 * <p/>
 * <p>There is an option to specify an addition search source using the {@link #setExtraSource(org.elasticsearch.search.builder.SearchSourceBuilder)}.
 *
 * @see org.elasticsearch.client.Requests#searchRequest(String...)
 * @see org.elasticsearch.client.Client#search(SearchRequest)
 * @see SearchResponse
 */
public class SearchRequest extends ActionRequest<SearchRequest> {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private BytesReference source;
    private boolean sourceUnsafe;

    private BytesReference extraSource;
    private boolean extraSourceUnsafe;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    private SearchOperationThreading operationThreading = SearchOperationThreading.THREAD_PER_SHARD;

    private IgnoreIndices ignoreIndices = IgnoreIndices.DEFAULT;

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
        this.source = new BytesArray(source);
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
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
        if (extraSource != null && extraSourceUnsafe) {
            extraSource = extraSource.copyBytesArray();
            extraSourceUnsafe = false;
        }
    }

    /**
     * Internal.
     */
    public void beforeLocalFork() {
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequest setIndices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Controls the the search operation threading model.
     */
    public SearchOperationThreading getOperationThreading() {
        return this.operationThreading;
    }

    /**
     * Controls the the search operation threading model.
     */
    public SearchRequest setOperationThreading(SearchOperationThreading operationThreading) {
        this.operationThreading = operationThreading;
        return this;
    }

    /**
     * Sets the string representation of the operation threading model. Can be one of
     * "no_threads", "single_thread" and "thread_per_shard".
     */
    public SearchRequest setOperationThreading(String operationThreading) {
        return setOperationThreading(SearchOperationThreading.fromString(operationThreading, this.operationThreading));
    }

    public IgnoreIndices getIgnoreIndices() {
        return ignoreIndices;
    }

    public SearchRequest setIgnoreIndices(IgnoreIndices ignoreIndices) {
        this.ignoreIndices = ignoreIndices;
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public String[] getTypes() {
        return types;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public SearchRequest setTypes(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String getRouting() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SearchRequest setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SearchRequest setRouting(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public SearchRequest setPreference(String preference) {
        this.preference = preference;
        return this;
    }

    public String getPreference() {
        return this.preference;
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public SearchRequest setSearchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequest setSearchType(String searchType) throws ElasticSearchIllegalArgumentException {
        return setSearchType(SearchType.fromString(searchType));
    }

    /**
     * The source of the search request.
     */
    public SearchRequest setSource(SearchSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(contentType);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source of the search request. Consider using either {@link #setSource(byte[])} or
     * {@link #setSource(org.elasticsearch.search.builder.SearchSourceBuilder)}.
     */
    public SearchRequest setSource(String source) {
        this.source = new BytesArray(source);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source of the search request in the form of a map.
     */
    public SearchRequest setSource(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return setSource(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public SearchRequest setSource(XContentBuilder builder) {
        this.source = builder.bytes();
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The search source to execute.
     */
    public SearchRequest setSource(byte[] source) {
        return setSource(source, 0, source.length, false);
    }


    /**
     * The search source to execute.
     */
    public SearchRequest setSource(byte[] source, int offset, int length) {
        return setSource(source, offset, length, false);
    }

    /**
     * The search source to execute.
     */
    public SearchRequest setSource(byte[] source, int offset, int length, boolean unsafe) {
        return setSource(new BytesArray(source, offset, length), unsafe);
    }

    /**
     * The search source to execute.
     */
    public SearchRequest setSource(BytesReference source, boolean unsafe) {
        this.source = source;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * The search source to execute.
     */
    public BytesReference getSource() {
        return source;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest setExtraSource(SearchSourceBuilder sourceBuilder) {
        if (sourceBuilder == null) {
            extraSource = null;
            return this;
        }
        this.extraSource = sourceBuilder.buildAsBytes(contentType);
        this.extraSourceUnsafe = false;
        return this;
    }

    public SearchRequest setExtraSource(Map extraSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(extraSource);
            return setExtraSource(builder);
        } catch (IOException e) {
            throw new ElasticSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public SearchRequest setExtraSource(XContentBuilder builder) {
        this.extraSource = builder.bytes();
        this.extraSourceUnsafe = false;
        return this;
    }

    /**
     * Allows to provide additional source that will use used as well.
     */
    public SearchRequest setExtraSource(String source) {
        this.extraSource = new BytesArray(source);
        this.extraSourceUnsafe = false;
        return this;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest setExtraSource(byte[] source) {
        return setExtraSource(source, 0, source.length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest setExtraSource(byte[] source, int offset, int length) {
        return setExtraSource(source, offset, length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest setExtraSource(byte[] source, int offset, int length, boolean unsafe) {
        return setExtraSource(new BytesArray(source, offset, length), unsafe);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest setExtraSource(BytesReference source, boolean unsafe) {
        this.extraSource = source;
        this.extraSourceUnsafe = unsafe;
        return this;
    }

    /**
     * Additional search source to execute.
     */
    public BytesReference getExtraSource() {
        return this.extraSource;
    }

    /**
     * The tye of search to execute.
     */
    public SearchType getSearchType() {
        return searchType;
    }

    /**
     * The indices
     */
    public String[] getIndices() {
        return indices;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public Scroll getScroll() {
        return scroll;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchRequest setScroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest setScroll(TimeValue keepAlive) {
        return setScroll(new Scroll(keepAlive));
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest setScroll(String keepAlive) {
        return setScroll(new Scroll(TimeValue.parseTimeValue(keepAlive, null)));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        operationThreading = SearchOperationThreading.fromId(in.readByte());
        searchType = SearchType.fromId(in.readByte());

        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readString();
        }

        routing = in.readOptionalString();
        preference = in.readOptionalString();

        if (in.readBoolean()) {
            scroll = readScroll(in);
        }

        sourceUnsafe = false;
        source = in.readBytesReference();

        extraSourceUnsafe = false;
        extraSource = in.readBytesReference();

        types = in.readStringArray();
        ignoreIndices = IgnoreIndices.fromId(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(operationThreading.id());
        out.writeByte(searchType.id());

        out.writeVInt(indices.length);
        for (String index : indices) {
            out.writeString(index);
        }

        out.writeOptionalString(routing);
        out.writeOptionalString(preference);

        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        out.writeBytesReference(source);
        out.writeBytesReference(extraSource);
        out.writeStringArray(types);
        out.writeByte(ignoreIndices.id());
    }
}

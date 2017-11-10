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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link org.elasticsearch.client.Requests#searchRequest(String...)}.
 * <p>
 * Note, the search {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including aggregations and such.
 * </p>
 *
 * @see org.elasticsearch.client.Requests#searchRequest(String...)
 * @see org.elasticsearch.client.Client#search(SearchRequest)
 * @see SearchResponse
 */
public final class SearchRequest extends ActionRequest implements IndicesRequest.Replaceable {
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(SearchRequest.class));

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 128;

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices = Strings.EMPTY_ARRAY;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private SearchSourceBuilder source;

    private Boolean requestCache;

    private Scroll scroll;

    private int batchedReduceSize = 512;

    private int maxConcurrentShardRequests = 0;

    private int preFilterShardSize = DEFAULT_PRE_FILTER_SHARD_SIZE;

    private String[] types = Strings.EMPTY_ARRAY;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public SearchRequest() {
    }

    /**
     * Constructs a new search request against the indices. No indices provided here means that search
     * will run against all indices.
     */
    public SearchRequest(String... indices) {
        this(indices, new SearchSourceBuilder());
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     */
    public SearchRequest(String[] indices, SearchSourceBuilder source) {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        indices(indices);
        this.source = source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source != null && source.trackTotalHits() == false && scroll() != null) {
            validationException =
                addValidationError("disabling [track_total_hits] is not allowed in a scroll context", validationException);
        }
        if (source != null && source.from() > 0 &&  scroll() != null) {
            validationException =
                addValidationError("using [from] is not allowed in a scroll context", validationException);
        }
        if (requestCache != null && requestCache && scroll() != null) {
            DEPRECATION_LOGGER.deprecated("Explicitly set [request_cache] for a scroll query is deprecated and will return a 400 " +
                "error in future versions");
        }
        return validationException;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public SearchRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public SearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        return this;
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
        Objects.requireNonNull(types, "types must not be null");
        for (String type : types) {
            Objects.requireNonNull(type, "type must not be null");
        }
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
        this.searchType = Objects.requireNonNull(searchType, "searchType must not be null");
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequest searchType(String searchType) {
        return searchType(SearchType.fromString(searchType));
    }

    /**
     * The source of the search request.
     */
    public SearchRequest source(SearchSourceBuilder sourceBuilder) {
        this.source = Objects.requireNonNull(sourceBuilder, "source must not be null");
        return this;
    }

    /**
     * The search source to execute.
     */
    public SearchSourceBuilder source() {
        return source;
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
    @Override
    public String[] indices() {
        return indices;
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
        return scroll(new Scroll(TimeValue.parseTimeValue(keepAlive, null, getClass().getSimpleName() + ".Scroll.keepAlive")));
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if request cache is enabled or not).
     */
    public SearchRequest requestCache(Boolean requestCache) {
        this.requestCache = requestCache;
        return this;
    }

    public Boolean requestCache() {
        return this.requestCache;
    }

    /**
     * Sets the number of shard results that should be reduced at once on the coordinating node. This value should be used as a protection
     * mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public void setBatchedReduceSize(int batchedReduceSize) {
        if (batchedReduceSize <= 1) {
            throw new IllegalArgumentException("batchedReduceSize must be >= 2");
        }
        this.batchedReduceSize = batchedReduceSize;
    }

    /**
     * Returns the number of shard results that should be reduced at once on the coordinating node. This value should be used as a
     * protection mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public int getBatchedReduceSize() {
        return batchedReduceSize;
    }

    /**
     * Returns the number of shard requests that should be executed concurrently. This value should be used as a protection mechanism to
     * reduce the number of shard reqeusts fired per high level search request. Searches that hit the entire cluster can be throttled
     * with this number to reduce the cluster load. The default grows with the number of nodes in the cluster but is at most <tt>256</tt>.
     */
    public int getMaxConcurrentShardRequests() {
        return maxConcurrentShardRequests == 0 ? 256 : maxConcurrentShardRequests;
    }

    /**
     * Sets the number of shard requests that should be executed concurrently. This value should be used as a protection mechanism to
     * reduce the number of shard requests fired per high level search request. Searches that hit the entire cluster can be throttled
     * with this number to reduce the cluster load. The default grows with the number of nodes in the cluster but is at most <tt>256</tt>.
     */
    public void setMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
        if (maxConcurrentShardRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentShardRequests must be >= 1");
        }
        this.maxConcurrentShardRequests = maxConcurrentShardRequests;
    }
    /**
     * Sets a threshold that enforces a pre-filter roundtrip to pre-filter search shards based on query rewriting if the number of shards
     * the search request expands to exceeds the threshold. This filter roundtrip can limit the number of shards significantly if for
     * instance a shard can not match any documents based on it's rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint. The default is <tt>128</tt>
     */
    public void setPreFilterShardSize(int preFilterShardSize) {
        if (preFilterShardSize < 1) {
            throw new IllegalArgumentException("preFilterShardSize must be >= 1");
        }
        this.preFilterShardSize = preFilterShardSize;
    }

    /**
     * Returns a threshold that enforces a pre-filter roundtrip to pre-filter search shards based on query rewriting if the number of shards
     * the search request expands to exceeds the threshold. This filter roundtrip can limit the number of shards significantly if for
     * instance a shard can not match any documents based on it's rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint. The default is <tt>128</tt>
     */
    public int getPreFilterShardSize() {
        return preFilterShardSize;
    }

    /**
     * Returns <code>true</code> iff the maxConcurrentShardRequest is set.
     */
    boolean isMaxConcurrentShardRequestsSet() {
        return maxConcurrentShardRequests != 0;
    }

    /**
     * @return true if the request only has suggest
     */
    public boolean isSuggestOnly() {
        return source != null && source.isSuggestOnly();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        // generating description in a lazy way since source can be quite big
        return new SearchTask(id, type, action, null, parentTaskId) {
            @Override
            public String getDescription() {
                StringBuilder sb = new StringBuilder();
                sb.append("indices[");
                Strings.arrayToDelimitedString(indices, ",", sb);
                sb.append("], ");
                sb.append("types[");
                Strings.arrayToDelimitedString(types, ",", sb);
                sb.append("], ");
                sb.append("search_type[").append(searchType).append("], ");
                if (source != null) {

                    sb.append("source[").append(source.toString(FORMAT_PARAMS)).append("]");
                } else {
                    sb.append("source[]");
                }
                return sb.toString();
            }
        };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        searchType = SearchType.fromId(in.readByte());
        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readString();
        }
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        requestCache = in.readOptionalBoolean();
        batchedReduceSize = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            maxConcurrentShardRequests = in.readVInt();
            preFilterShardSize = in.readVInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(searchType.id());
        out.writeVInt(indices.length);
        for (String index : indices) {
            out.writeString(index);
        }
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        out.writeStringArray(types);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalBoolean(requestCache);
        out.writeVInt(batchedReduceSize);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeVInt(maxConcurrentShardRequests);
            out.writeVInt(preFilterShardSize);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchRequest that = (SearchRequest) o;
        return searchType == that.searchType &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(routing, that.routing) &&
                Objects.equals(preference, that.preference) &&
                Objects.equals(source, that.source) &&
                Objects.equals(requestCache, that.requestCache)  &&
                Objects.equals(scroll, that.scroll) &&
                Arrays.equals(types, that.types) &&
                Objects.equals(batchedReduceSize, that.batchedReduceSize) &&
                Objects.equals(maxConcurrentShardRequests, that.maxConcurrentShardRequests) &&
                Objects.equals(preFilterShardSize, that.preFilterShardSize) &&
                Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchType, Arrays.hashCode(indices), routing, preference, source, requestCache,
                scroll, Arrays.hashCode(types), indicesOptions, batchedReduceSize, maxConcurrentShardRequests, preFilterShardSize);
    }

    @Override
    public String toString() {
        return "SearchRequest{" +
                "searchType=" + searchType +
                ", indices=" + Arrays.toString(indices) +
                ", indicesOptions=" + indicesOptions +
                ", types=" + Arrays.toString(types) +
                ", routing='" + routing + '\'' +
                ", preference='" + preference + '\'' +
                ", requestCache=" + requestCache +
                ", scroll=" + scroll +
                ", maxConcurrentShardRequests=" + maxConcurrentShardRequests +
                ", batchedReduceSize=" + batchedReduceSize +
                ", preFilterShardSize=" + preFilterShardSize +
                ", source=" + source + '}';
    }
}

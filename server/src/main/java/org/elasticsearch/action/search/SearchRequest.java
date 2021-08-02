/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public class SearchRequest extends ActionRequest implements IndicesRequest.Replaceable, Rewriteable<SearchRequest> {

    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 128;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 512;

    private static final long DEFAULT_ABSOLUTE_START_MILLIS = -1;

    private final String localClusterAlias;
    private final long absoluteStartMillis;
    private final boolean finalReduce;

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices = Strings.EMPTY_ARRAY;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private SearchSourceBuilder source;

    private Boolean requestCache;

    private Boolean allowPartialSearchResults;

    private Scroll scroll;

    private int batchedReduceSize = DEFAULT_BATCHED_REDUCE_SIZE;

    private int maxConcurrentShardRequests = 0;

    private Integer preFilterShardSize;

    private String[] types = Strings.EMPTY_ARRAY;
    private boolean ccsMinimizeRoundtrips;
    private boolean enableFieldsEmulation = false;

    @Nullable
    private final Version minCompatibleShardNode;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS =
        IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();

    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public SearchRequest() {
        this((Version) null);
    }

    public SearchRequest(Version minCompatibleShardNode) {
        this.localClusterAlias = null;
        this.absoluteStartMillis = DEFAULT_ABSOLUTE_START_MILLIS;
        this.finalReduce = true;
        this.minCompatibleShardNode = minCompatibleShardNode;
        this.ccsMinimizeRoundtrips = minCompatibleShardNode == null;
    }

    /**
     * Constructs a new search request from the provided search request
     */
    public SearchRequest(SearchRequest searchRequest) {
        this(searchRequest, searchRequest.indices, searchRequest.localClusterAlias,
            searchRequest.absoluteStartMillis, searchRequest.finalReduce);
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
        this();
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        indices(indices);
        this.source = source;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    /**
     * Creates a new sub-search request starting from the original search request that is provided.
     * For internal use only, allows to fork a search request into multiple search requests that will be executed independently.
     * Such requests will not be finally reduced, so that their results can be merged together in one response at completion.
     * Used when a {@link SearchRequest} is created and executed as part of a cross-cluster search request
     * performing reduction on each cluster in order to minimize network round-trips between the coordinating node and the remote clusters.
     *
     * @param parentTaskId the parent taskId of the original search request
     * @param originalSearchRequest the original search request
     * @param indices the indices to search against
     * @param clusterAlias the alias to prefix index names with in the returned search results
     * @param absoluteStartMillis the absolute start time to be used on the remote clusters to ensure that the same value is used
     * @param finalReduce whether the reduction should be final or not
     */
    static SearchRequest subSearchRequest(TaskId parentTaskId, SearchRequest originalSearchRequest, String[] indices,
                                          String clusterAlias, long absoluteStartMillis, boolean finalReduce) {
        Objects.requireNonNull(parentTaskId, "parentTaskId must be specified");
        Objects.requireNonNull(originalSearchRequest, "search request must not be null");
        validateIndices(indices);
        Objects.requireNonNull(clusterAlias, "cluster alias must not be null");
        if (absoluteStartMillis < 0) {
            throw new IllegalArgumentException("absoluteStartMillis must not be negative but was [" + absoluteStartMillis + "]");
        }
        final SearchRequest request = new SearchRequest(originalSearchRequest, indices, clusterAlias, absoluteStartMillis, finalReduce);
        request.setParentTask(parentTaskId);
        return request;
    }

    private SearchRequest(SearchRequest searchRequest, String[] indices, String localClusterAlias, long absoluteStartMillis,
                          boolean finalReduce) {
        this.allowPartialSearchResults = searchRequest.allowPartialSearchResults;
        this.batchedReduceSize = searchRequest.batchedReduceSize;
        this.ccsMinimizeRoundtrips = searchRequest.ccsMinimizeRoundtrips;
        this.indices = indices;
        this.indicesOptions = searchRequest.indicesOptions;
        this.maxConcurrentShardRequests = searchRequest.maxConcurrentShardRequests;
        this.preference = searchRequest.preference;
        this.preFilterShardSize = searchRequest.preFilterShardSize;
        this.requestCache = searchRequest.requestCache;
        this.routing = searchRequest.routing;
        this.scroll = searchRequest.scroll;
        this.searchType = searchRequest.searchType;
        this.source = searchRequest.source;
        this.types = searchRequest.types;
        this.localClusterAlias = localClusterAlias;
        this.absoluteStartMillis = absoluteStartMillis;
        this.finalReduce = finalReduce;
        this.minCompatibleShardNode = searchRequest.minCompatibleShardNode;
        this.enableFieldsEmulation = searchRequest.enableFieldsEmulation;
    }

    /**
     * Constructs a new search request from reading the specified stream.
     *
     * @param in The stream the request is read from
     * @throws IOException if there is an issue reading the stream
     */
    public SearchRequest(StreamInput in) throws IOException {
        super(in);
        searchType = SearchType.fromId(in.readByte());
        indices = in.readStringArray();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        requestCache = in.readOptionalBoolean();
        batchedReduceSize = in.readVInt();
        maxConcurrentShardRequests = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            preFilterShardSize = in.readOptionalVInt();
        } else {
            preFilterShardSize = in.readVInt();
        }
        if (in.getVersion().onOrAfter(Version.V_6_3_0)) {
            allowPartialSearchResults = in.readOptionalBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) {
            localClusterAlias = in.readOptionalString();
            if (localClusterAlias != null) {
                absoluteStartMillis = in.readVLong();
                finalReduce = in.readBoolean();
            } else {
                absoluteStartMillis = DEFAULT_ABSOLUTE_START_MILLIS;
                finalReduce = true;
            }
        } else {
            localClusterAlias = null;
            absoluteStartMillis = DEFAULT_ABSOLUTE_START_MILLIS;
            finalReduce = true;
        }
        if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
            ccsMinimizeRoundtrips = in.readBoolean();
        } else {
            ccsMinimizeRoundtrips = true;
        }
        if (in.getVersion().onOrAfter(Version.V_7_12_0) && in.readBoolean()) {
            minCompatibleShardNode = Version.readVersion(in);
        } else {
            minCompatibleShardNode = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_15_0)) {
            this.enableFieldsEmulation = in.readBoolean();
        } else {
            this.enableFieldsEmulation = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(searchType.id());
        out.writeStringArray(indices);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        out.writeStringArray(types);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalBoolean(requestCache);
        out.writeVInt(batchedReduceSize);
        out.writeVInt(maxConcurrentShardRequests);
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeOptionalVInt(preFilterShardSize);
        } else {
            out.writeVInt(preFilterShardSize == null ? DEFAULT_BATCHED_REDUCE_SIZE : preFilterShardSize);
        }
        if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
            out.writeOptionalBoolean(allowPartialSearchResults);
        }
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
            out.writeOptionalString(localClusterAlias);
            if (localClusterAlias != null) {
                out.writeVLong(absoluteStartMillis);
                out.writeBoolean(finalReduce);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeBoolean(ccsMinimizeRoundtrips);
        }
        if (out.getVersion().onOrAfter(Version.V_7_12_0)) {
            out.writeBoolean(minCompatibleShardNode != null);
            if (minCompatibleShardNode != null) {
                Version.writeVersion(minCompatibleShardNode, out);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_7_15_0)) {
            out.writeBoolean(enableFieldsEmulation);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        boolean scroll = scroll() != null;
        if (scroll) {
            if (source != null) {
                if (source.trackTotalHitsUpTo() != null && source.trackTotalHitsUpTo() != SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                    validationException =
                        addValidationError("disabling [track_total_hits] is not allowed in a scroll context", validationException);
                }
                if (source.from() > 0) {
                    validationException =
                        addValidationError("using [from] is not allowed in a scroll context", validationException);
                }
                if (source.size() == 0) {
                    validationException = addValidationError("[size] cannot be [0] in a scroll context", validationException);
                }
                if (source.rescores() != null && source.rescores().isEmpty() == false) {
                    validationException =
                        addValidationError("using [rescore] is not allowed in a scroll context", validationException);
                }
            }
            if (requestCache != null && requestCache) {
                validationException =
                    addValidationError("[request_cache] cannot be used in a scroll context", validationException);
            }
        }
        if (source != null) {
            if (source.aggregations() != null) {
                validationException = source.aggregations().validate(validationException);
            }
        }
        if (pointInTimeBuilder() != null) {
            if (scroll) {
                validationException = addValidationError("using [point in time] is not allowed in a scroll context", validationException);
            }
        } else if (source != null && source.sorts() != null) {
            for (SortBuilder<?> sortBuilder : source.sorts()) {
                if (sortBuilder instanceof FieldSortBuilder
                        && ShardDocSortField.NAME.equals(((FieldSortBuilder) sortBuilder).getFieldName())) {
                    validationException = addValidationError("[" + FieldSortBuilder.SHARD_DOC_FIELD_NAME
                        + "] sort field cannot be used without [point in time]", validationException);
                }
            }
        }
        if (minCompatibleShardNode() != null) {
            if (isCcsMinimizeRoundtrips()) {
                validationException = addValidationError("[ccs_minimize_roundtrips] cannot be [true] when setting a minimum compatible "
                    + "shard version", validationException);
            }
        }
        return validationException;
    }

    /**
     * Returns the alias of the cluster that this search request is being executed on. A non-null value indicates that this search request
     * is being executed as part of a locally reduced cross-cluster search request. The cluster alias is used to prefix index names
     * returned as part of search hits with the alias of the cluster they came from.
     */
    @Nullable
    String getLocalClusterAlias() {
        return localClusterAlias;
    }

    /**
     * Returns whether the reduction phase that will be performed needs to be final or not.
     */
    boolean isFinalReduce() {
        return finalReduce;
    }

    /**
     * Returns the current time in milliseconds from the time epoch, to be used for the execution of this search request. Used to
     * ensure that the same value, determined by the coordinating node, is used on all nodes involved in the execution of the search
     * request. When created through {@link #subSearchRequest(TaskId, SearchRequest, String[], String, long, boolean)}, this method returns
     * the provided current time, otherwise it will return {@link System#currentTimeMillis()}.
     */
    long getOrCreateAbsoluteStartMillis() {
        return absoluteStartMillis == DEFAULT_ABSOLUTE_START_MILLIS ? System.currentTimeMillis() : absoluteStartMillis;
    }

    /**
     * Returns the provided <code>absoluteStartMillis</code> when created through {@link #subSearchRequest} and
     * -1 otherwise.
     */
    long getAbsoluteStartMillis() {
        return absoluteStartMillis;
    }

    /**
     * Returns the minimum compatible shard version the search request needs to run on. If the version is null, then there are no
     * restrictions imposed on shards versions part of this search.
     */
    @Nullable
    public Version minCompatibleShardNode() {
        return minCompatibleShardNode;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public SearchRequest indices(String... indices) {
        validateIndices(indices);
        this.indices = indices;
        return this;
    }

    private static void validateIndices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public SearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns whether network round-trips should be minimized when executing cross-cluster search requests.
     * Defaults to <code>true</code>, unless <code>minCompatibleShardNode</code> is set in which case it's set to <code>false</code>.
     */
    public boolean isCcsMinimizeRoundtrips() {
        return ccsMinimizeRoundtrips;
    }

    /**
     * Sets whether network round-trips should be minimized when executing cross-cluster search requests. Defaults to <code>true</code>.
     */
    public void setCcsMinimizeRoundtrips(boolean ccsMinimizeRoundtrips) {
        this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
    }

    /**
     * Returns whether the "fields" option will be emulated via fetching from source on pre-7.10 nodes.
     * The default is false.
     */
    public boolean isFieldsOptionEmulationEnabled() {
        return enableFieldsEmulation;
    }

    /**
     * Sets whether the "fields" option will be emulated via fetching from source on pre-7.10 nodes.
     */
    public void setFieldsOptionEmulationEnabled(boolean enableFieldsEmulation) {
        this.enableFieldsEmulation = enableFieldsEmulation;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     *
     * @deprecated Types are in the process of being removed. Instead of using a type, prefer to
     * filter on a field on the document.
     */
    @Deprecated
    public String[] types() {
        return types;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     *
     * @deprecated Types are in the process of being removed. Instead of using a type, prefer to
     * filter on a field on the document.
     */
    @Deprecated
    public SearchRequest types(String... types) {
        Objects.requireNonNull(types, "types must not be null");
        for (String type : types) {
            Objects.requireNonNull(type, "type must not be null");
        }
        this.types = types;
        return this;
    }

    /**
     * Returns the default value of {@link #ccsMinimizeRoundtrips} of a search request
     */
    public static boolean defaultCcsMinimizeRoundtrips(SearchRequest request) {
        return request.minCompatibleShardNode == null;
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
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
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
     * one of "dfs_query_then_fetch" or "query_then_fetch".
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

    public PointInTimeBuilder pointInTimeBuilder() {
        if (source != null) {
            return source.pointInTimeBuilder();
        }
        return null;
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
     * Sets if this request should allow partial results. (If method is not called,
     * will default to the cluster level setting).
     */
    public SearchRequest allowPartialSearchResults(boolean allowPartialSearchResults) {
        this.allowPartialSearchResults = allowPartialSearchResults;
        return this;
    }

    public Boolean allowPartialSearchResults() {
        return this.allowPartialSearchResults;
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
     * Returns the number of shard requests that should be executed concurrently on a single node. This value should be used as a
     * protection mechanism to reduce the number of shard requests fired per high level search request. Searches that hit the entire
     * cluster can be throttled with this number to reduce the cluster load. The default is {@code 5}
     */
    public int getMaxConcurrentShardRequests() {
        return maxConcurrentShardRequests == 0 ? 5 : maxConcurrentShardRequests;
    }

    /**
     * Sets the number of shard requests that should be executed concurrently on a single node. This value should be used as a
     * protection mechanism to reduce the number of shard requests fired per high level search request. Searches that hit the entire
     * cluster can be throttled with this number to reduce the cluster load. The default is {@code 5}
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
     * instance a shard can not match any documents based on its rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint.
     *
     * When unspecified, the pre-filter phase is executed if any of these conditions is met:
     * <ul>
     * <li>The request targets more than 128 shards</li>
     * <li>The request targets one or more read-only index</li>
     * <li>The primary sort of the query targets an indexed field</li>
     * </ul>
     */
    public void setPreFilterShardSize(int preFilterShardSize) {
        if (preFilterShardSize < 1) {
            throw new IllegalArgumentException("preFilterShardSize must be >= 1");
        }
        this.preFilterShardSize = preFilterShardSize;
    }

    /**
     * Returns a threshold that enforces a pre-filter roundtrip to pre-filter search shards based on query rewriting if the number of shards
     * the search request expands to exceeds the threshold, or <code>null</code> if the threshold is unspecified.
     * This filter roundtrip can limit the number of shards significantly if for
     * instance a shard can not match any documents based on its rewrite method ie. if date filters are mandatory to match but the shard
     * bounds and the query are disjoint.
     *
     * When unspecified, the pre-filter phase is executed if any of these conditions is met:
     * <ul>
     * <li>The request targets more than 128 shards</li>
     * <li>The request targets one or more read-only index</li>
     * <li>The primary sort of the query targets an indexed field</li>
     * </ul>
     */
    @Nullable
    public Integer getPreFilterShardSize() {
        return preFilterShardSize;
    }

    /**
     * @return true if the request only has suggest
     */
    public boolean isSuggestOnly() {
        return source != null && source.isSuggestOnly();
    }

    public int resolveTrackTotalHitsUpTo() {
        return resolveTrackTotalHitsUpTo(scroll, source);
    }

    @Override
    public SearchRequest rewrite(QueryRewriteContext ctx) throws IOException {
        if (source == null) {
            return this;
        }

        SearchSourceBuilder source = this.source.rewrite(ctx);
        boolean hasChanged = source != this.source;

        // add a sort tiebreaker for PIT search requests if not explicitly set
        Object[] searchAfter = source.searchAfter();
        if (source.pointInTimeBuilder() != null
                && source.sorts() != null
                && source.sorts().isEmpty() == false
                // skip the tiebreaker if it is not provided in the search after values
                && (searchAfter == null || searchAfter.length == source.sorts().size()+1)) {
            SortBuilder<?> lastSort = source.sorts().get(source.sorts().size() - 1);
            if (lastSort instanceof FieldSortBuilder == false
                    || FieldSortBuilder.SHARD_DOC_FIELD_NAME.equals(((FieldSortBuilder) lastSort).getFieldName()) == false) {
                List<SortBuilder<?>> newSorts = new ArrayList<>(source.sorts());
                newSorts.add(SortBuilders.pitTiebreaker().unmappedType("long"));
                source = source.shallowCopy().sort(newSorts);
                hasChanged = true;
            }
        }

        return hasChanged ? new SearchRequest(this).source(source) : this;
    }

    public static int resolveTrackTotalHitsUpTo(Scroll scroll, SearchSourceBuilder source) {
        if (scroll != null) {
            // no matter what the value of track_total_hits is
            return SearchContext.TRACK_TOTAL_HITS_ACCURATE;
        }
        return source == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : source.trackTotalHitsUpTo() == null ?
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : source.trackTotalHitsUpTo();
    }

    @Override
    public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, this::buildDescription, parentTaskId, headers);
    }

    public final String buildDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("indices[");
        Strings.arrayToDelimitedString(indices, ",", sb);
        sb.append("], ");
        sb.append("types[");
        Strings.arrayToDelimitedString(types, ",", sb);
        sb.append("], ");
        sb.append("search_type[").append(searchType).append("], ");
        if (scroll != null) {
            sb.append("scroll[").append(scroll.keepAlive()).append("], ");
        }
        if (source != null) {
            sb.append("source[").append(source.toString(FORMAT_PARAMS)).append("]");
        } else {
            sb.append("source[]");
        }
        return sb.toString();
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
                Objects.equals(indicesOptions, that.indicesOptions) &&
                Objects.equals(allowPartialSearchResults, that.allowPartialSearchResults) &&
                Objects.equals(localClusterAlias, that.localClusterAlias) &&
                absoluteStartMillis == that.absoluteStartMillis &&
                ccsMinimizeRoundtrips == that.ccsMinimizeRoundtrips &&
                enableFieldsEmulation == that.enableFieldsEmulation &&
                Objects.equals(minCompatibleShardNode, that.minCompatibleShardNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            searchType,
            Arrays.hashCode(indices),
            routing,
            preference,
            source,
            requestCache,
            scroll,
            Arrays.hashCode(types),
            indicesOptions,
            batchedReduceSize,
            maxConcurrentShardRequests,
            preFilterShardSize,
            allowPartialSearchResults,
            localClusterAlias,
            absoluteStartMillis,
            ccsMinimizeRoundtrips,
            minCompatibleShardNode,
            enableFieldsEmulation
        );
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
                ", allowPartialSearchResults=" + allowPartialSearchResults +
                ", localClusterAlias=" + localClusterAlias +
                ", getOrCreateAbsoluteStartMillis=" + absoluteStartMillis +
                ", ccsMinimizeRoundtrips=" + ccsMinimizeRoundtrips +
                ", enableFieldsEmulation=" + enableFieldsEmulation +
                ", source=" + source + '}';
    }
}

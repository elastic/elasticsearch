/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AliasFilterParsingException;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchSortValuesAndFormats;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

/**
 * Shard level request that represents a search.
 * It provides all the methods that the {@link SearchContext} needs.
 * Provides a cache key based on its content that can be used to cache shard level response.
 */
public class ShardSearchRequest extends TransportRequest implements IndicesRequest {
    private final String clusterAlias;
    private final ShardId shardId;
    private final int shardRequestIndex;
    private final int numberOfShards;
    private final long waitForCheckpoint;
    private final TimeValue waitForCheckpointsTimeout;
    private final SearchType searchType;
    private final Scroll scroll;
    private final float indexBoost;
    private Boolean requestCache;
    private final long nowInMillis;
    private final boolean allowPartialSearchResults;
    private final OriginalIndices originalIndices;

    private boolean canReturnNullResponseIfMatchNoDocs;
    private SearchSortValuesAndFormats bottomSortValues;

    // these are the only mutable fields, as they are subject to rewriting
    private AliasFilter aliasFilter;
    private SearchSourceBuilder source;
    private final ShardSearchContextId readerId;
    private final TimeValue keepAlive;

    private final TransportVersion channelVersion;

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    private final boolean forceSyntheticSource;

    public ShardSearchRequest(
        OriginalIndices originalIndices,
        SearchRequest searchRequest,
        ShardId shardId,
        int shardRequestIndex,
        int numberOfShards,
        AliasFilter aliasFilter,
        float indexBoost,
        long nowInMillis,
        @Nullable String clusterAlias
    ) {
        this(
            originalIndices,
            searchRequest,
            shardId,
            shardRequestIndex,
            numberOfShards,
            aliasFilter,
            indexBoost,
            nowInMillis,
            clusterAlias,
            null,
            null
        );
    }

    public ShardSearchRequest(
        OriginalIndices originalIndices,
        SearchRequest searchRequest,
        ShardId shardId,
        int shardRequestIndex,
        int numberOfShards,
        AliasFilter aliasFilter,
        float indexBoost,
        long nowInMillis,
        @Nullable String clusterAlias,
        ShardSearchContextId readerId,
        TimeValue keepAlive
    ) {
        this(
            originalIndices,
            shardId,
            shardRequestIndex,
            numberOfShards,
            searchRequest.searchType(),
            searchRequest.source(),
            searchRequest.requestCache(),
            aliasFilter,
            indexBoost,
            searchRequest.allowPartialSearchResults(),
            searchRequest.scroll(),
            nowInMillis,
            clusterAlias,
            readerId,
            keepAlive,
            computeWaitForCheckpoint(searchRequest.getWaitForCheckpoints(), shardId, shardRequestIndex),
            searchRequest.getWaitForCheckpointsTimeout(),
            searchRequest.isForceSyntheticSource()
        );
        // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
        // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
        assert searchRequest.allowPartialSearchResults() != null;
    }

    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    public static long computeWaitForCheckpoint(Map<String, long[]> indexToWaitForCheckpoints, ShardId shardId, int shardRequestIndex) {
        final long[] waitForCheckpoints = indexToWaitForCheckpoints.getOrDefault(shardId.getIndex().getName(), EMPTY_LONG_ARRAY);

        long waitForCheckpoint;
        if (waitForCheckpoints.length == 0) {
            waitForCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        } else {
            assert waitForCheckpoints.length > shardRequestIndex;
            waitForCheckpoint = waitForCheckpoints[shardRequestIndex];
        }
        return waitForCheckpoint;
    }

    public ShardSearchRequest(ShardId shardId, long nowInMillis, AliasFilter aliasFilter) {
        this(shardId, nowInMillis, aliasFilter, null);
    }

    public ShardSearchRequest(ShardId shardId, long nowInMillis, AliasFilter aliasFilter, String clusterAlias) {
        this(
            OriginalIndices.NONE,
            shardId,
            -1,
            -1,
            SearchType.QUERY_THEN_FETCH,
            null,
            null,
            aliasFilter,
            1.0f,
            true,
            null,
            nowInMillis,
            clusterAlias,
            null,
            null,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SearchService.NO_TIMEOUT,
            false
        );
    }

    @SuppressWarnings("this-escape")
    public ShardSearchRequest(
        OriginalIndices originalIndices,
        ShardId shardId,
        int shardRequestIndex,
        int numberOfShards,
        SearchType searchType,
        SearchSourceBuilder source,
        Boolean requestCache,
        AliasFilter aliasFilter,
        float indexBoost,
        boolean allowPartialSearchResults,
        Scroll scroll,
        long nowInMillis,
        @Nullable String clusterAlias,
        ShardSearchContextId readerId,
        TimeValue keepAlive,
        long waitForCheckpoint,
        TimeValue waitForCheckpointsTimeout,
        boolean forceSyntheticSource
    ) {
        this.shardId = shardId;
        this.shardRequestIndex = shardRequestIndex;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source(source);
        this.requestCache = requestCache;
        this.aliasFilter = aliasFilter;
        this.indexBoost = indexBoost;
        this.allowPartialSearchResults = allowPartialSearchResults;
        this.scroll = scroll;
        this.nowInMillis = nowInMillis;
        this.clusterAlias = clusterAlias;
        this.originalIndices = originalIndices;
        this.readerId = readerId;
        this.keepAlive = keepAlive;
        assert keepAlive == null || readerId != null : "readerId: null keepAlive: " + keepAlive;
        this.channelVersion = TransportVersion.current();
        this.waitForCheckpoint = waitForCheckpoint;
        this.waitForCheckpointsTimeout = waitForCheckpointsTimeout;
        this.forceSyntheticSource = forceSyntheticSource;
    }

    @SuppressWarnings("this-escape")
    public ShardSearchRequest(ShardSearchRequest clone) {
        this.shardId = clone.shardId;
        this.shardRequestIndex = clone.shardRequestIndex;
        this.searchType = clone.searchType;
        this.numberOfShards = clone.numberOfShards;
        this.scroll = clone.scroll;
        this.source(clone.source);
        this.aliasFilter = clone.aliasFilter;
        this.indexBoost = clone.indexBoost;
        this.nowInMillis = clone.nowInMillis;
        this.requestCache = clone.requestCache;
        this.clusterAlias = clone.clusterAlias;
        this.allowPartialSearchResults = clone.allowPartialSearchResults;
        this.canReturnNullResponseIfMatchNoDocs = clone.canReturnNullResponseIfMatchNoDocs;
        this.bottomSortValues = clone.bottomSortValues;
        this.originalIndices = clone.originalIndices;
        this.readerId = clone.readerId;
        this.keepAlive = clone.keepAlive;
        this.channelVersion = clone.channelVersion;
        this.waitForCheckpoint = clone.waitForCheckpoint;
        this.waitForCheckpointsTimeout = clone.waitForCheckpointsTimeout;
        this.forceSyntheticSource = clone.forceSyntheticSource;
    }

    public ShardSearchRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        searchType = SearchType.fromId(in.readByte());
        shardRequestIndex = in.readVInt();
        numberOfShards = in.readVInt();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0) && in.getTransportVersion().before(TransportVersions.V_8_9_X)) {
            // to deserialize between the 8.8 and 8.500.020 version we need to translate
            // the rank queries into sub searches if we are ranking; if there are no rank queries
            // we deserialize the empty list and do nothing
            List<QueryBuilder> rankQueryBuilders = in.readNamedWriteableCollectionAsList(QueryBuilder.class);
            // if we are in the dfs phase in 8.8, we can have no rank queries
            // and if we are in the query/fetch phase we can have either no rank queries
            // for a standard query or hybrid search or 2+ rank queries, but we cannot have
            // exactly 1 rank query ever so we check for this
            assert rankQueryBuilders.size() != 1 : "[rank] requires at least [2] sub searches, but only found [1]";
            // if we have 2+ rank queries we know we are ranking, so we set our
            // sub searches from this; note this will override the boolean query deserialized from source
            // because we use the same data structure for a single query and multiple queries
            // but we will just re-create it as necessary
            if (rankQueryBuilders.size() >= 2) {
                assert source != null && source.rankBuilder() != null;
                List<SubSearchSourceBuilder> subSearchSourceBuilders = new ArrayList<>();
                for (QueryBuilder queryBuilder : rankQueryBuilders) {
                    subSearchSourceBuilders.add(new SubSearchSourceBuilder(queryBuilder));
                }
                source.subSearches(subSearchSourceBuilders);
            }
        }
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            // types no longer relevant so ignore
            String[] types = in.readStringArray();
            if (types.length > 0) {
                throw new IllegalStateException(
                    "types are no longer supported in search requests but found [" + Arrays.toString(types) + "]"
                );
            }
        }
        aliasFilter = AliasFilter.readFrom(in);
        indexBoost = in.readFloat();
        nowInMillis = in.readVLong();
        requestCache = in.readOptionalBoolean();
        clusterAlias = in.readOptionalString();
        allowPartialSearchResults = in.readBoolean();
        canReturnNullResponseIfMatchNoDocs = in.readBoolean();
        bottomSortValues = in.readOptionalWriteable(SearchSortValuesAndFormats::new);
        readerId = in.readOptionalWriteable(ShardSearchContextId::new);
        keepAlive = in.readOptionalTimeValue();
        assert keepAlive == null || readerId != null : "readerId: null keepAlive: " + keepAlive;
        channelVersion = TransportVersion.min(TransportVersion.readVersion(in), in.getTransportVersion());
        waitForCheckpoint = in.readLong();
        waitForCheckpointsTimeout = in.readTimeValue();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            forceSyntheticSource = in.readBoolean();
        } else {
            /*
             * Synthetic source is not supported before 8.3.0 so any request
             * from a coordinating node of that version will not want to
             * force it.
             */
            forceSyntheticSource = false;
        }
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        innerWriteTo(out, false);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    protected final void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        shardId.writeTo(out);
        out.writeByte(searchType.id());
        if (asKey == false) {
            out.writeVInt(shardRequestIndex);
            out.writeVInt(numberOfShards);
        }
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0) && out.getTransportVersion().before(TransportVersions.V_8_9_X)) {
            // to serialize between the 8.8 and 8.500.020 version we need to translate
            // the sub searches into rank queries if we are ranking, otherwise, we
            // ignore this because linear combination will have multiple sub searches in
            // 8.500.020+, but only use the combined boolean query in prior versions
            List<QueryBuilder> rankQueryBuilders = new ArrayList<>();
            if (source != null && source.rankBuilder() != null && source.subSearches().size() >= 2) {
                for (SubSearchSourceBuilder subSearchSourceBuilder : source.subSearches()) {
                    rankQueryBuilders.add(subSearchSourceBuilder.getQueryBuilder());
                }
            }
            out.writeNamedWriteableCollection(rankQueryBuilders);
        }
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            // types not supported so send an empty array to previous versions
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        aliasFilter.writeTo(out);
        out.writeFloat(indexBoost);
        if (asKey == false) {
            out.writeVLong(nowInMillis);
        }
        out.writeOptionalBoolean(requestCache);
        out.writeOptionalString(clusterAlias);
        out.writeBoolean(allowPartialSearchResults);
        if (asKey == false) {
            out.writeBoolean(canReturnNullResponseIfMatchNoDocs);
            out.writeOptionalWriteable(bottomSortValues);
            out.writeOptionalWriteable(readerId);
            out.writeOptionalTimeValue(keepAlive);
        }
        TransportVersion.writeVersion(channelVersion, out);
        out.writeLong(waitForCheckpoint);
        out.writeTimeValue(waitForCheckpointsTimeout);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeBoolean(forceSyntheticSource);
        } else {
            if (forceSyntheticSource) {
                throw new IllegalArgumentException("force_synthetic_source is not supported before 8.4.0");
            }
        }
    }

    @Override
    public String[] indices() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indicesOptions();
    }

    public ShardId shardId() {
        return shardId;
    }

    public SearchSourceBuilder source() {
        return source;
    }

    public AliasFilter getAliasFilter() {
        return aliasFilter;
    }

    public void setAliasFilter(AliasFilter aliasFilter) {
        this.aliasFilter = aliasFilter;
    }

    public void source(SearchSourceBuilder source) {
        if (source != null && source.pointInTimeBuilder() != null) {
            // Discard the actual point in time as data nodes don't use it to reduce the memory usage and the serialization cost
            // of shard-level search requests. However, we need to assign as a dummy PIT instead of null as we verify PIT for
            // slice requests on data nodes.
            source = source.shallowCopy();
            source.pointInTimeBuilder(new PointInTimeBuilder(""));
        }
        this.source = source;
    }

    /**
     * Returns the shard request ordinal that is used by the main search request
     * to reference this shard.
     */
    public int shardRequestIndex() {
        return shardRequestIndex;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public SearchType searchType() {
        return searchType;
    }

    public float indexBoost() {
        return indexBoost;
    }

    public long nowInMillis() {
        return nowInMillis;
    }

    public Boolean requestCache() {
        return requestCache;
    }

    public void requestCache(Boolean requestCache) {
        this.requestCache = requestCache;
    }

    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    public Scroll scroll() {
        return scroll;
    }

    /**
     * Sets the bottom sort values that can be used by the searcher to filter documents
     * that are after it. This value is computed by coordinating nodes that throttles the
     * query phase. After a partial merge of successful shards the sort values of the
     * bottom top document are passed as an hint on subsequent shard requests.
     */
    public void setBottomSortValues(SearchSortValuesAndFormats values) {
        this.bottomSortValues = values;
    }

    public SearchSortValuesAndFormats getBottomSortValues() {
        return bottomSortValues;
    }

    /**
     * Returns true if the caller can handle null response {@link QuerySearchResult#nullInstance()}.
     * Defaults to false since the coordinator node needs at least one shard response to build the global
     * response.
     */
    public boolean canReturnNullResponseIfMatchNoDocs() {
        return canReturnNullResponseIfMatchNoDocs;
    }

    public void canReturnNullResponseIfMatchNoDocs(boolean value) {
        this.canReturnNullResponseIfMatchNoDocs = value;
    }

    private static final ThreadLocal<BytesStreamOutput> scratch = ThreadLocal.withInitial(BytesStreamOutput::new);

    /**
     * Returns a non-null value if this request should execute using a specific point-in-time reader;
     * otherwise, using the most up to date point-in-time reader.
     */
    public ShardSearchContextId readerId() {
        return readerId;
    }

    /**
     * Returns a non-null to specify the time to live of the point-in-time reader that is used to execute this request.
     */
    public TimeValue keepAlive() {
        return keepAlive;
    }

    public long waitForCheckpoint() {
        return waitForCheckpoint;
    }

    public TimeValue getWaitForCheckpointsTimeout() {
        return waitForCheckpointsTimeout;
    }

    /**
     * Returns the cache key for this shard search request, based on its content
     */
    public BytesReference cacheKey(CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator) throws IOException {
        BytesStreamOutput out = scratch.get();
        try {
            this.innerWriteTo(out, true);
            if (differentiator != null) {
                differentiator.accept(this, out);
            }
            return new BytesArray(MessageDigests.digest(out.bytes(), MessageDigests.sha256()));
        } finally {
            out.reset();
        }
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        // Shard id is enough here, the request itself can be found by looking at the parent task description
        return "shardId[" + shardId() + "]";
    }

    @SuppressWarnings("rawtypes")
    public Rewriteable<Rewriteable> getRewriteable() {
        return new RequestRewritable(this);
    }

    @SuppressWarnings("rawtypes")
    static class RequestRewritable implements Rewriteable<Rewriteable> {

        final ShardSearchRequest request;

        RequestRewritable(ShardSearchRequest request) {
            this.request = request;
        }

        @Override
        public Rewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            SearchSourceBuilder newSource = request.source() == null ? null : Rewriteable.rewrite(request.source(), ctx);
            AliasFilter newAliasFilter = Rewriteable.rewrite(request.getAliasFilter(), ctx);
            SearchExecutionContext searchExecutionContext = ctx.convertToSearchExecutionContext();
            FieldSortBuilder primarySort = FieldSortBuilder.getPrimaryFieldSortOrNull(newSource);
            if (searchExecutionContext != null
                && primarySort != null
                && primarySort.isBottomSortShardDisjoint(searchExecutionContext, request.getBottomSortValues())) {
                assert newSource != null : "source should contain a primary sort field";
                newSource = newSource.shallowCopy();
                int trackTotalHitsUpTo = SearchRequest.resolveTrackTotalHitsUpTo(request.scroll, request.source);
                if (trackTotalHitsUpTo == TRACK_TOTAL_HITS_DISABLED && newSource.suggest() == null && newSource.aggregations() == null) {
                    newSource.query(new MatchNoneQueryBuilder());
                } else {
                    newSource.size(0);
                }
                request.source(newSource);
                request.setBottomSortValues(null);
            }

            if (newSource == request.source() && newAliasFilter == request.getAliasFilter()) {
                return this;
            } else {
                request.source(newSource);
                request.setAliasFilter(newAliasFilter);
                return new RequestRewritable(request);
            }
        }
    }

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p>
     * The list of filtering aliases should be obtained by calling Metadata.filteringAliases.
     * Returns {@code null} if no filtering is required.</p>
     */
    public static QueryBuilder parseAliasFilter(
        CheckedFunction<BytesReference, QueryBuilder, IOException> filterParser,
        IndexMetadata metadata,
        String... aliasNames
    ) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        Index index = metadata.getIndex();
        Map<String, AliasMetadata> aliases = metadata.getAliases();
        Function<AliasMetadata, QueryBuilder> parserFunction = (alias) -> {
            if (alias.filter() == null) {
                return null;
            }
            try {
                return filterParser.apply(alias.filter().uncompressed());
            } catch (IOException ex) {
                throw new AliasFilterParsingException(index, alias.getAlias(), "Invalid alias filter", ex);
            }
        };
        if (aliasNames.length == 1) {
            AliasMetadata alias = aliases.get(aliasNames[0]);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parserFunction.apply(alias);
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BoolQueryBuilder combined = new BoolQueryBuilder();
            for (String aliasName : aliasNames) {
                AliasMetadata alias = aliases.get(aliasName);
                if (alias == null) {
                    // This shouldn't happen unless alias disappeared after filteringAliases was called.
                    throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
                }
                QueryBuilder parsedFilter = parserFunction.apply(alias);
                if (parsedFilter != null) {
                    combined.should(parsedFilter);
                } else {
                    // The filter might be null only if filter was removed after filteringAliases was called
                    return null;
                }
            }
            return combined;
        }
    }

    public final Map<String, Object> getRuntimeMappings() {
        return source == null ? emptyMap() : source.runtimeMappings();
    }

    /**
     * Returns the minimum version of the channel that the request has been passed. If the request never passes around, then the channel
     * version is {@link TransportVersion#current()}; otherwise, it's the minimum transport version of the coordinating node and data node
     * (and the proxy node in case the request is sent to the proxy node of the remote cluster before reaching the data node).
     */
    public TransportVersion getChannelVersion() {
        return channelVersion;
    }

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    public boolean isForceSyntheticSource() {
        return forceSyntheticSource;
    }
}

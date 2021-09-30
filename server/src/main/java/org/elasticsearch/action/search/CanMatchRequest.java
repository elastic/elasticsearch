/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchSortValuesAndFormats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Node-level request used during can-match phase
 */
public class CanMatchRequest extends TransportRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final SearchSourceBuilder source;
    private final List<ShardLevelRequest> shards;

    public static class ShardLevelRequest implements Writeable {
        private final String clusterAlias;
        private final ShardId shardId;
        private final int shardRequestIndex;
        private final int numberOfShards;
        private final SearchType searchType;
        private final Scroll scroll;
        private final float indexBoost;
        private final Boolean requestCache;
        private final long nowInMillis;
        private final boolean allowPartialSearchResults;

        private final boolean canReturnNullResponseIfMatchNoDocs;
        private final SearchSortValuesAndFormats bottomSortValues;

        private final AliasFilter aliasFilter;
        private final ShardSearchContextId readerId;
        private final TimeValue keepAlive;

        private final Version channelVersion;

        public ShardLevelRequest(
                                  SearchRequest searchRequest,
                                  ShardId shardId,
                                  int shardRequestIndex,
                                  int numberOfShards,
                                  AliasFilter aliasFilter,
                                  float indexBoost,
                                  long nowInMillis,
                                  @Nullable String clusterAlias,
                                  ShardSearchContextId readerId,
                                  TimeValue keepAlive) {
            this(shardId,
                shardRequestIndex,
                numberOfShards,
                searchRequest.searchType(),
                searchRequest.requestCache(),
                aliasFilter,
                indexBoost,
                searchRequest.allowPartialSearchResults(),
                searchRequest.scroll(),
                nowInMillis,
                clusterAlias,
                readerId,
                keepAlive);
            // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
            // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
            assert searchRequest.allowPartialSearchResults() != null;
        }

        public ShardLevelRequest( ShardId shardId,
                                  int shardRequestIndex,
                                  int numberOfShards,
                                  SearchType searchType,
                                  Boolean requestCache,
                                  AliasFilter aliasFilter,
                                  float indexBoost,
                                  boolean allowPartialSearchResults,
                                  Scroll scroll,
                                  long nowInMillis,
                                  @Nullable String clusterAlias,
                                  ShardSearchContextId readerId,
                                  TimeValue keepAlive) {
            this.shardId = shardId;
            this.shardRequestIndex = shardRequestIndex;
            this.numberOfShards = numberOfShards;
            this.searchType = searchType;
            this.requestCache = requestCache;
            this.aliasFilter = aliasFilter;
            this.indexBoost = indexBoost;
            this.allowPartialSearchResults = allowPartialSearchResults;
            this.scroll = scroll;
            this.nowInMillis = nowInMillis;
            this.clusterAlias = clusterAlias;
            this.readerId = readerId;
            this.keepAlive = keepAlive;
            assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
            this.channelVersion = Version.CURRENT;
            // TODO: remove the following two fields
            this.canReturnNullResponseIfMatchNoDocs = false;
            this.bottomSortValues = null;
        }

        public ShardLevelRequest(StreamInput in) throws IOException {
            // TODO: parent task super(in);
            shardId = new ShardId(in);
            searchType = SearchType.fromId(in.readByte());
            shardRequestIndex = in.getVersion().onOrAfter(Version.V_7_11_0) ? in.readVInt() : -1;
            numberOfShards = in.readVInt();
            scroll = in.readOptionalWriteable(Scroll::new);
            if (in.getVersion().before(Version.V_8_0_0)) {
                // types no longer relevant so ignore
                String[] types = in.readStringArray();
                if (types.length > 0) {
                    throw new IllegalStateException(
                        "types are no longer supported in search requests but found [" + Arrays.toString(types) + "]");
                }
            }
            aliasFilter = new AliasFilter(in);
            indexBoost = in.readFloat();
            nowInMillis = in.readVLong();
            requestCache = in.readOptionalBoolean();
            clusterAlias = in.readOptionalString();
            allowPartialSearchResults = in.readBoolean();
            canReturnNullResponseIfMatchNoDocs = in.readBoolean();
            bottomSortValues = in.readOptionalWriteable(SearchSortValuesAndFormats::new);
            readerId = in.readOptionalWriteable(ShardSearchContextId::new);
            keepAlive = in.readOptionalTimeValue();
            assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
            channelVersion = Version.min(Version.readVersion(in), in.getVersion());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeByte(searchType.id());
            out.writeVInt(shardRequestIndex);
            out.writeVInt(numberOfShards);
            out.writeOptionalWriteable(scroll);
            if (out.getVersion().before(Version.V_8_0_0)) {
                // types not supported so send an empty array to previous versions
                out.writeStringArray(Strings.EMPTY_ARRAY);
            }
            aliasFilter.writeTo(out);
            out.writeFloat(indexBoost);
            out.writeVLong(nowInMillis);
            out.writeOptionalBoolean(requestCache);
            out.writeOptionalString(clusterAlias);
            out.writeBoolean(allowPartialSearchResults);
            out.writeStringArray(Strings.EMPTY_ARRAY);
            out.writeOptionalString(null);
            out.writeBoolean(canReturnNullResponseIfMatchNoDocs);
            out.writeOptionalWriteable(bottomSortValues);
            out.writeOptionalWriteable(readerId);
            out.writeOptionalTimeValue(keepAlive);
            Version.writeVersion(channelVersion, out);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(source);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    public CanMatchRequest(
        OriginalIndices originalIndices,
        SearchRequest searchRequest,
        List<ShardLevelRequest> shards
    ) {
       this.originalIndices = originalIndices;
       this.source = searchRequest.source();
       this.shards = new ArrayList<>(shards);
    }

    public CanMatchRequest(StreamInput in) throws IOException {
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shards = in.readList(ShardLevelRequest::new);
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

    public List<ShardSearchRequest> createShardSearchRequests() {
        return shards.stream().map(r -> new ShardSearchRequest(
            originalIndices, r.shardId, r.shardRequestIndex, r.numberOfShards, r.searchType,
            source, r.requestCache, r.aliasFilter, r.indexBoost, r.allowPartialSearchResults, r.scroll,
            r.nowInMillis, r.clusterAlias, r.readerId, r.keepAlive
        )).collect(Collectors.toList());
    }

}

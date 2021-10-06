/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Node-level request used during can-match phase
 */
public class CanMatchRequest extends TransportRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final SearchSourceBuilder source;
    private final List<Shard> shards;
    private final SearchType searchType;
    private final Boolean requestCache;
    private final boolean allowPartialSearchResults;
    private final Scroll scroll;
    private final int numberOfShards;
    private final long nowInMillis;
    @Nullable
    private final String clusterAlias;

    public static class Shard implements Writeable {
        private final ShardId shardId;
        private final int shardRequestIndex;
        private final AliasFilter aliasFilter;
        private final float indexBoost;
        private final ShardSearchContextId readerId;
        private final TimeValue keepAlive;

        public Shard(ShardId shardId,
                     int shardRequestIndex,
                     AliasFilter aliasFilter,
                     float indexBoost,
                     ShardSearchContextId readerId,
                     TimeValue keepAlive) {
            this.shardId = shardId;
            this.shardRequestIndex = shardRequestIndex;
            this.aliasFilter = aliasFilter;
            this.indexBoost = indexBoost;
            this.readerId = readerId;
            this.keepAlive = keepAlive;
            assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
        }

        public Shard(StreamInput in) throws IOException {
            shardId = new ShardId(in);
            shardRequestIndex = in.readVInt();
            aliasFilter = new AliasFilter(in);
            indexBoost = in.readFloat();
            readerId = in.readOptionalWriteable(ShardSearchContextId::new);
            keepAlive = in.readOptionalTimeValue();
            assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeVInt(shardRequestIndex);
            aliasFilter.writeTo(out);
            out.writeFloat(indexBoost);
            out.writeOptionalWriteable(readerId);
            out.writeOptionalTimeValue(keepAlive);
        }

        public int getShardRequestIndex() {
            return shardRequestIndex;
        }

        public ShardId shardId() {
            return shardId;
        }
    }

    public CanMatchRequest(
        OriginalIndices originalIndices,
        SearchRequest searchRequest,
        List<Shard> shards,
        int numberOfShards,
        long nowInMillis,
        @Nullable String clusterAlias
        ) {
        this.originalIndices = originalIndices;
        this.source = searchRequest.source();
        this.shards = new ArrayList<>(shards);
        this.searchType = searchRequest.searchType();
        this.requestCache = searchRequest.requestCache();
        // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
        // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
        assert searchRequest.allowPartialSearchResults() != null;
        this.allowPartialSearchResults = searchRequest.allowPartialSearchResults();
        this.scroll = searchRequest.scroll();
        this.numberOfShards = numberOfShards;
        this.nowInMillis = nowInMillis;
        this.clusterAlias = clusterAlias;
    }

    public CanMatchRequest(StreamInput in) throws IOException {
        super(in);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        searchType = SearchType.fromId(in.readByte());
        scroll = in.readOptionalWriteable(Scroll::new);
        requestCache = in.readOptionalBoolean();
        allowPartialSearchResults = in.readBoolean();
        numberOfShards = in.readVInt();
        nowInMillis = in.readVLong();
        clusterAlias = in.readOptionalString();
        shards = in.readList(Shard::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(source);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeByte(searchType.id());
        out.writeOptionalWriteable(scroll);
        out.writeOptionalBoolean(requestCache);
        out.writeBoolean(allowPartialSearchResults);
        out.writeVInt(numberOfShards);
        out.writeVLong(nowInMillis);
        out.writeOptionalString(clusterAlias);
        out.writeList(shards);
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

    public List<Shard> getShardLevelRequests() {
        return shards;
    }

    public List<ShardSearchRequest> createShardSearchRequests() {
        return shards.stream().map(this::createShardSearchRequest).collect(Collectors.toList());
    }

    public ShardSearchRequest createShardSearchRequest(Shard r) {
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            originalIndices, r.shardId, r.shardRequestIndex, numberOfShards, searchType,
            source, requestCache, r.aliasFilter, r.indexBoost, allowPartialSearchResults, scroll,
            nowInMillis, clusterAlias, r.readerId, r.keepAlive
        );
        shardSearchRequest.setParentTask(getParentTask());
        return shardSearchRequest;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        // Shard id is enough here, the request itself can be found by looking at the parent task description
        return "shardIds[" + shards.stream().map(slr -> slr.shardId).collect(Collectors.toList()) + "]";
    }

}

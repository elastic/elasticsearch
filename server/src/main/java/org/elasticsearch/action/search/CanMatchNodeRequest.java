/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Node-level request used during can-match phase
 */
public class CanMatchNodeRequest extends TransportRequest implements IndicesRequest {

    private final SearchSourceBuilder source;

    private final SearchSourceBuilder originalSource;
    private final List<Shard> shards;
    private final SearchType searchType;
    private final Boolean requestCache;
    private final boolean allowPartialSearchResults;
    private final Scroll scroll;
    private final int numberOfShards;
    private final long nowInMillis;
    @Nullable
    private final String clusterAlias;
    private final String[] indices;
    private final IndicesOptions indicesOptions;
    private final TimeValue waitForCheckpointsTimeout;

    public static class Shard implements Writeable {
        private final String[] indices;
        private final ShardId shardId;
        private final int shardRequestIndex;
        private final AliasFilter aliasFilter;
        private final float indexBoost;
        private final ShardSearchContextId readerId;
        private final TimeValue keepAlive;
        private final long waitForCheckpoint;

        public Shard(
            String[] indices,
            ShardId shardId,
            int shardRequestIndex,
            AliasFilter aliasFilter,
            float indexBoost,
            ShardSearchContextId readerId,
            TimeValue keepAlive,
            long waitForCheckpoint
        ) {
            this.indices = indices;
            this.shardId = shardId;
            this.shardRequestIndex = shardRequestIndex;
            this.aliasFilter = aliasFilter;
            this.indexBoost = indexBoost;
            this.readerId = readerId;
            this.keepAlive = keepAlive;
            this.waitForCheckpoint = waitForCheckpoint;
            assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
        }

        public Shard(StreamInput in) throws IOException {
            indices = in.readStringArray();
            shardId = new ShardId(in);
            shardRequestIndex = in.readVInt();
            aliasFilter = AliasFilter.readFrom(in);
            indexBoost = in.readFloat();
            readerId = in.readOptionalWriteable(ShardSearchContextId::new);
            keepAlive = in.readOptionalTimeValue();
            waitForCheckpoint = in.readLong();
            assert keepAlive == null || readerId != null : "readerId: " + readerId + " keepAlive: " + keepAlive;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(indices);
            shardId.writeTo(out);
            out.writeVInt(shardRequestIndex);
            aliasFilter.writeTo(out);
            out.writeFloat(indexBoost);
            out.writeOptionalWriteable(readerId);
            out.writeOptionalTimeValue(keepAlive);
            out.writeLong(waitForCheckpoint);
        }

        public int getShardRequestIndex() {
            return shardRequestIndex;
        }

        public String[] getOriginalIndices() {
            return indices;
        }

        public ShardId shardId() {
            return shardId;
        }
    }

    public CanMatchNodeRequest(
        SearchRequest searchRequest,
        IndicesOptions indicesOptions,
        List<Shard> shards,
        int numberOfShards,
        long nowInMillis,
        @Nullable String clusterAlias
    ) {
        this.source = searchRequest.source();
        this.originalSource = searchRequest.source();
        this.indicesOptions = indicesOptions;
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
        this.waitForCheckpointsTimeout = searchRequest.getWaitForCheckpointsTimeout();
        indices = shards.stream().map(Shard::getOriginalIndices).flatMap(Arrays::stream).distinct().toArray(String[]::new);
    }

    public CanMatchNodeRequest(StreamInput in) throws IOException {
        super(in);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        originalSource = in.readOptionalWriteable(SearchSourceBuilder::new);
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        searchType = SearchType.fromId(in.readByte());
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            // types no longer relevant so ignore
            String[] types = in.readStringArray();
            if (types.length > 0) {
                throw new IllegalStateException(
                    "types are no longer supported in search requests but found [" + Arrays.toString(types) + "]"
                );
            }
        }
        scroll = in.readOptionalWriteable(Scroll::new);
        requestCache = in.readOptionalBoolean();
        allowPartialSearchResults = in.readBoolean();
        numberOfShards = in.readVInt();
        nowInMillis = in.readVLong();
        clusterAlias = in.readOptionalString();
        waitForCheckpointsTimeout = in.readTimeValue();
        shards = in.readList(Shard::new);
        indices = shards.stream().map(Shard::getOriginalIndices).flatMap(Arrays::stream).distinct().toArray(String[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(source);
        out.writeOptionalWriteable(originalSource);
        indicesOptions.writeIndicesOptions(out);
        out.writeByte(searchType.id());
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            // types not supported so send an empty array to previous versions
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        out.writeOptionalWriteable(scroll);
        out.writeOptionalBoolean(requestCache);
        out.writeBoolean(allowPartialSearchResults);
        out.writeVInt(numberOfShards);
        out.writeVLong(nowInMillis);
        out.writeOptionalString(clusterAlias);
        out.writeTimeValue(waitForCheckpointsTimeout);
        out.writeList(shards);
    }

    public List<Shard> getShardLevelRequests() {
        return shards;
    }

    public List<ShardSearchRequest> createShardSearchRequests() {
        return shards.stream().map(this::createShardSearchRequest).toList();
    }

    public ShardSearchRequest createShardSearchRequest(Shard r) {
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            new OriginalIndices(r.indices, indicesOptions),
            r.shardId,
            r.shardRequestIndex,
            numberOfShards,
            searchType,
            source,
            requestCache,
            r.aliasFilter,
            r.indexBoost,
            allowPartialSearchResults,
            scroll,
            nowInMillis,
            clusterAlias,
            r.readerId,
            r.keepAlive,
            r.waitForCheckpoint,
            waitForCheckpointsTimeout,
            false
        );
        shardSearchRequest.setParentTask(getParentTask());
        return shardSearchRequest;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        // Shard id is enough here, the request itself can be found by looking at the parent task description
        return "shardIds[" + shards.stream().map(slr -> slr.shardId).toList() + "]";
    }

}

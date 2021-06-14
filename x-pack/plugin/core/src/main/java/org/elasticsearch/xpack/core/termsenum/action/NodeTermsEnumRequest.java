/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Internal terms enum request executed directly against a specific node, querying potentially many
 * shards in one request
 */
public class NodeTermsEnumRequest extends TransportRequest implements IndicesRequest {

    private String field;
    private String string;
    private String searchAfter;
    private long taskStartedTimeMillis;
    private long nodeStartedTimeMillis;
    private boolean caseInsensitive;
    private int size;
    private long timeout;
    private final QueryBuilder indexFilter;
    private Set<ShardId> shardIds;
    private String nodeId;

    public NodeTermsEnumRequest(final String nodeId,
                                final Set<ShardId> shardIds,
                                TermsEnumRequest request,
                                long taskStartTimeMillis) {
        this.field = request.field();
        this.string = request.string();
        this.searchAfter = request.searchAfter();
        this.caseInsensitive = request.caseInsensitive();
        this.size = request.size();
        this.timeout = request.timeout().getMillis();
        this.taskStartedTimeMillis = taskStartTimeMillis;
        this.indexFilter = request.indexFilter();
        this.nodeId = nodeId;
        this.shardIds = shardIds;
    }

    public NodeTermsEnumRequest(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        string = in.readOptionalString();
        searchAfter = in.readOptionalString();
        caseInsensitive = in.readBoolean();
        size = in.readVInt();
        timeout = in.readVLong();
        taskStartedTimeMillis = in.readVLong();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nodeId = in.readString();
        int numShards = in.readVInt();
        shardIds = new HashSet<>(numShards);
        for (int i = 0; i < numShards; i++) {
            shardIds.add(new ShardId(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeOptionalString(string);
        out.writeOptionalString(searchAfter);
        out.writeBoolean(caseInsensitive);
        out.writeVInt(size);
        // Adjust the amount of permitted time the shard has remaining to gather terms.
        long timeSpentSoFarInCoordinatingNode = System.currentTimeMillis() - taskStartedTimeMillis;
        long remainingTimeForShardToUse =  (timeout - timeSpentSoFarInCoordinatingNode);
        // TODO - if already timed out can we shortcut the trip somehow? Throw exception if remaining time < 0?
        out.writeVLong(remainingTimeForShardToUse);
        out.writeVLong(taskStartedTimeMillis);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeString(nodeId);
        out.writeVInt(shardIds.size());
        for (ShardId shardId : shardIds) {
            shardId.writeTo(out);
        }
    }

    public String field() {
        return field;
    }

    @Nullable
    public String string() {
        return string;
    }

    @Nullable
    public String searchAfter() {
        return searchAfter;
    }

    public long taskStartedTimeMillis() {
        return this.taskStartedTimeMillis;
    }

    /**
     * The time this request was materialized on a node
     */
    long nodeStartedTimeMillis() {
        // In case startTimerOnDataNode has not been called (should never happen in normal circumstances?)
        if (nodeStartedTimeMillis == 0) {
            nodeStartedTimeMillis = System.currentTimeMillis();
        }
        return this.nodeStartedTimeMillis;
    }

    public void startTimerOnDataNode() {
        nodeStartedTimeMillis = System.currentTimeMillis();
    }

    public Set<ShardId> shardIds() {
        return Collections.unmodifiableSet(shardIds);
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public int size() {
        return size;
    }

    public long timeout() {
        return timeout;
    }
    public String nodeId() {
        return nodeId;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    @Override
    public String[] indices() {
        HashSet<String> indicesNames = new HashSet<>();
        for (ShardId shardId : shardIds) {
            indicesNames.add(shardId.getIndexName());
        }
        return indicesNames.toArray(new String[0]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return null;
    }

    public boolean remove(ShardId shardId) {
        return shardIds.remove(shardId);
    }
}

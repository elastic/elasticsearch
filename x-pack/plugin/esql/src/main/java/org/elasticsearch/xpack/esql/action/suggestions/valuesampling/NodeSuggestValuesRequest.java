/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions.valuesampling;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A node-grouped request to raw-read a keyword field's term dictionary off a set of hot-tier shard
 * copies on one node, modeled on {@code NodeTermsEnumRequest} — see the suggestions API spec. Unlike
 * that request, the response this drives carries per-term doc frequency, not a bare term list.
 *
 * <p>Implements {@link IndicesRequest}, deriving its declared indices from {@link #shardIds()}, so RBAC
 * authorizes this internal node-to-node action per index (FLS/DLS metadata included) exactly like a
 * normal {@code indices:data/read/*} request — mirroring {@code NodeTermsEnumRequest}'s own use of
 * {@link IndicesRequest} for the same reason.
 */
public class NodeSuggestValuesRequest extends AbstractTransportRequest implements IndicesRequest {

    private final String field;
    private final String prefix;
    private final Set<ShardId> shardIds;
    private final int size;
    private final long timeoutMillis;
    private final long coordinatorStartedTimeMillis;

    private long nodeStartedTimeMillis;

    public NodeSuggestValuesRequest(
        String field,
        String prefix,
        Set<ShardId> shardIds,
        int size,
        long timeoutMillis,
        long coordinatorStartedTimeMillis
    ) {
        this.field = field;
        this.prefix = prefix;
        this.shardIds = shardIds;
        this.size = size;
        this.timeoutMillis = timeoutMillis;
        this.coordinatorStartedTimeMillis = coordinatorStartedTimeMillis;
    }

    public NodeSuggestValuesRequest(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.prefix = in.readString();
        int numShards = in.readVInt();
        shardIds = Sets.newHashSetWithExpectedSize(numShards);
        for (int i = 0; i < numShards; i++) {
            shardIds.add(new ShardId(in));
        }
        this.size = in.readVInt();
        this.timeoutMillis = in.readVLong();
        this.coordinatorStartedTimeMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeString(prefix);
        out.writeVInt(shardIds.size());
        for (ShardId shardId : shardIds) {
            shardId.writeTo(out);
        }
        out.writeVInt(size);
        out.writeVLong(timeoutMillis);
        out.writeVLong(coordinatorStartedTimeMillis);
    }

    public String field() {
        return field;
    }

    /** The text already typed before the caret inside the string literal; empty for no prefix filter. */
    public String prefix() {
        return prefix;
    }

    public Set<ShardId> shardIds() {
        return shardIds;
    }

    public int size() {
        return size;
    }

    public long timeoutMillis() {
        return timeoutMillis;
    }

    public void startTimerOnDataNode() {
        nodeStartedTimeMillis = System.currentTimeMillis();
    }

    /** The wall-clock deadline this node must stop working by, using this node's own local clock. */
    public long nodeDeadlineMillis() {
        if (nodeStartedTimeMillis == 0) {
            nodeStartedTimeMillis = System.currentTimeMillis();
        }
        return nodeStartedTimeMillis + timeoutMillis;
    }

    @Override
    public String[] indices() {
        Set<String> indices = new LinkedHashSet<>();
        for (ShardId shardId : shardIds) {
            indices.add(shardId.getIndexName());
        }
        return indices.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.STRICT_EXPAND_OPEN;
    }
}

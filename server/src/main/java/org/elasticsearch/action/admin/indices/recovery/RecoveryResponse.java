/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Information regarding the recovery state of indices and their associated shards.
 */
public class RecoveryResponse extends BaseBroadcastResponse implements ChunkedToXContentObject {

    private final Map<String, List<RecoveryState>> shardRecoveryStates;

    public RecoveryResponse(StreamInput in) throws IOException {
        super(in);
        shardRecoveryStates = in.readMapOfLists(RecoveryState::readRecoveryState);
    }

    /**
     * Constructs recovery information for a collection of indices and associated shards. Keeps track of how many total shards
     * were seen, and out of those how many were successfully processed and how many failed.
     *
     * @param totalShards       Total count of shards seen
     * @param successfulShards  Count of shards successfully processed
     * @param failedShards      Count of shards which failed to process
     * @param shardRecoveryStates    Map of indices to shard recovery information
     * @param shardFailures     List of failures processing shards
     */
    public RecoveryResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        Map<String, List<RecoveryState>> shardRecoveryStates,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardRecoveryStates = shardRecoveryStates;
    }

    public boolean hasRecoveries() {
        return shardRecoveryStates.size() > 0;
    }

    public Map<String, List<RecoveryState>> shardRecoveryStates() {
        return shardRecoveryStates;
    }

    @Override
    public Iterator<ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            Iterators.single((b, p) -> b.startObject()),
            shardRecoveryStates.entrySet()
                .stream()
                .filter(entry -> entry != null && entry.getValue().isEmpty() == false)
                .map(entry -> (ToXContent) (b, p) -> {
                    b.startObject(entry.getKey());
                    b.startArray("shards");
                    for (RecoveryState recoveryState : entry.getValue()) {
                        b.startObject();
                        recoveryState.toXContent(b, p);
                        b.endObject();
                    }
                    b.endArray();
                    b.endObject();
                    return b;
                })
                .iterator(),
            Iterators.single((b, p) -> b.endObject())
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMapOfLists(shardRecoveryStates, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}

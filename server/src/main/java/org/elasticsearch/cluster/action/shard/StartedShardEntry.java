/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Objects;

public class StartedShardEntry extends AbstractTransportRequest {
    final ShardId shardId;
    final String allocationId;
    final long primaryTerm;
    final String message;
    final ShardLongFieldRange timestampRange;
    final ShardLongFieldRange eventIngestedRange;

    StartedShardEntry(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        allocationId = in.readString();
        primaryTerm = in.readVLong();
        this.message = in.readString();
        this.timestampRange = ShardLongFieldRange.readFrom(in);
        this.eventIngestedRange = ShardLongFieldRange.readFrom(in);
    }

    public StartedShardEntry(
        final ShardId shardId,
        final String allocationId,
        final long primaryTerm,
        final String message,
        final ShardLongFieldRange timestampRange,
        final ShardLongFieldRange eventIngestedRange
    ) {
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.primaryTerm = primaryTerm;
        this.message = message;
        this.timestampRange = timestampRange;
        this.eventIngestedRange = eventIngestedRange;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeString(allocationId);
        out.writeVLong(primaryTerm);
        out.writeString(message);
        timestampRange.writeTo(out);
        eventIngestedRange.writeTo(out);
    }

    @Override
    public String toString() {
        return Strings.format(
            "StartedShardEntry{shardId [%s], allocationId [%s], primary term [%d], message [%s]}",
            shardId,
            allocationId,
            primaryTerm,
            message
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StartedShardEntry that = (StartedShardEntry) o;
        return primaryTerm == that.primaryTerm
            && shardId.equals(that.shardId)
            && allocationId.equals(that.allocationId)
            && message.equals(that.message)
            && timestampRange.equals(that.timestampRange)
            && eventIngestedRange.equals(that.eventIngestedRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, allocationId, primaryTerm, message, timestampRange, eventIngestedRange);
    }
}

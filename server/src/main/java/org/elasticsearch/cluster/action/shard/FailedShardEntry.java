/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Objects;

public class FailedShardEntry extends AbstractTransportRequest {
    final ShardId shardId;
    final String allocationId;
    final long primaryTerm;
    final String message;
    @Nullable
    final Exception failure;
    final boolean markAsStale;

    FailedShardEntry(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        allocationId = in.readString();
        primaryTerm = in.readVLong();
        message = in.readString();
        failure = in.readException();
        markAsStale = in.readBoolean();
    }

    public FailedShardEntry(
        ShardId shardId,
        String allocationId,
        long primaryTerm,
        String message,
        @Nullable Exception failure,
        boolean markAsStale
    ) {
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.primaryTerm = primaryTerm;
        this.message = message;
        this.failure = failure;
        this.markAsStale = markAsStale;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public Exception getFailure() {
        return failure;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeString(allocationId);
        out.writeVLong(primaryTerm);
        out.writeString(message);
        out.writeException(failure);
        out.writeBoolean(markAsStale);
    }

    @Override
    public String toString() {
        return toString(true);
    }

    public String toStringNoFailureStackTrace() {
        return toString(false);
    }

    private String toString(boolean includeStackTrace) {
        return Strings.format(
            "FailedShardEntry{shardId [%s], allocationId [%s], primary term [%d], message [%s], markAsStale [%b], failure [%s]}",
            shardId,
            allocationId,
            primaryTerm,
            message,
            markAsStale,
            failure == null ? null : (includeStackTrace ? ExceptionsHelper.stackTrace(failure) : failure.getMessage())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailedShardEntry that = (FailedShardEntry) o;
        // Exclude message and exception from equals and hashCode
        return Objects.equals(this.shardId, that.shardId)
            && Objects.equals(this.allocationId, that.allocationId)
            && primaryTerm == that.primaryTerm
            && markAsStale == that.markAsStale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, allocationId, primaryTerm, markAsStale);
    }
}

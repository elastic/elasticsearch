/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/// Transport action for batch cancellation of now-undesired recoveries.
/// The elected master node uses this action to directly request cancellation of recoveries.
// TODO: Introduce transport version when we wire this up to the master
public class CancelRecoveriesAction {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("internal:index/shard/recovery/cancel_recoveries");

    /// Request to cancel multiple recoveries in a single batch.
    public static class Request extends LegacyActionRequest {
        private final long clusterStateVersion;
        private final List<ShardRecoveryCancellation> shardRecoveryCancellations;

        public Request(long clusterStateVersion, List<ShardRecoveryCancellation> shardRecoveryCancellations) {
            this.clusterStateVersion = clusterStateVersion;
            this.shardRecoveryCancellations = shardRecoveryCancellations;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.clusterStateVersion = in.readVLong();
            this.shardRecoveryCancellations = in.readCollectionAsList(ShardRecoveryCancellation::new);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(clusterStateVersion);
            out.writeCollection(shardRecoveryCancellations);
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        public List<ShardRecoveryCancellation> cancellations() {
            return shardRecoveryCancellations;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return clusterStateVersion == request.clusterStateVersion
                && Objects.equals(shardRecoveryCancellations, request.shardRecoveryCancellations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterStateVersion, shardRecoveryCancellations);
        }
    }

    /// Details of a single shard recovery to be cancelled.
    public static class ShardRecoveryCancellation implements Writeable {
        private final ShardId shardId;
        private final String allocationId;
        private final boolean cancelIfStarted;
        @Nullable
        private final String reason;

        public ShardRecoveryCancellation(ShardId shardId, String allocationId, boolean cancelInProgress, @Nullable String reason) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.cancelIfStarted = cancelInProgress;
            this.reason = reason;
        }

        public ShardRecoveryCancellation(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);
            this.allocationId = in.readString();
            this.cancelIfStarted = in.readBoolean();
            this.reason = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeBoolean(cancelIfStarted);
            out.writeOptionalString(reason);
        }

        public ShardId shardId() {
            return shardId;
        }

        public String allocationId() {
            return allocationId;
        }

        public boolean cancelIfStarted() {
            return cancelIfStarted;
        }

        @Nullable
        public String reason() {
            return reason;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ShardRecoveryCancellation that = (ShardRecoveryCancellation) o;
            return cancelIfStarted == that.cancelIfStarted
                && Objects.equals(shardId, that.shardId)
                && Objects.equals(allocationId, that.allocationId)
                && Objects.equals(reason, that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, allocationId, cancelIfStarted, reason);
        }

        @Override
        public String toString() {
            return "ShardRecoveryCancellation{"
                + "shardId="
                + shardId
                + ", allocationId='"
                + allocationId
                + ", cancelIfStarted="
                + cancelIfStarted
                + ", reason='"
                + reason
                + '}';
        }
    }
}

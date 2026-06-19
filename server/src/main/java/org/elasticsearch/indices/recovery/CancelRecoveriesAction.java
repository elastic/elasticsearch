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
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/// Transport action for batch cancellation of now-undesired recoveries.
/// The elected master node uses this action to directly request cancellation of recoveries.
// TODO: Introduce transport version when we wire this up on the master
public class CancelRecoveriesAction {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:index/shard/recovery/cancel_recoveries");

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

        public ShardRecoveryCancellation(ShardId shardId, String allocationId, boolean cancelIfStarted) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.cancelIfStarted = cancelIfStarted;
        }

        public ShardRecoveryCancellation(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);
            this.allocationId = in.readString();
            this.cancelIfStarted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeBoolean(cancelIfStarted);
        }

        public ShardId shardId() {
            return shardId;
        }

        public String allocationId() {
            return allocationId;
        }

        /// Whether to cancel the recovery if it has already started, or only if it is still enqueued.
        public boolean cancelIfStarted() {
            return cancelIfStarted;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ShardRecoveryCancellation that = (ShardRecoveryCancellation) o;
            return cancelIfStarted == that.cancelIfStarted
                && Objects.equals(shardId, that.shardId)
                && Objects.equals(allocationId, that.allocationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, allocationId, cancelIfStarted);
        }

        @Override
        public String toString() {
            return "ShardRecoveryCancellation{"
                + "shardId="
                + shardId
                + ", allocationId='"
                + allocationId
                + "', cancelIfStarted="
                + cancelIfStarted
                + "}";
        }
    }

    /// Response containing the allocation IDs of recoveries that were found in the throttling queue and cancelled.
    /// The master can use this information to immediately update cluster state without waiting for a separate
    /// `ShardStateAction.shardFailed` notification from the data node.
    public static class Response extends ActionResponse {
        private final Set<String> cancelledInQueue;

        public Response(Set<String> cancelledInQueue) {
            this.cancelledInQueue = Set.copyOf(cancelledInQueue);
        }

        public Response(StreamInput in) throws IOException {
            this.cancelledInQueue = in.readCollectionAsImmutableSet(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(cancelledInQueue);
        }

        /// Returns the allocation IDs of recoveries that were cancelled from the throttling queue.
        public Set<String> cancelledInQueue() {
            return cancelledInQueue;
        }
    }
}

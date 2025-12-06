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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class StatelessPrimaryRelocationAction {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(
        "internal:index/shard/recovery/stateless_primary_relocation"
    );

    public static class Request extends LegacyActionRequest {

        private final long recoveryId;
        private final ShardId shardId;
        private final DiscoveryNode targetNode;
        private final String targetAllocationId;
        private final long clusterStateVersion;

        public Request(long recoveryId, ShardId shardId, DiscoveryNode targetNode, String targetAllocationId, long clusterStateVersion) {
            this.recoveryId = recoveryId;
            this.shardId = shardId;
            this.targetNode = targetNode;
            this.targetAllocationId = targetAllocationId;
            this.clusterStateVersion = clusterStateVersion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            recoveryId = in.readVLong();
            shardId = new ShardId(in);
            targetNode = new DiscoveryNode(in);
            targetAllocationId = in.readString();
            clusterStateVersion = in.readVLong();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(recoveryId);
            shardId.writeTo(out);
            targetNode.writeTo(out);
            out.writeString(targetAllocationId);
            out.writeVLong(clusterStateVersion);
        }

        public long recoveryId() {
            return recoveryId;
        }

        public ShardId shardId() {
            return shardId;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
        }

        public String targetAllocationId() {
            return targetAllocationId;
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return recoveryId == request.recoveryId
                && shardId.equals(request.shardId)
                && targetNode.equals(request.targetNode)
                && targetAllocationId.equals(request.targetAllocationId)
                && clusterStateVersion == request.clusterStateVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(recoveryId, shardId, targetNode, targetAllocationId, clusterStateVersion);
        }

        @Override
        public String toString() {
            return "Request{"
                + "shardId="
                + shardId
                + ", targetNode="
                + targetNode.descriptionWithoutAttributes()
                + ", recoveryId="
                + recoveryId
                + ", targetAllocationId='"
                + targetAllocationId
                + "', clusterStateVersion="
                + clusterStateVersion
                + '}';
        }
    }
}

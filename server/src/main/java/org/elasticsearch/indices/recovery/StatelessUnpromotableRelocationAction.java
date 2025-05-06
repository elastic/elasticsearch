/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class StatelessUnpromotableRelocationAction {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(
        "internal:index/shard/recovery/stateless_unpromotable_relocation"
    );

    public static class Request extends ActionRequest {
        private final long recoveryId;
        private final ShardId shardId;
        private final String targetAllocationId;
        private final long clusterStateVersion;

        public Request(long recoveryId, ShardId shardId, String targetAllocationId, long clusterStateVersion) {
            this.recoveryId = recoveryId;
            this.shardId = shardId;
            this.targetAllocationId = targetAllocationId;
            this.clusterStateVersion = clusterStateVersion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            recoveryId = in.readVLong();
            shardId = new ShardId(in);
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
            out.writeString(targetAllocationId);
            out.writeVLong(clusterStateVersion);
        }

        public long getRecoveryId() {
            return recoveryId;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public long getClusterStateVersion() {
            return clusterStateVersion;
        }

        public String getTargetAllocationId() {
            return targetAllocationId;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return recoveryId == request.recoveryId
                && clusterStateVersion == request.clusterStateVersion
                && Objects.equals(shardId, request.shardId)
                && Objects.equals(targetAllocationId, request.targetAllocationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recoveryId, shardId, targetAllocationId, clusterStateVersion);
        }
    }
}

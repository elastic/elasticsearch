/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class StatelessPrimaryRelocationAction {

    private static final Logger logger = LogManager.getLogger(StatelessPrimaryRelocationAction.class);

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(
        "internal:index/shard/recovery/stateless_primary_relocation"
    );

    public static class Request extends ActionRequest {

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
    }

    public static final String LOGGING_BLOB_STORE_ACCESS_HEADER = StatelessPrimaryRelocationAction.class.getCanonicalName()
        + "/logging_blob_store_access";

    public static Releasable loggingBlobStoreAccess(ThreadContext threadContext, ShardId shardId, String message) {
        final var storedContext = threadContext.newStoredContext(List.of(LOGGING_BLOB_STORE_ACCESS_HEADER), List.of());
        threadContext.putTransient(LOGGING_BLOB_STORE_ACCESS_HEADER, new BlobStoreAccessLogger() {
            @Override
            public void onDemandRead(String caller, String blobPath, long start, int len) {
                logger.info(
                    "{} [{}][{}]: [{}] demand read [{}] start={} len={}",
                    shardId,
                    Thread.currentThread().getName(),
                    message,
                    caller,
                    blobPath,
                    start,
                    len
                );
            }

            @Override
            public void onStartRead(String caller, String blobPath, long start, int len) {
                logger.info(
                    "{} [{}][{}]: [{}] start read [{}] start={} len={}",
                    shardId,
                    Thread.currentThread().getName(),
                    message,
                    caller,
                    blobPath,
                    start,
                    len
                );
            }

            @Override
            public void onEndRead(String caller, String blobPath, long start, int len) {
                logger.info(
                    "{} [{}][{}]: [{}] end read [{}] start={} len={}",
                    shardId,
                    Thread.currentThread().getName(),
                    message,
                    caller,
                    blobPath,
                    start,
                    len
                );
            }
        });
        return storedContext;
    }

    public interface BlobStoreAccessLogger {
        void onDemandRead(String caller, String blobPath, long start, int len);

        void onStartRead(String caller, String blobPath, long start, int len);

        void onEndRead(String caller, String blobPath, long start, int len);
    }

    public static final BlobStoreAccessLogger NO_OP_BLOB_STORE_ACCESS_LOGGER = new BlobStoreAccessLogger() {
        @Override
        public void onDemandRead(String caller, String blobPath, long start, int len) {}

        @Override
        public void onStartRead(String caller, String blobPath, long start, int len) {}

        @Override
        public void onEndRead(String caller, String blobPath, long start, int len) {}
    };

    public static BlobStoreAccessLogger getBlobStoreAccessLogger(ThreadContext threadContext) {
        return Objects.requireNonNullElse(threadContext.getTransient(LOGGING_BLOB_STORE_ACCESS_HEADER), NO_OP_BLOB_STORE_ACCESS_LOGGER);
    }
}

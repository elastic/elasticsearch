/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Response for {@link IndicesShardStoresAction}
 *
 * Consists of {@link StoreStatus}s for requested indices grouped by
 * indices and shard ids and a list of encountered node {@link Failure}s
 */
public class IndicesShardStoresResponse extends ActionResponse implements ChunkedToXContentObject {

    /**
     * Shard store information from a node
     */
    public static class StoreStatus implements Writeable, ToXContentFragment, Comparable<StoreStatus> {
        private final DiscoveryNode node;
        private final String allocationId;
        private Exception storeException;
        private final AllocationStatus allocationStatus;

        /**
         * The status of the shard store with respect to the cluster
         */
        public enum AllocationStatus {

            /**
             * Allocated as primary
             */
            PRIMARY((byte) 0),

            /**
             * Allocated as a replica
             */
            REPLICA((byte) 1),

            /**
             * Not allocated
             */
            UNUSED((byte) 2);

            private final byte id;

            AllocationStatus(byte id) {
                this.id = id;
            }

            private static AllocationStatus fromId(byte id) {
                return switch (id) {
                    case 0 -> PRIMARY;
                    case 1 -> REPLICA;
                    case 2 -> UNUSED;
                    default -> throw new IllegalArgumentException("unknown id for allocation status [" + id + "]");
                };
            }

            public String value() {
                return switch (id) {
                    case 0 -> "primary";
                    case 1 -> "replica";
                    case 2 -> "unused";
                    default -> throw new IllegalArgumentException("unknown id for allocation status [" + id + "]");
                };
            }

            private static AllocationStatus readFrom(StreamInput in) throws IOException {
                return fromId(in.readByte());
            }

            private void writeTo(StreamOutput out) throws IOException {
                out.writeByte(id);
            }
        }

        public StoreStatus(StreamInput in) throws IOException {
            node = new DiscoveryNode(in);
            allocationId = in.readOptionalString();
            allocationStatus = AllocationStatus.readFrom(in);
            if (in.readBoolean()) {
                storeException = in.readException();
            }
        }

        public StoreStatus(DiscoveryNode node, String allocationId, AllocationStatus allocationStatus, Exception storeException) {
            this.node = node;
            this.allocationId = allocationId;
            this.allocationStatus = allocationStatus;
            this.storeException = storeException;
        }

        /**
         * Node the store belongs to
         */
        public DiscoveryNode getNode() {
            return node;
        }

        /**
         * AllocationStatus id of the store, used to select the store that will be
         * used as a primary.
         */
        public String getAllocationId() {
            return allocationId;
        }

        /**
         * Exception while trying to open the
         * shard index or from when the shard failed
         */
        public Exception getStoreException() {
            return storeException;
        }

        /**
         * The allocationStatus status of the store.
         * {@link AllocationStatus#PRIMARY} indicates a primary shard copy
         * {@link AllocationStatus#REPLICA} indicates a replica shard copy
         * {@link AllocationStatus#UNUSED} indicates an unused shard copy
         */
        public AllocationStatus getAllocationStatus() {
            return allocationStatus;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            out.writeOptionalString(allocationId);
            allocationStatus.writeTo(out);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeException(storeException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            node.toXContent(builder, params);
            if (allocationId != null) {
                builder.field(Fields.ALLOCATION_ID, allocationId);
            }
            builder.field(Fields.ALLOCATED, allocationStatus.value());
            if (storeException != null) {
                builder.startObject(Fields.STORE_EXCEPTION);
                ElasticsearchException.generateThrowableXContent(builder, params, storeException);
                builder.endObject();
            }
            return builder;
        }

        @Override
        public int compareTo(StoreStatus other) {
            if (storeException != null && other.storeException == null) {
                return 1;
            } else if (other.storeException != null && storeException == null) {
                return -1;
            }
            if (allocationId != null && other.allocationId == null) {
                return -1;
            } else if (allocationId == null && other.allocationId != null) {
                return 1;
            } else if (allocationId == null && other.allocationId == null) {
                return Integer.compare(allocationStatus.id, other.allocationStatus.id);
            } else {
                int compare = Integer.compare(allocationStatus.id, other.allocationStatus.id);
                if (compare == 0) {
                    return allocationId.compareTo(other.allocationId);
                }
                return compare;
            }
        }
    }

    /**
     * Single node failure while retrieving shard store information
     */
    public static class Failure extends DefaultShardOperationFailedException {
        private final String nodeId;

        public Failure(String nodeId, String index, int shardId, Throwable reason) {
            super(index, shardId, reason);
            this.nodeId = nodeId;
        }

        private Failure(StreamInput in) throws IOException {
            readFrom(in, this);
            nodeId = in.readString();
        }

        public String nodeId() {
            return nodeId;
        }

        static Failure readFailure(StreamInput in) throws IOException {
            return new Failure(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("node", nodeId());
            return super.innerToXContent(builder, params);
        }
    }

    private final Map<String, Map<Integer, List<StoreStatus>>> storeStatuses;
    private final List<Failure> failures;

    public IndicesShardStoresResponse(Map<String, Map<Integer, List<StoreStatus>>> storeStatuses, List<Failure> failures) {
        this.storeStatuses = storeStatuses;
        this.failures = failures;
    }

    public IndicesShardStoresResponse(StreamInput in) throws IOException {
        super(in);
        storeStatuses = in.readImmutableMap(
            StreamInput::readString,
            i -> i.readImmutableMap(StreamInput::readInt, j -> j.readImmutableList(StoreStatus::new))
        );
        failures = in.readImmutableList(Failure::readFailure);
    }

    /**
     * Returns {@link StoreStatus}s
     * grouped by their index names and shard ids.
     */
    public Map<String, Map<Integer, List<StoreStatus>>> getStoreStatuses() {
        return storeStatuses;
    }

    /**
     * Returns node {@link Failure}s encountered
     * while executing the request
     */
    public List<Failure> getFailures() {
        return failures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(
            storeStatuses,
            StreamOutput::writeString,
            (o, v) -> o.writeMap(v, StreamOutput::writeInt, StreamOutput::writeCollection)
        );
        out.writeList(failures);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),

            failures.isEmpty()
                ? Collections.emptyIterator()
                : Iterators.concat(
                    ChunkedToXContentHelper.startArray(Fields.FAILURES),
                    failures.iterator(),
                    ChunkedToXContentHelper.endArray()
                ),

            ChunkedToXContentHelper.startObject(Fields.INDICES),

            Iterators.flatMap(
                storeStatuses.entrySet().iterator(),
                indexShards -> Iterators.concat(
                    ChunkedToXContentHelper.startObject(indexShards.getKey()),
                    ChunkedToXContentHelper.startObject(Fields.SHARDS),
                    Iterators.flatMap(
                        indexShards.getValue().entrySet().iterator(),
                        shardStatusesEntry -> Iterators.single((ToXContent) (builder, params) -> {
                            builder.startObject(String.valueOf(shardStatusesEntry.getKey())).startArray(Fields.STORES);
                            for (StoreStatus storeStatus : shardStatusesEntry.getValue()) {
                                builder.startObject();
                                storeStatus.toXContent(builder, params);
                                builder.endObject();
                            }
                            return builder.endArray().endObject();
                        })
                    ),
                    ChunkedToXContentHelper.endObject(),
                    ChunkedToXContentHelper.endObject()
                )
            ),

            ChunkedToXContentHelper.endObject(),
            ChunkedToXContentHelper.endObject()
        );
    }

    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String FAILURES = "failures";
        static final String STORES = "stores";
        // StoreStatus fields
        static final String ALLOCATION_ID = "allocation_id";
        static final String STORE_EXCEPTION = "store_exception";
        static final String ALLOCATED = "allocation";
    }
}

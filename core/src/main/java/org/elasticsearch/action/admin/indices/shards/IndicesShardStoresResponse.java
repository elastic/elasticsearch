/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.shards;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus.*;

/**
 * Response for {@link IndicesShardStoresAction}
 *
 * Consists of {@link StoreStatus}s for requested indices grouped by
 * indices and shard ids and a list of encountered node {@link Failure}s
 */
public class IndicesShardStoresResponse extends ActionResponse implements ToXContent {

    /**
     * Shard store information from a node
     */
    public static class StoreStatus implements Streamable, ToXContent, Comparable<StoreStatus> {
        private DiscoveryNode node;
        private long version;
        private Throwable storeException;
        private Allocation allocation;

        /**
         * The status of the shard store with respect to the cluster
         */
        public enum Allocation {

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

            Allocation(byte id) {
                this.id = id;
            }

            private static Allocation fromId(byte id) {
                switch (id) {
                    case 0: return PRIMARY;
                    case 1: return REPLICA;
                    case 2: return UNUSED;
                    default: throw new IllegalArgumentException("unknown id for allocation [" + id + "]");
                }
            }

            public String value() {
                switch (id) {
                    case 0: return "primary";
                    case 1: return "replica";
                    case 2: return "unused";
                    default: throw new IllegalArgumentException("unknown id for allocation [" + id + "]");
                }
            }

            private static Allocation readFrom(StreamInput in) throws IOException {
                return fromId(in.readByte());
            }

            private void writeTo(StreamOutput out) throws IOException {
                out.writeByte(id);
            }
        }

        private StoreStatus() {
        }

        public StoreStatus(DiscoveryNode node, long version, Allocation allocation, Throwable storeException) {
            this.node = node;
            this.version = version;
            this.allocation = allocation;
            this.storeException = storeException;
        }

        /**
         * Node the store belongs to
         */
        public DiscoveryNode getNode() {
            return node;
        }

        /**
         * Version of the store, used to select the store that will be
         * used as a primary.
         */
        public long getVersion() {
            return version;
        }

        /**
         * Exception while trying to open the
         * shard index or from when the shard failed
         */
        public Throwable getStoreException() {
            return storeException;
        }

        /**
         * The allocation status of the store.
         * {@link Allocation#PRIMARY} indicates a primary shard copy
         * {@link Allocation#REPLICA} indicates a replica shard copy
         * {@link Allocation#UNUSED} indicates an unused shard copy
         */
        public Allocation getAllocation() {
            return allocation;
        }

        static StoreStatus readStoreStatus(StreamInput in) throws IOException {
            StoreStatus storeStatus = new StoreStatus();
            storeStatus.readFrom(in);
            return storeStatus;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            node = DiscoveryNode.readNode(in);
            version = in.readLong();
            allocation = Allocation.readFrom(in);
            if (in.readBoolean()) {
                storeException = in.readThrowable();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            out.writeLong(version);
            allocation.writeTo(out);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeThrowable(storeException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            node.toXContent(builder, params);
            builder.field(Fields.VERSION, version);
            builder.field(Fields.ALLOCATED, allocation.value());
            if (storeException != null) {
                builder.startObject(Fields.STORE_EXCEPTION);
                ElasticsearchException.toXContent(builder, params, storeException);
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
            } else {
                int compare = Long.compare(other.version, version);
                if (compare == 0) {
                    return Integer.compare(allocation.id, other.allocation.id);
                }
                return compare;
            }
        }
    }

    /**
     * Single node failure while retrieving shard store information
     */
    public static class Failure extends DefaultShardOperationFailedException {
        private String nodeId;

        public Failure(String nodeId, String index, int shardId, Throwable reason) {
            super(index, shardId, reason);
            this.nodeId = nodeId;
        }

        private Failure() {
        }

        public String nodeId() {
            return nodeId;
        }

        public static Failure readFailure(StreamInput in) throws IOException {
            Failure failure = new Failure();
            failure.readFrom(in);
            return failure;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            nodeId = in.readString();
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            super.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("node", nodeId());
            super.toXContent(builder, params);
            return builder;
        }
    }

    private ImmutableOpenMap<String, ImmutableOpenIntMap<List<StoreStatus>>> storeStatuses;
    private List<Failure> failures;

    public IndicesShardStoresResponse(ImmutableOpenMap<String, ImmutableOpenIntMap<List<StoreStatus>>> storeStatuses, List<Failure> failures) {
        this.storeStatuses = storeStatuses;
        this.failures = failures;
    }

    IndicesShardStoresResponse() {
        this(ImmutableOpenMap.<String, ImmutableOpenIntMap<List<StoreStatus>>>of(), Collections.<Failure>emptyList());
    }

    /**
     * Returns {@link StoreStatus}s
     * grouped by their index names and shard ids.
     */
    public ImmutableOpenMap<String, ImmutableOpenIntMap<List<StoreStatus>>> getStoreStatuses() {
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numResponse = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<List<StoreStatus>>> storeStatusesBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < numResponse; i++) {
            String index = in.readString();
            int indexEntries = in.readVInt();
            ImmutableOpenIntMap.Builder<List<StoreStatus>> shardEntries = ImmutableOpenIntMap.builder();
            for (int shardCount = 0; shardCount < indexEntries; shardCount++) {
                int shardID = in.readInt();
                int nodeEntries = in.readVInt();
                List<StoreStatus> storeStatuses = new ArrayList<>(nodeEntries);
                for (int nodeCount = 0; nodeCount < nodeEntries; nodeCount++) {
                    storeStatuses.add(readStoreStatus(in));
                }
                shardEntries.put(shardID, storeStatuses);
            }
            storeStatusesBuilder.put(index, shardEntries.build());
        }
        int numFailure = in.readVInt();
        List<Failure> failureBuilder = new ArrayList<>();
        for (int i = 0; i < numFailure; i++) {
            failureBuilder.add(Failure.readFailure(in));
        }
        storeStatuses = storeStatusesBuilder.build();
        failures = Collections.unmodifiableList(failureBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(storeStatuses.size());
        for (ObjectObjectCursor<String, ImmutableOpenIntMap<List<StoreStatus>>> indexShards : storeStatuses) {
            out.writeString(indexShards.key);
            out.writeVInt(indexShards.value.size());
            for (IntObjectCursor<List<StoreStatus>> shardStatusesEntry : indexShards.value) {
                out.writeInt(shardStatusesEntry.key);
                out.writeVInt(shardStatusesEntry.value.size());
                for (StoreStatus storeStatus : shardStatusesEntry.value) {
                    storeStatus.writeTo(out);
                }
            }
        }
        out.writeVInt(failures.size());
        for (ShardOperationFailedException failure : failures) {
            failure.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (failures.size() > 0) {
            builder.startArray(Fields.FAILURES);
            for (ShardOperationFailedException failure : failures) {
                builder.startObject();
                failure.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }

        builder.startObject(Fields.INDICES);
        for (ObjectObjectCursor<String, ImmutableOpenIntMap<List<StoreStatus>>> indexShards : storeStatuses) {
            builder.startObject(indexShards.key);

            builder.startObject(Fields.SHARDS);
            for (IntObjectCursor<List<StoreStatus>> shardStatusesEntry : indexShards.value) {
                builder.startObject(String.valueOf(shardStatusesEntry.key));
                builder.startArray(Fields.STORES);
                for (StoreStatus storeStatus : shardStatusesEntry.value) {
                    builder.startObject();
                    storeStatus.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();

                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString STORES = new XContentBuilderString("stores");
        // StoreStatus fields
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString STORE_EXCEPTION = new XContentBuilderString("store_exception");
        static final XContentBuilderString ALLOCATED = new XContentBuilderString("allocation");
    }
}

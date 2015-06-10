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
import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;

/**
 * Consists of {@link ShardStoreStatus}s for request indices grouped by
 * indices and shard ids and a list of node {@link Failure}s encountered
 */
public class IndicesShardsStoresResponse extends ActionResponse implements ToXContent {

    /**
     * Shard store information from a node
     */
    public static class ShardStoreStatus implements Streamable, ToXContent {
        private DiscoveryNode node;
        private long version;
        private Throwable storeException;

        private ShardStoreStatus() {
        }

        ShardStoreStatus(DiscoveryNode node, long version, Throwable storeException) {
            this.node = node;
            this.version = version;
            this.storeException = storeException;
        }

        /**
         * node the shard belongs to
         */
        public DiscoveryNode getNode() {
            return node;
        }

        /**
         * version of the shard, the highest
         * version of the replica shards are
         * promoted to primary if needed
         */
        public long getVersion() {
            return version;
        }

        /**
         * exception while trying to open the
         * shard index or from when the shard failed
         */
        public Throwable getStoreException() {
            return storeException;
        }

        private static ShardStoreStatus readShardStoreStatus(StreamInput in) throws IOException {
            ShardStoreStatus shardStoreStatus = new ShardStoreStatus();
            shardStoreStatus.readFrom(in);
            return shardStoreStatus;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            node = DiscoveryNode.readNode(in);
            version = in.readLong();
            if (in.readBoolean()) {
                storeException = in.readThrowable();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            out.writeLong(version);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeThrowable(storeException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.NODE);
            builder.field("name", node.name());
            builder.field("host", node.getHostName());
            builder.field("address", node.getHostAddress());
            builder.endObject();
            builder.field(Fields.VERSION, version);
            if (storeException != null) {
                builder.startObject(Fields.STORE_EXCEPTION);
                ElasticsearchException.toXContent(builder, params, storeException);
                builder.endObject();
            }
            return builder;
        }
    }

    /**
     * Single node failure while retrieving shard store information
     */
    public static class Failure implements ShardOperationFailedException {
        private String nodeId;
        private String index;
        private int shardId;
        private Throwable reason;
        private RestStatus status;

        Failure(String nodeId, String index, int shardId, Throwable reason) {
            this.nodeId = nodeId;
            this.index = index;
            this.shardId = shardId;
            this.reason = reason;
            status = ExceptionsHelper.status(reason);
        }

        private Failure() {
        }

        public String nodeId() {
            return nodeId;
        }

        @Override
        public String index() {
            return index;
        }

        @Override
        public int shardId() {
            return shardId;
        }

        @Override
        public String reason() {
            return detailedMessage(reason);
        }

        @Override
        public RestStatus status() {
            return status;
        }

        @Override
        public Throwable getCause() {
            return reason;
        }

        public static Failure readFailure(StreamInput in) throws IOException {
            Failure failure = new Failure();
            failure.readFrom(in);
            return failure;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            nodeId = in.readString();
            index = in.readString();
            shardId = in.readInt();
            reason = in.readThrowable();
            status = RestStatus.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(index);
            out.writeInt(shardId);
            out.writeThrowable(reason);
            RestStatus.writeTo(out, status);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("node", nodeId());
            builder.field("shard", shardId());
            builder.field("index", index());
            builder.field("status", status.name());
            if (reason != null) {
                builder.field("reason");
                builder.startObject();
                ElasticsearchException.toXContent(builder, params, reason);
                builder.endObject();
            }
            return builder;
        }
    }

    private ImmutableOpenMap<String, ImmutableOpenIntMap<List<ShardStoreStatus>>> shardStatuses = ImmutableOpenMap.of();
    private ImmutableList<Failure> failures = ImmutableList.of();

    IndicesShardsStoresResponse(ImmutableOpenMap<String, ImmutableOpenIntMap<List<ShardStoreStatus>>> shardStatuses, ImmutableList<Failure> failures) {
        this.shardStatuses = shardStatuses;
        this.failures = failures;
    }

    IndicesShardsStoresResponse() {
    }

    /**
     * Returns {@link ShardStoreStatus}s
     * grouped by their index names and shard ids.
     */
    public ImmutableOpenMap<String, ImmutableOpenIntMap<List<ShardStoreStatus>>> getShardStatuses() {
        return shardStatuses;
    }

    /**
     * Returns node {@link Failure}s encountered
     * while executing the request
     */
    public ImmutableList<Failure> getFailures() {
        return failures;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numResponse = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<List<ShardStoreStatus>>> shardStatusesBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < numResponse; i++) {
            String index = in.readString();
            int indexEntries = in.readVInt();
            ImmutableOpenIntMap.Builder<List<ShardStoreStatus>> shardEntries = ImmutableOpenIntMap.builder();
            for (int shardCount = 0; shardCount < indexEntries; shardCount++) {
                int shardID = in.readInt();
                int nodeEntries = in.readVInt();
                List<ShardStoreStatus> shardStoreStatuses = new ArrayList<>(nodeEntries);
                for (int nodeCount = 0; nodeCount < nodeEntries; nodeCount++) {
                    shardStoreStatuses.add(ShardStoreStatus.readShardStoreStatus(in));
                }
                shardEntries.put(shardID, shardStoreStatuses);
            }
            shardStatusesBuilder.put(index, shardEntries.build());
        }
        int numFailure = in.readVInt();
        ImmutableList.Builder<Failure> failureBuilder = ImmutableList.builder();
        for (int i = 0; i < numFailure; i++) {
            failureBuilder.add(Failure.readFailure(in));
        }
        shardStatuses = shardStatusesBuilder.build();
        failures = failureBuilder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardStatuses.size());
        for (ObjectObjectCursor<String, ImmutableOpenIntMap<List<ShardStoreStatus>>> indexShards : shardStatuses) {
            out.writeString(indexShards.key);
            out.writeVInt(indexShards.value.size());
            for (IntObjectCursor<List<ShardStoreStatus>> shardStatusesEntry : indexShards.value) {
                out.writeInt(shardStatusesEntry.key);
                out.writeVInt(shardStatusesEntry.value.size());
                for (ShardStoreStatus shardStoreStatus : shardStatusesEntry.value) {
                    shardStoreStatus.writeTo(out);
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
        for (ObjectObjectCursor<String, ImmutableOpenIntMap<List<ShardStoreStatus>>> indexShards : shardStatuses) {
            builder.startObject(indexShards.key);

            builder.startObject(Fields.SHARDS);
            for (IntObjectCursor<List<ShardStoreStatus>> shardStatusesEntry : indexShards.value) {
                builder.startObject(String.valueOf(shardStatusesEntry.key));
                builder.field(Fields.ALLOCATED, allocated(shardStatusesEntry.value));
                builder.startArray(Fields.STORES);
                for (ShardStoreStatus shardStoreStatus : shardStatusesEntry.value) {
                    builder.startObject();
                    shardStoreStatus.toXContent(builder, params);
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

    private boolean allocated(List<ShardStoreStatus> shardStoreStatuses) {
        for (ShardStoreStatus shardStoreStatuse : shardStoreStatuses) {
            if (shardStoreStatuse.getVersion() != -1) {
                return true;
            }
        }
        return false;
    }

    static final class Fields {
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        public static final XContentBuilderString STORES = new XContentBuilderString("stores");
        public static final XContentBuilderString ALLOCATED = new XContentBuilderString("primary_allocated");
        // ShardStatus fields
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString STORE_EXCEPTION = new XContentBuilderString("store_exception");
    }
}

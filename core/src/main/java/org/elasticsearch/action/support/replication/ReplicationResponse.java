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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Base class for write action responses.
 */
public class ReplicationResponse extends ActionResponse {

    public final static ReplicationResponse.ShardInfo.Failure[] EMPTY = new ReplicationResponse.ShardInfo.Failure[0];

    private ShardInfo shardInfo;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardInfo = ReplicationResponse.ShardInfo.readShardInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardInfo.writeTo(out);
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public void setShardInfo(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }

    public static class ShardInfo implements Streamable, ToXContent {

        private int total;
        private int successful;
        private Failure[] failures = EMPTY;

        public ShardInfo() {
        }

        public ShardInfo(int total, int successful, Failure... failures) {
            assert total >= 0 && successful >= 0;
            this.total = total;
            this.successful = successful;
            this.failures = failures;
        }

        /**
         * @return the total number of shards the write should go to (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getTotal() {
            return total;
        }

        /**
         * @return the total number of shards the write succeeded on (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getSuccessful() {
            return successful;
        }

        /**
         * @return The total number of replication failures.
         */
        public int getFailed() {
            return failures.length;
        }

        /**
         * @return The replication failures that have been captured in the case writes have failed on replica shards.
         */
        public Failure[] getFailures() {
            return failures;
        }

        public RestStatus status() {
            RestStatus status = RestStatus.OK;
            for (Failure failure : failures) {
                if (failure.primary() && failure.status().getStatus() > status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            total = in.readVInt();
            successful = in.readVInt();
            int size = in.readVInt();
            failures = new Failure[size];
            for (int i = 0; i < size; i++) {
                Failure failure = new Failure();
                failure.readFrom(in);
                failures[i] = failure;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(failures.length);
            for (Failure failure : failures) {
                failure.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields._SHARDS);
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.SUCCESSFUL, successful);
            builder.field(Fields.FAILED, getFailed());
            if (failures.length > 0) {
                builder.startArray(Fields.FAILURES);
                for (Failure failure : failures) {
                    failure.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public static ShardInfo readShardInfo(StreamInput in) throws IOException {
            ShardInfo shardInfo = new ShardInfo();
            shardInfo.readFrom(in);
            return shardInfo;
        }

        public static class Failure implements ShardOperationFailedException, ToXContent {

            private ShardId shardId;
            private String nodeId;
            private Throwable cause;
            private RestStatus status;
            private boolean primary;

            public Failure(ShardId  shardId, @Nullable String nodeId, Throwable cause, RestStatus status, boolean primary) {
                this.shardId = shardId;
                this.nodeId = nodeId;
                this.cause = cause;
                this.status = status;
                this.primary = primary;
            }

            Failure() {
            }

            /**
             * @return On what index the failure occurred.
             */
            @Override
            public String index() {
                return shardId.getIndexName();
            }

            /**
             * @return On what shard id the failure occurred.
             */
            @Override
            public int shardId() {
                return shardId.id();
            }

            public ShardId fullShardId() {
                return shardId;
            }

            /**
             * @return On what node the failure occurred.
             */
            @Nullable
            public String nodeId() {
                return nodeId;
            }

            /**
             * @return A text description of the failure
             */
            @Override
            public String reason() {
                return ExceptionsHelper.detailedMessage(cause);
            }

            /**
             * @return The status to report if this failure was a primary failure.
             */
            @Override
            public RestStatus status() {
                return status;
            }

            @Override
            public Throwable getCause() {
                return cause;
            }

            /**
             * @return Whether this failure occurred on a primary shard.
             * (this only reports true for delete by query)
             */
            public boolean primary() {
                return primary;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                shardId = ShardId.readShardId(in);
                nodeId = in.readOptionalString();
                cause = in.readThrowable();
                status = RestStatus.readFrom(in);
                primary = in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                shardId.writeTo(out);
                out.writeOptionalString(nodeId);
                out.writeThrowable(cause);
                RestStatus.writeTo(out, status);
                out.writeBoolean(primary);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(Fields._INDEX, shardId.getIndexName());
                builder.field(Fields._SHARD, shardId.id());
                builder.field(Fields._NODE, nodeId);
                builder.field(Fields.REASON);
                builder.startObject();
                ElasticsearchException.toXContent(builder, params, cause);
                builder.endObject();
                builder.field(Fields.STATUS, status);
                builder.field(Fields.PRIMARY, primary);
                builder.endObject();
                return builder;
            }

            private static class Fields {

                private static final String _INDEX = "_index";
                private static final String _SHARD = "_shard";
                private static final String _NODE = "_node";
                private static final String REASON = "reason";
                private static final String STATUS = "status";
                private static final String PRIMARY = "primary";

            }
        }

        private static class Fields {

            private static final String _SHARDS = "_shards";
            private static final String TOTAL = "total";
            private static final String SUCCESSFUL = "successful";
            private static final String PENDING = "pending";
            private static final String FAILED = "failed";
            private static final String FAILURES = "failures";

        }
    }
}

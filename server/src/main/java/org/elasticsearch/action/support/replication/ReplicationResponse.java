/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Base class for write action responses.
 */
public class ReplicationResponse extends ActionResponse {

    public static final ReplicationResponse.ShardInfo.Failure[] NO_FAILURES = new ReplicationResponse.ShardInfo.Failure[0];

    private ShardInfo shardInfo;

    public ReplicationResponse() {}

    public ReplicationResponse(StreamInput in) throws IOException {
        shardInfo = ReplicationResponse.ShardInfo.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardInfo.writeTo(out);
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public void setShardInfo(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }

    public static class ShardInfo implements Writeable, ToXContentObject {

        // cache the most commonly used instances where all shard operations succeeded to save allocations on the transport layer
        private static final ShardInfo[] COMMON_INSTANCES = IntStream.range(0, 10)
            .mapToObj(i -> new ShardInfo(i, i, NO_FAILURES))
            .toArray(ShardInfo[]::new);

        public static final ShardInfo EMPTY = COMMON_INSTANCES[0];

        private static final String TOTAL = "total";
        private static final String SUCCESSFUL = "successful";
        private static final String FAILED = "failed";
        private static final String FAILURES = "failures";

        private final int total;
        private final int successful;
        private final Failure[] failures;

        public static ShardInfo readFrom(StreamInput in) throws IOException {
            int total = in.readVInt();
            int successful = in.readVInt();
            int size = in.readVInt();

            final Failure[] failures;
            if (size > 0) {
                failures = new Failure[size];
                for (int i = 0; i < size; i++) {
                    failures[i] = new Failure(in);
                }
            } else {
                failures = NO_FAILURES;
            }
            return ShardInfo.of(total, successful, failures);
        }

        public static ShardInfo allSuccessful(int total) {
            if (total < COMMON_INSTANCES.length) {
                return COMMON_INSTANCES[total];
            }
            return new ShardInfo(total, total, NO_FAILURES);
        }

        public static ShardInfo of(int total, int successful) {
            if (total == successful) {
                return allSuccessful(total);
            }
            return new ShardInfo(total, successful, ReplicationResponse.NO_FAILURES);
        }

        public static ShardInfo of(int total, int successful, Failure[] failures) {
            if (failures.length == 0) {
                return of(total, successful);
            }
            return new ShardInfo(total, successful, failures);
        }

        private ShardInfo(int total, int successful, Failure[] failures) {
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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeArray(failures);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOTAL, total);
            builder.field(SUCCESSFUL, successful);
            builder.field(FAILED, getFailed());
            if (failures.length > 0) {
                builder.startArray(FAILURES);
                for (Failure failure : failures) {
                    failure.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        public static ShardInfo fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

            int total = 0, successful = 0;
            List<Failure> failuresList = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (TOTAL.equals(currentFieldName)) {
                        total = parser.intValue();
                    } else if (SUCCESSFUL.equals(currentFieldName)) {
                        successful = parser.intValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (FAILURES.equals(currentFieldName)) {
                        failuresList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            failuresList.add(Failure.fromXContent(parser));
                        }
                    } else {
                        parser.skipChildren(); // skip potential inner arrays for forward compatibility
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    parser.skipChildren(); // skip potential inner arrays for forward compatibility
                }
            }
            Failure[] failures = ReplicationResponse.NO_FAILURES;
            if (failuresList != null) {
                failures = failuresList.toArray(ReplicationResponse.NO_FAILURES);
            }
            return new ShardInfo(total, successful, failures);
        }

        @Override
        public String toString() {
            return "ShardInfo{" + "total=" + total + ", successful=" + successful + ", failures=" + Arrays.toString(failures) + '}';
        }

        public static class Failure extends ShardOperationFailedException implements ToXContentObject {

            private static final String _INDEX = "_index";
            private static final String _SHARD = "_shard";
            private static final String _NODE = "_node";
            private static final String REASON = "reason";
            private static final String STATUS = "status";
            private static final String PRIMARY = "primary";

            private final ShardId shardId;
            private final String nodeId;
            private final boolean primary;

            public Failure(StreamInput in) throws IOException {
                shardId = new ShardId(in);
                super.shardId = shardId.getId();
                index = shardId.getIndexName();
                nodeId = in.readOptionalString();
                cause = in.readException();
                status = RestStatus.readFrom(in);
                primary = in.readBoolean();
            }

            public Failure(ShardId shardId, @Nullable String nodeId, Exception cause, RestStatus status, boolean primary) {
                super(shardId.getIndexName(), shardId.getId(), ExceptionsHelper.stackTrace(cause), status, cause);
                this.shardId = shardId;
                this.nodeId = nodeId;
                this.primary = primary;
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
             * @return Whether this failure occurred on a primary shard.
             * (this only reports true for delete by query)
             */
            public boolean primary() {
                return primary;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                shardId.writeTo(out);
                out.writeOptionalString(nodeId);
                out.writeException(cause);
                RestStatus.writeTo(out, status);
                out.writeBoolean(primary);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(_INDEX, shardId.getIndexName());
                builder.field(_SHARD, shardId.id());
                builder.field(_NODE, nodeId);
                builder.field(REASON);
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, cause);
                builder.endObject();
                builder.field(STATUS, status);
                builder.field(PRIMARY, primary);
                builder.endObject();
                return builder;
            }

            public static Failure fromXContent(XContentParser parser) throws IOException {
                XContentParser.Token token = parser.currentToken();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

                String shardIndex = null, nodeId = null;
                int shardId = -1;
                boolean primary = false;
                RestStatus status = null;
                ElasticsearchException reason = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (_INDEX.equals(currentFieldName)) {
                            shardIndex = parser.text();
                        } else if (_SHARD.equals(currentFieldName)) {
                            shardId = parser.intValue();
                        } else if (_NODE.equals(currentFieldName)) {
                            nodeId = parser.text();
                        } else if (STATUS.equals(currentFieldName)) {
                            status = RestStatus.valueOf(parser.text());
                        } else if (PRIMARY.equals(currentFieldName)) {
                            primary = parser.booleanValue();
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (REASON.equals(currentFieldName)) {
                            reason = ElasticsearchException.fromXContent(parser);
                        } else {
                            parser.skipChildren(); // skip potential inner objects for forward compatibility
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        parser.skipChildren(); // skip potential inner arrays for forward compatibility
                    }
                }
                return new Failure(new ShardId(shardIndex, IndexMetadata.INDEX_UUID_NA_VALUE, shardId), nodeId, reason, status, primary);
            }
        }
    }
}

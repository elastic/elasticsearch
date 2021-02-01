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
package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

public class AddIndexBlockResponse extends ShardsAcknowledgedResponse {

    private final List<AddBlockResult> indices;

    AddIndexBlockResponse(StreamInput in) throws IOException {
        super(in, true);
        indices = unmodifiableList(in.readList(AddBlockResult::new));
    }

    public AddIndexBlockResponse(final boolean acknowledged, final boolean shardsAcknowledged, final List<AddBlockResult> indices) {
        super(acknowledged, shardsAcknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indices));
    }

    public List<AddBlockResult> getIndices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
        out.writeList(indices);
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.startArray("indices");
        for (AddBlockResult index : indices) {
            index.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class AddBlockResult implements Writeable, ToXContentFragment {

        private final Index index;
        private final @Nullable Exception exception;
        private final @Nullable AddBlockShardResult[] shards;

        public AddBlockResult(final Index index) {
            this(index, null, null);
        }

        public AddBlockResult(final Index index, final Exception failure) {
            this(index, Objects.requireNonNull(failure), null);
        }

        public AddBlockResult(final Index index, final AddBlockShardResult[] shards) {
            this(index, null, Objects.requireNonNull(shards));
        }

        private AddBlockResult(final Index index, @Nullable final Exception exception, @Nullable final AddBlockShardResult[] shards) {
            this.index = Objects.requireNonNull(index);
            this.exception = exception;
            this.shards = shards;
        }

        AddBlockResult(final StreamInput in) throws IOException {
            this.index = new Index(in);
            this.exception = in.readException();
            this.shards = in.readOptionalArray(AddBlockShardResult::new, AddBlockShardResult[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            index.writeTo(out);
            out.writeException(exception);
            out.writeOptionalArray(shards);
        }

        public Index getIndex() {
            return index;
        }

        public Exception getException() {
            return exception;
        }

        public AddBlockShardResult[] getShards() {
            return shards;
        }

        public boolean hasFailures() {
            if (exception != null) {
                return true;
            }
            if (shards != null) {
                for (AddBlockShardResult shard : shards) {
                    if (shard.hasFailures()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", index.getName());
                if (hasFailures()) {
                    if (exception != null) {
                        builder.startObject("exception");
                        ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                        builder.endObject();
                    } else {
                        builder.startArray("failed_shards");
                        for (AddBlockShardResult shard : shards) {
                            if (shard.hasFailures()) {
                                shard.toXContent(builder, params);
                            }
                        }
                        builder.endArray();
                    }
                } else {
                    builder.field("blocked", true);
                }
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class AddBlockShardResult implements Writeable, ToXContentFragment {

        private final int id;
        private final Failure[] failures;

        public AddBlockShardResult(final int id, final Failure[] failures) {
            this.id = id;
            this.failures = failures;
        }

        AddBlockShardResult(final StreamInput in) throws IOException {
            this.id = in.readVInt();
            this.failures = in.readOptionalArray(Failure::readFailure, Failure[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeVInt(id);
            out.writeOptionalArray(failures);
        }

        public boolean hasFailures() {
            return CollectionUtils.isEmpty(failures) == false;
        }

        public int getId() {
            return id;
        }

        public Failure[] getFailures() {
            return failures;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("id", String.valueOf(id));
                builder.startArray("failures");
                if (failures != null) {
                    for (Failure failure : failures) {
                        failure.toXContent(builder, params);
                    }
                }
                builder.endArray();
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public static class Failure extends DefaultShardOperationFailedException {

            private @Nullable String nodeId;

            private Failure(StreamInput in) throws IOException {
                super(in);
                nodeId = in.readOptionalString();
            }

            public Failure(final String index, final int shardId, final Throwable reason) {
                this(index, shardId, reason, null);
            }

            public Failure(final String index, final int shardId, final Throwable reason, final String nodeId) {
                super(index, shardId, reason);
                this.nodeId = nodeId;
            }

            public String getNodeId() {
                return nodeId;
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalString(nodeId);
            }

            @Override
            public XContentBuilder innerToXContent(final XContentBuilder builder, final Params params) throws IOException {
                if (nodeId != null) {
                    builder.field("node", nodeId);
                }
                return super.innerToXContent(builder, params);
            }

            @Override
            public String toString() {
                return Strings.toString(this);
            }

            static Failure readFailure(final StreamInput in) throws IOException {
                return new Failure(in);
            }
        }
    }
}

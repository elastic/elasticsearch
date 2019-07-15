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
package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class CloseIndexResponse extends ShardsAcknowledgedResponse {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<CloseIndexResponse, Void> PARSER = new ConstructingObjectParser<>("close_index_response",
        true, args -> {
            boolean acknowledged = (boolean) args[0];
            boolean shardsAcknowledged = args[1] != null ? (boolean) args[1] : acknowledged;
            List<IndexResult> indices = args[2] != null ? (List<IndexResult>) args[2] : emptyList();
            return new CloseIndexResponse(acknowledged, shardsAcknowledged, indices);
    });

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareField(optionalConstructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED,
            ObjectParser.ValueType.BOOLEAN);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, name) -> IndexResult.fromXContent(p, name), new ParseField("indices"));
    }

    private final List<IndexResult> indices;

    CloseIndexResponse(StreamInput in) throws IOException {
        super(in, in.getVersion().onOrAfter(Version.V_7_2_0));
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            indices = unmodifiableList(in.readList(IndexResult::new));
        } else {
            indices = unmodifiableList(emptyList());
        }
    }

    public CloseIndexResponse(final boolean acknowledged, final boolean shardsAcknowledged, final List<IndexResult> indices) {
        super(acknowledged, shardsAcknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indices));
    }

    public List<IndexResult> getIndices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            writeShardsAcknowledged(out);
        }
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeList(indices);
        }
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.startObject("indices");
        for (IndexResult index : indices) {
            index.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static CloseIndexResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class IndexResult implements Writeable, ToXContentFragment {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<IndexResult, String> PARSER = new ConstructingObjectParser<>("index_result", true,
            (args, name) -> {
                final Index index = new Index(name, "_na_");
                Exception exception = (Exception) args[1];
                if (exception != null) {
                    assert (boolean) args[0] == false;
                    return new IndexResult(index, exception);
                }
                ShardResult[] shardResults = args[2] != null ? ((List<ShardResult>) args[2]).toArray(new ShardResult[0]) : null;
                if (shardResults != null) {
                    assert (boolean) args[0] == false;
                    return new IndexResult(index, shardResults);
                }
                assert (boolean) args[0];
                return new IndexResult(index);
            });
        static {
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("closed"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p::getTokenLocation);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p::getTokenLocation);
                Exception e = ElasticsearchException.failureFromXContent(p);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p::getTokenLocation);
                return e;
            }, new ParseField("exception"));
            PARSER.declareNamedObjects(optionalConstructorArg(),
                (p, c, id) -> ShardResult.fromXContent(p, id), new ParseField("failedShards"));
        }

        private final Index index;
        private final @Nullable Exception exception;
        private final @Nullable ShardResult[] shards;

        public IndexResult(final Index index) {
            this(index, null, null);
        }

        public IndexResult(final Index index, final Exception failure) {
            this(index, Objects.requireNonNull(failure), null);
        }

        public IndexResult(final Index index, final ShardResult[] shards) {
            this(index, null, Objects.requireNonNull(shards));
        }

        private IndexResult(final Index index, @Nullable final Exception exception, @Nullable final ShardResult[] shards) {
            this.index = Objects.requireNonNull(index);
            this.exception = exception;
            this.shards = shards;
        }

        IndexResult(final StreamInput in) throws IOException {
            this.index = new Index(in);
            this.exception = in.readException();
            this.shards = in.readOptionalArray(ShardResult::new, ShardResult[]::new);
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

        public ShardResult[] getShards() {
            return shards;
        }

        public boolean hasFailures() {
            if (exception != null) {
                return true;
            }
            if (shards != null) {
                for (ShardResult shard : shards) {
                    if (shard.hasFailures()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject(index.getName());
            {
                if (hasFailures()) {
                    builder.field("closed", false);
                    if (exception != null) {
                        builder.startObject("exception");
                        ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                        builder.endObject();
                    } else {
                        builder.startObject("failedShards");
                        for (ShardResult shard : shards) {
                            if (shard.hasFailures()) {
                                shard.toXContent(builder, params);
                            }
                        }
                        builder.endObject();
                    }
                } else {
                    builder.field("closed", true);
                }
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        static IndexResult fromXContent(final XContentParser parser, final String name) {
            return PARSER.apply(parser, name);
        }
    }

    public static class ShardResult implements Writeable, ToXContentFragment {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ShardResult, String> PARSER = new ConstructingObjectParser<>("shard_result", true,
            (arg, id) -> {
                Failure[] failures = arg[0] != null ? ((List<Failure>) arg[0]).toArray(new Failure[0]) : new Failure[0];
                return new ShardResult(Integer.parseInt(id), failures);
            });

        static {
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> Failure.PARSER.apply(p, null), new ParseField("failures"));
        }

        private final int id;
        private final Failure[] failures;

        public ShardResult(final int id, final Failure[] failures) {
            this.id = id;
            this.failures = failures;
        }

        ShardResult(final StreamInput in) throws IOException {
            this.id = in.readVInt();
            this.failures = in.readOptionalArray(Failure::readFailure, Failure[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeVInt(id);
            out.writeOptionalArray(failures);
        }

        public boolean hasFailures() {
            return failures != null && failures.length > 0;
        }

        public int getId() {
            return id;
        }

        public Failure[] getFailures() {
            return failures;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject(String.valueOf(id));
            {
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

        static ShardResult fromXContent(final XContentParser parser, final String id) {
            return PARSER.apply(parser, id);
        }

        public static class Failure extends DefaultShardOperationFailedException {

            static final ConstructingObjectParser<Failure, Void> PARSER = new ConstructingObjectParser<>("failure", true,
                arg -> new Failure((String) arg[0], (int) arg[1], (Throwable) arg[2], (String) arg[3]));

            static {
                declareFields(PARSER);
                PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("node"));
            }

            private @Nullable String nodeId;

            private Failure() {
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
            public void readFrom(final StreamInput in) throws IOException {
                super.readFrom(in);
                nodeId = in.readOptionalString();
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
                final Failure failure = new Failure();
                failure.readFrom(in);
                return failure;
            }
        }
    }
}

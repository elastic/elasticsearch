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

package org.elasticsearch.repositories.blobstore;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Immutable snapshot description for BlobStoreRepository
 * <p/>
 * Stored in the the root of the repository, see {@link BlobStoreRepository} for more information.
 */
public class BlobStoreSnapshot implements Snapshot {
    private final String name;

    private final Version version;

    private final SnapshotState state;

    private final String reason;

    private final ImmutableList<String> indices;

    private final long startTime;

    private final long endTime;

    private final int totalShard;

    private final int successfulShards;

    private final ImmutableList<SnapshotShardFailure> shardFailures;

    private BlobStoreSnapshot(String name, ImmutableList<String> indices, SnapshotState state, String reason, Version version, long startTime, long endTime,
                              int totalShard, int successfulShards, ImmutableList<SnapshotShardFailure> shardFailures) {
        assert name != null;
        assert indices != null;
        assert state != null;
        assert shardFailures != null;
        this.name = name;
        this.indices = indices;
        this.state = state;
        this.reason = reason;
        this.version = version;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalShard = totalShard;
        this.successfulShards = successfulShards;
        this.shardFailures = shardFailures;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SnapshotState state() {
        return state;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String reason() {
        return reason;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Version version() {
        return version;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableList<String> indices() {
        return indices;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long startTime() {
        return startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long endTime() {
        return endTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int totalShard() {
        return totalShard;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int successfulShards() {
        return successfulShards;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableList<SnapshotShardFailure> shardFailures() {
        return shardFailures;
    }

    /**
     * Creates new BlobStoreSnapshot builder
     *
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Compares two snapshots by their start time
     *
     * @param o other snapshot
     * @return the value {@code 0} if snapshots were created at the same time;
     *         a value less than {@code 0} if this snapshot was created before snapshot {@code o}; and
     *         a value greater than {@code 0} if this snapshot was created after snapshot {@code o};
     */
    @Override
    public int compareTo(Snapshot o) {
        return Long.compare(startTime, ((BlobStoreSnapshot) o).startTime);
    }

    /**
     * BlobStoreSnapshot builder
     */
    public static class Builder {

        private String name;

        private Version version = Version.CURRENT;

        private SnapshotState state = SnapshotState.IN_PROGRESS;

        private String reason;

        private ImmutableList<String> indices;

        private long startTime;

        private long endTime;

        private int totalShard;

        private int successfulShards;

        private ImmutableList<SnapshotShardFailure> shardFailures = ImmutableList.of();

        /**
         * Copies data from another snapshot into the builder
         *
         * @param snapshot another snapshot
         * @return this builder
         */
        public Builder snapshot(BlobStoreSnapshot snapshot) {
            name = snapshot.name();
            indices = snapshot.indices();
            version = snapshot.version();
            reason = snapshot.reason();
            state = snapshot.state();
            startTime = snapshot.startTime();
            endTime = snapshot.endTime();
            totalShard = snapshot.totalShard();
            successfulShards = snapshot.successfulShards();
            shardFailures = snapshot.shardFailures();
            return this;
        }

        /**
         * Sets snapshot name
         *
         * @param name snapshot name
         * @return this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets list of indices in the snapshot
         *
         * @param indices list of indices
         * @return this builder
         */
        public Builder indices(Collection<String> indices) {
            this.indices = ImmutableList.copyOf(indices);
            return this;
        }

        /**
         * Sets list of indices in the snapshot
         *
         * @param indices list of indices
         * @return this builder
         */
        public Builder indices(String[] indices) {
            this.indices = ImmutableList.copyOf(indices);
            return this;
        }

        /**
         * Sets snapshot state
         *
         * @param state snapshot state
         * @return this builder
         */
        public Builder state(SnapshotState state) {
            this.state = state;
            return this;
        }

        /**
         * Sets snapshot failure reason
         *
         * @param reason snapshot failure reason
         * @return this builder
         */
        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * Marks snapshot as successful
         *
         * @return this builder
         */
        public Builder success() {
            this.state = SnapshotState.SUCCESS;
            return this;
        }

        /**
         * Marks snapshot as partially successful
         *
         * @return this builder
         */
        public Builder partial() {
            this.state = SnapshotState.PARTIAL;
            return this;
        }

        /**
         * Marks snapshot as failed and saves failure reason
         *
         * @param reason failure reason
         * @return this builder
         */
        public Builder failed(String reason) {
            this.state = SnapshotState.FAILED;
            this.reason = reason;
            return this;
        }

        /**
         * Sets version of Elasticsearch that created this snapshot
         *
         * @param version version
         * @return this builder
         */
        public Builder version(Version version) {
            this.version = version;
            return this;
        }

        /**
         * Sets snapshot start time
         *
         * @param startTime snapshot start time
         * @return this builder
         */
        public Builder startTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets snapshot end time
         *
         * @param endTime snapshot end time
         * @return this builder
         */
        public Builder endTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Sets total number of shards across all snapshot indices
         *
         * @param totalShard number of shards
         * @return this builder
         */
        public Builder totalShard(int totalShard) {
            this.totalShard = totalShard;
            return this;
        }

        /**
         * Sets total number fo shards that were successfully snapshotted
         *
         * @param successfulShards number of successful shards
         * @return this builder
         */
        public Builder successfulShards(int successfulShards) {
            this.successfulShards = successfulShards;
            return this;
        }

        /**
         * Sets the list of individual shard failures
         *
         * @param shardFailures list of shard failures
         * @return this builder
         */
        public Builder shardFailures(ImmutableList<SnapshotShardFailure> shardFailures) {
            this.shardFailures = shardFailures;
            return this;
        }

        /**
         * Sets the total number of shards and the list of individual shard failures
         *
         * @param totalShard    number of shards
         * @param shardFailures list of shard failures
         * @return this builder
         */
        public Builder failures(int totalShard, ImmutableList<SnapshotShardFailure> shardFailures) {
            this.totalShard = totalShard;
            this.successfulShards = totalShard - shardFailures.size();
            this.shardFailures = shardFailures;
            return this;
        }

        /**
         * Builds new BlobStoreSnapshot
         *
         * @return
         */
        public BlobStoreSnapshot build() {
            return new BlobStoreSnapshot(name, indices, state, reason, version, startTime, endTime, totalShard, successfulShards, shardFailures);
        }

        static final class Fields {
            static final XContentBuilderString SNAPSHOT = new XContentBuilderString("snapshot");
            static final XContentBuilderString NAME = new XContentBuilderString("name");
            static final XContentBuilderString VERSION_ID = new XContentBuilderString("version_id");
            static final XContentBuilderString INDICES = new XContentBuilderString("indices");
            static final XContentBuilderString STATE = new XContentBuilderString("state");
            static final XContentBuilderString REASON = new XContentBuilderString("reason");
            static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
            static final XContentBuilderString END_TIME = new XContentBuilderString("end_time");
            static final XContentBuilderString TOTAL_SHARDS = new XContentBuilderString("total_shards");
            static final XContentBuilderString SUCCESSFUL_SHARDS = new XContentBuilderString("successful_shards");
            static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        }

        /**
         * Serializes the snapshot
         *
         * @param snapshot snapshot to be serialized
         * @param builder  XContent builder
         * @param params   serialization parameters
         * @throws IOException
         */
        public static void toXContent(BlobStoreSnapshot snapshot, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(Fields.SNAPSHOT);
            builder.field(Fields.NAME, snapshot.name);
            builder.field(Fields.VERSION_ID, snapshot.version.id);
            builder.startArray(Fields.INDICES);
            for (String index : snapshot.indices) {
                builder.value(index);
            }
            builder.endArray();
            builder.field(Fields.STATE, snapshot.state);
            if (snapshot.reason != null) {
                builder.field(Fields.REASON, snapshot.reason);
            }
            builder.field(Fields.START_TIME, snapshot.startTime);
            builder.field(Fields.END_TIME, snapshot.endTime);
            builder.field(Fields.TOTAL_SHARDS, snapshot.totalShard);
            builder.field(Fields.SUCCESSFUL_SHARDS, snapshot.successfulShards);
            builder.startArray(Fields.FAILURES);
            for (SnapshotShardFailure shardFailure : snapshot.shardFailures) {
                SnapshotShardFailure.toXContent(shardFailure, builder, params);
            }
            builder.endArray();
            builder.endObject();
        }

        /**
         * Parses the snapshot
         *
         * @param parser XContent parser
         * @return snapshot
         * @throws IOException
         */
        public static BlobStoreSnapshot fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = parser.currentName();
                if ("snapshot".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                            token = parser.nextToken();
                            if (token.isValue()) {
                                if ("name".equals(currentFieldName)) {
                                    builder.name(parser.text());
                                } else if ("state".equals(currentFieldName)) {
                                    builder.state(SnapshotState.valueOf(parser.text()));
                                } else if ("reason".equals(currentFieldName)) {
                                    builder.reason(parser.text());
                                } else if ("start_time".equals(currentFieldName)) {
                                    builder.startTime(parser.longValue());
                                } else if ("end_time".equals(currentFieldName)) {
                                    builder.endTime(parser.longValue());
                                } else if ("total_shards".equals(currentFieldName)) {
                                    builder.totalShard(parser.intValue());
                                } else if ("successful_shards".equals(currentFieldName)) {
                                    builder.successfulShards(parser.intValue());
                                } else if ("version_id".equals(currentFieldName)) {
                                    builder.version(Version.fromId(parser.intValue()));
                                }
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                if ("indices".equals(currentFieldName)) {
                                    ArrayList<String> indices = new ArrayList<>();
                                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                        indices.add(parser.text());
                                    }
                                    builder.indices(indices);
                                } else if ("failures".equals(currentFieldName)) {
                                    ArrayList<SnapshotShardFailure> failures = new ArrayList<>();
                                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                        failures.add(SnapshotShardFailure.fromXContent(parser));
                                    }
                                    builder.shardFailures(ImmutableList.copyOf(failures));
                                } else {
                                    // It was probably created by newer version - ignoring
                                    parser.skipChildren();
                                }
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                // It was probably created by newer version - ignoring
                                parser.skipChildren();
                            }
                        }
                    }
                }
            }
            return builder.build();
        }
    }
}

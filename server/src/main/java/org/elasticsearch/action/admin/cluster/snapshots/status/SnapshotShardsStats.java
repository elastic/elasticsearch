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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Collection;

/**
 * Status of a snapshot shards
 */
public class SnapshotShardsStats implements ToXContentFragment {

    private int initializingShards;
    private int startedShards;
    private int finalizingShards;
    private int doneShards;
    private int failedShards;
    private int totalShards;

    SnapshotShardsStats(Collection<SnapshotIndexShardStatus> shards) {
        for (SnapshotIndexShardStatus shard : shards) {
            totalShards++;
            switch (shard.getStage()) {
                case INIT:
                    initializingShards++;
                    break;
                case STARTED:
                    startedShards++;
                    break;
                case FINALIZE:
                    finalizingShards++;
                    break;
                case DONE:
                    doneShards++;
                    break;
                case FAILURE:
                    failedShards++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown stage type " + shard.getStage());
            }
        }
    }

    public SnapshotShardsStats(int initializingShards, int startedShards, int finalizingShards, int doneShards, int failedShards,
                               int totalShards) {
        this.initializingShards = initializingShards;
        this.startedShards = startedShards;
        this.finalizingShards = finalizingShards;
        this.doneShards = doneShards;
        this.failedShards = failedShards;
        this.totalShards = totalShards;
    }

    /**
     * Number of shards with the snapshot in the initializing stage
     */
    public int getInitializingShards() {
        return initializingShards;
    }

    /**
     * Number of shards with the snapshot in the started stage
     */
    public int getStartedShards() {
        return startedShards;
    }

    /**
     * Number of shards with the snapshot in the finalizing stage
     */
    public int getFinalizingShards() {
        return finalizingShards;
    }

    /**
     * Number of shards with completed snapshot
     */
    public int getDoneShards() {
        return doneShards;
    }

    /**
     * Number of shards with failed snapshot
     */
    public int getFailedShards() {
        return failedShards;
    }

    /**
     * Total number of shards
     */
    public int getTotalShards() {
        return totalShards;
    }

    static final class Fields {
        static final String SHARDS_STATS = "shards_stats";
        static final String INITIALIZING = "initializing";
        static final String STARTED = "started";
        static final String FINALIZING = "finalizing";
        static final String DONE = "done";
        static final String FAILED = "failed";
        static final String TOTAL = "total";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SHARDS_STATS);
        builder.field(Fields.INITIALIZING, getInitializingShards());
        builder.field(Fields.STARTED, getStartedShards());
        builder.field(Fields.FINALIZING, getFinalizingShards());
        builder.field(Fields.DONE, getDoneShards());
        builder.field(Fields.FAILED, getFailedShards());
        builder.field(Fields.TOTAL, getTotalShards());
        builder.endObject();
        return builder;
    }

    public static SnapshotShardsStats fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParser.Token token;
        int initializingShards = 0;
        int startedShards = 0;
        int finalizingShards = 0;
        int doneShards = 0;
        int failedShards = 0;
        int totalShards = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentName = parser.currentName();
                if (currentName.equals(Fields.INITIALIZING)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new ElasticsearchParseException("failed to parse snapshot shards stats, expected number for field [{}]",
                            currentName);
                    }
                    initializingShards = parser.intValue();
                } else if (currentName.equals(Fields.STARTED)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new ElasticsearchParseException("failed to parse snapshot shards stats, expected number for field [{}]",
                            currentName);
                    }
                    startedShards = parser.intValue();
                } else if (currentName.equals(Fields.FINALIZING)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new ElasticsearchParseException("failed to parse snapshot shards stats, expected number for field [{}]",
                            currentName);
                    }
                    finalizingShards = parser.intValue();
                } else if (currentName.equals(Fields.DONE)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new ElasticsearchParseException("failed to parse snapshot shards stats, expected number for field [{}]",
                            currentName);
                    }
                    doneShards = parser.intValue();
                } else if (currentName.equals(Fields.FAILED)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new ElasticsearchParseException("failed to parse snapshot shards stats, expected number for field [{}]",
                            currentName);
                    }
                    failedShards = parser.intValue();
                } else if (currentName.equals(Fields.TOTAL)) {
                    if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                        throw new ElasticsearchParseException("failed to parse snapshot shards stats, expected number for field [{}]",
                            currentName);
                    }
                    totalShards = parser.intValue();
                } else {
                    throw new ElasticsearchParseException("failed to parse snapshot shards stats, unexpected field [{}]", currentName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshot shards stats");
            }
        }
        return new SnapshotShardsStats(initializingShards, startedShards, finalizingShards, doneShards, failedShards, totalShards);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotShardsStats that = (SnapshotShardsStats) o;

        if (initializingShards != that.initializingShards) return false;
        if (startedShards != that.startedShards) return false;
        if (finalizingShards != that.finalizingShards) return false;
        if (doneShards != that.doneShards) return false;
        if (failedShards != that.failedShards) return false;
        return totalShards == that.totalShards;
    }

    @Override
    public int hashCode() {
        int result = initializingShards;
        result = 31 * result + startedShards;
        result = 31 * result + finalizingShards;
        result = 31 * result + doneShards;
        result = 31 * result + failedShards;
        result = 31 * result + totalShards;
        return result;
    }
}

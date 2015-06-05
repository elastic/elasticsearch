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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Collection;

/**
 * Status of a snapshot shards
 */
public class SnapshotShardsStats  implements ToXContent {

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
        static final XContentBuilderString SHARDS_STATS = new XContentBuilderString("shards_stats");
        static final XContentBuilderString INITIALIZING = new XContentBuilderString("initializing");
        static final XContentBuilderString STARTED = new XContentBuilderString("started");
        static final XContentBuilderString FINALIZING = new XContentBuilderString("finalizing");
        static final XContentBuilderString DONE = new XContentBuilderString("done");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
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

}

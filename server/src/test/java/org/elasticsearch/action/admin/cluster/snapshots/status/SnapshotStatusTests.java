package org.elasticsearch.action.admin.cluster.snapshots.status;

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

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;


public class SnapshotStatusTests extends AbstractXContentTestCase<SnapshotStatus> {


    public void testToString() throws Exception {
        SnapshotsInProgress.State state = randomFrom(SnapshotsInProgress.State.values());
        String uuid = UUIDs.randomBase64UUID();
        SnapshotId id = new SnapshotId("test-snap", uuid);
        Snapshot snapshot = new Snapshot("test-repo", id);

        String indexName = randomAlphaOfLengthBetween(3, 50);
        int shardId = randomInt();
        ShardId testShardId = ShardId.fromString("[" + indexName + "][" + shardId + "]");
        SnapshotIndexShardStage shardStage = randomFrom(SnapshotIndexShardStage.values());
        SnapshotIndexShardStatus snapshotIndexShardStatus = new SnapshotIndexShardStatus(testShardId, shardStage);
        List<SnapshotIndexShardStatus> snapshotIndexShardStatuses = new ArrayList<>();
        snapshotIndexShardStatuses.add(snapshotIndexShardStatus);
        boolean includeGlobalState = randomBoolean();
        SnapshotStatus status = new SnapshotStatus(snapshot, state, snapshotIndexShardStatuses, includeGlobalState, 0L, 0L);

        int initializingShards = 0;
        int startedShards = 0;
        int finalizingShards = 0;
        int doneShards = 0;
        int failedShards = 0;
        int totalShards = 1;

        switch (shardStage) {
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
                break;
        }

        String expected = "{\n" +
            "  \"snapshot\" : \"test-snap\",\n" +
            "  \"repository\" : \"test-repo\",\n" +
            "  \"uuid\" : \"" + uuid + "\",\n" +
            "  \"state\" : \"" + state.toString() + "\",\n" +
            "  \"include_global_state\" : " + includeGlobalState + ",\n" +
            "  \"shards_stats\" : {\n" +
            "    \"initializing\" : " + initializingShards + ",\n" +
            "    \"started\" : " + startedShards + ",\n" +
            "    \"finalizing\" : " + finalizingShards + ",\n" +
            "    \"done\" : " + doneShards + ",\n" +
            "    \"failed\" : " + failedShards + ",\n" +
            "    \"total\" : " + totalShards + "\n" +
            "  },\n" +
            "  \"stats\" : {\n" +
            "    \"incremental\" : {\n" +
            "      \"file_count\" : 0,\n" +
            "      \"size_in_bytes\" : 0\n" +
            "    },\n" +
            "    \"total\" : {\n" +
            "      \"file_count\" : 0,\n" +
            "      \"size_in_bytes\" : 0\n" +
            "    },\n" +
            "    \"start_time_in_millis\" : 0,\n" +
            "    \"time_in_millis\" : 0\n" +
            "  },\n" +
            "  \"indices\" : {\n" +
            "    \"" + indexName + "\" : {\n" +
            "      \"shards_stats\" : {\n" +
            "        \"initializing\" : " + initializingShards + ",\n" +
            "        \"started\" : " + startedShards + ",\n" +
            "        \"finalizing\" : " + finalizingShards + ",\n" +
            "        \"done\" : " + doneShards + ",\n" +
            "        \"failed\" : " + failedShards + ",\n" +
            "        \"total\" : " + totalShards + "\n" +
            "      },\n" +
            "      \"stats\" : {\n" +
            "        \"incremental\" : {\n" +
            "          \"file_count\" : 0,\n" +
            "          \"size_in_bytes\" : 0\n" +
            "        },\n" +
            "        \"total\" : {\n" +
            "          \"file_count\" : 0,\n" +
            "          \"size_in_bytes\" : 0\n" +
            "        },\n" +
            "        \"start_time_in_millis\" : 0,\n" +
            "        \"time_in_millis\" : 0\n" +
            "      },\n" +
            "      \"shards\" : {\n" +
            "        \"" + shardId + "\" : {\n" +
            "          \"stage\" : \"" + shardStage.toString() + "\",\n" +
            "          \"stats\" : {\n" +
            "            \"incremental\" : {\n" +
            "              \"file_count\" : 0,\n" +
            "              \"size_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"total\" : {\n" +
            "              \"file_count\" : 0,\n" +
            "              \"size_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"start_time_in_millis\" : 0,\n" +
            "            \"time_in_millis\" : 0\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        assertEquals(expected, status.toString());
    }

    @Override
    protected SnapshotStatus createTestInstance() {
        SnapshotsInProgress.State state = randomFrom(SnapshotsInProgress.State.values());
        String uuid = UUIDs.randomBase64UUID();
        SnapshotId id = new SnapshotId("test-snap", uuid);
        Snapshot snapshot = new Snapshot("test-repo", id);

        SnapshotIndexShardStatusTests builder = new SnapshotIndexShardStatusTests();
        builder.createTestInstance();

        List<SnapshotIndexShardStatus> snapshotIndexShardStatuses = new ArrayList<>();
        for (int idx = 0; idx < randomIntBetween(0, 10); idx++) {
            SnapshotIndexShardStatus snapshotIndexShardStatus = builder.createTestInstance();
            snapshotIndexShardStatuses.add(snapshotIndexShardStatus);
        }
        boolean includeGlobalState = randomBoolean();
        return new SnapshotStatus(snapshot, state, snapshotIndexShardStatuses, includeGlobalState, 0L, 0L);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Do not place random fields in the indices field or shards field since their fields correspond to names.
        return (s) -> s.endsWith("shards") || s.endsWith("indices");
    }

    @Override
    protected SnapshotStatus doParseInstance(XContentParser parser) throws IOException {
        return SnapshotStatus.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}

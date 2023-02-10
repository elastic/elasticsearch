/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class SnapshotStatusTests extends AbstractChunkedSerializingTestCase<SnapshotStatus> {

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
            case INIT -> initializingShards++;
            case STARTED -> startedShards++;
            case FINALIZE -> finalizingShards++;
            case DONE -> doneShards++;
            case FAILURE -> failedShards++;
        }

        String expected = Strings.format(
            """
                {
                  "snapshot" : "test-snap",
                  "repository" : "test-repo",
                  "uuid" : "%s",
                  "state" : "%s",
                  "include_global_state" : %s,
                  "shards_stats" : {
                    "initializing" : %s,
                    "started" : %s,
                    "finalizing" : %s,
                    "done" : %s,
                    "failed" : %s,
                    "total" : %s
                  },
                  "stats" : {
                    "incremental" : {
                      "file_count" : 0,
                      "size_in_bytes" : 0
                    },
                    "total" : {
                      "file_count" : 0,
                      "size_in_bytes" : 0
                    },
                    "start_time_in_millis" : 0,
                    "time_in_millis" : 0
                  },
                  "indices" : {
                    "%s" : {
                      "shards_stats" : {
                        "initializing" : %s,
                        "started" : %s,
                        "finalizing" : %s,
                        "done" : %s,
                        "failed" : %s,
                        "total" : %s
                      },
                      "stats" : {
                        "incremental" : {
                          "file_count" : 0,
                          "size_in_bytes" : 0
                        },
                        "total" : {
                          "file_count" : 0,
                          "size_in_bytes" : 0
                        },
                        "start_time_in_millis" : 0,
                        "time_in_millis" : 0
                      },
                      "shards" : {
                        "%s" : {
                          "stage" : "%s",
                          "stats" : {
                            "incremental" : {
                              "file_count" : 0,
                              "size_in_bytes" : 0
                            },
                            "total" : {
                              "file_count" : 0,
                              "size_in_bytes" : 0
                            },
                            "start_time_in_millis" : 0,
                            "time_in_millis" : 0
                          }
                        }
                      }
                    }
                  }
                }""",
            uuid,
            state.toString(),
            includeGlobalState,
            initializingShards,
            startedShards,
            finalizingShards,
            doneShards,
            failedShards,
            totalShards,
            indexName,
            initializingShards,
            startedShards,
            finalizingShards,
            doneShards,
            failedShards,
            totalShards,
            shardId,
            shardStage.toString()
        );
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
    protected SnapshotStatus mutateInstance(SnapshotStatus instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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

    @Override
    protected Writeable.Reader<SnapshotStatus> instanceReader() {
        return SnapshotStatus::new;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotInfoTests extends AbstractWireSerializingTestCase<SnapshotInfo> {

    @Override
    protected SnapshotInfo createTestInstance() {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        List<String> indices = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        List<String> dataStreams = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        String reason = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 15);

        long startTime = randomNonNegativeLong();
        long endTime = randomNonNegativeLong();

        int totalShards = randomIntBetween(0, 100);
        int failedShards = randomIntBetween(0, totalShards);

        List<SnapshotShardFailure> shardFailures = Arrays.asList(randomArray(failedShards, failedShards,
            SnapshotShardFailure[]::new, () -> {
                String indexName = randomAlphaOfLengthBetween(3, 50);
                int id = randomInt();
                ShardId shardId = ShardId.fromString("[" + indexName + "][" + id + "]");

                return new SnapshotShardFailure(randomAlphaOfLengthBetween(5, 10), shardId, randomAlphaOfLengthBetween(5, 10));
            }));

        Boolean includeGlobalState = randomBoolean() ? null : randomBoolean();

        Map<String, Object> userMetadata = randomUserMetadata();

        return new SnapshotInfo(snapshotId, indices, dataStreams, Collections.emptyList(), reason, endTime, totalShards, shardFailures,
            includeGlobalState, userMetadata, startTime
        );
    }

    @Override
    protected Writeable.Reader<SnapshotInfo> instanceReader() {
        return SnapshotInfo::new;
    }

    @Override
    protected SnapshotInfo mutateInstance(SnapshotInfo instance) {
        switch (randomIntBetween(0, 8)) {
            case 0:
                SnapshotId snapshotId = new SnapshotId(
                    randomValueOtherThan(instance.snapshotId().getName(), () -> randomAlphaOfLength(5)),
                    randomValueOtherThan(instance.snapshotId().getUUID(), () -> randomAlphaOfLength(5)));
                return new SnapshotInfo(snapshotId, instance.indices(), instance.dataStreams(), Collections.emptyList(), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    instance.userMetadata(), instance.startTime()
                );
            case 1:
                int indicesSize = randomValueOtherThan(instance.indices().size(), () -> randomIntBetween(1, 10));
                List<String> indices = Arrays.asList(randomArray(indicesSize, indicesSize, String[]::new,
                    () -> randomAlphaOfLengthBetween(2, 20)));
                return new SnapshotInfo(instance.snapshotId(), indices, instance.dataStreams(), Collections.emptyList(), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    instance.userMetadata(), instance.startTime()
                );
            case 2:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.dataStreams(),
                    Collections.emptyList(), instance.reason(), instance.endTime(), instance.totalShards(), instance.shardFailures(),
                    instance.includeGlobalState(), instance.userMetadata(), randomValueOtherThan(instance.startTime(),
                    ESTestCase::randomNonNegativeLong)
                );
            case 3:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.dataStreams(), Collections.emptyList(),
                    randomValueOtherThan(instance.reason(), () -> randomAlphaOfLengthBetween(5, 15)), instance.endTime(),
                    instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(), instance.userMetadata(),
                    instance.startTime()
                );
            case 4:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.dataStreams(),
                    Collections.emptyList(), instance.reason(), randomValueOtherThan(instance.endTime(), ESTestCase::randomNonNegativeLong),
                    instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(), instance.userMetadata(),
                    instance.startTime()
                );
            case 5:
                int totalShards = randomValueOtherThan(instance.totalShards(), () -> randomIntBetween(0, 100));
                int failedShards = randomIntBetween(0, totalShards);

                List<SnapshotShardFailure> shardFailures = Arrays.asList(randomArray(failedShards, failedShards,
                    SnapshotShardFailure[]::new, () -> {
                        String indexName = randomAlphaOfLengthBetween(3, 50);
                        int id = randomInt();
                        ShardId shardId = ShardId.fromString("[" + indexName + "][" + id + "]");

                        return new SnapshotShardFailure(randomAlphaOfLengthBetween(5, 10), shardId, randomAlphaOfLengthBetween(5, 10));
                    }));
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.dataStreams(), Collections.emptyList(),
                    instance.reason(), instance.endTime(), totalShards, shardFailures, instance.includeGlobalState(),
                    instance.userMetadata(), instance.startTime()
                );
            case 6:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.dataStreams(), Collections.emptyList(),
                    instance.reason(), instance.endTime(), instance.totalShards(), instance.shardFailures(),
                    Boolean.FALSE.equals(instance.includeGlobalState()), instance.userMetadata(), instance.startTime()
                );
            case 7:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.dataStreams(), Collections.emptyList(),
                    instance.reason(), instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    randomValueOtherThan(instance.userMetadata(), SnapshotInfoTests::randomUserMetadata), instance.startTime()
                );
            case 8:
                List<String> dataStreams = randomValueOtherThan(instance.dataStreams(),
                    () -> Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20))));
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), dataStreams,
                    Collections.emptyList(), instance.reason(), instance.endTime(), instance.totalShards(), instance.shardFailures(),
                    instance.includeGlobalState(), instance.userMetadata(), instance.startTime()
                );
            default:
                throw new IllegalArgumentException("invalid randomization case");
        }
    }

    public static Map<String, Object> randomUserMetadata() {
        if (randomBoolean()) {
            return null;
        }

        Map<String, Object> metadata = new HashMap<>();
        long fields = randomLongBetween(0, 25);
        for (int i = 0; i < fields; i++) {
            if (randomBoolean()) {
                metadata.put(randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2,10)),
                    randomAlphaOfLengthBetween(5, 15));
            } else {
                Map<String, Object> nested = new HashMap<>();
                long nestedFields = randomLongBetween(0, 25);
                for (int j = 0; j < nestedFields; j++) {
                    nested.put(randomValueOtherThanMany(nested::containsKey, () -> randomAlphaOfLengthBetween(2,10)),
                        randomAlphaOfLengthBetween(5, 15));
                }
                metadata.put(randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2,10)), nested);
            }
        }
        return metadata;
    }
}

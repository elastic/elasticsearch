package org.elasticsearch.snapshots;


import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotInfoTests extends AbstractWireSerializingTestCase<SnapshotInfo> {

    @Override
    protected SnapshotInfo createTestInstance() {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        List<String> indices = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        String reason = randomBoolean() ? null : randomAlphaOfLengthBetween(5,15);

        long startTime = randomNonNegativeLong();
        long endTime = randomNonNegativeLong();

        int totalShards = randomIntBetween(0,100);
        int failedShards = randomIntBetween(0, totalShards);

        List<SnapshotShardFailure> shardFailures = Arrays.asList(randomArray(failedShards, failedShards, SnapshotShardFailure[]::new, () -> {
            String indexName = randomAlphaOfLengthBetween(3,50);
            int id = randomInt();
            ShardId shardId = ShardId.fromString("["+indexName+"]["+id+"]");

            return new SnapshotShardFailure(randomAlphaOfLengthBetween(5,10), shardId, randomAlphaOfLengthBetween(5,10));
        }));

        Boolean includeGlobalState = randomBoolean() ? null : randomBoolean();

        Map<String, Object> userMetadata = randomUserMetadata();

        return new SnapshotInfo(snapshotId, indices, startTime, reason, endTime, totalShards, shardFailures,
            includeGlobalState, userMetadata);
    }

    @Override
    protected Writeable.Reader<SnapshotInfo> instanceReader() {
        return SnapshotInfo::new;
    }

    @Override
    protected SnapshotInfo mutateInstance(SnapshotInfo instance) {
        switch (randomIntBetween(0,7)) {
            case 0:
                SnapshotId snapshotId = new SnapshotId(
                    randomValueOtherThan(instance.snapshotId().getName(), () -> randomAlphaOfLength(5)),
                    randomValueOtherThan(instance.snapshotId().getUUID(), () -> randomAlphaOfLength(5)));
                return new SnapshotInfo(snapshotId, instance.indices(), instance.startTime(), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    instance.userMetadata());
            case 1:
                int indicesSize = randomValueOtherThan(instance.indices().size(), () -> randomIntBetween(1, 10));
                List<String> indices = Arrays.asList(randomArray(indicesSize, indicesSize, String[]::new,
                    () -> randomAlphaOfLengthBetween(2, 20)));
                return new SnapshotInfo(instance.snapshotId(), indices, instance.startTime(), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    instance.userMetadata());
            case 2:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(),
                    randomValueOtherThan(instance.startTime(), ESTestCase::randomNonNegativeLong), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    instance.userMetadata());
            case 3:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.startTime(),
                    randomValueOtherThan(instance.reason(), () -> randomAlphaOfLengthBetween(5,15)), instance.endTime(),
                    instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(), instance.userMetadata());
            case 4:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.startTime(), instance.reason(),
                    randomValueOtherThan(instance.endTime(), ESTestCase::randomNonNegativeLong), instance.totalShards(),
                    instance.shardFailures(), instance.includeGlobalState(), instance.userMetadata());
            case 5:
                int totalShards = randomValueOtherThan(instance.totalShards(), () -> randomIntBetween(0,100));
                int failedShards = randomIntBetween(0, totalShards);

                List<SnapshotShardFailure> shardFailures = Arrays.asList(randomArray(failedShards, failedShards, SnapshotShardFailure[]::new, () -> {
                    String indexName = randomAlphaOfLengthBetween(3,50);
                    int id = randomInt();
                    ShardId shardId = ShardId.fromString("["+indexName+"]["+id+"]");

                    return new SnapshotShardFailure(randomAlphaOfLengthBetween(5,10), shardId, randomAlphaOfLengthBetween(5,10));
                }));
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.startTime(), instance.reason(),
                    instance.endTime(), totalShards, shardFailures, instance.includeGlobalState(), instance.userMetadata());
            case 6:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.startTime(), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(),
                    Boolean.FALSE.equals(instance.includeGlobalState()), instance.userMetadata());
            case 7:
                return new SnapshotInfo(instance.snapshotId(), instance.indices(), instance.startTime(), instance.reason(),
                    instance.endTime(), instance.totalShards(), instance.shardFailures(), instance.includeGlobalState(),
                    randomValueOtherThan(instance.userMetadata(), SnapshotInfoTests::randomUserMetadata));
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
            metadata.put(randomAlphaOfLengthBetween(2,10), randomAlphaOfLengthBetween(5,15));
        }
        return metadata;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomArray;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThan;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

public class SnapshotInfoTestUtils {
    private SnapshotInfoTestUtils() {}

    static SnapshotInfo createRandomSnapshotInfo() {
        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(5), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final List<String> indices = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        final List<String> dataStreams = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        final String reason = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 15);

        final long startTime = randomNonNegativeLong();
        final long endTime = randomNonNegativeLong();

        final int totalShards = randomIntBetween(0, 100);
        final List<SnapshotShardFailure> shardFailures = randomShardFailures(randomIntBetween(0, totalShards));

        final Boolean includeGlobalState = randomBoolean() ? null : randomBoolean();

        final Map<String, Object> userMetadata = randomUserMetadata();

        final List<SnapshotFeatureInfo> snapshotFeatureInfos = randomSnapshotFeatureInfos();

        final Map<String, SnapshotInfo.IndexSnapshotDetails> indexSnapshotDetails = randomIndexSnapshotDetails();

        return new SnapshotInfo(
            snapshot,
            indices,
            dataStreams,
            snapshotFeatureInfos,
            reason,
            endTime,
            totalShards,
            shardFailures,
            includeGlobalState,
            userMetadata,
            startTime,
            indexSnapshotDetails
        );
    }

    public static Map<String, SnapshotInfo.IndexSnapshotDetails> randomIndexSnapshotDetails() {
        final Map<String, SnapshotInfo.IndexSnapshotDetails> result = new HashMap<>();
        final int size = between(0, 10);
        while (result.size() < size) {
            result.put(
                randomAlphaOfLengthBetween(5, 10),
                new SnapshotInfo.IndexSnapshotDetails(between(1, 10), new ByteSizeValue(between(0, Integer.MAX_VALUE)), between(1, 100))
            );
        }
        return result;
    }

    private static List<SnapshotShardFailure> randomShardFailures(int failedShards) {
        return randomList(
            failedShards,
            failedShards,
            () -> new SnapshotShardFailure(
                randomAlphaOfLengthBetween(5, 10),
                ShardId.fromString("[" + randomAlphaOfLengthBetween(3, 50) + "][" + randomInt() + "]"),
                randomAlphaOfLengthBetween(5, 10)
            )
        );
    }

    public static Map<String, Object> randomUserMetadata() {
        if (randomBoolean()) {
            return null;
        }

        Map<String, Object> metadata = new HashMap<>();
        long fields = randomLongBetween(0, 25);
        for (int i = 0; i < fields; i++) {
            if (randomBoolean()) {
                metadata.put(
                    randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                    randomAlphaOfLengthBetween(5, 15)
                );
            } else {
                Map<String, Object> nested = new HashMap<>();
                long nestedFields = randomLongBetween(0, 25);
                for (int j = 0; j < nestedFields; j++) {
                    nested.put(
                        randomValueOtherThanMany(nested::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                        randomAlphaOfLengthBetween(5, 15)
                    );
                }
                metadata.put(randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)), nested);
            }
        }
        return metadata;
    }

    private static List<SnapshotFeatureInfo> randomSnapshotFeatureInfos() {
        final List<SnapshotFeatureInfo> snapshotFeatureInfos = new ArrayList<>();
        for (int i = between(0, 5); i >= 0; i--) {
            snapshotFeatureInfos.add(
                new SnapshotFeatureInfo(randomAlphaOfLengthBetween(1, 10), randomList(0, 5, () -> randomAlphaOfLengthBetween(1, 10)))
            );
        }
        return snapshotFeatureInfos;
    }

    static SnapshotInfo mutateSnapshotInfo(SnapshotInfo instance) {
        switch (randomIntBetween(0, 10)) {
            case 0:
                final String newName = randomValueOtherThan(instance.snapshotId().getName(), () -> randomAlphaOfLength(5));
                final String newUuid = randomValueOtherThan(instance.snapshotId().getUUID(), () -> randomAlphaOfLength(5));
                final SnapshotId snapshotId = randomBoolean()
                    ? new SnapshotId(instance.snapshotId().getName(), newUuid)
                    : new SnapshotId(newName, instance.snapshotId().getUUID());
                final String repo = randomBoolean() ? instance.repository() : randomAlphaOfLength(5);
                return new SnapshotInfo(
                    new Snapshot(repo, snapshotId),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 1:
                final int indicesSize = randomValueOtherThan(instance.indices().size(), () -> randomIntBetween(1, 10));
                final List<String> indices = Arrays.asList(
                    randomArray(indicesSize, indicesSize, String[]::new, () -> randomAlphaOfLengthBetween(2, 20))
                );
                return new SnapshotInfo(
                    instance.snapshot(),
                    indices,
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 2:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    randomValueOtherThan(instance.startTime(), ESTestCase::randomNonNegativeLong),
                    instance.indexSnapshotDetails()
                );
            case 3:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    randomValueOtherThan(instance.reason(), () -> randomAlphaOfLengthBetween(5, 15)),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 4:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    randomValueOtherThan(instance.endTime(), ESTestCase::randomNonNegativeLong),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 5:
                final int totalShards = randomValueOtherThan(instance.totalShards(), () -> randomIntBetween(0, 100));
                final List<SnapshotShardFailure> shardFailures = randomShardFailures(randomIntBetween(0, totalShards));
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    totalShards,
                    shardFailures,
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 6:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    Boolean.FALSE.equals(instance.includeGlobalState()),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 7:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    randomValueOtherThan(instance.userMetadata(), SnapshotInfoTestUtils::randomUserMetadata),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 8:
                final List<String> dataStreams = randomValueOtherThan(
                    instance.dataStreams(),
                    () -> Arrays.asList(randomArray(0, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)))
                );
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    dataStreams,
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 9:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    randomValueOtherThan(instance.featureStates(), SnapshotInfoTestUtils::randomSnapshotFeatureInfos),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    instance.indexSnapshotDetails()
                );
            case 10:
                return new SnapshotInfo(
                    instance.snapshot(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.featureStates(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.startTime(),
                    randomValueOtherThan(instance.indexSnapshotDetails(), SnapshotInfoTestUtils::randomIndexSnapshotDetails)
                );
            default:
                throw new IllegalArgumentException("invalid randomization case");
        }

    }
}

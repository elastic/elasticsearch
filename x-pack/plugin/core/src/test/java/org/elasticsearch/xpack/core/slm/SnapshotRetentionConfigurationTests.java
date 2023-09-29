/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotRetentionConfigurationTests extends ESTestCase {

    private static final String REPO = "repo";

    public void testConflictingSettings() {
        IllegalArgumentException e;
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, 0, null));
        assertThat(e.getMessage(), containsString("minimum snapshot count must be at least 1, but was: 0"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, -2, null));
        assertThat(e.getMessage(), containsString("minimum snapshot count must be at least 1, but was: -2"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, null, 0));
        assertThat(e.getMessage(), containsString("maximum snapshot count must be at least 1, but was: 0"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, null, -2));
        assertThat(e.getMessage(), containsString("maximum snapshot count must be at least 1, but was: -2"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, 3, 1));
        assertThat(e.getMessage(), containsString("minimum snapshot count 3 cannot be larger than maximum snapshot count 1"));
    }

    private static Map<SnapshotId, RepositoryData.SnapshotDetails> detailsMap(SnapshotInfo... snapshotInfos) {
        return Arrays.stream(snapshotInfos)
            .collect(Collectors.toMap(SnapshotInfo::snapshotId, RepositoryData.SnapshotDetails::fromSnapshotInfo));
    }

    private static boolean isSnapshotEligibleForDeletion(
        SnapshotRetentionConfiguration snapshotRetentionConfiguration,
        SnapshotInfo si,
        Map<SnapshotId, RepositoryData.SnapshotDetails> allSnapshots
    ) {
        return snapshotRetentionConfiguration.isSnapshotEligibleForDeletion(
            si.snapshotId(),
            RepositoryData.SnapshotDetails.fromSnapshotInfo(si),
            allSnapshots
        );
    }

    public void testExpireAfter() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1),
            null,
            null
        );
        SnapshotInfo oldInfo = makeInfo(0);
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, detailsMap(oldInfo)), equalTo(true));

        SnapshotInfo newInfo = makeInfo(1);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, detailsMap(newInfo)), equalTo(false));

        final var infos = detailsMap(newInfo, oldInfo);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, infos), equalTo(true));
    }

    public void testExpiredWithMinimum() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1),
            2,
            null
        );
        SnapshotInfo oldInfo = makeInfo(0);
        SnapshotInfo newInfo = makeInfo(1);

        final var infos = detailsMap(newInfo, oldInfo);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, infos), equalTo(false));

        conf = new SnapshotRetentionConfiguration(() -> TimeValue.timeValueDays(1).millis() + 1, TimeValue.timeValueDays(1), 1, null);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, infos), equalTo(true));
    }

    public void testMaximum() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 5);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeInfo(3);
        SnapshotInfo s4 = makeInfo(4);
        SnapshotInfo s5 = makeInfo(5);
        SnapshotInfo s6 = makeInfo(6);
        SnapshotInfo s7 = makeInfo(7);
        SnapshotInfo s8 = makeInfo(8);
        SnapshotInfo s9 = makeInfo(9);

        final var infos = detailsMap(s1, s2, s3, s4, s5, s6, s7, s8, s9);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s3, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s4, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s5, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s6, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s7, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s8, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s9, infos), equalTo(false));
    }

    public void testMaximumWithExpireAfter() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 2,
            TimeValue.timeValueDays(1),
            null,
            2
        );
        SnapshotInfo old1 = makeInfo(0);
        SnapshotInfo old2 = makeInfo(1);
        SnapshotInfo new1 = makeInfo(2);

        final var infos = detailsMap(old1, old2, new1);
        assertThat(isSnapshotEligibleForDeletion(conf, old1, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, old2, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, new1, infos), equalTo(false));
    }

    public void testMaximumWithFailedOrPartial() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, null, 1);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeFailureOrPartial(2, randomBoolean());
        SnapshotInfo s3 = makeInfo(3);
        SnapshotInfo s4 = makeInfo(4);

        final var infos = detailsMap(s1, s2, s3, s4);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s3, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s4, infos), equalTo(false));
    }

    public void testFailuresDeletedIfExpired() {
        assertUnsuccessfulDeletedIfExpired(true);
    }

    public void testPartialsDeletedIfExpired() {
        assertUnsuccessfulDeletedIfExpired(false);
    }

    private void assertUnsuccessfulDeletedIfExpired(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1),
            null,
            null
        );
        SnapshotInfo oldInfo = makeFailureOrPartial(0, failure);
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, detailsMap(oldInfo)), equalTo(true));

        SnapshotInfo newInfo = makeFailureOrPartial(1, failure);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, detailsMap(newInfo)), equalTo(false));

        final var infos = detailsMap(oldInfo, newInfo);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, infos), equalTo(true));
    }

    public void testFailuresDeletedIfNoExpiryAndMoreRecentSuccessExists() {
        assertUnsuccessfulDeletedIfNoExpiryAndMoreRecentSuccessExists(true);
    }

    public void testPartialsDeletedIfNoExpiryAndMoreRecentSuccessExists() {
        assertUnsuccessfulDeletedIfNoExpiryAndMoreRecentSuccessExists(false);
    }

    private void assertUnsuccessfulDeletedIfNoExpiryAndMoreRecentSuccessExists(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 5);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeFailureOrPartial(3, failure);
        SnapshotInfo s4 = makeInfo(4);

        final var infos = detailsMap(s1, s2, s3, s4);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s3, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, s4, infos), equalTo(false));
    }

    public void testFailuresKeptIfNoExpiryAndNoMoreRecentSuccess() {
        assertUnsuccessfulKeptIfNoExpiryAndNoMoreRecentSuccess(true);
    }

    public void testPartialsKeptIfNoExpiryAndNoMoreRecentSuccess() {
        assertUnsuccessfulKeptIfNoExpiryAndNoMoreRecentSuccess(false);
    }

    private void assertUnsuccessfulKeptIfNoExpiryAndNoMoreRecentSuccess(boolean failure) {
        // Also tests that failures are not counted towards the maximum
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 3);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeInfo(3);
        SnapshotInfo s4 = makeFailureOrPartial(4, failure);

        final var infos = detailsMap(s1, s2, s3, s4);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s3, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s4, infos), equalTo(false));
    }

    public void testFailuresNotCountedTowardsMaximum() {
        assertUnsuccessfulNotCountedTowardsMaximum(true);
    }

    public void testPartialsNotCountedTowardsMaximum() {
        assertUnsuccessfulNotCountedTowardsMaximum(false);
    }

    private void assertUnsuccessfulNotCountedTowardsMaximum(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 5, TimeValue.timeValueDays(1), 2, 2);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeFailureOrPartial(2, failure);
        SnapshotInfo s3 = makeFailureOrPartial(3, failure);
        SnapshotInfo s4 = makeFailureOrPartial(4, failure);
        SnapshotInfo s5 = makeInfo(5);

        final var infos = detailsMap(s1, s2, s3, s4, s5);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s3, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s4, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s5, infos), equalTo(false));
    }

    public void testFailuresNotCountedTowardsMinimum() {
        assertUnsuccessfulNotCountedTowardsMinimum(true);
    }

    public void testPartialsNotCountedTowardsMinimum() {
        assertUnsuccessfulNotCountedTowardsMinimum(false);
    }

    private void assertUnsuccessfulNotCountedTowardsMinimum(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1),
            2,
            null
        );
        SnapshotInfo oldInfo = makeInfo(0);
        SnapshotInfo failureInfo = makeFailureOrPartial(1, failure);
        SnapshotInfo newInfo = makeInfo(2);

        final var infos = detailsMap(newInfo, failureInfo, oldInfo);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, failureInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, infos), equalTo(false));

        conf = new SnapshotRetentionConfiguration(() -> TimeValue.timeValueDays(1).millis() + 2, TimeValue.timeValueDays(1), 1, null);
        assertThat(isSnapshotEligibleForDeletion(conf, newInfo, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, failureInfo, infos), equalTo(true));
        assertThat(isSnapshotEligibleForDeletion(conf, oldInfo, infos), equalTo(true));
    }

    public void testMostRecentSuccessfulTimestampIsUsed() {
        boolean failureBeforePartial = randomBoolean();
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 2);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeFailureOrPartial(3, failureBeforePartial);
        SnapshotInfo s4 = makeFailureOrPartial(4, failureBeforePartial == false);

        final var infos = detailsMap(s1, s2, s3, s4);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s3, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s4, infos), equalTo(false));
    }

    public void testFewerSuccessesThanMinWithPartial() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, TimeValue.timeValueSeconds(5), 10, 20);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo sP = makePartialInfo(2);
        SnapshotInfo s2 = makeInfo(3);

        final var infos = detailsMap(s1, sP, s2);
        assertThat(isSnapshotEligibleForDeletion(conf, s1, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, sP, infos), equalTo(false));
        assertThat(isSnapshotEligibleForDeletion(conf, s2, infos), equalTo(false));
    }

    private SnapshotInfo makeInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotsService.POLICY_ID_METADATA_FIELD, REPO);
        final int totalShards = between(1, 20);
        SnapshotInfo snapInfo = new SnapshotInfo(
            new Snapshot(REPO, new SnapshotId("snap-" + randomAlphaOfLength(3), "uuid")),
            Collections.singletonList("foo"),
            Collections.singletonList("bar"),
            Collections.emptyList(),
            null,
            startTime + between(1, 10000),
            totalShards,
            new ArrayList<>(),
            false,
            meta,
            startTime,
            Collections.emptyMap()
        );
        assertThat(snapInfo.state(), equalTo(SnapshotState.SUCCESS));
        return snapInfo;
    }

    private SnapshotInfo makeFailureOrPartial(long startTime, boolean failure) {
        if (failure) {
            return makeFailureInfo(startTime);
        } else {
            return makePartialInfo(startTime);
        }
    }

    private SnapshotInfo makeFailureInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotsService.POLICY_ID_METADATA_FIELD, REPO);
        final int totalShards = between(1, 20);
        final List<SnapshotShardFailure> failures = new ArrayList<>();
        final int failureCount = between(1, totalShards);
        for (int i = 0; i < failureCount; i++) {
            failures.add(new SnapshotShardFailure("nodeId", new ShardId("index-name", "index-uuid", i), "failed"));
        }
        assert failureCount == failures.size();
        SnapshotInfo snapInfo = new SnapshotInfo(
            new Snapshot(REPO, new SnapshotId("snap-fail-" + randomAlphaOfLength(3), "uuid-fail")),
            Collections.singletonList("foo-fail"),
            Collections.singletonList("bar-fail"),
            Collections.emptyList(),
            "forced-failure",
            startTime + between(1, 10000),
            totalShards,
            failures,
            randomBoolean(),
            meta,
            startTime,
            Collections.emptyMap()
        );
        assertThat(snapInfo.state(), equalTo(SnapshotState.FAILED));
        return snapInfo;
    }

    private SnapshotInfo makePartialInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotsService.POLICY_ID_METADATA_FIELD, REPO);
        final int totalShards = between(2, 20);
        final List<SnapshotShardFailure> failures = new ArrayList<>();
        final int failureCount = between(1, totalShards - 1);
        for (int i = 0; i < failureCount; i++) {
            failures.add(new SnapshotShardFailure("nodeId", new ShardId("index-name", "index-uuid", i), "failed"));
        }
        assert failureCount == failures.size();
        SnapshotInfo snapInfo = new SnapshotInfo(
            new Snapshot(REPO, new SnapshotId("snap-fail-" + randomAlphaOfLength(3), "uuid-fail")),
            Collections.singletonList("foo-fail"),
            Collections.singletonList("bar-fail"),
            Collections.emptyList(),
            null,
            startTime + between(1, 10000),
            totalShards,
            failures,
            randomBoolean(),
            meta,
            startTime,
            Collections.emptyMap()
        );
        assertThat(snapInfo.state(), equalTo(SnapshotState.PARTIAL));
        return snapInfo;
    }
}

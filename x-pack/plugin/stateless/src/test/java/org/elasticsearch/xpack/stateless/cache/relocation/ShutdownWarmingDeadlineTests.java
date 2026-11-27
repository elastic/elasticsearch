/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ShutdownWarmingDeadlineTests extends ESTestCase {

    private static final long STARTED_AT_MILLIS = 1_000_000L;

    public void testDeadlineIsTheShutdownStartPlusTheGracePeriodBelowTheCap() {
        final var cap = TimeValue.timeValueMinutes(randomIntBetween(2, 14));
        final var grace = TimeValue.timeValueSeconds(randomLongBetween(1, cap.seconds() - 1));
        final var deadline = newDeadline(cap);

        assertThat(deadline.deadlineMillis(sigterm(grace)), equalTo(STARTED_AT_MILLIS + grace.millis()));
    }

    public void testDeadlineIsTheShutdownStartPlusTheCapWhenTheGracePeriodExceedsIt() {
        final var cap = TimeValue.timeValueMinutes(randomIntBetween(1, 14));
        final var grace = TimeValue.timeValueSeconds(cap.seconds() + randomLongBetween(1, 3600));
        final var deadline = newDeadline(cap);

        assertThat(deadline.deadlineMillis(sigterm(grace)), equalTo(STARTED_AT_MILLIS + cap.millis()));
    }

    public void testDeadlineIsTheShutdownStartPlusTheCapWhenThereIsNoGracePeriod() {
        final var cap = TimeValue.timeValueMinutes(randomIntBetween(1, 14));
        final var deadline = newDeadline(cap);

        // only a SIGTERM shutdown has a grace period
        final var shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId("node")
            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
            .setReason("ShutdownWarmingDeadlineTests")
            .setStartedAtMillis(STARTED_AT_MILLIS)
            .build();
        assertThat(deadline.deadlineMillis(shutdown), equalTo(STARTED_AT_MILLIS + cap.millis()));
    }

    public void testDeadlineFollowsTheDynamicCapUpdates() {
        final var grace = TimeValue.timeValueMinutes(10);
        final var clusterSettings = newClusterSettings(TimeValue.timeValueMinutes(14));
        final var deadline = new ShutdownWarmingDeadline(clusterSettings);
        assertThat(deadline.deadlineMillis(sigterm(grace)), equalTo(STARTED_AT_MILLIS + grace.millis()));

        final var lowerCap = TimeValue.timeValueMinutes(3);
        clusterSettings.applySettings(capSettings(lowerCap));
        assertThat(deadline.deadlineMillis(sigterm(grace)), equalTo(STARTED_AT_MILLIS + lowerCap.millis()));
    }

    private static ShutdownWarmingDeadline newDeadline(TimeValue cap) {
        return new ShutdownWarmingDeadline(newClusterSettings(cap));
    }

    private static ClusterSettings newClusterSettings(TimeValue cap) {
        return new ClusterSettings(
            capSettings(cap),
            Set.of(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING)
        );
    }

    private static Settings capSettings(TimeValue cap) {
        return Settings.builder().put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), cap).build();
    }

    private static SingleNodeShutdownMetadata sigterm(TimeValue grace) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId("node")
            .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
            .setReason("ShutdownWarmingDeadlineTests")
            .setStartedAtMillis(STARTED_AT_MILLIS)
            .setGracePeriod(grace)
            .build();
    }
}

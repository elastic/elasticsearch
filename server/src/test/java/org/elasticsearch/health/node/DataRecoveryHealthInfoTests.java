/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.node.DataRecoveryHealthInfo.ProjectSummary;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.health.node.DataRecoveryHealthInfo.ProjectSummary.NONE;
import static org.hamcrest.Matchers.containsString;

public class DataRecoveryHealthInfoTests extends AbstractWireSerializingTestCase<DataRecoveryHealthInfo> {

    @Override
    protected Writeable.Reader<DataRecoveryHealthInfo> instanceReader() {
        return DataRecoveryHealthInfo::new;
    }

    @Override
    protected DataRecoveryHealthInfo createTestInstance() {
        return HealthInfoTests.randomDataRecoveryHealthInfo();
    }

    @Override
    protected DataRecoveryHealthInfo mutateInstance(DataRecoveryHealthInfo instance) {
        return switch (randomInt(2)) {
            case 0 -> {
                Map<ProjectId, ProjectSummary> projects = new HashMap<>(instance.projects());
                projects.put(randomUniqueProjectId(), HealthInfoTests.randomDataRecoveryProjectSummary());
                yield new DataRecoveryHealthInfo(projects, instance.generatedAtMillis(), instance.publishIntervalMillis());
            }
            case 1 -> new DataRecoveryHealthInfo(
                instance.projects(),
                randomValueOtherThan(instance.generatedAtMillis(), ESTestCase::randomNonNegativeLong),
                instance.publishIntervalMillis()
            );
            case 2 -> new DataRecoveryHealthInfo(
                instance.projects(),
                instance.generatedAtMillis(),
                randomValueOtherThan(instance.publishIntervalMillis(), () -> randomLongBetween(1, Long.MAX_VALUE))
            );
            default -> throw new IllegalStateException("unexpected random value");
        };
    }

    public void testRejectsNonPositivePublishInterval() {
        long invalidInterval = randomFrom(0L, randomLongBetween(Long.MIN_VALUE, -1));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataRecoveryHealthInfo(Map.of(), randomNonNegativeLong(), invalidInterval)
        );
        assertThat(e.getMessage(), containsString("publish interval must be positive"));
    }

    public void testRejectsNegativeGeneratedAt() {
        long invalidGeneratedAt = randomLongBetween(Long.MIN_VALUE, -1);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataRecoveryHealthInfo(Map.of(), invalidGeneratedAt, randomLongBetween(1, Long.MAX_VALUE))
        );
        assertThat(e.getMessage(), containsString("generation time must not be negative"));
    }

    public void testEmptySummaryIsValid() {
        ProjectSummary summary = new ProjectSummary(NONE, NONE, 0, 0, 0);
        assertFalse(summary.hasRecoveryPoints());
    }

    public void testSummaryWithRecoveryPointsIsValid() {
        ProjectSummary summary = new ProjectSummary(200L, 100L, 3, 3, 2);
        assertTrue(summary.hasRecoveryPoints());
        // the oldest and the newest start can be the same recovery point
        assertTrue(new ProjectSummary(100L, 100L, 1, 0, 0).hasRecoveryPoints());
    }

    public void testRejectsInconsistentSummaries() {
        // no recovery points, but there is a start time or a count
        assertInconsistent(100L, NONE, 0, 0, 0);
        assertInconsistent(NONE, 100L, 0, 0, 0);
        assertInconsistent(NONE, NONE, 0, 1, 0);
        assertInconsistent(NONE, NONE, 0, 0, 1);
        // recovery points, but a missing start time
        assertInconsistent(NONE, 100L, 1, 0, 0);
        assertInconsistent(100L, NONE, 1, 0, 0);
        // the oldest is after the newest
        assertInconsistent(100L, 200L, 2, 0, 0);
        // the partial count is out of range
        assertInconsistent(200L, 100L, 2, 3, 0);
        assertInconsistent(200L, 100L, 2, -1, 0);
        // negative counts
        assertInconsistent(200L, 100L, -1, 0, 0);
        assertInconsistent(200L, 100L, 2, 0, -1);
    }

    private static void assertInconsistent(long newest, long oldest, int count, int partial, int incompleteSources) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ProjectSummary(newest, oldest, count, partial, incompleteSources)
        );
        assertThat(e.getMessage(), containsString("inconsistent recovery point summary"));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.Diagnosis.Resource.Type;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.xpack.core.ilm.OperationMode.RUNNING;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPING;
import static org.elasticsearch.xpack.slm.SlmHealthIndicatorService.NAME;
import static org.elasticsearch.xpack.slm.SlmHealthIndicatorService.SLM_NOT_RUNNING;
import static org.elasticsearch.xpack.slm.SlmHealthIndicatorService.checkRecentlyFailedSnapshots;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlmHealthIndicatorServiceTests extends ESTestCase {

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("iso8601").withZone(ZoneOffset.UTC);

    public void testIsGreenWhenRunningAndPoliciesConfigured() {
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(createSlmPolicy(), RUNNING, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "Snapshot Lifecycle Management is running",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 1)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsYellowWhenNotRunningAndPoliciesConfigured() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(createSlmPolicy(), status, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Snapshot Lifecycle Management is not running",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", status, "policies", 1)),
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            NAME,
                            SlmHealthIndicatorService.AUTOMATION_DISABLED_IMPACT_ID,
                            3,
                            "Scheduled snapshots are not running. New backup snapshots will not be created automatically.",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of(SLM_NOT_RUNNING)
                )
            )
        );
    }

    public void testIsGreenWhenNotRunningAndNoPolicies() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(Map.of(), status, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "No Snapshot Lifecycle Management policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", status, "policies", 0)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "No Snapshot Lifecycle Management policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 0)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenPoliciesHaveFailedForLessThanWarningThreshold() {
        long execTime = System.currentTimeMillis();
        long window = TimeUnit.HOURS.toMillis(24) - 5000L; // Just under 24 hours.
        var clusterState = createClusterStateWith(
            new SnapshotLifecycleMetadata(
                createSlmPolicy(
                    snapshotInvocation(randomBoolean() ? null : execTime, execTime + 1000L),
                    snapshotInvocation(null, execTime + window + 1000L),
                    randomLongBetween(0, 4),
                    null
                ),
                RUNNING,
                null
            )
        );
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "Snapshot Lifecycle Management is running",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 1)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsYellowWhenPoliciesHaveFailedForMoreThanWarningThreshold() {
        long execTime = System.currentTimeMillis();
        long window = TimeUnit.HOURS.toMillis(24) + 5000L; // 24 hours and some extra room.
        long failedInvocations1 = randomLongBetween(5L, Long.MAX_VALUE);
        long failedInvocations2 = randomLongBetween(5L, Long.MAX_VALUE);
        long failedInvocations3 = randomLongBetween(5L, Long.MAX_VALUE);
        var clusterState = createClusterStateWith(
            new SnapshotLifecycleMetadata(
                Map.of(
                    "test-policy",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy("test-policy", "<test-policy-{now/d}>", "", "test-repository", null, null, null)
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(snapshotInvocation(execTime, execTime + 1000L))
                        .setLastFailure(snapshotInvocation(null, execTime + window + 1000L))
                        .setInvocationsSinceLastSuccess(failedInvocations1)
                        .build(),
                    "test-policy-without-any-success",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy(
                                "test-policy-without-any-success",
                                "<test-policy-{now/d}>",
                                "",
                                "test-repository",
                                null,
                                null,
                                null
                            )
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(null)
                        .setLastFailure(snapshotInvocation(null, execTime + window + 1000L))
                        .setInvocationsSinceLastSuccess(failedInvocations2)
                        .build(),
                    "test-policy-without-success-start-time",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy(
                                "test-policy-without-success-start-time",
                                "<test-policy-{now/d}>",
                                "",
                                "test-repository",
                                null,
                                null,
                                null
                            )
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(snapshotInvocation(null, execTime))
                        .setLastFailure(snapshotInvocation(null, execTime + window + 1000L))
                        .setInvocationsSinceLastSuccess(failedInvocations3)
                        .build()
                ),
                RUNNING,
                null
            )
        );
        var service = createSlmHealthIndicatorService(clusterState);

        HealthIndicatorResult calculate = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertThat(
            calculate,
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Encountered [3] unhealthy snapshot lifecycle management policies",
                    new SimpleHealthIndicatorDetails(
                        Map.of(
                            "slm_status",
                            RUNNING,
                            "policies",
                            3,
                            "unhealthy_policies",
                            Map.of(
                                "count",
                                3,
                                "invocations_since_last_success",
                                Map.of(
                                    "test-policy",
                                    failedInvocations1,
                                    "test-policy-without-any-success",
                                    failedInvocations2,
                                    "test-policy-without-success-start-time",
                                    failedInvocations3
                                )
                            )
                        )
                    ),
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            NAME,
                            SlmHealthIndicatorService.STALE_SNAPSHOTS_IMPACT_ID,
                            2,
                            "Some automated snapshots have not had a successful execution recently. Indices restored from affected "
                                + "snapshots may not contain recent changes.",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of(
                        new Diagnosis(
                            SlmHealthIndicatorService.checkRecentlyFailedSnapshots(
                                "Several automated snapshot policies are unhealthy:\n"
                                    + "- [test-policy] had ["
                                    + failedInvocations1
                                    + "] repeated failures without successful execution since ["
                                    + FORMATTER.formatMillis(execTime)
                                    + "]\n"
                                    + "- [test-policy-without-any-success] had ["
                                    + failedInvocations2
                                    + "] repeated failures without successful execution\n"
                                    + "- [test-policy-without-success-start-time] had ["
                                    + failedInvocations3
                                    + "] repeated failures without successful execution",
                                "Check the snapshot lifecycle policies for detailed failure info:\n"
                                    + "- GET /_slm/policy/test-policy?human\n"
                                    + "- GET /_slm/policy/test-policy-without-any-success?human\n"
                                    + "- GET /_slm/policy/test-policy-without-success-start-time?human"

                            ),
                            List.of(
                                new Diagnosis.Resource(
                                    Type.SLM_POLICY,
                                    List.of("test-policy", "test-policy-without-any-success", "test-policy-without-success-start-time")
                                )
                            )
                        )
                    )
                )
            )
        );
    }

    public void testIsYellowWhenPoliciesExceedsUnhealthyIfNoSnapshotWithin() {
        long tenMinutesAgo = Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli();
        long fiveMinutesAgo = Instant.now().minus(5, ChronoUnit.MINUTES).toEpochMilli();

        TimeValue threshold = TimeValue.ONE_MINUTE;

        var clusterState = createClusterStateWith(
            new SnapshotLifecycleMetadata(
                Map.of(
                    "test-policy-no-time-configured",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy("test-policy-no-time-configured", "test", "", "test-repository", null, null, null)
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(snapshotInvocation(tenMinutesAgo, fiveMinutesAgo))
                        .build(),
                    "test-policy-does-not-exceed-time",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy(
                                "test-policy-does-not-exceeds-time",
                                "test",
                                "",
                                "test-repository",
                                null,
                                null,
                                new TimeValue(1, TimeUnit.HOURS)
                            )
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(snapshotInvocation(tenMinutesAgo, fiveMinutesAgo))
                        .build(),
                    "test-policy-exceeds-time",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy("test-policy-exceeds-time", "test", "", "test-repository", null, null, threshold)
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(snapshotInvocation(tenMinutesAgo, fiveMinutesAgo))
                        .build(),
                    "test-policy-exceeds-time-without-success-start-time",
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(
                            new SnapshotLifecyclePolicy(
                                "test-policy-exceeds-time-without-success-start-time",
                                "test",
                                "",
                                "test-repository",
                                null,
                                null,
                                threshold
                            )
                        )
                        .setVersion(1L)
                        .setModifiedDate(System.currentTimeMillis())
                        .setLastSuccess(snapshotInvocation(null, fiveMinutesAgo))
                        .build()
                    // TODO: first snapshot
                ),
                RUNNING,
                null
            )
        );
        SlmHealthIndicatorService service = createSlmHealthIndicatorService(clusterState);

        HealthIndicatorResult calculate = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertThat(
            calculate,
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Encountered [2] unhealthy snapshot lifecycle management policies",
                    new SimpleHealthIndicatorDetails(
                        Map.of(
                            "slm_status",
                            RUNNING,
                            "policies",
                            4,
                            "unhealthy_policies",
                            Map.of(
                                "count",
                                2,
                                "invocations_since_last_success",
                                Map.of("test-policy-exceeds-time", 0L, "test-policy-exceeds-time-without-success-start-time", 0L)
                            )
                        )
                    ),
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            NAME,
                            SlmHealthIndicatorService.MISSING_SNAPSHOT_IMPACT_ID,
                            2,
                            "Some snapshot lifecycle policies have not had a snapshot for some time",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of(
                        new Diagnosis(
                            SlmHealthIndicatorService.contactSupport(
                                "Several automated snapshot policies are unhealthy:\n"
                                    + "- [test-policy-exceeds-time] has not had a snapshot for "
                                    + threshold.toHumanReadableString(2)
                                    + ", since ["
                                    + FORMATTER.formatMillis(tenMinutesAgo)
                                    + "]\n"
                                    + "- [test-policy-exceeds-time-without-success-start-time] has not had a snapshot for "
                                    + threshold.toHumanReadableString(2)
                                    + ", since ["
                                    + FORMATTER.formatMillis(fiveMinutesAgo)
                                    + "]",
                                "Check the snapshot lifecycle policies for detailed failure info:\n"
                                    + "- GET /_slm/policy/test-policy-exceeds-time?human\n"
                                    + "- GET /_slm/policy/test-policy-exceeds-time-without-success-start-time?human"
                            ),
                            List.of(
                                new Diagnosis.Resource(
                                    Type.SLM_POLICY,
                                    List.of("test-policy-exceeds-time", "test-policy-exceeds-time-without-success-start-time")
                                )
                            )
                        )
                    )
                )
            )
        );
    }

    public void testSnapshotPolicyExceedsWarningThresholdPredicate() {
        SnapshotLifecyclePolicyMetadata slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, null))
            .setVersion(1L)
            .setModifiedDate(System.currentTimeMillis())
            .build();

        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(15L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(5L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(1L, slmPolicyMetadata), is(false));

        slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, null))
            .setVersion(1L)
            .setModifiedDate(System.currentTimeMillis())
            .setLastSuccess(snapshotInvocation(1000L, 2000L))
            .setInvocationsSinceLastSuccess(0L)
            .build();

        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(15L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(5L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(1L, slmPolicyMetadata), is(false));

        slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, null))
            .setVersion(1L)
            .setModifiedDate(System.currentTimeMillis())
            .setLastSuccess(snapshotInvocation(1000L, 2000L))
            .setLastFailure(snapshotInvocation(null, 9000L))
            .setInvocationsSinceLastSuccess(randomLongBetween(5L, 10L))
            .build();

        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(15L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(5L, slmPolicyMetadata), is(true));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(1L, slmPolicyMetadata), is(true));

        slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, null))
            .setVersion(1L)
            .setModifiedDate(System.currentTimeMillis())
            .setLastSuccess(snapshotInvocation(8000L, 9000L))
            .setLastFailure(snapshotInvocation(null, 2000L))
            .setInvocationsSinceLastSuccess(0L)
            .build();

        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(15L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(5L, slmPolicyMetadata), is(false));
        assertThat(SlmHealthIndicatorService.snapshotFailuresExceedWarningCount(1L, slmPolicyMetadata), is(false));
    }

    public void testSnapshotPolicyMissingSnapshotTimeExceededPredicate() {
        long tenMinutesAgo = Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli();
        long fiveMinutesAgo = Instant.now().minus(5, ChronoUnit.MINUTES).toEpochMilli();
        // null unhealthyIfNoSnapshotWithin
        {
            SnapshotLifecyclePolicyMetadata slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, null))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                .setLastSuccess(new SnapshotInvocationRecord("test-snapshot", tenMinutesAgo, fiveMinutesAgo, null))
                .build();
            assertThat(SlmHealthIndicatorService.missingSnapshotTimeExceeded(slmPolicyMetadata), is(false));
        }
        // does not exceed unhealthyIfNoSnapshotWithin
        {
            SnapshotLifecyclePolicyMetadata slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, TimeValue.MAX_VALUE))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                .setLastSuccess(new SnapshotInvocationRecord("test-snapshot", tenMinutesAgo, fiveMinutesAgo, null))
                .build();
            assertThat(SlmHealthIndicatorService.missingSnapshotTimeExceeded(slmPolicyMetadata), is(false));
        }
        // exceed unhealthyIfNoSnapshotWithin
        {
            SnapshotLifecyclePolicyMetadata slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, TimeValue.ONE_MINUTE))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                .setLastSuccess(new SnapshotInvocationRecord("test-snapshot", tenMinutesAgo, fiveMinutesAgo, null))
                .build();
            assertThat(SlmHealthIndicatorService.missingSnapshotTimeExceeded(slmPolicyMetadata), is(true));
        }
        // first snapshot, does not exceed unhealthyIfNoSnapshotWithin
        {
            SnapshotLifecyclePolicyMetadata slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, TimeValue.MAX_VALUE))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                // TODO: set first trigger time
                .build();
            assertThat(SlmHealthIndicatorService.missingSnapshotTimeExceeded(slmPolicyMetadata), is(false));
        }
        // first snapshot, exceed unhealthyIfNoSnapshotWithin
        {
            SnapshotLifecyclePolicyMetadata slmPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null, TimeValue.ONE_MINUTE))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                // TODO: set first trigger time
                .build();
            assertThat(SlmHealthIndicatorService.missingSnapshotTimeExceeded(slmPolicyMetadata), is(false));
        }
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(createSlmPolicy(), status, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(false, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Snapshot Lifecycle Management is not running",
                    HealthIndicatorDetails.EMPTY,
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            NAME,
                            SlmHealthIndicatorService.AUTOMATION_DISABLED_IMPACT_ID,
                            3,
                            "Scheduled snapshots are not running. New backup snapshots will not be created automatically.",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of()
                )
            )
        );
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(SlmHealthIndicatorService.NAME, equalTo("slm"));
        assertThat(
            checkRecentlyFailedSnapshots("cause", "action").getUniqueId(),
            equalTo("elasticsearch:health:slm:diagnosis:check_recent_snapshot_failures")
        );
        assertThat(SLM_NOT_RUNNING.definition().getUniqueId(), equalTo("elasticsearch:health:slm:diagnosis:slm_disabled"));
    }

    private static ClusterState createClusterStateWith(SnapshotLifecycleMetadata metadata) {
        var builder = new ClusterState.Builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(new Metadata.Builder().putCustom(SnapshotLifecycleMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static Map<String, SnapshotLifecyclePolicyMetadata> createSlmPolicy() {
        return createSlmPolicy(null, null, 0L, null);
    }

    private static Map<String, SnapshotLifecyclePolicyMetadata> createSlmPolicy(
        SnapshotInvocationRecord lastSuccess,
        SnapshotInvocationRecord lastFailure,
        long invocationsSinceLastSuccess,
        TimeValue unhealthyIfNoSnapshotWithin
    ) {
        return Map.of(
            "test-policy",
            SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(
                    new SnapshotLifecyclePolicy("policy-id", "test-policy", "", "test-repository", null, null, unhealthyIfNoSnapshotWithin)
                )
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                .setLastSuccess(lastSuccess)
                .setLastFailure(lastFailure)
                .setInvocationsSinceLastSuccess(invocationsSinceLastSuccess)
                .build()
        );
    }

    private static SnapshotInvocationRecord snapshotInvocation(@Nullable Long startTime, long stopTime) {
        return new SnapshotInvocationRecord("test-policy-snapshot", startTime, stopTime, null);
    }

    private static SlmHealthIndicatorService createSlmHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(LifecycleSettings.SLM_HEALTH_FAILED_SNAPSHOT_WARN_THRESHOLD_SETTING)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new SlmHealthIndicatorService(clusterService);
    }
}

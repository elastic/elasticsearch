/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HEALTH_FAILED_SNAPSHOT_WARN_THRESHOLD_SETTING;

/**
 * This indicator reports health for snapshot lifecycle management component.
 *
 * Indicator will report YELLOW status when SLM is not running and there are configured policies.
 * Data might not be backed up timely in such cases.
 *
 * SLM must be running to fix warning reported by this indicator.
 */
public final class SlmHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "slm";

    public static final String HELP_URL = "https://ela.st/fix-slm";
    public static final Diagnosis SLM_NOT_RUNNING = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "slm_disabled",
            "Snapshot Lifecycle Management is stopped",
            "Start Snapshot Lifecycle Management using [POST /_slm/start].",
            HELP_URL
        ),
        null
    );

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("iso8601").withZone(ZoneOffset.UTC);

    public static final String DIAGNOSIS_CHECK_RECENTLY_FAILED_SNAPSHOTS_ID = "check_recent_snapshot_failures";
    public static final String DIAGNOSIS_CHECK_RECENTLY_FAILED_SNAPSHOTS_HELP_URL = "https://ela.st/fix-recent-snapshot-failures";

    // Visible for testing
    static Diagnosis.Definition checkRecentlyFailedSnapshots(String causeText, String actionText) {
        return new Diagnosis.Definition(
            NAME,
            DIAGNOSIS_CHECK_RECENTLY_FAILED_SNAPSHOTS_ID,
            causeText,
            actionText,
            DIAGNOSIS_CHECK_RECENTLY_FAILED_SNAPSHOTS_HELP_URL
        );
    }

    public static final String AUTOMATION_DISABLED_IMPACT_ID = "automation_disabled";
    public static final String STALE_SNAPSHOTS_IMPACT_ID = "stale_snapshots";

    private final ClusterService clusterService;
    private volatile long failedSnapshotWarnThreshold;

    public SlmHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.failedSnapshotWarnThreshold = clusterService.getClusterSettings().get(SLM_HEALTH_FAILED_SNAPSHOT_WARN_THRESHOLD_SETTING);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SLM_HEALTH_FAILED_SNAPSHOT_WARN_THRESHOLD_SETTING, this::setFailedSnapshotWarnThreshold);
    }

    public void setFailedSnapshotWarnThreshold(long value) {
        this.failedSnapshotWarnThreshold = value;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        final ClusterState currentState = clusterService.state();
        var slmMetadata = currentState.metadata().custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
        final OperationMode currentMode = currentSLMMode(currentState);
        if (slmMetadata.getSnapshotConfigurations().isEmpty()) {
            return createIndicator(
                GREEN,
                "No Snapshot Lifecycle Management policies configured",
                createDetails(verbose, Collections.emptyList(), slmMetadata, currentMode),
                Collections.emptyList(),
                Collections.emptyList()
            );
        } else if (currentMode != OperationMode.RUNNING) {
            List<HealthIndicatorImpact> impacts = Collections.singletonList(
                new HealthIndicatorImpact(
                    NAME,
                    AUTOMATION_DISABLED_IMPACT_ID,
                    3,
                    "Scheduled snapshots are not running. New backup snapshots will not be created automatically.",
                    List.of(ImpactArea.BACKUP)
                )
            );
            return createIndicator(
                YELLOW,
                "Snapshot Lifecycle Management is not running",
                createDetails(verbose, Collections.emptyList(), slmMetadata, currentMode),
                impacts,
                List.of(SLM_NOT_RUNNING)
            );
        } else {
            List<SnapshotLifecyclePolicyMetadata> unhealthyPolicies = slmMetadata.getSnapshotConfigurations()
                .values()
                .stream()
                .filter(metadata -> snapshotFailuresExceedWarningCount(failedSnapshotWarnThreshold, metadata))
                .sorted(Comparator.comparing(SnapshotLifecyclePolicyMetadata::getId))
                .toList();

            if (unhealthyPolicies.size() > 0) {
                List<HealthIndicatorImpact> impacts = Collections.singletonList(
                    new HealthIndicatorImpact(
                        NAME,
                        STALE_SNAPSHOTS_IMPACT_ID,
                        2,
                        "Some automated snapshots have not had a successful execution recently. Indices restored from affected "
                            + "snapshots may not contain recent changes.",
                        List.of(ImpactArea.BACKUP)
                    )
                );

                String unhealthyPolicyCauses = unhealthyPolicies.stream()
                    .map(
                        policy -> "- ["
                            + policy.getId()
                            + "] had ["
                            + policy.getInvocationsSinceLastSuccess()
                            + "] repeated failures without successful execution"
                            + (policy.getLastSuccess() != null && policy.getLastSuccess().getSnapshotStartTimestamp() != null
                                ? " since [" + FORMATTER.formatMillis(policy.getLastSuccess().getSnapshotStartTimestamp()) + "]"
                                : "")
                    )
                    .collect(Collectors.joining("\n"));
                String cause = (unhealthyPolicies.size() > 1
                    ? "Several automated snapshot policies are unhealthy:\n"
                    : "An automated snapshot policy is unhealthy:\n") + unhealthyPolicyCauses;

                String unhealthyPolicyActions = unhealthyPolicies.stream()
                    .map(policy -> "- GET /_slm/policy/" + policy.getId() + "?human")
                    .collect(Collectors.joining("\n"));
                String action = "Check the snapshot lifecycle "
                    + (unhealthyPolicies.size() > 1 ? "policies" : "policy")
                    + " for detailed failure info:\n"
                    + unhealthyPolicyActions;

                return createIndicator(
                    YELLOW,
                    "Encountered [" + unhealthyPolicies.size() + "] unhealthy snapshot lifecycle management policies.",
                    createDetails(verbose, unhealthyPolicies, slmMetadata, currentMode),
                    impacts,
                    List.of(
                        new Diagnosis(
                            checkRecentlyFailedSnapshots(cause, action),
                            List.of(
                                new Diagnosis.Resource(
                                    Diagnosis.Resource.Type.SLM_POLICY,
                                    unhealthyPolicies.stream()
                                        .map(SnapshotLifecyclePolicyMetadata::getId)
                                        .limit(Math.min(unhealthyPolicies.size(), maxAffectedResourcesCount))
                                        .toList()
                                )
                            )
                        )
                    )
                );
            }

            return createIndicator(
                GREEN,
                "Snapshot Lifecycle Management is running",
                createDetails(verbose, Collections.emptyList(), slmMetadata, currentMode),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
    }

    static boolean snapshotFailuresExceedWarningCount(long failedSnapshotWarnThreshold, SnapshotLifecyclePolicyMetadata policyMetadata) {
        SnapshotInvocationRecord lastFailure = policyMetadata.getLastFailure();
        if (lastFailure == null) {
            // No failures yet to act on
            return false;
        }

        // Determine if the most recent snapshot is a failure
        SnapshotInvocationRecord lastSuccess = policyMetadata.getLastSuccess();
        if (lastSuccess != null && policyMetadata.getInvocationsSinceLastSuccess() == 0) {
            // Success was more recent than last failure
            return false;
        }

        return policyMetadata.getInvocationsSinceLastSuccess() >= failedSnapshotWarnThreshold;
    }

    private static HealthIndicatorDetails createDetails(
        boolean verbose,
        Collection<SnapshotLifecyclePolicyMetadata> unhealthyPolicies,
        SnapshotLifecycleMetadata metadata,
        OperationMode mode
    ) {
        if (verbose) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("slm_status", mode);
            details.put("policies", metadata.getSnapshotConfigurations().size());
            if (unhealthyPolicies.size() > 0) {
                details.put(
                    "unhealthy_policies",
                    Map.of(
                        "count",
                        unhealthyPolicies.size(),
                        "invocations_since_last_success",
                        unhealthyPolicies.stream()
                            .collect(
                                Collectors.toMap(
                                    SnapshotLifecyclePolicyMetadata::getId,
                                    SnapshotLifecyclePolicyMetadata::getInvocationsSinceLastSuccess
                                )
                            )
                    )
                );
            }
            return new SimpleHealthIndicatorDetails(details);
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }
}

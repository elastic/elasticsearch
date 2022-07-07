/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.SNAPSHOT;

/**
 * This indicator reports health for snapshot lifecycle management component.
 *
 * Indicator will report YELLOW status when SLM is not running and there are configured policies.
 * Data might not be backed up timely in such cases.
 *
 * SLM must be running to fix warning reported by this indicator.
 */
public class SlmHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "slm";

    public static final String HELP_URL = "https://ela.st/fix-slm";
    public static final UserAction SLM_NOT_RUNNING = new UserAction(
        new UserAction.Definition("slm-not-running", "Start Snapshot Lifecycle Management using [POST /_slm/start].", HELP_URL),
        null
    );

    /**
     * A grace period for snapshot policy failures. When a snapshot policy encounters a failed snapshot, if the last successful snapshot
     * exists outside this look-back window then we report degraded health of the snapshot policy.
     */
    private static final TimeValue SNAPSHOT_FAILURE_WARNING_WINDOW = TimeValue.timeValueHours(24);

    public static final UserAction.Definition ACTION_CHECK_RECENTLY_FAILED_SNAPSHOTS = new UserAction.Definition(
        "check_recent_snapshot_failures",
        "The following snapshot lifecycle policies have not had successful snapshot executions for longer than 24 hours. Check the " +
            "cluster logs for failures.",
        null
    );

    private final ClusterService clusterService;

    public SlmHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return SNAPSHOT;
    }

    @Override
    public String helpURL() {
        return HELP_URL;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        var slmMetadata = clusterService.state().metadata().custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
        if (slmMetadata.getSnapshotConfigurations().isEmpty()) {
            return createIndicator(
                GREEN,
                "No Snapshot Lifecycle Management policies configured",
                createDetails(explain, Collections.emptyList(), slmMetadata),
                Collections.emptyList(),
                Collections.emptyList()
            );
        } else if (slmMetadata.getOperationMode() != OperationMode.RUNNING) {
            List<HealthIndicatorImpact> impacts = Collections.singletonList(
                new HealthIndicatorImpact(
                    3,
                    "Scheduled snapshots are not running. New backup snapshots will not be created automatically.",
                    List.of(ImpactArea.BACKUP)
                )
            );
            return createIndicator(
                YELLOW,
                "Snapshot Lifecycle Management is not running",
                createDetails(explain, Collections.emptyList(), slmMetadata),
                impacts,
                List.of(SLM_NOT_RUNNING)
            );
        } else {
            List<SnapshotLifecyclePolicyMetadata> unhealthyPolicies = slmMetadata.getSnapshotConfigurations()
                .values()
                .stream()
                .filter(this::snapshotFailuresExceedWarningThreshold)
                .toList();

            if (unhealthyPolicies.size() > 0) {
                List<HealthIndicatorImpact> impacts = Collections.singletonList(
                    new HealthIndicatorImpact(
                        2,
                        "Some automated snapshots have not had a successful execution recently. Indices restored from affected "
                            + "snapshots may not contain recent changes.",
                        List.of(ImpactArea.BACKUP)
                    )
                );

                return createIndicator(
                    RED,
                    "Encountered [" + unhealthyPolicies.size() + "] unhealthy snapshot lifecycle management policies.",
                    createDetails(explain, unhealthyPolicies, slmMetadata),
                    impacts,
                    List.of(
                        new UserAction(
                            ACTION_CHECK_RECENTLY_FAILED_SNAPSHOTS,
                            unhealthyPolicies.stream().map(SnapshotLifecyclePolicyMetadata::getName).toList()
                        )
                    )
                );
            }

            return createIndicator(
                GREEN,
                "Snapshot Lifecycle Management is running",
                createDetails(explain, Collections.emptyList(), slmMetadata),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
    }

    private boolean snapshotFailuresExceedWarningThreshold(SnapshotLifecyclePolicyMetadata policyMetadata) {
        SnapshotInvocationRecord lastFailure = policyMetadata.getLastFailure();
        if (lastFailure == null) {
            // No failures yet to act on
            return false;
        }

        // Determine if the most recent snapshot is a failure
        SnapshotInvocationRecord lastSuccess = policyMetadata.getLastSuccess();
        if (lastSuccess != null && lastSuccess.getSnapshotStartTimestamp() > lastFailure.getSnapshotStartTimestamp()) {
            // Success was more recent than last failure
            return false;
        }

        long periodStart;
        if (lastSuccess == null) {
            // Base the period start on the modification time for the policy
            periodStart = policyMetadata.getModifiedDate();
        } else {
            periodStart = lastSuccess.getSnapshotStartTimestamp();
        }

        long snapshotFailureWarningThreshold = periodStart + SNAPSHOT_FAILURE_WARNING_WINDOW.getMillis();
        return lastFailure.getSnapshotStartTimestamp() > snapshotFailureWarningThreshold;
    }

    private static HealthIndicatorDetails createDetails(
        boolean explain,
        Collection<SnapshotLifecyclePolicyMetadata> unhealthyPolicies,
        SnapshotLifecycleMetadata metadata
    ) {
        if (explain) {
            return new SimpleHealthIndicatorDetails(
                Map.of(
                    "slm_status",
                    metadata.getOperationMode(),
                    "policies",
                    metadata.getSnapshotConfigurations().size(),
                    "unhealthy_policies",
                    unhealthyPolicies.size()
                )
            );
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }
}

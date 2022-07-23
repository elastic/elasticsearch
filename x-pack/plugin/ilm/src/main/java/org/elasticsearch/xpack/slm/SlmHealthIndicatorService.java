/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
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
                createDetails(explain, slmMetadata),
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
                createDetails(explain, slmMetadata),
                impacts,
                List.of(SLM_NOT_RUNNING)
            );
        } else {
            return createIndicator(
                GREEN,
                "Snapshot Lifecycle Management is running",
                createDetails(explain, slmMetadata),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
    }

    private static HealthIndicatorDetails createDetails(boolean explain, SnapshotLifecycleMetadata metadata) {
        if (explain) {
            return new SimpleHealthIndicatorDetails(
                Map.of("slm_status", metadata.getOperationMode(), "policies", metadata.getSnapshotConfigurations().size())
            );
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }
}

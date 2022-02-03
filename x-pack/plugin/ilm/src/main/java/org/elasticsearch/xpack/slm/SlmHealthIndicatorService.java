/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.SNAPSHOT;

public class SlmHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "SLM";

    private final ClusterService clusterService;

    public SlmHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public HealthIndicatorResult calculate() {
        var slmMetadata = clusterService.state().metadata().custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
        if (slmMetadata.getSnapshotConfigurations().isEmpty()) {
            return new HealthIndicatorResult(NAME, SNAPSHOT, GREEN, "No policies configured", null);
        } else if (slmMetadata.getOperationMode() != OperationMode.RUNNING) {
            return new HealthIndicatorResult(NAME, SNAPSHOT, YELLOW, "Slm is not running", null);
        } else {
            return new HealthIndicatorResult(NAME, SNAPSHOT, GREEN, "Slm is running", null);
        }
    }
}

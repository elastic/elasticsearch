/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.watcher;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.HealthInfo;

import java.util.List;

import static org.apache.logging.log4j.LogManager.getLogger;

public class WatcherHealthIndicator implements HealthIndicatorService {

    private static final Logger logger = getLogger(WatcherHealthIndicator.class);
    private final ClusterService clusterService;

    public WatcherHealthIndicator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return "watcher_service_status";
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        logger.error("Running watcher health indicator - this is a placeholder implementation");
        return createIndicator(HealthStatus.RED, "Watcher is disabled", HealthIndicatorDetails.EMPTY, List.of(), List.of());
    }
}

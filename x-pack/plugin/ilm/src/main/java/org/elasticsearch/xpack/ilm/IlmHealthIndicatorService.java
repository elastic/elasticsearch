/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;

/**
 * This indicator reports health for index lifecycle management component.
 *
 * Indicator will report YELLOW status when ILM is not running and there are configured policies.
 * Constant indexing could eventually use entire disk space on hot topology in such cases.
 *
 * ILM must be running to fix warning reported by this indicator.
 */
public class IlmHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "ilm";

    private final ClusterService clusterService;

    public IlmHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return DATA;
    }

    @Override
    public HealthIndicatorResult calculate() {
        var ilmMetadata = clusterService.state().metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        if (ilmMetadata.getPolicyMetadatas().isEmpty()) {
            return createIndicator(GREEN, "No policies configured", createDetails(ilmMetadata), Collections.emptyList());
        } else if (ilmMetadata.getOperationMode() != OperationMode.RUNNING) {
            return createIndicator(YELLOW, "ILM is not running", createDetails(ilmMetadata), Collections.emptyList());
        } else {
            return createIndicator(GREEN, "ILM is running", createDetails(ilmMetadata), Collections.emptyList());
        }
    }

    private static HealthIndicatorDetails createDetails(IndexLifecycleMetadata metadata) {
        return new SimpleHealthIndicatorDetails(
            Map.of("ilm_status", metadata.getOperationMode(), "policies", metadata.getPolicies().size())
        );
    }
}

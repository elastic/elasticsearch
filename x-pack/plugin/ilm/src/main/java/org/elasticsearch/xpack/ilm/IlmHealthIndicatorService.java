/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;

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

    public static final String HELP_URL = "https://ela.st/fix-ilm";
    public static final Diagnosis ILM_NOT_RUNNING = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "ilm_disabled",
            "Index Lifecycle Management is stopped",
            "Start Index Lifecycle Management using [POST /_ilm/start].",
            HELP_URL
        ),
        null
    );

    public static final String AUTOMATION_DISABLED_IMPACT_ID = "automation_disabled";

    private final ClusterService clusterService;

    public IlmHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain, HealthInfo healthInfo) {
        var ilmMetadata = clusterService.state().metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        if (ilmMetadata.getPolicyMetadatas().isEmpty()) {
            return createIndicator(
                GREEN,
                "No Index Lifecycle Management policies configured",
                createDetails(explain, ilmMetadata),
                Collections.emptyList(),
                Collections.emptyList()
            );
        } else if (ilmMetadata.getOperationMode() != OperationMode.RUNNING) {
            List<HealthIndicatorImpact> impacts = Collections.singletonList(
                new HealthIndicatorImpact(
                    NAME,
                    AUTOMATION_DISABLED_IMPACT_ID,
                    3,
                    "Automatic index lifecycle and data retention management is disabled. The performance and stability of the cluster "
                        + "could be impacted.",
                    List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                )
            );
            return createIndicator(
                YELLOW,
                "Index Lifecycle Management is not running",
                createDetails(explain, ilmMetadata),
                impacts,
                List.of(ILM_NOT_RUNNING)
            );
        } else {
            return createIndicator(
                GREEN,
                "Index Lifecycle Management is running",
                createDetails(explain, ilmMetadata),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
    }

    private static HealthIndicatorDetails createDetails(boolean explain, IndexLifecycleMetadata metadata) {
        if (explain) {
            return new SimpleHealthIndicatorDetails(
                Map.of("ilm_status", metadata.getOperationMode(), "policies", metadata.getPolicies().size())
            );
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }
}

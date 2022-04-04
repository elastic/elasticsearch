/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;
import static org.elasticsearch.xpack.core.ilm.OperationMode.RUNNING;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPING;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IlmHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenRunningAndPoliciesConfigured() {
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), RUNNING));
        var service = createIlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    GREEN,
                    "ILM is running",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", RUNNING, "policies", 1)),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsYellowWhenNotRunningAndPoliciesConfigured() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), status));
        var service = createIlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    YELLOW,
                    "ILM is not running",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", status, "policies", 1)),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenNotRunningAndNoPolicies() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(Map.of(), status));
        var service = createIlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    GREEN,
                    "No policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", status, "policies", 0)),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createIlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    GREEN,
                    "No policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", RUNNING, "policies", 0)),
                    Collections.emptyList()
                )
            )
        );
    }

    private static ClusterState createClusterStateWith(IndexLifecycleMetadata metadata) {
        var builder = new ClusterState.Builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(new Metadata.Builder().putCustom(IndexLifecycleMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static Map<String, LifecyclePolicyMetadata> createIlmPolicy() {
        return Map.of(
            "test-policy",
            new LifecyclePolicyMetadata(new LifecyclePolicy("test-policy", Map.of()), Map.of(), 1L, System.currentTimeMillis())
        );
    }

    private static IlmHealthIndicatorService createIlmHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new IlmHealthIndicatorService(clusterService);
    }
}

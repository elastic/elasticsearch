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
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.SNAPSHOT;
import static org.elasticsearch.xpack.core.ilm.OperationMode.RUNNING;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPING;
import static org.elasticsearch.xpack.slm.SlmHealthIndicatorService.NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlmHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenRunningAndPoliciesConfigured() {
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(createSlmPolicy(), RUNNING, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "SLM is running",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 1)),
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
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    YELLOW,
                    "SLM is not running",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", status, "policies", 1)),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenNotRunningAndNoPolicies() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(Map.of(), status, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "No policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", status, "policies", 0)),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "No policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 0)),
                    Collections.emptyList()
                )
            )
        );
    }

    private static ClusterState createClusterStateWith(SnapshotLifecycleMetadata metadata) {
        var builder = new ClusterState.Builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(new Metadata.Builder().putCustom(SnapshotLifecycleMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static Map<String, SnapshotLifecyclePolicyMetadata> createSlmPolicy() {
        return Map.of(
            "test-policy",
            SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                .build()
        );
    }

    private static SlmHealthIndicatorService createSlmHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new SlmHealthIndicatorService(clusterService);
    }
}

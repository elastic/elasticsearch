/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.DataStreamLifecycleHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class ShardsAvailabilityHealthIndicatorServiceIT extends ESIntegTestCase {

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99951")
    public void testIsGreenDuringIndexCreate() {
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertHealthDuring(equalTo(GREEN), () -> {
            var index = randomIdentifier();
            prepareCreate(index).setSettings(indexSettings(1, 1)).get();
            ensureGreen(index);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99951")
    public void testIsGreenWhenNewReplicaAdded() {
        internalCluster().ensureAtLeastNumDataNodes(2);

        var index = randomIdentifier();
        prepareCreate(index).setSettings(indexSettings(1, 0)).get();
        ensureGreen(index);

        assertHealthDuring(equalTo(GREEN), () -> {
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), index);
            ensureGreen(index);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99951")
    public void testIsGreenDuringSnapshotRestore() {

        internalCluster().ensureAtLeastNumDataNodes(2);

        var index = randomIdentifier();
        prepareCreate(index).setSettings(indexSettings(1, 1)).get();
        ensureGreen(index);

        var repositoryName = "repository";
        var snapshotName = randomIdentifier();
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repositoryName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
            .setIndices(index)
            .setWaitForCompletion(true)
            .get();
        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareDelete(index));
        } else {
            assertAcked(indicesAdmin().prepareClose(index));
        }
        ensureGreen();

        assertHealthDuring(equalTo(GREEN), () -> {
            clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                .setIndices(index)
                .setWaitForCompletion(true)
                .get();
            ensureGreen(index);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99951")
    public void testIsGreenDuringIndexClone() {

        internalCluster().ensureAtLeastNumDataNodes(2);

        var sourceIndex = randomIdentifier();
        var targetIndex = randomIdentifier();
        prepareCreate(sourceIndex).setSettings(indexSettings(1, 1)).get();
        ensureGreen(sourceIndex);
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);

        assertHealthDuring(equalTo(GREEN), () -> {
            indicesAdmin().prepareResizeIndex(sourceIndex, targetIndex).setResizeType(ResizeType.CLONE).get();
            ensureGreen(targetIndex);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99951")
    public void testIsGreenDuringOpeningAndClosingIndex() {

        internalCluster().ensureAtLeastNumDataNodes(2);

        var index = randomIdentifier();
        prepareCreate(index).setSettings(indexSettings(1, 1)).get();
        ensureGreen(index);

        assertHealthDuring(equalTo(GREEN), () -> {
            indicesAdmin().prepareClose(index).get();
            ensureGreen(index);
            indicesAdmin().prepareClose(index).get();
            ensureGreen(index);
        });
    }

    private void assertHealthDuring(Matcher<HealthStatus> statusMatcher, Runnable action) {
        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        var allocationService = internalCluster().getCurrentMasterNodeInstance(AllocationService.class);
        var systemIndices = internalCluster().getCurrentMasterNodeInstance(SystemIndices.class);
        var projectResolver = internalCluster().getCurrentMasterNodeInstance(ProjectResolver.class);

        var service = new ShardsAvailabilityHealthIndicatorService(clusterService, allocationService, systemIndices, projectResolver);
        var states = new ArrayList<RoutingNodesAndHealth>();
        var listener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                states.add(
                    new RoutingNodesAndHealth(
                        event.state().getRoutingNodes(),
                        service.calculate(
                            false,
                            1,
                            new HealthInfo(
                                Map.of(),
                                DataStreamLifecycleHealthInfo.NO_DSL_ERRORS,
                                Map.of(),
                                FileSettingsHealthInfo.INDETERMINATE
                            )
                        )
                    )
                );
            }
        };

        clusterService.addListener(listener);
        try {
            action.run();

            for (RoutingNodesAndHealth state : states) {
                state.assertHealth(statusMatcher);
            }
        } finally {
            clusterService.removeListener(listener);
        }
    }

    private record RoutingNodesAndHealth(RoutingNodes routing, HealthIndicatorResult health) {
        private void assertHealth(Matcher<HealthStatus> statusMatcher) {
            assertThat("Health [" + health + "] for routing: " + routing, health.status(), statusMatcher);
        }
    }
}

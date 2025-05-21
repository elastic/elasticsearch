/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectStateObserverTests extends ESTestCase {

    public void testObserveProjectRemoval() {
        final var projectId = randomProjectIdOrDefault();

        final Metadata.Builder metadataBuilder = Metadata.builder().put(ProjectMetadata.builder(projectId));
        final int otherProjectCount = randomIntBetween(1, 5);
        for (int i = 0; i < otherProjectCount; i++) {
            final ProjectId otherId = randomUniqueProjectId();
            assertThat("Other projects must not be the same as the main project", otherId, not(equalTo(projectId)));
            metadataBuilder.put(ProjectMetadata.builder(otherId));
        }
        final Metadata metadata = metadataBuilder.generateClusterUuidIfNeeded().build();

        final ClusterState clusterState1 = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();
        final ProjectState initialProjectState = clusterState1.projectState(projectId);

        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            when(clusterApplierService.threadPool()).thenReturn(threadPool);
            final ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
            final ProjectStateObserver observer = new ProjectStateObserver(
                initialProjectState,
                clusterService,
                TimeValue.timeValueSeconds(randomIntBetween(1, 60)),
                logger,
                new ThreadContext(Settings.EMPTY)
            );

            final var clusterState2 = ClusterState.builder(clusterState1).incrementVersion().build();
            when(clusterApplierService.state()).thenReturn(clusterState2);

            final AtomicBoolean listenerCalled = new AtomicBoolean(false);
            observer.waitForNextChange(new ProjectStateObserver.Listener() {
                @Override
                public void onProjectStateChange(ProjectState projectState) {
                    listenerCalled.set(true);
                    assertThat(projectState.projectId(), is(projectId));
                    assertThat(projectState.cluster(), sameInstance(clusterState2));
                }

                @Override
                public void onProjectMissing(ProjectId projectId, ClusterState clusterState) {
                    Assert.fail("Project should exist");
                }

                @Override
                public void onClusterServiceClose() {
                    Assert.fail("Cluster Service should not be closed");
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    Assert.fail("Observation should not timeout");
                }
            }, TimeValue.timeValueSeconds(randomIntBetween(1, 60)));

            assertThat(listenerCalled.get(), is(true));

            listenerCalled.set(false);
            final Metadata updatedMetadata = Metadata.builder(metadata).removeProject(projectId).build();
            final var clusterState3 = ClusterState.builder(clusterState2)
                .metadata(updatedMetadata)
                .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(updatedMetadata, RoutingTable.Builder::addAsNew))
                .incrementVersion()
                .build();
            when(clusterApplierService.state()).thenReturn(clusterState3);
            observer.waitForNextChange(new ProjectStateObserver.Listener() {
                @Override
                public void onProjectStateChange(ProjectState projectState) {
                    Assert.fail("Project should not exist");
                }

                @Override
                public void onProjectMissing(ProjectId missingProjectId, ClusterState clusterState) {
                    listenerCalled.set(true);
                    assertThat(missingProjectId, is(projectId));
                    assertThat(clusterState, sameInstance(clusterState3));
                }

                @Override
                public void onClusterServiceClose() {
                    Assert.fail("Cluster Service should not be closed");
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    Assert.fail("Observation should not timeout");
                }
            }, TimeValue.timeValueSeconds(randomIntBetween(1, 60)));

            assertThat(listenerCalled.get(), is(true));
        } finally {
            threadPool.shutdownNow();
        }
    }
}

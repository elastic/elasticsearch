/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.sameInstance;

public class ReservedStateUpdateTaskTests extends ESTestCase {
    public void testBlockedClusterState() {
        ReservedStateUpdateTask<?> task = new ReservedClusterStateUpdateTask(
            "dummy",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(1L, BuildVersion.current())),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        ClusterState notRecoveredClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        assertThat(task.execute(notRecoveredClusterState), sameInstance(notRecoveredClusterState));

        task = new ReservedProjectStateUpdateTask(
            randomProjectIdOrDefault(),
            "dummy",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(1L, BuildVersion.current())),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat(task.execute(notRecoveredClusterState), sameInstance(notRecoveredClusterState));
    }

    public void testNewProjectGetsCreationBlock() {
        ProjectId projectId = randomUniqueProjectId();
        var task = new ReservedProjectStateUpdateTask(
            projectId,
            "dummy",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(1L, BuildVersion.current())),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );

        // project does not yet exist in cluster state
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterState after = task.execute(before);

        assertTrue(after.blocks().hasGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_CREATION_BLOCK));
        assertTrue(after.metadata().hasProject(projectId));
    }

    public void testExistingProjectDoesNotGetCreationBlock() {
        ProjectId projectId = randomUniqueProjectId();
        var task = new ReservedProjectStateUpdateTask(
            projectId,
            "dummy",
            new ReservedStateChunk(Map.of(), new ReservedStateVersion(1L, BuildVersion.current())),
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );

        // project already exists
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(ProjectMetadata.builder(projectId)).build();
        ClusterState after = task.execute(before);

        assertFalse(after.blocks().hasGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_CREATION_BLOCK));
    }
}

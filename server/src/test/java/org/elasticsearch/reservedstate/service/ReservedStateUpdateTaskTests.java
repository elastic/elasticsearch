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
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.sameInstance;

public class ReservedStateUpdateTaskTests extends ESTestCase {
    public void testBlockedClusterState() {
        ReservedStateUpdateTask<?> task = new ReservedClusterStateUpdateTask(
            "dummy",
            null,
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
            null,
            ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
            Map.of(),
            List.of(),
            e -> {},
            ActionListener.noop()
        );
        assertThat(task.execute(notRecoveredClusterState), sameInstance(notRecoveredClusterState));
    }
}

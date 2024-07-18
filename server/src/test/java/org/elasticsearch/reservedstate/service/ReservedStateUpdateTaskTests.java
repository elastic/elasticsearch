/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        var task = new ReservedStateUpdateTask("dummy", null, Map.of(), List.of(), e -> {}, ActionListener.noop());
        ClusterState notRecoveredClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        assertThat(task.execute(notRecoveredClusterState), sameInstance(notRecoveredClusterState));
    }
}

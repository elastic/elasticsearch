/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class HealthNodeTests extends ESTestCase {

    private final DiscoveryNode node1 = new DiscoveryNode(
        "node_1",
        buildNewFakeTransportAddress(),
        Collections.emptyMap(),
        Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
        Version.CURRENT
    );
    private final DiscoveryNode node2 = new DiscoveryNode(
        "node_2",
        buildNewFakeTransportAddress(),
        Collections.emptyMap(),
        Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
        Version.CURRENT
    );
    private final DiscoveryNode[] allNodes = new DiscoveryNode[] { node1, node2 };

    public void testFindTask() {
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        assertThat(HealthNode.findTask(state), notNullValue());
    }

    public void testFindNoTask() {
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, allNodes);
        assertThat(HealthNode.findTask(state), nullValue());
    }

    public void testFindHealthNode() {
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        assertThat(HealthNode.findHealthNode(state), equalTo(node1));
    }

    public void testFindHealthNodeNoTask() {
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, allNodes);
        assertThat(HealthNode.findHealthNode(state), nullValue());
    }

    public void testfindHealthNodeNoAssignment() {
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        tasks.addTask(HealthNode.TASK_NAME, HealthNode.TASK_NAME, HealthNodeTaskParams.INSTANCE, NO_NODE_FOUND);
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, allNodes)
            .copyAndUpdateMetadata(b -> b.putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build()));
        assertThat(HealthNode.findHealthNode(state), nullValue());
    }

    public void testFindHealthNodeMissingNode() {
        ClusterState state = ClusterStateCreationUtils.state(node1, node1);
        assertThat(HealthNode.findHealthNode(state), nullValue());
    }
}

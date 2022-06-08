/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class RemoteInfoIT extends AbstractMultiClustersTestCase {
    @Override
    protected Collection<String> remoteClusterAlias() {
        if (randomBoolean()) {
            return List.of();
        } else {
            return List.of("remote_cluster");
        }
    }

    public void testRemoteClusterClientRole() {
        final InternalTestCluster localCluster = cluster(LOCAL_CLUSTER);
        localCluster.client().execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).actionGet();

        final String nodeWithoutRemoteClientRole = localCluster.startNode(NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE)));
        final IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> localCluster.client(nodeWithoutRemoteClientRole).execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).actionGet()
        );
        assertThat(
            error.getMessage(),
            equalTo("node [" + nodeWithoutRemoteClientRole + "] does not have the [remote_cluster_client] role")
        );

        final Set<DiscoveryNodeRole> roles = new HashSet<>();
        roles.add(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
        if (randomBoolean()) {
            roles.add(DiscoveryNodeRole.DATA_ROLE);
        }
        final String nodeWithRemoteClientRole = cluster(LOCAL_CLUSTER).startNode(NodeRoles.onlyRoles(roles));
        localCluster.client(nodeWithRemoteClientRole).execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).actionGet();
    }

    public void testAllowStartingNodeWithUnreachableRemoteCluster() throws Exception {
        final InternalTestCluster localCluster = cluster(LOCAL_CLUSTER);
        final String newNode = localCluster.startNode(
            Settings.builder()
                .put(NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)))
                .putList("cluster.remote.cluster-1.seeds", List.of("unreachable_node_on_remote_cluster:" + randomIntBetween(1, 10000)))
                .build()
        );
        localCluster.stopNode(newNode);
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
        final IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () ->
            localCluster.client(nodeWithoutRemoteClientRole).execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).actionGet());
        assertThat(error.getMessage(),
            equalTo("node [" + nodeWithoutRemoteClientRole + "] does not have the [remote_cluster_client] role"));

        final Set<DiscoveryNodeRole> roles = new HashSet<>();
        roles.add(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
        if (randomBoolean()) {
            roles.add(DiscoveryNodeRole.DATA_ROLE);
        }
        final String nodeWithRemoteClientRole = cluster(LOCAL_CLUSTER).startNode(NodeRoles.onlyRoles(roles));
        localCluster.client(nodeWithRemoteClientRole).execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).actionGet();
    }
}

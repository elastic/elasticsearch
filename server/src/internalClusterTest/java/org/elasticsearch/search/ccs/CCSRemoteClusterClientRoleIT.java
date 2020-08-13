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

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class CCSRemoteClusterClientRoleIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("cluster_a");
    }

    private int indexDocs(Client client, String index) {
        assertAcked(client.admin().indices().prepareCreate(index));
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public void testRemoteClusterClientRole() throws Exception {
        final int demoDocs = indexDocs(client(LOCAL_CLUSTER), "demo");
        final int prodDocs = indexDocs(client("cluster_a"), "prod");
        final InternalTestCluster localCluster = cluster(LOCAL_CLUSTER);
        if (randomBoolean()) {
            localCluster.startDataOnlyNode();
        }
        final String nodeWithoutRemoteClusterClientRole = localCluster.startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_ROLE));
        ElasticsearchAssertions.assertFutureThrows(
            localCluster.client(nodeWithoutRemoteClusterClientRole)
                .prepareSearch("demo", "cluster_a:prod")
                .setQuery(new MatchAllQueryBuilder())
                .setAllowPartialSearchResults(false)
                .setSize(1000)
                .execute(),
            IllegalArgumentException.class,
            RestStatus.BAD_REQUEST,
            "node [" + nodeWithoutRemoteClusterClientRole + "] does not have the remote cluster client role enabled"
        );

        final String nodeWithRemoteClusterClientRole = randomFrom(
            StreamSupport.stream(localCluster.clusterService().state().nodes().spliterator(), false)
                .map(DiscoveryNode::getName)
                .filter(nodeName -> nodeWithoutRemoteClusterClientRole.equals(nodeName) == false)
                .collect(Collectors.toList()));

        final SearchResponse resp = localCluster.client(nodeWithRemoteClusterClientRole)
            .prepareSearch("demo", "cluster_a:prod")
            .setQuery(new MatchAllQueryBuilder())
            .setAllowPartialSearchResults(false)
            .setSize(1000)
            .get();
        assertHitCount(resp, demoDocs + prodDocs);
    }
}

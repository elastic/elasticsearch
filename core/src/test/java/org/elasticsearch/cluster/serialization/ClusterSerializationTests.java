/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.serialization;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESAllocationTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ClusterSerializationTests extends ESAllocationTestCase {
    public void testClusterStateSerialization() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).localNodeId("node1").masterNodeId("node2").build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("clusterName1")).nodes(nodes).metaData(metaData).routingTable(routingTable).build();

        AllocationService strategy = createAllocationService();
        clusterState = ClusterState.builder(clusterState).routingTable(strategy.reroute(clusterState, "reroute").routingTable()).build();

        ClusterState serializedClusterState = ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState), newNode("node1"));

        assertThat(serializedClusterState.getClusterName().value(), equalTo(clusterState.getClusterName().value()));

        assertThat(serializedClusterState.routingTable().prettyPrint(), equalTo(clusterState.routingTable().prettyPrint()));
    }

    public void testRoutingTableSerialization() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2")).put(newNode("node3")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();

        AllocationService strategy = createAllocationService();
        RoutingTable source = strategy.reroute(clusterState, "reroute").routingTable();

        BytesStreamOutput outStream = new BytesStreamOutput();
        source.writeTo(outStream);
        StreamInput inStream = StreamInput.wrap(outStream.bytes().toBytes());
        RoutingTable target = RoutingTable.Builder.readFrom(inStream);

        assertThat(target.prettyPrint(), equalTo(source.prettyPrint()));
    }

}

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
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClusterSerializationTests extends ESAllocationTestCase {

    public void testClusterStateSerialization() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).localNodeId("node1").masterNodeId("node2").build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("clusterName1")).nodes(nodes).metaData(metaData).routingTable(routingTable).build();

        AllocationService strategy = createAllocationService();
        clusterState = ClusterState.builder(clusterState).routingTable(strategy.reroute(clusterState, "reroute").routingTable()).build();

        ClusterState serializedClusterState = ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState), newNode("node1"),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));

        assertThat(serializedClusterState.getClusterName().value(), equalTo(clusterState.getClusterName().value()));

        assertThat(serializedClusterState.routingTable().toString(), equalTo(clusterState.routingTable().toString()));
    }

    public void testRoutingTableSerialization() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).nodes(nodes)
            .metaData(metaData).routingTable(routingTable).build();

        AllocationService strategy = createAllocationService();
        RoutingTable source = strategy.reroute(clusterState, "reroute").routingTable();

        BytesStreamOutput outStream = new BytesStreamOutput();
        source.writeTo(outStream);
        StreamInput inStream = outStream.bytes().streamInput();
        RoutingTable target = RoutingTable.readFrom(inStream);

        assertThat(target.toString(), equalTo(source.toString()));
    }

    public void testSnapshotDeletionsInProgressSerialization() throws Exception {

        boolean includeRestore = randomBoolean();

        ClusterState.Builder builder = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putCustom(SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.newInstance(
                    new SnapshotDeletionsInProgress.Entry(
                        new Snapshot("repo1", new SnapshotId("snap1", UUIDs.randomBase64UUID())),
                        randomNonNegativeLong(), randomNonNegativeLong())
                ));
        if (includeRestore) {
            builder.putCustom(RestoreInProgress.TYPE,
                new RestoreInProgress(
                    new RestoreInProgress.Entry(
                        new Snapshot("repo2", new SnapshotId("snap2", UUIDs.randomBase64UUID())),
                        RestoreInProgress.State.STARTED,
                        Collections.singletonList("index_name"),
                        ImmutableOpenMap.of()
                    )
                ));
        }

        ClusterState clusterState = builder.incrementVersion().build();

        Diff<ClusterState> diffs = clusterState.diff(ClusterState.EMPTY_STATE);

        // serialize with current version
        BytesStreamOutput outStream = new BytesStreamOutput();
        diffs.writeTo(outStream);
        StreamInput inStream = outStream.bytes().streamInput();
        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        Diff<ClusterState> serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        ClusterState stateAfterDiffs = serializedDiffs.apply(ClusterState.EMPTY_STATE);
        assertThat(stateAfterDiffs.custom(RestoreInProgress.TYPE), includeRestore ? notNullValue() : nullValue());
        assertThat(stateAfterDiffs.custom(SnapshotDeletionsInProgress.TYPE), notNullValue());

        // serialize with old version
        outStream = new BytesStreamOutput();
        outStream.setVersion(Version.CURRENT.minimumIndexCompatibilityVersion());
        diffs.writeTo(outStream);
        inStream = outStream.bytes().streamInput();
        inStream.setVersion(outStream.getVersion());
        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        stateAfterDiffs = serializedDiffs.apply(ClusterState.EMPTY_STATE);
        assertThat(stateAfterDiffs.custom(RestoreInProgress.TYPE), includeRestore ? notNullValue() : nullValue());
        assertThat(stateAfterDiffs.custom(SnapshotDeletionsInProgress.TYPE), nullValue());

        // remove the custom and try serializing again with old version
        clusterState = ClusterState.builder(clusterState).removeCustom(SnapshotDeletionsInProgress.TYPE).incrementVersion().build();
        outStream = new BytesStreamOutput();
        diffs.writeTo(outStream);
        inStream = outStream.bytes().streamInput();
        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        stateAfterDiffs = serializedDiffs.apply(stateAfterDiffs);
        assertThat(stateAfterDiffs.custom(RestoreInProgress.TYPE), includeRestore ? notNullValue() : nullValue());
        assertThat(stateAfterDiffs.custom(SnapshotDeletionsInProgress.TYPE), nullValue());
    }

    private ClusterState updateUsingSerialisedDiff(ClusterState original, Diff<ClusterState> diff) throws IOException {
        BytesStreamOutput outStream = new BytesStreamOutput();
        outStream.setVersion(Version.CURRENT);
        diff.writeTo(outStream);
        StreamInput inStream = new NamedWriteableAwareStreamInput(outStream.bytes().streamInput(),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        diff = ClusterState.readDiffFrom(inStream, newNode("node-name"));
        return diff.apply(original);
    }

    public void testObjectReuseWhenApplyingClusterStateDiff() throws Exception {
        IndexMetaData indexMetaData
            = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1).build();
        IndexTemplateMetaData indexTemplateMetaData = IndexTemplateMetaData.builder("test-template")
            .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false))).build();
        MetaData metaData = MetaData.builder().put(indexMetaData, true).put(indexTemplateMetaData).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();

        ClusterState clusterState1 = ClusterState.builder(new ClusterName("clusterName1"))
            .metaData(metaData).routingTable(routingTable).build();
        BytesStreamOutput outStream = new BytesStreamOutput();
        outStream.setVersion(Version.CURRENT);
        clusterState1.writeTo(outStream);
        StreamInput inStream = new NamedWriteableAwareStreamInput(outStream.bytes().streamInput(),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        ClusterState serializedClusterState1 = ClusterState.readFrom(inStream, newNode("node4"));

        // Create a new, albeit equal, IndexMetadata object
        ClusterState clusterState2 = ClusterState.builder(clusterState1).incrementVersion()
            .metaData(MetaData.builder().put(IndexMetaData.builder(indexMetaData).numberOfReplicas(1).build(), true)).build();
        assertNotSame("Should have created a new, equivalent, IndexMetaData object in clusterState2",
            clusterState1.metaData().index("test"), clusterState2.metaData().index("test"));

        ClusterState serializedClusterState2 = updateUsingSerialisedDiff(serializedClusterState1, clusterState2.diff(clusterState1));
        assertSame("Unchanged metadata should not create new IndexMetaData objects",
            serializedClusterState1.metaData().index("test"), serializedClusterState2.metaData().index("test"));
        assertSame("Unchanged routing table should not create new IndexRoutingTable objects",
            serializedClusterState1.routingTable().index("test"), serializedClusterState2.routingTable().index("test"));

        // Create a new and different IndexMetadata object
        ClusterState clusterState3 = ClusterState.builder(clusterState1).incrementVersion()
            .metaData(MetaData.builder().put(IndexMetaData.builder(indexMetaData).numberOfReplicas(2).build(), true)).build();
        ClusterState serializedClusterState3 = updateUsingSerialisedDiff(serializedClusterState2, clusterState3.diff(clusterState2));
        assertNotEquals("Should have a new IndexMetaData object",
            serializedClusterState2.metaData().index("test"), serializedClusterState3.metaData().index("test"));
        assertSame("Unchanged routing table should not create new IndexRoutingTable objects",
            serializedClusterState2.routingTable().index("test"), serializedClusterState3.routingTable().index("test"));

        assertSame("nodes", serializedClusterState2.nodes(), serializedClusterState3.nodes());
        assertSame("blocks", serializedClusterState2.blocks(), serializedClusterState3.blocks());
        assertSame("template", serializedClusterState2.metaData().templates().get("test-template"),
            serializedClusterState3.metaData().templates().get("test-template"));
    }
}

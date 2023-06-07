/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.serialization;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClusterSerializationTests extends ESAllocationTestCase {

    public void testClusterStateSerialization() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(newNode("node1"))
            .add(newNode("node2"))
            .add(newNode("node3"))
            .localNodeId("node1")
            .masterNodeId("node2")
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("clusterName1"))
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        AllocationService strategy = createAllocationService();
        clusterState = ClusterState.builder(clusterState)
            .routingTable(strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable())
            .build();

        ClusterState serializedClusterState = ClusterState.Builder.fromBytes(
            ClusterState.Builder.toBytes(clusterState),
            newNode("node1"),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables())
        );

        assertThat(serializedClusterState.getClusterName().value(), equalTo(clusterState.getClusterName().value()));

        assertThat(serializedClusterState.routingTable().toString(), equalTo(clusterState.routingTable().toString()));
    }

    public void testRoutingTableSerialization() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        AllocationService strategy = createAllocationService();
        RoutingTable source = strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();

        BytesStreamOutput outStream = new BytesStreamOutput();
        source.writeTo(outStream);
        StreamInput inStream = outStream.bytes().streamInput();
        RoutingTable target = RoutingTable.readFrom(inStream);

        assertThat(target.toString(), equalTo(source.toString()));
    }

    public void testSnapshotDeletionsInProgressSerialization() throws Exception {

        boolean includeRestore = randomBoolean();

        ClusterState.Builder builder = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putCustom(
                SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.of(
                    List.of(
                        new SnapshotDeletionsInProgress.Entry(
                            Collections.singletonList(new SnapshotId("snap1", UUIDs.randomBase64UUID())),
                            "repo1",
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            SnapshotDeletionsInProgress.State.STARTED
                        )
                    )
                )
            );
        if (includeRestore) {
            builder.putCustom(
                RestoreInProgress.TYPE,
                new RestoreInProgress.Builder().add(
                    new RestoreInProgress.Entry(
                        UUIDs.randomBase64UUID(),
                        new Snapshot("repo2", new SnapshotId("snap2", UUIDs.randomBase64UUID())),
                        RestoreInProgress.State.STARTED,
                        false,
                        Collections.singletonList("index_name"),
                        Map.of()
                    )
                ).build()
            );
        }

        ClusterState clusterState = builder.incrementVersion().build();

        Diff<ClusterState> diffs = clusterState.diff(ClusterState.EMPTY_STATE);

        // serialize with current version
        BytesStreamOutput outStream = new BytesStreamOutput();
        TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            TransportVersion.current()
        );
        outStream.setTransportVersion(version);
        diffs.writeTo(outStream);
        StreamInput inStream = outStream.bytes().streamInput();
        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        inStream.setTransportVersion(version);
        Diff<ClusterState> serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        ClusterState stateAfterDiffs = serializedDiffs.apply(ClusterState.EMPTY_STATE);
        assertThat(stateAfterDiffs.custom(RestoreInProgress.TYPE), includeRestore ? notNullValue() : nullValue());
        assertThat(stateAfterDiffs.custom(SnapshotDeletionsInProgress.TYPE), notNullValue());

        // remove the custom and try serializing again
        clusterState = ClusterState.builder(clusterState).removeCustom(SnapshotDeletionsInProgress.TYPE).incrementVersion().build();
        outStream = new BytesStreamOutput();
        outStream.setTransportVersion(version);
        diffs.writeTo(outStream);
        inStream = outStream.bytes().streamInput();
        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
        inStream.setTransportVersion(version);
        serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        stateAfterDiffs = serializedDiffs.apply(stateAfterDiffs);
        assertThat(stateAfterDiffs.custom(RestoreInProgress.TYPE), includeRestore ? notNullValue() : nullValue());
        assertThat(stateAfterDiffs.custom(SnapshotDeletionsInProgress.TYPE), notNullValue());
    }

    private ClusterState updateUsingSerialisedDiff(ClusterState original, Diff<ClusterState> diff) throws IOException {
        BytesStreamOutput outStream = new BytesStreamOutput();
        outStream.setTransportVersion(TransportVersion.current());
        diff.writeTo(outStream);
        StreamInput inStream = new NamedWriteableAwareStreamInput(
            outStream.bytes().streamInput(),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables())
        );
        diff = ClusterState.readDiffFrom(inStream, newNode("node-name"));
        return diff.apply(original);
    }

    public void testObjectReuseWhenApplyingClusterStateDiff() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(10)
            .numberOfReplicas(1)
            .build();
        IndexTemplateMetadata indexTemplateMetadata = IndexTemplateMetadata.builder("test-template")
            .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
            .build();
        Metadata metadata = Metadata.builder().put(indexMetadata, true).put(indexTemplateMetadata).build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState1 = ClusterState.builder(new ClusterName("clusterName1"))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        BytesStreamOutput outStream = new BytesStreamOutput();
        outStream.setTransportVersion(TransportVersion.current());
        clusterState1.writeTo(outStream);
        StreamInput inStream = new NamedWriteableAwareStreamInput(
            outStream.bytes().streamInput(),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables())
        );
        ClusterState serializedClusterState1 = ClusterState.readFrom(inStream, null);

        // Create a new, albeit equal, IndexMetadata object
        ClusterState clusterState2 = ClusterState.builder(clusterState1)
            .incrementVersion()
            .metadata(Metadata.builder().put(IndexMetadata.builder(indexMetadata).numberOfReplicas(1).build(), true))
            .build();
        assertNotSame(
            "Should have created a new, equivalent, IndexMetadata object in clusterState2",
            clusterState1.metadata().index("test"),
            clusterState2.metadata().index("test")
        );

        ClusterState serializedClusterState2 = updateUsingSerialisedDiff(serializedClusterState1, clusterState2.diff(clusterState1));
        assertSame(
            "Unchanged metadata should not create new IndexMetadata objects",
            serializedClusterState1.metadata().index("test"),
            serializedClusterState2.metadata().index("test")
        );
        assertSame(
            "Unchanged routing table should not create new IndexRoutingTable objects",
            serializedClusterState1.routingTable().index("test"),
            serializedClusterState2.routingTable().index("test")
        );

        // Create a new and different IndexMetadata object
        ClusterState clusterState3 = ClusterState.builder(clusterState1)
            .incrementVersion()
            .metadata(Metadata.builder().put(IndexMetadata.builder(indexMetadata).numberOfReplicas(2).build(), true))
            .build();
        ClusterState serializedClusterState3 = updateUsingSerialisedDiff(serializedClusterState2, clusterState3.diff(clusterState2));
        assertNotEquals(
            "Should have a new IndexMetadata object",
            serializedClusterState2.metadata().index("test"),
            serializedClusterState3.metadata().index("test")
        );
        assertSame(
            "Unchanged routing table should not create new IndexRoutingTable objects",
            serializedClusterState2.routingTable().index("test"),
            serializedClusterState3.routingTable().index("test")
        );

        assertSame("nodes", serializedClusterState2.nodes(), serializedClusterState3.nodes());
        assertSame("blocks", serializedClusterState2.blocks(), serializedClusterState3.blocks());
        assertSame(
            "template",
            serializedClusterState2.metadata().templates().get("test-template"),
            serializedClusterState3.metadata().templates().get("test-template")
        );
    }

    public static class TestCustomOne extends AbstractNamedDiffable<Custom> implements Custom {

        public static final String TYPE = "test_custom_one";
        private final String strObject;

        public TestCustomOne(String strObject) {
            this.strObject = strObject;
        }

        public TestCustomOne(StreamInput in) throws IOException {
            this.strObject = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(strObject);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
            return Iterators.concat(
                Iterators.single((builder, params) -> builder.startObject()),
                Iterators.single((builder, params) -> builder.field("custom_string_object", strObject)),
                Iterators.single((builder, params) -> builder.endObject())
            );
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Custom.class, TYPE, in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

    }

    public static class TestCustomTwo extends AbstractNamedDiffable<Custom> implements Custom {

        public static final String TYPE = "test_custom_two";
        private final Integer intObject;

        public TestCustomTwo(Integer intObject) {
            this.intObject = intObject;
        }

        public TestCustomTwo(StreamInput in) throws IOException {
            this.intObject = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(intObject);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
            return Iterators.concat(
                Iterators.single((builder, params) -> builder.startObject()),
                Iterators.single((builder, params) -> builder.field("custom_integer_object", intObject)),
                Iterators.single((builder, params) -> builder.endObject())
            );
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(Custom.class, TYPE, in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.MINIMUM_COMPATIBLE;
        }

    }

    public void testCustomSerialization() throws Exception {
        ClusterState.Builder builder = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putCustom(TestCustomOne.TYPE, new TestCustomOne("test_custom_one"))
            .putCustom(TestCustomTwo.TYPE, new TestCustomTwo(10));

        ClusterState clusterState = builder.incrementVersion().build();

        Diff<ClusterState> diffs = clusterState.diff(ClusterState.EMPTY_STATE);

        // Add the new customs to named writeables
        final List<NamedWriteableRegistry.Entry> entries = ClusterModule.getNamedWriteables();
        entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TestCustomOne.TYPE, TestCustomOne::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, TestCustomOne.TYPE, TestCustomOne::readDiffFrom));
        entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TestCustomTwo.TYPE, TestCustomTwo::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, TestCustomTwo.TYPE, TestCustomTwo::readDiffFrom));

        // serialize with current version
        BytesStreamOutput outStream = new BytesStreamOutput();
        TransportVersion version = TransportVersion.current();
        outStream.setTransportVersion(version);
        diffs.writeTo(outStream);
        StreamInput inStream = outStream.bytes().streamInput();

        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(entries));
        inStream.setTransportVersion(version);
        Diff<ClusterState> serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        ClusterState stateAfterDiffs = serializedDiffs.apply(ClusterState.EMPTY_STATE);

        // Current version - Both the customs are non null
        assertThat(stateAfterDiffs.custom(TestCustomOne.TYPE), notNullValue());
        assertThat(stateAfterDiffs.custom(TestCustomTwo.TYPE), notNullValue());

        // serialize with minimum compatibile version
        outStream = new BytesStreamOutput();
        version = TransportVersion.MINIMUM_COMPATIBLE;
        outStream.setTransportVersion(version);
        diffs.writeTo(outStream);
        inStream = outStream.bytes().streamInput();

        inStream = new NamedWriteableAwareStreamInput(inStream, new NamedWriteableRegistry(entries));
        inStream.setTransportVersion(version);
        serializedDiffs = ClusterState.readDiffFrom(inStream, clusterState.nodes().getLocalNode());
        stateAfterDiffs = serializedDiffs.apply(ClusterState.EMPTY_STATE);

        // Old version - TestCustomOne is null and TestCustomTwo is not null
        assertThat(stateAfterDiffs.custom(TestCustomOne.TYPE), nullValue());
        assertThat(stateAfterDiffs.custom(TestCustomTwo.TYPE), notNullValue());
    }

}

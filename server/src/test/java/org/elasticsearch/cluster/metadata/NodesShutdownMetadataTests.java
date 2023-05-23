/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class NodesShutdownMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.Custom> {

    public void testInsertNewNodeShutdownMetadata() {
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(new HashMap<>());
        SingleNodeShutdownMetadata newNodeMetadata = randomNodeShutdownInfo();

        nodesShutdownMetadata = nodesShutdownMetadata.putSingleNodeMetadata(newNodeMetadata);

        assertThat(nodesShutdownMetadata.getAllNodeMetadataMap().get(newNodeMetadata.getNodeId()), equalTo(newNodeMetadata));
        assertThat(nodesShutdownMetadata.getAllNodeMetadataMap().values(), contains(newNodeMetadata));
    }

    public void testRemoveShutdownMetadata() {
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(new HashMap<>());
        List<SingleNodeShutdownMetadata> nodes = randomList(1, 20, this::randomNodeShutdownInfo);

        for (SingleNodeShutdownMetadata node : nodes) {
            nodesShutdownMetadata = nodesShutdownMetadata.putSingleNodeMetadata(node);
        }

        SingleNodeShutdownMetadata nodeToRemove = randomFrom(nodes);
        nodesShutdownMetadata = nodesShutdownMetadata.removeSingleNodeMetadata(nodeToRemove.getNodeId());

        assertThat(nodesShutdownMetadata.getAllNodeMetadataMap().get(nodeToRemove.getNodeId()), nullValue());
        assertThat(nodesShutdownMetadata.getAllNodeMetadataMap().values(), hasSize(nodes.size() - 1));
        assertThat(nodesShutdownMetadata.getAllNodeMetadataMap().values(), not(hasItem(nodeToRemove)));
    }

    public void testIsNodeShuttingDown() {
        for (SingleNodeShutdownMetadata.Type type : List.of(
            SingleNodeShutdownMetadata.Type.RESTART,
            SingleNodeShutdownMetadata.Type.REMOVE,
            SingleNodeShutdownMetadata.Type.SIGTERM
        )) {
            NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(
                Collections.singletonMap(
                    "this_node",
                    SingleNodeShutdownMetadata.builder()
                        .setNodeId("this_node")
                        .setReason("shutdown for a unit test")
                        .setType(type)
                        .setStartedAtMillis(randomNonNegativeLong())
                        .setGracePeriod(
                            type == SingleNodeShutdownMetadata.Type.SIGTERM
                                ? TimeValue.parseTimeValue(randomTimeValue(), this.getTestName())
                                : null
                        )
                        .build()
                )
            );

            DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
            nodes.add(DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), "this_node"));
            nodes.localNodeId("this_node");
            nodes.masterNodeId("this_node");

            ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();

            state = ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
                .nodes(DiscoveryNodes.builder(state.nodes()).add(TestDiscoveryNode.create("_node_1")).build())
                .build();

            assertThat(NodesShutdownMetadata.isNodeShuttingDown(state, "this_node"), equalTo(true));
            assertThat(NodesShutdownMetadata.isNodeShuttingDown(state, "_node_1"), equalTo(false));
        }
    }

    public void testSigtermIsRemoveInOlderVersions() throws IOException {
        SingleNodeShutdownMetadata metadata = SingleNodeShutdownMetadata.builder()
            .setNodeId("myid")
            .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
            .setReason("myReason")
            .setStartedAtMillis(0L)
            .setGracePeriod(new TimeValue(1_000))
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.V_8_7_1);
        metadata.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.V_8_7_1);
        assertThat(new SingleNodeShutdownMetadata(in).getType(), equalTo(SingleNodeShutdownMetadata.Type.REMOVE));

        out = new BytesStreamOutput();
        metadata.writeTo(out);
        assertThat(new SingleNodeShutdownMetadata(out.bytes().streamInput()).getType(), equalTo(SingleNodeShutdownMetadata.Type.SIGTERM));
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return NodesShutdownMetadata.NodeShutdownMetadataDiff::new;
    }

    @Override
    protected NodesShutdownMetadata doParseInstance(XContentParser parser) throws IOException {
        return NodesShutdownMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return NodesShutdownMetadata::new;
    }

    @Override
    protected NodesShutdownMetadata createTestInstance() {
        Map<String, SingleNodeShutdownMetadata> nodes = randomList(0, 10, this::randomNodeShutdownInfo).stream()
            .collect(Collectors.toMap(SingleNodeShutdownMetadata::getNodeId, Function.identity()));
        return new NodesShutdownMetadata(nodes);
    }

    private SingleNodeShutdownMetadata randomNodeShutdownInfo() {
        final SingleNodeShutdownMetadata.Type type = randomFrom(SingleNodeShutdownMetadata.Type.values());
        final SingleNodeShutdownMetadata.Builder builder = SingleNodeShutdownMetadata.builder()
            .setNodeId(randomAlphaOfLength(5))
            .setType(type)
            .setReason(randomAlphaOfLength(5))
            .setStartedAtMillis(randomNonNegativeLong());
        if (type.equals(SingleNodeShutdownMetadata.Type.RESTART) && randomBoolean()) {
            builder.setAllocationDelay(TimeValue.parseTimeValue(randomTimeValue(), this.getTestName()));
        } else if (type.equals(SingleNodeShutdownMetadata.Type.REPLACE)) {
            builder.setTargetNodeName(randomAlphaOfLengthBetween(5, 10));
        } else if (type.equals(SingleNodeShutdownMetadata.Type.SIGTERM)) {
            builder.setGracePeriod(TimeValue.parseTimeValue(randomTimeValue(), this.getTestName()));
        }
        return builder.setNodeSeen(randomBoolean()).build();
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        return randomValueOtherThan(testInstance, this::createTestInstance);
    }

    @Override
    protected Metadata.Custom mutateInstance(Metadata.Custom instance) {
        return makeTestChanges(instance);
    }
}

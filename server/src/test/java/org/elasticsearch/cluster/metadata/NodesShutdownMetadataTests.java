/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
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

public class NodesShutdownMetadataTests extends SimpleDiffableSerializationTestCase<Metadata.Custom> {

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
        }
        return builder.setNodeSeen(randomBoolean()).build();
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        return randomValueOtherThan(testInstance, this::createTestInstance);
    }

    @Override
    protected Metadata.Custom mutateInstance(Metadata.Custom instance) throws IOException {
        return makeTestChanges(instance);
    }
}

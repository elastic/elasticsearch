/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NodeShutdownMetadataTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return NodeShutdownMetadata.NodeShutdownMetadataDiff::new;
    }

    @Override
    protected NodeShutdownMetadata doParseInstance(XContentParser parser) throws IOException {
        return NodeShutdownMetadata.parse(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return NodeShutdownMetadata::new;
    }

    @Override
    protected NodeShutdownMetadata createTestInstance() {
        Map<String, NodeShutdownMetadata.NodeShutdownInfo> nodes = randomList(0, 10, this::randomNodeShutdownInfo).stream()
            .collect(Collectors.toMap(NodeShutdownMetadata.NodeShutdownInfo::getNodeId, Function.identity()));
        return new NodeShutdownMetadata(nodes);
    }

    private NodeShutdownMetadata.NodeShutdownInfo randomNodeShutdownInfo() {
        return new NodeShutdownMetadata.NodeShutdownInfo(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomBoolean(),
            randomNonNegativeLong()
        );
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

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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NodesShutdownMetadataTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {

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
        return new SingleNodeShutdownMetadata(
            randomAlphaOfLength(5),
            randomBoolean() ? SingleNodeShutdownMetadata.Type.REMOVE : SingleNodeShutdownMetadata.Type.RESTART,
            randomAlphaOfLength(5),
            randomStatus(),
            randomNonNegativeLong(),
            randomComponentStatus(),
            randomComponentStatus(),
            randomComponentStatus());
    }

    private SingleNodeShutdownMetadata.Status randomStatus() {
        return randomFrom(new ArrayList<>(EnumSet.allOf(SingleNodeShutdownMetadata.Status.class)));
    }

    private NodeShutdownComponentStatus randomComponentStatus() {
        return new NodeShutdownComponentStatus(
            randomStatus(),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(4, 10)
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

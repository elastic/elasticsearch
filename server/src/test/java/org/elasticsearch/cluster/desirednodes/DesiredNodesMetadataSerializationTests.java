/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DesiredNodesMetadataSerializationTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {
    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        if (randomBoolean()) {
            return testInstance;
        }
        return mutate((DesiredNodesMetadata) testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return DesiredNodesMetadata::readDiffFrom;
    }

    @Override
    protected Metadata.Custom doParseInstance(XContentParser parser) throws IOException {
        return DesiredNodesMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return DesiredNodesMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(Metadata.Custom.class, DesiredNodesMetadata.TYPE, DesiredNodesMetadata::new)
            )
        );
    }

    @Override
    protected Metadata.Custom createTestInstance() {
        return getRandomDesiredNodesMetadata();
    }

    private DesiredNodesMetadata getRandomDesiredNodesMetadata() {
        List<DesiredNode> nodes = randomList(0, 10, DesiredNodeSerializationTests::randomDesiredNode);
        return new DesiredNodesMetadata(new DesiredNodes(randomAlphaOfLength(10), randomIntBetween(1, 10), nodes));
    }

    private DesiredNodesMetadata mutate(DesiredNodesMetadata base) {
        // new historyID
        if (randomBoolean()) {
            return getRandomDesiredNodesMetadata();
        }
        DesiredNodes currentDesiredNodes = base.getCurrentDesiredNodes();
        return new DesiredNodesMetadata(
            new DesiredNodes(currentDesiredNodes.historyID(), currentDesiredNodes.version() + 1, currentDesiredNodes.nodes())
        );
    }
}

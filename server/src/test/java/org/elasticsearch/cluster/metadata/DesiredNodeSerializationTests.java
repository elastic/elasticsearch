/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DesiredNodeSerializationTests extends AbstractSerializingTestCase<DesiredNode> {
    @Override
    protected DesiredNode doParseInstance(XContentParser parser) throws IOException {
        return DesiredNode.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DesiredNode> instanceReader() {
        return DesiredNode::readFrom;
    }

    @Override
    protected DesiredNode createTestInstance() {
        return DesiredNodesTestCase.randomDesiredNode();
    }

    @Override
    protected DesiredNode mutateInstance(DesiredNode instance) throws IOException {
        return mutateDesiredNode(instance);
    }

    public static DesiredNode mutateDesiredNode(DesiredNode instance) {
        final var mutationBranch = randomInt(5);
        return switch (mutationBranch) {
            case 0 -> new DesiredNode(
                Settings.builder().put(instance.settings()).put(randomAlphaOfLength(10), randomInt()).build(),
                instance.processors(),
                instance.processorsRange(),
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 1 -> new DesiredNode(
                instance.settings(),
                randomValueOtherThan(instance.processors(), () -> Processors.of(randomDouble() + randomIntBetween(1, 128))),
                null,
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 2 -> new DesiredNode(
                instance.settings(),
                randomValueOtherThan(instance.processorsRange(), DesiredNodesTestCase::randomProcessorRange),
                instance.memory(),
                instance.storage(),
                instance.version()
            );
            case 3 -> new DesiredNode(
                instance.settings(),
                instance.processors(),
                instance.processorsRange(),
                ByteSizeValue.ofGb(randomValueOtherThan(instance.memory().getGb(), () -> (long) randomIntBetween(1, 128))),
                instance.storage(),
                instance.version()
            );
            case 4 -> new DesiredNode(
                instance.settings(),
                instance.processors(),
                instance.processorsRange(),
                instance.memory(),
                ByteSizeValue.ofGb(randomValueOtherThan(instance.storage().getGb(), () -> (long) randomIntBetween(1, 128))),
                instance.version()
            );
            case 5 -> new DesiredNode(
                instance.settings(),
                instance.processors(),
                instance.processorsRange(),
                instance.memory(),
                instance.storage(),
                instance.version().previousMajor()
            );
            default -> throw new IllegalStateException("Unexpected value: " + mutationBranch);
        };
    }
}

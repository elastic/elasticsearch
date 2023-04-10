/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.NodeResult;

public class NodesRemovalPrevalidationSerializationTests extends AbstractXContentSerializingTestCase<NodesRemovalPrevalidation> {

    @Override
    protected NodesRemovalPrevalidation doParseInstance(XContentParser parser) throws IOException {
        return NodesRemovalPrevalidation.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<NodesRemovalPrevalidation> instanceReader() {
        return NodesRemovalPrevalidation::readFrom;
    }

    @Override
    protected NodesRemovalPrevalidation createTestInstance() {
        return randomNodesRemovalPrevalidation();
    }

    @Override
    protected NodesRemovalPrevalidation mutateInstance(NodesRemovalPrevalidation instance) {
        return randomBoolean() ? mutateOverallResult(instance) : mutateNodes(instance);
    }

    public static NodesRemovalPrevalidation mutateOverallResult(NodesRemovalPrevalidation instance) {
        return new NodesRemovalPrevalidation(instance.isSafe() ? false : true, randomAlphaOfLengthBetween(0, 1000), instance.nodes());
    }

    public static NodesRemovalPrevalidation mutateNodes(final NodesRemovalPrevalidation instance) {
        int i = randomInt(instance.nodes().size() - 1);
        NodeResult nodeResult = instance.nodes().get(i);
        NodeResult mutatedNode = mutateNodeResult(nodeResult);
        List<NodeResult> mutatedNodes = new ArrayList<>(instance.nodes());
        mutatedNodes.set(i, mutatedNode);
        return new NodesRemovalPrevalidation(instance.isSafe(), instance.message(), mutatedNodes);
    }

    public static NodeResult mutateNodeResult(final NodeResult instance) {
        int mutationBranch = randomInt(3);
        return switch (mutationBranch) {
            case 0 -> new NodeResult(
                randomValueOtherThan(instance.name(), () -> randomAlphaOfLength(10)),
                instance.Id(),
                instance.externalId(),
                instance.result()
            );
            case 1 -> new NodeResult(
                instance.name(),
                randomValueOtherThan(instance.Id(), () -> randomAlphaOfLength(10)),
                instance.externalId(),
                instance.result()
            );
            case 2 -> new NodeResult(
                instance.name(),
                instance.Id(),
                randomValueOtherThan(instance.externalId(), () -> randomAlphaOfLength(10)),
                instance.result()
            );
            case 3 -> new NodeResult(
                instance.name(),
                instance.Id(),
                instance.externalId(),
                randomValueOtherThan(instance.result(), NodesRemovalPrevalidationSerializationTests::randomResult)
            );
            default -> throw new IllegalStateException("Unexpected mutation branch value: " + mutationBranch);
        };
    }

    public static NodesRemovalPrevalidation randomNodesRemovalPrevalidation() {
        int noOfNodes = randomIntBetween(1, 10);
        List<NodesRemovalPrevalidation.NodeResult> nodes = new ArrayList<>(noOfNodes);
        for (int i = 0; i < noOfNodes; i++) {
            nodes.add(
                new NodesRemovalPrevalidation.NodeResult(
                    randomAlphaOfLength(10),
                    randomAlphaOfLength(10),
                    randomAlphaOfLength(10),
                    randomResult()
                )
            );
        }
        return new NodesRemovalPrevalidation(randomBoolean(), randomAlphaOfLengthBetween(0, 1000), nodes);
    }

    private static NodesRemovalPrevalidation.Result randomResult() {
        return new NodesRemovalPrevalidation.Result(
            randomBoolean(),
            randomFrom(NodesRemovalPrevalidation.Reason.values()),
            randomAlphaOfLengthBetween(0, 1000)
        );
    }
}

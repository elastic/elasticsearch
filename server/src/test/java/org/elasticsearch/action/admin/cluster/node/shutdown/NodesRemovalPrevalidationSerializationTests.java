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

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.NodeResult;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;

public class NodesRemovalPrevalidationSerializationTests extends AbstractXContentSerializingTestCase<NodesRemovalPrevalidation> {

    @Override
    protected NodesRemovalPrevalidation doParseInstance(XContentParser parser) throws IOException {
        return NodesRemovalPrevalidation.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<NodesRemovalPrevalidation> instanceReader() {
        return NodesRemovalPrevalidation::new;
    }

    @Override
    protected NodesRemovalPrevalidation createTestInstance() {
        return randomNodesRemovalPrevalidation();
    }

    @Override
    protected NodesRemovalPrevalidation mutateInstance(NodesRemovalPrevalidation instance) throws IOException {
        return randomBoolean() ? mutateOverallResult(instance) : mutateNodes(instance);
    }

    public static NodesRemovalPrevalidation mutateOverallResult(NodesRemovalPrevalidation instance) {
        return new NodesRemovalPrevalidation(mutateResult(instance.getResult()), instance.getNodes());
    }

    public static NodesRemovalPrevalidation mutateNodes(final NodesRemovalPrevalidation instance) {
        int i = randomInt(instance.getNodes().size() - 1);
        NodeResult nodeResult = instance.getNodes().get(i);
        NodeResult mutatedNode = mutateNodeResult(nodeResult);
        List<NodeResult> mutatedNodes = new ArrayList<>(instance.getNodes());
        mutatedNodes.set(i, mutatedNode);
        return new NodesRemovalPrevalidation(instance.getResult(), mutatedNodes);
    }

    public static Result mutateResult(Result instance) {
        return randomBoolean()
            ? new Result(randomValueOtherThan(instance.isSafe(), () -> randomFrom(IsSafe.values())), instance.reason())
            : new Result(instance.isSafe(), randomValueOtherThan(instance.reason(), () -> randomAlphaOfLength(10)));
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
            case 3 -> new NodeResult(instance.name(), instance.Id(), instance.externalId(), mutateResult(instance.result()));
            default -> throw new IllegalStateException("Unexpected mutation branch value: " + mutationBranch);
        };
    }

    public static NodesRemovalPrevalidation randomNodesRemovalPrevalidation() {
        int noOfNodes = randomIntBetween(1, 10);
        List<NodesRemovalPrevalidation.NodeResult> nodes = new ArrayList<>(noOfNodes);
        NodesRemovalPrevalidation.Result result = randomResult();
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
        return new NodesRemovalPrevalidation(result, nodes);
    }

    private static NodesRemovalPrevalidation.Result randomResult() {
        NodesRemovalPrevalidation.IsSafe isSafe = randomFrom(NodesRemovalPrevalidation.IsSafe.values());
        String reason = randomReason(isSafe);
        return new NodesRemovalPrevalidation.Result(isSafe, reason);
    }

    private static String randomReason(NodesRemovalPrevalidation.IsSafe isSafe) {
        return isSafe == NodesRemovalPrevalidation.IsSafe.YES ? "" : randomAlphaOfLengthBetween(0, 1000);
    }
}

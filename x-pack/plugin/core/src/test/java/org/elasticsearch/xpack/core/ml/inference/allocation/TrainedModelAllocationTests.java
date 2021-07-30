/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

public class TrainedModelAllocationTests extends AbstractSerializingTestCase<TrainedModelAllocation> {

    public static TrainedModelAllocation randomInstance() {
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(
            new StartTrainedModelDeploymentAction.TaskParams(randomAlphaOfLength(10), randomAlphaOfLength(10), randomNonNegativeLong())
        );
        List<String> nodes = Stream.generate(() -> randomAlphaOfLength(10)).limit(randomInt(5)).collect(Collectors.toList());
        for (String node : nodes) {
            if (randomBoolean()) {
                builder.addNewFailedRoutingEntry(node, randomAlphaOfLength(10));
            } else {
                builder.addNewRoutingEntry(node);
            }
        }
        return builder.build();
    }

    @Override
    protected TrainedModelAllocation doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelAllocation.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TrainedModelAllocation> instanceReader() {
        return TrainedModelAllocation::new;
    }

    @Override
    protected TrainedModelAllocation createTestInstance() {
        return randomInstance();
    }

    public void testBuilderChanged() {
        TrainedModelAllocation original = randomInstance();
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.fromAllocation(original);
        assertThat(builder.isChanged(), is(false));
        String addingNode = "foo";

        assertUnchanged(builder, b -> b.removeRoutingEntry(addingNode));

        if (randomBoolean()) {
            builder.addNewRoutingEntry(addingNode);
        } else {
            builder.addNewFailedRoutingEntry(addingNode, "test failed");
        }
        assertThat(builder.isChanged(), is(true));

        TrainedModelAllocation.Builder builderWithNode = TrainedModelAllocation.Builder.fromAllocation(builder.build());
        assertThat(builderWithNode.isChanged(), is(false));

        builderWithNode.removeRoutingEntry(addingNode);
        assertThat(builderWithNode.isChanged(), is(true));
    }

    public void testBuilderAddingExistingRoute() {
        TrainedModelAllocation original = randomInstance();
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.fromAllocation(original);
        String addingNode = "new-node";
        if (randomBoolean()) {
            builder.addNewRoutingEntry(addingNode);
        } else {
            builder.addNewFailedRoutingEntry(addingNode, "test failed");
        }
        expectThrows(ResourceAlreadyExistsException.class, () -> builder.addNewFailedRoutingEntry("new-node", "anything"));
        expectThrows(ResourceAlreadyExistsException.class, () -> builder.addNewRoutingEntry("new-node"));
    }

    public void testBuilderUpdatingMissingRoute() {
        TrainedModelAllocation original = randomInstance();
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.fromAllocation(original);
        String addingNode = "new-node";
        expectThrows(
            ResourceNotFoundException.class,
            () -> builder.updateExistingRoutingEntry(addingNode, RoutingStateAndReasonTests.randomInstance())
        );
    }

    private static void assertUnchanged(
        TrainedModelAllocation.Builder builder,
        Function<TrainedModelAllocation.Builder, TrainedModelAllocation.Builder> function
    ) {
        function.apply(builder);
        assertThat(builder.isChanged(), is(false));
    }

}

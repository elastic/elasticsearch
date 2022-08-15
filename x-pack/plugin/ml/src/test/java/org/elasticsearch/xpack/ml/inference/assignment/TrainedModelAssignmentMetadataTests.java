/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

public class TrainedModelAssignmentMetadataTests extends AbstractSerializingTestCase<TrainedModelAssignmentMetadata> {

    public static TrainedModelAssignmentMetadata randomInstance() {
        LinkedHashMap<String, TrainedModelAssignment> map = Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomInt(5))
            .collect(
                Collectors.toMap(Function.identity(), (k) -> TrainedModelAssignmentTests.randomInstance(), (k, k1) -> k, LinkedHashMap::new)
            );
        return new TrainedModelAssignmentMetadata(map);
    }

    @Override
    protected TrainedModelAssignmentMetadata doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelAssignmentMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TrainedModelAssignmentMetadata> instanceReader() {
        return TrainedModelAssignmentMetadata::fromStream;
    }

    @Override
    protected TrainedModelAssignmentMetadata createTestInstance() {
        return new TrainedModelAssignmentMetadata(new HashMap<>());
    }

    public void testIsAllocated() {
        String allocatedModelId = "test_model_id";
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(allocatedModelId, TrainedModelAssignment.Builder.empty(randomParams(allocatedModelId)))
            .build();
        assertThat(metadata.isAssigned(allocatedModelId), is(true));
        assertThat(metadata.isAssigned("unknown_model_id"), is(false));
    }

    private static StartTrainedModelDeploymentAction.TaskParams randomParams(String modelId) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            randomNonNegativeLong(),
            randomIntBetween(1, 8),
            randomIntBetween(1, 8),
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomNonNegativeLong())
        );
    }

}

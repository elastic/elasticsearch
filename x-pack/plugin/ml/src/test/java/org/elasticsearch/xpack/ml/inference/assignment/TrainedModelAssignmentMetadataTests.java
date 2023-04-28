/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TrainedModelAssignmentMetadataTests extends AbstractChunkedSerializingTestCase<TrainedModelAssignmentMetadata> {

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

    @Override
    protected TrainedModelAssignmentMetadata mutateInstance(TrainedModelAssignmentMetadata instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testIsAssigned() {
        String allocatedModelId = "test_model_id";
        String allocatedDeploymentId = "test_deployment";
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                allocatedDeploymentId,
                TrainedModelAssignment.Builder.empty(randomParams(allocatedDeploymentId, allocatedModelId))
            )
            .build();
        assertThat(metadata.isAssigned(allocatedDeploymentId), is(true));
        assertThat(metadata.isAssigned(allocatedModelId), is(false));
        assertThat(metadata.isAssigned("unknown_model_id"), is(false));
    }

    public void testModelIsDeployed() {
        String allocatedModelId = "test_model_id";
        String allocatedDeploymentId = "test_deployment";
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                allocatedDeploymentId,
                TrainedModelAssignment.Builder.empty(randomParams(allocatedDeploymentId, allocatedModelId))
            )
            .build();
        assertThat(metadata.modelIsDeployed(allocatedDeploymentId), is(false));
        assertThat(metadata.modelIsDeployed(allocatedModelId), is(true));
        assertThat(metadata.modelIsDeployed("unknown_model_id"), is(false));
    }

    public void testGetDeploymentsUsingModel() {
        String modelId1 = "test_model_id_1";
        String deployment1 = "test_deployment_1";
        String deployment2 = "test_deployment_2";
        String deployment3 = "test_deployment_3";
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(deployment1, TrainedModelAssignment.Builder.empty(randomParams(deployment1, modelId1)))
            .addNewAssignment(deployment2, TrainedModelAssignment.Builder.empty(randomParams(deployment2, modelId1)))
            .addNewAssignment(deployment3, TrainedModelAssignment.Builder.empty(randomParams(deployment3, "different_model")))
            .build();
        var assignments = metadata.getDeploymentsUsingModel(modelId1);
        assertThat(assignments, hasSize(2));
        assertEquals(assignments.get(0).getModelId(), modelId1);
        assertEquals(assignments.get(1).getModelId(), modelId1);

        assignments = metadata.getDeploymentsUsingModel("not-deployed");
        assertThat(assignments, hasSize(0));
    }

    private static StartTrainedModelDeploymentAction.TaskParams randomParams(String deploymentId, String modelId) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            deploymentId,
            randomNonNegativeLong(),
            randomIntBetween(1, 8),
            randomIntBetween(1, 8),
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomNonNegativeLong()),
            randomFrom(Priority.values())
        );
    }
}

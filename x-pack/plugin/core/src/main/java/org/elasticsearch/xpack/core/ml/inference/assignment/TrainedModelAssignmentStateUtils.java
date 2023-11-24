/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;

import java.util.List;
import java.util.Optional;

public class TrainedModelAssignmentStateUtils {

    public static List<TrainedModelAssignment> modelAssignments(String modelId, ClusterState state) {
        String concreteModelId = Optional.ofNullable(ModelAliasMetadata.fromState(state).getModelId(modelId))
            .orElse(modelId);

        List<TrainedModelAssignment> assignments;

        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(state);
        TrainedModelAssignment assignment = trainedModelAssignmentMetadata.getDeploymentAssignment(concreteModelId);
        if (assignment != null) {
            assignments = List.of(assignment);
        } else {
            // look up by model
            assignments = trainedModelAssignmentMetadata.getDeploymentsUsingModel(concreteModelId);
        }

        return assignments;
    }

    private TrainedModelAssignmentStateUtils() {

    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElserInternalModelTests extends ESTestCase {
    public void testUpdateNumAllocation() {
        var model = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElasticsearchInternalService.NAME,
            new ElserInternalServiceSettings(new ElasticsearchInternalServiceSettings(null, 1, "elser", null, null)),
            new ElserMlNodeTaskSettings(),
            null
        );

        AssignmentStats assignmentStats = mock();
        when(assignmentStats.getNumberOfAllocations()).thenReturn(1);
        model.updateServiceSettings(assignmentStats);

        assertThat(model.getServiceSettings().getNumAllocations(), equalTo(1));
        assertNull(model.getServiceSettings().getAdaptiveAllocationsSettings());

        TrainedModelAssignment trainedModelAssignment = TrainedModelAssignmentTests.randomInstance();
        CreateTrainedModelAssignmentAction.Response response = mock();
        when(response.getTrainedModelAssignment()).thenReturn(trainedModelAssignment);
        model.getCreateTrainedModelAssignmentActionListener(model, ActionListener.noop()).onResponse(response);

        assertThat(model.getServiceSettings().getNumAllocations(), equalTo(1));
        assertThat(
            model.getServiceSettings().getAdaptiveAllocationsSettings(),
            equalTo(trainedModelAssignment.getAdaptiveAllocationsSettings())
        );
    }
}

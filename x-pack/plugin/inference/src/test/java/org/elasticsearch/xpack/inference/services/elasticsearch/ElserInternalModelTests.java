/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

public class ElserInternalModelTests extends ESTestCase {
    public void testUpdateNumAllocation() {
        var model = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElasticsearchInternalService.NAME,
            new ElserInternalServiceSettings(null, 1, "elser", null),
            new ElserMlNodeTaskSettings(),
            null
        );

        model.updateNumAllocation(1);
        assertEquals(1, model.internalServiceSettings.getNumAllocations().intValue());

        model.updateNumAllocation(null);
        assertNull(model.internalServiceSettings.getNumAllocations());
    }
}

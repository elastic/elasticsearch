/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Regression coverage for mixed-cluster creation of {@code .tasks}.
 * Nodes that bumped {@code TASK_RESULTS_INDEX_MAPPINGS_VERSION} to 1 must still
 * advertise a prior descriptor for version 0 so 8.19 nodes can create the index.
 */
public class TaskResultsServiceTests extends ESTestCase {
    public void testTasksDescriptorCompatibleWithMappingsVersion0() {
        assertThat(TaskResultsService.TASKS_DESCRIPTOR.getMappingsVersion().version(), equalTo(1));

        SystemIndexDescriptor.MappingsVersion version0 = new SystemIndexDescriptor.MappingsVersion(0, randomInt());
        SystemIndexDescriptor compatible = TaskResultsService.TASKS_DESCRIPTOR.getDescriptorCompatibleWith(version0);
        assertThat(
            "A 9.5 node must accept .tasks mappings version 0 so mixed clusters with 8.19 nodes can create the index",
            compatible,
            notNullValue()
        );
        assertThat(compatible.getMappingsVersion().version(), equalTo(0));
    }
}

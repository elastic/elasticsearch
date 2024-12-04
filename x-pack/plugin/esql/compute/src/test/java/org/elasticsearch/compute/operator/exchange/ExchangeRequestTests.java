/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ExchangeRequestTests extends ESTestCase {

    public void testParentTask() {
        ExchangeRequest r1 = new ExchangeRequest("1", true);
        r1.setParentTask(new TaskId("node-1", 1));
        assertSame(TaskId.EMPTY_TASK_ID, r1.getParentTask());

        ExchangeRequest r2 = new ExchangeRequest("1", false);
        r2.setParentTask(new TaskId("node-2", 2));
        assertTrue(r2.getParentTask().isSet());
        assertThat(r2.getParentTask(), equalTo((new TaskId("node-2", 2))));
    }
}

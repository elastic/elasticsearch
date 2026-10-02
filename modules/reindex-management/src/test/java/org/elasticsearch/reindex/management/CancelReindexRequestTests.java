/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CancelReindexRequestTests extends ESTestCase {

    public void testValidationFailsWithoutTaskId() {
        final CancelReindexRequest request = new CancelReindexRequest(TaskId.EMPTY_TASK_ID, randomBoolean());
        final ActionRequestValidationException validation = request.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.getMessage(), containsString("task id must be provided"));
    }

    public void testValidationPassesWithTaskId() {
        final CancelReindexRequest request = new CancelReindexRequest(new TaskId("node", randomNonNegativeLong()), randomBoolean());
        assertThat(request.validate(), is(nullValue()));
    }
}

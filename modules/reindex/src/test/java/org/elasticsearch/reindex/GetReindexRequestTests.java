/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetReindexRequestTests extends ESTestCase {

    public void testValidation() {
        GetReindexRequest request = new GetReindexRequest();
        ActionRequestValidationException validation = request.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.validationErrors().get(0), containsString("id is required"));

        request.setTaskId(new TaskId("node1", 123));
        validation = request.validate();
        assertThat(validation, nullValue());
    }

    public void testSerialization() throws IOException {
        GetReindexRequest original = new GetReindexRequest();
        original.setTaskId(new TaskId("node1", 456));
        original.setWaitForCompletion(randomBoolean());
        if (randomBoolean()) {
            original.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, 100)));
        }

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        GetReindexRequest deserialized = new GetReindexRequest(in);

        assertEquals(original.getTaskId(), deserialized.getTaskId());
        assertEquals(original.getWaitForCompletion(), deserialized.getWaitForCompletion());
        assertEquals(original.getTimeout(), deserialized.getTimeout());
    }

    public void testSetters() {
        GetReindexRequest request = new GetReindexRequest();
        TaskId taskId = new TaskId("node1", 789);
        boolean waitForCompletion = randomBoolean();
        TimeValue timeout = TimeValue.timeValueSeconds(30);

        request.setTaskId(taskId);
        request.setWaitForCompletion(waitForCompletion);
        request.setTimeout(timeout);

        assertEquals(taskId, request.getTaskId());
        assertEquals(waitForCompletion, request.getWaitForCompletion());
        assertEquals(timeout, request.getTimeout());
    }
}

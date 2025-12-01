/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetReindexResponseTests extends ESTestCase {

    public void testFromTaskResult() {
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            null,
            100,
            50,
            10,
            0,
            5,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            1.0f,
            null,
            TimeValue.ZERO
        );

        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/reindex",
            "test description",
            "test",
            status,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        TaskResult taskResult = new TaskResult(false, taskInfo);
        GetReindexResponse response = GetReindexResponse.fromTaskResult(taskResult);

        TaskResult task = response.getTask();
        assertThat(task.isCompleted(), equalTo(false));
        assertThat(task.getError(), nullValue());
        assertThat(task.getResponse(), nullValue());
    }

    public void testFromTaskResultCompleted() throws IOException {
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            null,
            100,
            100,
            0,
            0,
            10,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            1.0f,
            null,
            TimeValue.ZERO
        );

        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/reindex",
            "test description",
            "test",
            status,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        BytesReference responseBytes = new BytesArray("{\"took\":100}");
        TaskResult taskResult = new TaskResult(taskInfo, responseBytes);
        GetReindexResponse response = GetReindexResponse.fromTaskResult(taskResult);

        TaskResult task = response.getTask();
        assertThat(task.isCompleted(), equalTo(true));
        assertThat(task.getResponse(), notNullValue());
        assertThat(task.getError(), nullValue());
    }

    public void testSerialization() throws IOException {
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/reindex",
            "test description",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        BytesReference error = randomBoolean() ? new BytesArray("{\"error\":\"test\"}") : null;
        BytesReference response = randomBoolean() ? new BytesArray("{\"result\":\"ok\"}") : null;
        boolean completed = randomBoolean();

        TaskResult taskResult = new TaskResult(completed, taskInfo, error, response);
        GetReindexResponse original = new GetReindexResponse(taskResult);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        GetReindexResponse deserialized = new GetReindexResponse(in);

        TaskResult originalTask = original.getTask();
        TaskResult deserializedTask = deserialized.getTask();
        assertEquals(originalTask.isCompleted(), deserializedTask.isCompleted());
        assertEquals(originalTask.getError(), deserializedTask.getError());
        assertEquals(originalTask.getResponse(), deserializedTask.getResponse());
    }

    public void testFromTaskResultWithNullStatus() {
        // TaskResult with null status should still work
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/reindex",
            "test description",
            "test",
            null, // null status
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        TaskResult taskResult = new TaskResult(false, taskInfo);
        GetReindexResponse response = GetReindexResponse.fromTaskResult(taskResult);

        TaskResult task = response.getTask();
        assertThat(task.isCompleted(), equalTo(false));
        assertThat(task.getError(), nullValue());
        assertThat(task.getResponse(), nullValue());
    }

    public void testFromTaskResultWithError() throws IOException {
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/reindex",
            "test description",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        Exception error = new RuntimeException("test error");
        TaskResult taskResult = new TaskResult(taskInfo, error);
        GetReindexResponse response = GetReindexResponse.fromTaskResult(taskResult);

        TaskResult task = response.getTask();
        assertThat(task.isCompleted(), equalTo(true));
        assertThat(task.getError(), notNullValue());
        assertThat(task.getResponse(), nullValue());
    }

    public void testGetTask() {
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("node1", 123),
            "test",
            "indices:data/write/reindex",
            "test description",
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        TaskResult taskResult = new TaskResult(false, taskInfo);
        GetReindexResponse response = new GetReindexResponse(taskResult);

        assertThat(response.getTask(), equalTo(taskResult));
    }

    public void testSerializationWithNullTask() throws IOException {
        // Test that we can handle null task in deserialization (though constructor requires non-null)
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeOptionalWriteable(null);
        StreamInput in = out.bytes().streamInput();
        
        // This should handle null gracefully
        GetReindexResponse deserialized = new GetReindexResponse(in);
        assertThat(deserialized.getTask(), nullValue());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class TaskTests extends ESTestCase {

    public void testTaskInfoToString() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomIntBetween(0, 100000);
        long startTime = randomNonNegativeLong();
        long runningTime = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable && randomBoolean();
        TaskInfo taskInfo = new TaskInfo(
                new TaskId(nodeId, taskId),
                "test_type",
                "test_action",
                "test_description",
                null,
                startTime,
                runningTime,
                cancellable,
                cancelled,
                TaskId.EMPTY_TASK_ID,
                Collections.singletonMap("foo", "bar"));
        String taskInfoString = taskInfo.toString();
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(taskInfoString.getBytes(StandardCharsets.UTF_8)), true).v2();
        assertEquals(((Number)map.get("id")).longValue(), taskId);
        assertEquals(map.get("type"), "test_type");
        assertEquals(map.get("action"), "test_action");
        assertEquals(map.get("description"), "test_description");
        assertEquals(((Number)map.get("start_time_in_millis")).longValue(), startTime);
        assertEquals(((Number)map.get("running_time_in_nanos")).longValue(), runningTime);
        assertEquals(map.get("cancellable"), cancellable);
        if (cancellable) {
            assertEquals(map.get("cancelled"), cancelled);
        } else {
            assertFalse(map.containsKey("cancelled"));
        }
        assertEquals(map.get("headers"), Collections.singletonMap("foo", "bar"));
    }

}

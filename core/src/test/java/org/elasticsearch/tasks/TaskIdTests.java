/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class TaskIdTests extends ESTestCase {
    private static final int ROUNDS = 30;

    public void testSerialization() throws IOException {
        /*
         * The size of the serialized representation of the TaskId doesn't really matter that much because most requests don't contain a
         * full TaskId.
         */
        int expectedSize = 31; // 8 for the task number, 1 for the string length of the uuid, 22 for the actual uuid
        for (int i = 0; i < ROUNDS; i++) {
            TaskId taskId = new TaskId(UUIDs.randomBase64UUID(random()), randomInt());
            TaskId roundTripped = roundTrip(taskId, expectedSize);
            assertNotSame(taskId, roundTripped);
            assertEquals(taskId, roundTripped);
            assertEquals(taskId.hashCode(), roundTripped.hashCode());
        }
    }

    public void testSerializationOfEmpty() throws IOException {
        //The size of the serialized representation of the EMPTY_TASK_ID matters a lot because many requests contain it.
        int expectedSize = 1;
        TaskId roundTripped = roundTrip(TaskId.EMPTY_TASK_ID, expectedSize);
        assertSame(TaskId.EMPTY_TASK_ID, roundTripped);
    }

    private TaskId roundTrip(TaskId taskId, int expectedSize) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            taskId.writeTo(out);
            BytesReference bytes = out.bytes();
            assertEquals(expectedSize, bytes.length());
            try (StreamInput in = StreamInput.wrap(bytes)) {
                return TaskId.readFromStream(in);
            }
        }
    }
}

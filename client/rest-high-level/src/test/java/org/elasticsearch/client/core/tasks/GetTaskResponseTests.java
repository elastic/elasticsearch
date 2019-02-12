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

package org.elasticsearch.client.core.tasks;

import org.elasticsearch.client.Requests;
import org.elasticsearch.client.tasks.GetTaskResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class GetTaskResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            GetTaskResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(true)
            .randomFieldsExcludeFilter(field ->field.endsWith("headers") || field.endsWith("status"))
            .test();
    }

    private GetTaskResponse createTestInstance() {
        return new GetTaskResponse(randomBoolean(), randomTaskInfo());
    }

    private void toXContent(GetTaskResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field(GetTaskResponse.COMPLETED.getPreferredName(), response.isCompleted());
            builder.startObject(GetTaskResponse.TASK.getPreferredName());
            response.getTaskInfo().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
        builder.endObject();
    }

    private void assertEqualInstances(GetTaskResponse expectedInstance, GetTaskResponse newInstance) {
        assertEquals(expectedInstance.isCompleted(), newInstance.isCompleted());
        assertEquals(expectedInstance.getTaskInfo(), newInstance.getTaskInfo());
    }
    
    static TaskInfo randomTaskInfo() {
        TaskId taskId = randomTaskId();
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        Task.Status status = randomBoolean() ? randomRawTaskStatus() : null;
        String description = randomBoolean() ? randomAlphaOfLength(5) : null;
        long startTime = randomLong();
        long runningTimeNanos = randomLong();
        boolean cancellable = randomBoolean();
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId();
        Map<String, String> headers = randomBoolean() ?
                Collections.emptyMap() :
                Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(taskId, type, action, description, status, startTime, runningTimeNanos, cancellable, parentTaskId, headers);
    }

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomLong());
    }

    private static RawTaskStatus randomRawTaskStatus() {
        try (XContentBuilder builder = XContentBuilder.builder(Requests.INDEX_CONTENT_TYPE.xContent())) {
            builder.startObject();
            int fields = between(0, 10);
            for (int f = 0; f < fields; f++) {
                builder.field(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            builder.endObject();
            return new RawTaskStatus(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }    
}

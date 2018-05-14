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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ListTasksResponseTests extends AbstractXContentTestCase<ListTasksResponse> {

    public void testEmptyToString() {
        assertEquals("{\"tasks\":[]}", new ListTasksResponse().toString());
    }

    public void testNonEmptyToString() {
        TaskInfo info = new TaskInfo(
            new TaskId("node1", 1), "dummy-type", "dummy-action", "dummy-description", null, 0, 1, true, new TaskId("node1", 0),
            Collections.singletonMap("foo", "bar"));
        ListTasksResponse tasksResponse = new ListTasksResponse(singletonList(info), emptyList(), emptyList());
        assertEquals("{\"tasks\":[{\"node\":\"node1\",\"id\":1,\"type\":\"dummy-type\",\"action\":\"dummy-action\","
                + "\"description\":\"dummy-description\",\"start_time_in_millis\":0,\"running_time_in_nanos\":1,\"cancellable\":true,"
                + "\"parent_task_id\":\"node1:0\",\"headers\":{\"foo\":\"bar\"}}]}", tasksResponse.toString());
    }

    @Override
    protected ListTasksResponse createTestInstance() {
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            tasks.add(TaskInfoTests.randomTaskInfo());
        }
        List<TaskOperationFailure> taskFailures = new ArrayList<>();
        for (int i = 0; i < randomInt(5); i++) {
            taskFailures.add(new TaskOperationFailure(
                    randomAlphaOfLength(5), randomNonNegativeLong(), new IllegalStateException("message")));
        }
        return new ListTasksResponse(tasks, taskFailures, Collections.singletonList(new FailedNodeException("", "message", null)));
    }

    @Override
    protected ListTasksResponse doParseInstance(XContentParser parser) throws IOException {
        return ListTasksResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(ListTasksResponse expectedInstance, ListTasksResponse newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getTasks(), equalTo(expectedInstance.getTasks()));
        assertThat(newInstance.getNodeFailures().size(), equalTo(1));
        for (ElasticsearchException failure : newInstance.getNodeFailures()) {
            assertThat(failure, notNullValue());
            assertThat(failure.getMessage(), equalTo("Elasticsearch exception [type=failed_node_exception, reason=message]"));
        }
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
}

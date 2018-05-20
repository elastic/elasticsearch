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
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CancelTaskResponseTests  extends AbstractXContentTestCase<CancelTasksResponse> {

    @Override
    protected CancelTasksResponse createTestInstance() {
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            tasks.add(TaskInfoTests.randomTaskInfo());
        }
        List<TaskOperationFailure> taskFailures = new ArrayList<>();
        for (int i = 0; i < randomInt(5); i++) {
            taskFailures.add(new TaskOperationFailure(
                randomAlphaOfLength(5), randomNonNegativeLong(), new IllegalStateException("message")));
        }
        return new CancelTasksResponse(tasks, taskFailures, Collections.singletonList(new FailedNodeException("", "message", null)));
    }

    @Override
    protected CancelTasksResponse doParseInstance(XContentParser parser) {
        return CancelTasksResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(CancelTasksResponse expectedInstance, CancelTasksResponse newInstance) {
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

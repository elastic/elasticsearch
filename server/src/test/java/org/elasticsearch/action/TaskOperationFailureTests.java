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

package org.elasticsearch.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TaskOperationFailureTests extends AbstractXContentTestCase<TaskOperationFailure> {

    @Override
    protected TaskOperationFailure createTestInstance() {
        return new TaskOperationFailure(randomAlphaOfLength(5), randomNonNegativeLong(), new IllegalStateException("message"));
    }

    @Override
    protected TaskOperationFailure doParseInstance(XContentParser parser) throws IOException {
        return TaskOperationFailure.fromXContent(parser);
    }

    @Override
    protected void assertEqualInstances(TaskOperationFailure expectedInstance, TaskOperationFailure newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getNodeId(), equalTo(expectedInstance.getNodeId()));
        assertThat(newInstance.getTaskId(), equalTo(expectedInstance.getTaskId()));
        assertThat(newInstance.getStatus(), equalTo(expectedInstance.getStatus()));
        // XContent loses the original exception and wraps it as a message in Elasticsearch exception
        assertThat(newInstance.getCause().getMessage(), equalTo("Elasticsearch exception [type=illegal_state_exception, reason=message]"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
}
